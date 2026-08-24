use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use quinn_proto::crypto::rustls::HandshakeData;
use quinn_proto::{
    Connection, ConnectionError, ConnectionHandle, DatagramEvent, Dir, Endpoint, EndpointConfig,
    EndpointEvent, Event, ReadError, SendDatagramError, StreamEvent, StreamId, VarInt, WriteError,
};
use rskynet_core::{
    BoxFuture, Ctx, Error, Message, MsgType, Payload, Result, Service, SvcCell, log,
};
use rskynet_net::{SocketEvent, SocketId};

use crate::NAME;
use crate::command::{Answer, Command, QuicClientOptions, QuicServerOptions};
use crate::config::QuicConfig;
use crate::event::{
    QuicConnectionId, QuicEvent, QuicInfo, QuicListenerId, QuicStreamId, StreamDirection,
};

#[derive(Clone, Copy)]
struct Pending {
    source: rskynet_core::Handle,
    session: u64,
}

struct Listener {
    id: QuicListenerId,
    owner: rskynet_core::Handle,
}

struct EndpointState {
    local: Option<SocketAddr>,
    endpoint: Endpoint,
    listener: Option<Listener>,
    connections: HashMap<ConnectionHandle, QuicConnectionId>,
    timer_generation: u64,
    timer_deadline: Option<Instant>,
}

struct QueuedWrite {
    data: Vec<u8>,
    offset: usize,
    pending: Option<Pending>,
}

struct QuicConnection {
    id: QuicConnectionId,
    endpoint_socket: SocketId,
    connection: Connection,
    owner: rskynet_core::Handle,
    listener: Option<QuicListenerId>,
    local: Option<SocketAddr>,
    started: bool,
    announced: bool,
    alpn: Option<Vec<u8>>,
    connect_pending: Option<Pending>,
    queued_events: VecDeque<QuicEvent>,
    writes: HashMap<StreamId, VecDeque<QueuedWrite>>,
    finishes: HashMap<StreamId, Pending>,
    buffered_bytes: usize,
    warned: bool,
}

impl QuicConnection {
    fn info(&mut self) -> QuicInfo {
        let stats = self.connection.stats();
        QuicInfo {
            id: self.id,
            owner: self.owner,
            listener: self.listener,
            state: if self.connection.is_closed() {
                "closed"
            } else if self.connection.is_handshaking() {
                "handshaking"
            } else {
                "connected"
            },
            started: self.started,
            local: self.local,
            peer: self.connection.remote_address(),
            alpn: self.alpn.clone(),
            rtt_micros: QuicInfo::rtt_micros(stats.path.rtt),
            congestion_window: stats.path.cwnd,
            open_send_streams: self.connection.streams().send_streams(),
            buffered_bytes: self.buffered_bytes,
        }
    }
}

#[derive(Default)]
struct State {
    next_listener: u32,
    next_connection: u64,
    endpoints: HashMap<SocketId, EndpointState>,
    connections: HashMap<QuicConnectionId, QuicConnection>,
    reserved_connections: usize,
    reserved_handshakes: usize,
}

impl State {
    fn listener_id(&mut self) -> QuicListenerId {
        self.next_listener = self.next_listener.wrapping_add(1).max(1);
        QuicListenerId(self.next_listener)
    }

    fn connection_id(&mut self) -> QuicConnectionId {
        self.next_connection = self.next_connection.wrapping_add(1).max(1);
        QuicConnectionId(self.next_connection)
    }

    fn handshake_count(&self) -> usize {
        self.connections
            .values()
            .filter(|connection| connection.connection.is_handshaking())
            .count()
            + self.reserved_handshakes
    }

    fn has_connection_capacity(&self, config: &QuicConfig) -> bool {
        self.connections.len() + self.reserved_connections < config.max_connections
            && self.handshake_count() < config.max_handshakes
    }

    fn reserve_connect(&mut self, config: &QuicConfig) -> bool {
        if !self.has_connection_capacity(config) {
            return false;
        }
        self.reserved_connections += 1;
        self.reserved_handshakes += 1;
        true
    }

    fn release_connect(&mut self) {
        self.reserved_connections = self.reserved_connections.saturating_sub(1);
        self.reserved_handshakes = self.reserved_handshakes.saturating_sub(1);
    }
}

struct OutboundPacket {
    socket: SocketId,
    destination: SocketAddr,
    data: Vec<u8>,
}

struct OwnedEvent {
    owner: rskynet_core::Handle,
    event: QuicEvent,
}

struct TimerArm {
    socket: SocketId,
    generation: u64,
    delay_ms: u64,
}

#[derive(Default)]
struct DriveOutput {
    packets: Vec<OutboundPacket>,
    close_sockets: Vec<SocketId>,
    events: Vec<OwnedEvent>,
    replies: Vec<(Pending, Answer)>,
    timer: Option<TimerArm>,
}

pub struct QuicService {
    state: SvcCell<State>,
    config: SvcCell<QuicConfig>,
}

impl QuicService {
    pub fn new() -> Self {
        Self {
            state: SvcCell::new(State::default()),
            config: SvcCell::new(QuicConfig::default()),
        }
    }
}

impl Default for QuicService {
    fn default() -> Self {
        Self::new()
    }
}

impl Service for QuicService {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let config: QuicConfig = ctx.node().section(NAME)?.unwrap_or_default();
            config.validate()?;
            self.config.replace(config);
            let name = match args.trim() {
                "" => NAME,
                value => value,
            };
            if !ctx.register_name(name) {
                return Err(Error::Service(format!("名字 `.{name}` 已经被占用")));
            }
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if msg.mtype == MsgType::SOCKET {
                match msg.take_payload().downcast::<SocketEvent>() {
                    Ok(event) => self.on_socket(&ctx, *event).await,
                    Err(_) => log!(ctx, "QUIC 服务收到无法识别的 socket 事件"),
                }
                return;
            }
            if msg.mtype != MsgType::USER {
                let _ = ctx.reply_error(&msg);
                return;
            }
            let command = match msg.take_payload().downcast::<Command>() {
                Ok(command) => *command,
                Err(_) => {
                    let _ = ctx.reply_error(&msg);
                    return;
                }
            };
            let pending = Pending {
                source: msg.source,
                session: msg.session,
            };
            self.on_command(&ctx, pending, command).await;
        })
    }
}

impl QuicService {
    async fn on_command(&self, ctx: &Ctx, pending: Pending, command: Command) {
        match command {
            Command::Listen(options) => self.listen(ctx, pending, options).await,
            Command::Connect(options) => self.connect(ctx, pending, options).await,
            Command::Tick { socket, generation } => {
                let due = self
                    .state
                    .borrow()
                    .endpoints
                    .get(&socket)
                    .is_some_and(|endpoint| endpoint.timer_generation == generation);
                if due {
                    let now = Instant::now();
                    {
                        let mut state = self.state.borrow_mut();
                        if let Some(endpoint) = state.endpoints.get_mut(&socket) {
                            endpoint.timer_deadline = None;
                        }
                        for connection in state
                            .connections
                            .values_mut()
                            .filter(|connection| connection.endpoint_socket == socket)
                        {
                            if connection
                                .connection
                                .poll_timeout()
                                .is_some_and(|at| at <= now)
                            {
                                connection.connection.handle_timeout(now);
                            }
                        }
                    }
                    let output = self.drive(socket, now);
                    self.emit(ctx, output).await;
                }
            }
            Command::ConnectTimeout(id) => self.connect_timeout(ctx, id).await,
            other => {
                let mut output = DriveOutput::default();
                let socket = self.perform(pending, other, &mut output);
                if let Some(socket) = socket {
                    output.extend(self.drive(socket, Instant::now()));
                }
                self.emit(ctx, output).await;
            }
        }
    }

    async fn listen(&self, ctx: &Ctx, pending: Pending, options: QuicServerOptions) {
        let server = match options.proto_config() {
            Ok(config) => config,
            Err(error) => return reply(ctx, pending, Answer::Failed(error.to_string())),
        };
        let socket = match rskynet_net::udp(ctx, Some(&options.address)).await {
            Ok(socket) => socket,
            Err(error) => return reply(ctx, pending, Answer::Failed(error.to_string())),
        };
        let local = rskynet_net::info(ctx, socket)
            .await
            .ok()
            .and_then(|info| info.local);
        if local.is_some_and(|address| address.ip().is_unspecified()) {
            let _ = rskynet_net::shutdown(ctx, socket);
            return reply(
                ctx,
                pending,
                Answer::Failed("QUIC listener 首版必须绑定明确 IP，不能使用 wildcard 地址".into()),
            );
        }
        let mut state = self.state.borrow_mut();
        let id = state.listener_id();
        state.endpoints.insert(
            socket,
            EndpointState {
                local,
                endpoint: Endpoint::new(
                    Arc::new(EndpointConfig::default()),
                    Some(Arc::new(server)),
                    true,
                    None,
                ),
                listener: Some(Listener {
                    id,
                    owner: pending.source,
                }),
                connections: HashMap::new(),
                timer_generation: 0,
                timer_deadline: None,
            },
        );
        drop(state);
        reply(ctx, pending, Answer::Listener(id));
    }

    async fn connect(&self, ctx: &Ctx, pending: Pending, options: QuicClientOptions) {
        let client_config = match options.proto_config() {
            Ok(config) => config,
            Err(error) => return reply(ctx, pending, Answer::Failed(error.to_string())),
        };
        let config = self.config.borrow().clone();
        let capacity_exhausted = {
            let mut state = self.state.borrow_mut();
            !state.reserve_connect(&config)
        };
        if capacity_exhausted {
            return reply(
                ctx,
                pending,
                Answer::Failed("QUIC 连接/握手容量已满".into()),
            );
        }
        let socket_result = match options.bind.as_deref() {
            Some(bind) => rskynet_net::udp(ctx, Some(bind)).await,
            None => rskynet_net::udp_for_peer(ctx, options.address.clone()).await,
        };
        let socket = match socket_result {
            Ok(socket) => socket,
            Err(error) => {
                self.release_connect_reservation();
                return reply(ctx, pending, Answer::Failed(error.to_string()));
            }
        };
        if options.bind.is_some() {
            if let Err(error) = rskynet_net::udp_connect(ctx, socket, options.address.clone()).await
            {
                self.release_connect_reservation();
                let _ = rskynet_net::shutdown(ctx, socket);
                return reply(ctx, pending, Answer::Failed(error.to_string()));
            }
        }
        let info = match rskynet_net::info(ctx, socket).await {
            Ok(info) => info,
            Err(error) => {
                self.release_connect_reservation();
                let _ = rskynet_net::shutdown(ctx, socket);
                return reply(ctx, pending, Answer::Failed(error.to_string()));
            }
        };
        let Some(peer) = info.peer else {
            self.release_connect_reservation();
            let _ = rskynet_net::shutdown(ctx, socket);
            return reply(
                ctx,
                pending,
                Answer::Failed("QUIC UDP 端点没有对端地址".into()),
            );
        };
        let now = Instant::now();
        let mut endpoint = Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let (handle, connection) =
            match endpoint.connect(now, client_config, peer, &options.server_name) {
                Ok(value) => value,
                Err(error) => {
                    self.release_connect_reservation();
                    let _ = rskynet_net::shutdown(ctx, socket);
                    return reply(ctx, pending, Answer::Failed(error.to_string()));
                }
            };
        let id = {
            let mut state = self.state.borrow_mut();
            state.release_connect();
            let id = state.connection_id();
            let mut handles = HashMap::new();
            handles.insert(handle, id);
            state.endpoints.insert(
                socket,
                EndpointState {
                    local: info.local,
                    endpoint,
                    listener: None,
                    connections: handles,
                    timer_generation: 0,
                    timer_deadline: None,
                },
            );
            state.connections.insert(
                id,
                QuicConnection {
                    id,
                    endpoint_socket: socket,
                    connection,
                    owner: pending.source,
                    listener: None,
                    local: info.local,
                    started: true,
                    announced: false,
                    alpn: None,
                    connect_pending: Some(pending),
                    queued_events: VecDeque::new(),
                    writes: HashMap::new(),
                    finishes: HashMap::new(),
                    buffered_bytes: 0,
                    warned: false,
                },
            );
            id
        };

        let timeout_ms = options
            .connect_timeout_ms
            .unwrap_or(config.handshake_timeout_ms);
        let service = ctx.handle();
        let node = ctx.node();
        let sleeper = ctx.clone();
        ctx.spawn(async move {
            sleeper.sleep(timeout_ms).await;
            let _ = node.send(
                service,
                service,
                MsgType::USER,
                0,
                Payload::of(Command::ConnectTimeout(id)),
            );
        });
        let output = self.drive(socket, now);
        self.emit(ctx, output).await;
    }

    fn release_connect_reservation(&self) {
        self.state.borrow_mut().release_connect();
    }

    async fn connect_timeout(&self, ctx: &Ctx, id: QuicConnectionId) {
        let socket = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                return;
            };
            if !connection.connection.is_handshaking() || connection.connect_pending.is_none() {
                return;
            }
            connection.connection.close(
                Instant::now(),
                VarInt::from_u32(0),
                Bytes::from_static(b"connect timeout"),
            );
            if let Some(pending) = connection.connect_pending.take() {
                reply(ctx, pending, Answer::Failed("QUIC 连接超时".into()));
            }
            connection.endpoint_socket
        };
        let output = self.drive(socket, Instant::now());
        self.emit(ctx, output).await;
    }

    fn perform(
        &self,
        pending: Pending,
        command: Command,
        output: &mut DriveOutput,
    ) -> Option<SocketId> {
        let mut state = self.state.borrow_mut();
        match command {
            Command::Start(id) => {
                let Some(connection) = state.connections.get_mut(&id) else {
                    output
                        .replies
                        .push((pending, Answer::Failed(format!("{id} 不存在"))));
                    return None;
                };
                connection.owner = pending.source;
                connection.started = true;
                while let Some(event) = connection.queued_events.pop_front() {
                    output.events.push(OwnedEvent {
                        owner: connection.owner,
                        event,
                    });
                }
                output.replies.push((pending, Answer::Done));
                Some(connection.endpoint_socket)
            }
            Command::ListenerLocalAddr(id) => {
                let answer = state
                    .endpoints
                    .values()
                    .find(|endpoint| {
                        endpoint.listener.as_ref().is_some_and(|listener| {
                            listener.id == id && listener.owner == pending.source
                        })
                    })
                    .and_then(|endpoint| endpoint.local)
                    .map(Answer::LocalAddr)
                    .unwrap_or_else(|| Answer::Failed(format!("{id} 不存在或没有本地地址")));
                output.replies.push((pending, answer));
                None
            }
            Command::CloseListener(id) => {
                let socket = state.endpoints.iter().find_map(|(socket, endpoint)| {
                    endpoint.listener.as_ref().and_then(|listener| {
                        (listener.id == id && listener.owner == pending.source).then_some(*socket)
                    })
                });
                let answer = if let Some(socket) = socket {
                    let remove = if let Some(endpoint) = state.endpoints.get_mut(&socket) {
                        endpoint.listener = None;
                        endpoint.connections.is_empty()
                    } else {
                        false
                    };
                    if remove {
                        state.endpoints.remove(&socket);
                        output.close_sockets.push(socket);
                    }
                    Answer::Done
                } else {
                    Answer::Failed(format!("{id} 不存在或不属于调用方"))
                };
                output.replies.push((pending, answer));
                None
            }
            Command::OpenBi(id) | Command::OpenUni(id) => {
                let dir = if matches!(command, Command::OpenBi(_)) {
                    Dir::Bi
                } else {
                    Dir::Uni
                };
                let answer = match owned_connection(&mut state, id, pending.source) {
                    Ok(connection) => connection
                        .connection
                        .streams()
                        .open(dir)
                        .map(|stream| Answer::Stream(stream_id(stream)))
                        .unwrap_or_else(|| Answer::Failed("对端 stream 配额已用完".into())),
                    Err(reason) => Answer::Failed(reason),
                };
                let socket = state
                    .connections
                    .get(&id)
                    .map(|connection| connection.endpoint_socket);
                output.replies.push((pending, answer));
                socket
            }
            Command::Send {
                id,
                stream,
                data,
                wait,
            } => {
                let max_chunk = self.config.borrow().max_chunk_size;
                if data.len() > max_chunk {
                    if wait {
                        output.replies.push((
                            pending,
                            Answer::Failed(format!(
                                "单次 QUIC 写入 {} 字节，超过上限 {max_chunk}",
                                data.len()
                            )),
                        ));
                    }
                    return None;
                }
                let high_water = self.config.borrow().write_high_water;
                let Ok(connection) = owned_connection(&mut state, id, pending.source) else {
                    if wait {
                        output.replies.push((
                            pending,
                            Answer::Failed(format!("{id} 不存在或不属于调用方")),
                        ));
                    }
                    return None;
                };
                let proto_stream = match proto_stream_id(stream) {
                    Ok(value) => value,
                    Err(reason) => {
                        if wait {
                            output.replies.push((pending, Answer::Failed(reason)));
                        }
                        return None;
                    }
                };
                if connection.finishes.contains_key(&proto_stream) {
                    if wait {
                        output.replies.push((
                            pending,
                            Answer::Failed(format!("{stream} 已经在等待 finish")),
                        ));
                    }
                    return Some(connection.endpoint_socket);
                }
                let mut offset = 0;
                match connection.connection.send_stream(proto_stream).write(&data) {
                    Ok(written) => offset = written,
                    Err(WriteError::Blocked) => {}
                    Err(error) => {
                        if wait {
                            output
                                .replies
                                .push((pending, Answer::Failed(error.to_string())));
                        }
                        return Some(connection.endpoint_socket);
                    }
                }
                if offset == data.len() {
                    if wait {
                        output.replies.push((pending, Answer::Done));
                    }
                } else {
                    connection.buffered_bytes += data.len() - offset;
                    connection
                        .writes
                        .entry(proto_stream)
                        .or_default()
                        .push_back(QueuedWrite {
                            data,
                            offset,
                            pending: wait.then_some(pending),
                        });
                    if connection.buffered_bytes >= high_water && !connection.warned {
                        connection.warned = true;
                        queue_event(
                            connection,
                            QuicEvent::Warning {
                                id,
                                buffered_bytes: connection.buffered_bytes,
                            },
                            output,
                        );
                    }
                }
                Some(connection.endpoint_socket)
            }
            Command::Finish { id, stream } => {
                let connection = match owned_connection(&mut state, id, pending.source) {
                    Ok(connection) => connection,
                    Err(reason) => {
                        output.replies.push((pending, Answer::Failed(reason)));
                        return None;
                    }
                };
                let proto_stream = match proto_stream_id(stream) {
                    Ok(stream) => stream,
                    Err(reason) => {
                        output.replies.push((pending, Answer::Failed(reason)));
                        return Some(connection.endpoint_socket);
                    }
                };
                let socket = connection.endpoint_socket;
                if connection.finishes.contains_key(&proto_stream) {
                    output.replies.push((
                        pending,
                        Answer::Failed(format!("{stream} 已经在等待 finish")),
                    ));
                } else if connection
                    .writes
                    .get(&proto_stream)
                    .is_some_and(|queue| !queue.is_empty())
                {
                    connection.finishes.insert(proto_stream, pending);
                } else {
                    let answer = connection
                        .connection
                        .send_stream(proto_stream)
                        .finish()
                        .map(|()| Answer::Done)
                        .unwrap_or_else(|error| Answer::Failed(error.to_string()));
                    output.replies.push((pending, answer));
                }
                Some(socket)
            }
            Command::Reset {
                id,
                stream,
                error_code,
            } => {
                let code = match checked_error_code(error_code) {
                    Ok(code) => code,
                    Err(reason) => {
                        output.replies.push((pending, Answer::Failed(reason)));
                        return None;
                    }
                };
                let answer =
                    operate_send_stream(&mut state, id, pending.source, stream, |stream| {
                        stream.reset(code).map_err(|e| e.to_string())
                    });
                let socket = state
                    .connections
                    .get(&id)
                    .map(|connection| connection.endpoint_socket);
                output.replies.push((pending, answer));
                socket
            }
            Command::Stop {
                id,
                stream,
                error_code,
            } => {
                let code = match checked_error_code(error_code) {
                    Ok(code) => code,
                    Err(reason) => {
                        output.replies.push((pending, Answer::Failed(reason)));
                        return None;
                    }
                };
                let answer = match (
                    owned_connection(&mut state, id, pending.source),
                    proto_stream_id(stream),
                ) {
                    (Ok(connection), Ok(stream)) => connection
                        .connection
                        .recv_stream(stream)
                        .stop(code)
                        .map(|()| Answer::Done)
                        .unwrap_or_else(|error| Answer::Failed(error.to_string())),
                    (Err(reason), _) | (_, Err(reason)) => Answer::Failed(reason),
                };
                let socket = state
                    .connections
                    .get(&id)
                    .map(|connection| connection.endpoint_socket);
                output.replies.push((pending, answer));
                socket
            }
            Command::SendDatagram { id, data } => {
                let answer = match owned_connection(&mut state, id, pending.source) {
                    Ok(connection) => connection
                        .connection
                        .datagrams()
                        .send(Bytes::from(data), false)
                        .map(|()| Answer::Done)
                        .unwrap_or_else(|error| Answer::Failed(datagram_error(error))),
                    Err(reason) => Answer::Failed(reason),
                };
                let socket = state
                    .connections
                    .get(&id)
                    .map(|connection| connection.endpoint_socket);
                output.replies.push((pending, answer));
                socket
            }
            Command::Close {
                id,
                error_code,
                reason,
            } => {
                let code = match checked_error_code(error_code) {
                    Ok(code) => code,
                    Err(reason) => {
                        output.replies.push((pending, Answer::Failed(reason)));
                        return None;
                    }
                };
                let answer = match owned_connection(&mut state, id, pending.source) {
                    Ok(connection) => {
                        connection
                            .connection
                            .close(Instant::now(), code, Bytes::from(reason));
                        Answer::Done
                    }
                    Err(reason) => Answer::Failed(reason),
                };
                let socket = state
                    .connections
                    .get(&id)
                    .map(|connection| connection.endpoint_socket);
                output.replies.push((pending, answer));
                socket
            }
            Command::Shutdown(id) => {
                owned_connection(&mut state, id, pending.source)
                    .ok()
                    .map(|connection| {
                        let socket = connection.endpoint_socket;
                        connection.connection.close(
                            Instant::now(),
                            VarInt::from_u32(0),
                            Bytes::new(),
                        );
                        socket
                    })
            }
            Command::Info(id) => {
                let answer = state
                    .connections
                    .get_mut(&id)
                    .map(|connection| Answer::Info(connection.info()))
                    .unwrap_or_else(|| Answer::Failed(format!("{id} 不存在")));
                output.replies.push((pending, answer));
                None
            }
            Command::Netstat => {
                let mut infos = state
                    .connections
                    .values_mut()
                    .map(|connection| connection.info())
                    .collect::<Vec<_>>();
                infos.sort_unstable_by_key(|info| info.id);
                output.replies.push((pending, Answer::Infos(infos)));
                None
            }
            Command::Listen(_)
            | Command::Connect(_)
            | Command::Tick { .. }
            | Command::ConnectTimeout(_) => unreachable!(),
        }
    }

    async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) {
        let SocketEvent::Udp {
            id: socket,
            from,
            data,
        } = event
        else {
            let id = event.id();
            if event.is_gone() {
                self.endpoint_failed(ctx, id, "底层 UDP socket 已关闭")
                    .await;
            }
            return;
        };
        let now = Instant::now();
        let mut direct = DriveOutput::default();
        {
            let mut state = self.state.borrow_mut();
            let Some(mut endpoint) = state.endpoints.remove(&socket) else {
                return;
            };
            let mut buffer = Vec::new();
            match endpoint.endpoint.handle(
                now,
                from,
                None,
                None,
                BytesMut::from(data.as_slice()),
                &mut buffer,
            ) {
                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                    if let Some(id) = endpoint.connections.get(&handle).copied() {
                        if let Some(connection) = state.connections.get_mut(&id) {
                            connection.connection.handle_event(event);
                        }
                    }
                }
                Some(DatagramEvent::NewConnection(incoming)) => {
                    let listener = endpoint
                        .listener
                        .as_ref()
                        .map(|listener| (listener.id, listener.owner));
                    let capacity = state.has_connection_capacity(&self.config.borrow());
                    if let Some((listener_id, owner)) = listener.filter(|_| capacity) {
                        match endpoint.endpoint.accept(incoming, now, &mut buffer, None) {
                            Ok((handle, connection)) => {
                                let id = state.connection_id();
                                endpoint.connections.insert(handle, id);
                                state.connections.insert(
                                    id,
                                    QuicConnection {
                                        id,
                                        endpoint_socket: socket,
                                        connection,
                                        owner,
                                        listener: Some(listener_id),
                                        local: endpoint.local,
                                        started: false,
                                        announced: false,
                                        alpn: None,
                                        connect_pending: None,
                                        queued_events: VecDeque::new(),
                                        writes: HashMap::new(),
                                        finishes: HashMap::new(),
                                        buffered_bytes: 0,
                                        warned: false,
                                    },
                                );
                            }
                            Err(error) => {
                                if let Some(response) = error.response {
                                    direct.packets.push(packet(socket, response, &buffer));
                                }
                            }
                        }
                    } else {
                        let response = endpoint.endpoint.refuse(incoming, &mut buffer);
                        direct.packets.push(packet(socket, response, &buffer));
                    }
                }
                Some(DatagramEvent::Response(response)) => {
                    direct.packets.push(packet(socket, response, &buffer))
                }
                None => {}
            }
            state.endpoints.insert(socket, endpoint);
        }
        direct.extend(self.drive(socket, now));
        self.emit(ctx, direct).await;
    }

    async fn endpoint_failed(&self, ctx: &Ctx, socket: SocketId, reason: &str) {
        let mut output = DriveOutput::default();
        {
            let mut state = self.state.borrow_mut();
            let ids = state
                .endpoints
                .remove(&socket)
                .map(|endpoint| endpoint.connections.into_values().collect::<Vec<_>>())
                .unwrap_or_default();
            for id in ids {
                if let Some(mut connection) = state.connections.remove(&id) {
                    if let Some(pending) = connection.connect_pending.take() {
                        output
                            .replies
                            .push((pending, Answer::Failed(reason.into())));
                    }
                    if connection.announced {
                        output.events.push(OwnedEvent {
                            owner: connection.owner,
                            event: QuicEvent::Error {
                                id,
                                reason: reason.into(),
                            },
                        });
                    }
                    fail_pending_writes(&mut connection, reason, &mut output);
                }
            }
        }
        self.emit(ctx, output).await;
    }

    fn drive(&self, socket: SocketId, now: Instant) -> DriveOutput {
        let mut output = DriveOutput::default();
        let mut state = self.state.borrow_mut();
        let handles = match state.endpoints.get(&socket) {
            Some(endpoint) => endpoint
                .connections
                .iter()
                .map(|(handle, id)| (*handle, *id))
                .collect::<Vec<_>>(),
            None => return output,
        };

        loop {
            let mut endpoint_events: Vec<(ConnectionHandle, EndpointEvent)> = Vec::new();
            for (handle, id) in &handles {
                let Some(connection) = state.connections.get_mut(id) else {
                    continue;
                };
                while let Some(event) = connection.connection.poll_endpoint_events() {
                    endpoint_events.push((*handle, event));
                }
            }
            if endpoint_events.is_empty() {
                break;
            }
            for (handle, event) in endpoint_events {
                let response = state
                    .endpoints
                    .get_mut(&socket)
                    .and_then(|endpoint| endpoint.endpoint.handle_event(handle, event));
                if let Some(response) = response {
                    if let Some(id) = state
                        .endpoints
                        .get(&socket)
                        .and_then(|endpoint| endpoint.connections.get(&handle))
                        .copied()
                    {
                        if let Some(connection) = state.connections.get_mut(&id) {
                            connection.connection.handle_event(response);
                        }
                    }
                }
            }
        }

        for (_, id) in &handles {
            let Some(connection) = state.connections.get_mut(id) else {
                continue;
            };
            self.poll_application(connection, &mut output);
            self.flush_writes(connection, &mut output);
            let mut buffer = Vec::with_capacity(2048);
            while let Some(transmit) = connection.connection.poll_transmit(now, 1, &mut buffer) {
                output.packets.push(packet(socket, transmit, &buffer));
                buffer.clear();
            }
        }

        let mut deadline: Option<Instant> = None;
        let mut drained = Vec::new();
        for (handle, id) in &handles {
            if let Some(connection) = state.connections.get_mut(id) {
                if connection.connection.is_drained() {
                    drained.push((*handle, *id));
                } else if let Some(value) = connection.connection.poll_timeout() {
                    deadline = Some(deadline.map_or(value, |current| current.min(value)));
                }
            }
        }
        for (handle, id) in drained {
            if let Some(endpoint) = state.endpoints.get_mut(&socket) {
                endpoint.connections.remove(&handle);
            }
            state.connections.remove(&id);
        }
        if let Some(endpoint) = state.endpoints.get_mut(&socket) {
            if endpoint.timer_deadline != deadline {
                endpoint.timer_generation = endpoint.timer_generation.wrapping_add(1).max(1);
                endpoint.timer_deadline = deadline;
                if let Some(deadline) = deadline {
                    let delay = deadline.saturating_duration_since(now);
                    output.timer = Some(TimerArm {
                        socket,
                        generation: endpoint.timer_generation,
                        delay_ms: duration_ceil_ms(delay),
                    });
                }
            }
        }
        let remove_endpoint = state
            .endpoints
            .get(&socket)
            .is_some_and(|endpoint| endpoint.listener.is_none() && endpoint.connections.is_empty());
        if remove_endpoint {
            state.endpoints.remove(&socket);
            output.close_sockets.push(socket);
            output.timer = None;
        }
        output
    }

    fn poll_application(&self, connection: &mut QuicConnection, output: &mut DriveOutput) {
        while let Some(event) = connection.connection.poll() {
            match event {
                Event::HandshakeDataReady => {
                    connection.alpn = connection
                        .connection
                        .crypto_session()
                        .handshake_data()
                        .and_then(|data| data.downcast::<HandshakeData>().ok())
                        .and_then(|data| data.protocol);
                }
                Event::Connected => {
                    connection.announced = true;
                    if let Some(pending) = connection.connect_pending.take() {
                        output
                            .replies
                            .push((pending, Answer::Connection(connection.id)));
                    }
                    output.events.push(OwnedEvent {
                        owner: connection.owner,
                        event: QuicEvent::Connected {
                            id: connection.id,
                            listener: connection.listener,
                            peer: connection.connection.remote_address(),
                            alpn: connection.alpn.clone(),
                        },
                    });
                }
                Event::ConnectionLost { reason } => {
                    let was_announced = connection.announced;
                    if let Some(pending) = connection.connect_pending.take() {
                        output
                            .replies
                            .push((pending, Answer::Failed(reason.to_string())));
                    }
                    let event = match reason {
                        ConnectionError::ApplicationClosed(close) => QuicEvent::Close {
                            id: connection.id,
                            error_code: Some(close.error_code.into_inner()),
                            reason: close.reason.to_vec(),
                        },
                        reason => QuicEvent::Error {
                            id: connection.id,
                            reason: reason.to_string(),
                        },
                    };
                    if was_announced {
                        output.events.push(OwnedEvent {
                            owner: connection.owner,
                            event,
                        });
                    }
                    fail_pending_writes(connection, "QUIC 连接已关闭", output);
                }
                Event::Stream(StreamEvent::Opened { dir }) => {
                    while let Some(stream) = connection.connection.streams().accept(dir) {
                        queue_event(
                            connection,
                            QuicEvent::StreamOpened {
                                id: connection.id,
                                stream: stream_id(stream),
                                direction: direction(dir),
                            },
                            output,
                        );
                        // `Opened` 表示「新 stream 已出现且可能已可读」。首包同时带数据时
                        // 不一定再单独产生 `Readable`，所以 accept 后必须立即试读。
                        self.read_stream(connection, stream, output);
                    }
                }
                Event::Stream(StreamEvent::Readable { id }) => {
                    self.read_stream(connection, id, output)
                }
                Event::Stream(StreamEvent::Writable { id }) => {
                    self.flush_stream_writes(connection, id, output)
                }
                Event::Stream(StreamEvent::Finished { id }) => queue_event(
                    connection,
                    QuicEvent::SendFinished {
                        id: connection.id,
                        stream: stream_id(id),
                    },
                    output,
                ),
                Event::Stream(StreamEvent::Stopped { id, error_code }) => queue_event(
                    connection,
                    QuicEvent::StreamStopped {
                        id: connection.id,
                        stream: stream_id(id),
                        error_code: error_code.into_inner(),
                    },
                    output,
                ),
                Event::Stream(StreamEvent::Available { .. }) => {}
                Event::DatagramReceived => {
                    while let Some(data) = connection.connection.datagrams().recv() {
                        queue_event(
                            connection,
                            QuicEvent::Datagram {
                                id: connection.id,
                                data: data.to_vec(),
                            },
                            output,
                        );
                    }
                }
                Event::DatagramsUnblocked => {}
            }
        }
    }

    fn read_stream(
        &self,
        connection: &mut QuicConnection,
        stream: StreamId,
        output: &mut DriveOutput,
    ) {
        let max_chunk = self.config.borrow().max_chunk_size;
        loop {
            let result = {
                let mut recv = connection.connection.recv_stream(stream);
                let Ok(mut chunks) = recv.read(true) else {
                    return;
                };
                let result = chunks.next(max_chunk);
                let _ = chunks.finalize();
                result
            };
            match result {
                Ok(Some(chunk)) => queue_event(
                    connection,
                    QuicEvent::StreamData {
                        id: connection.id,
                        stream: stream_id(stream),
                        data: chunk.bytes.to_vec(),
                    },
                    output,
                ),
                Ok(None) => {
                    queue_event(
                        connection,
                        QuicEvent::ReceiveFinished {
                            id: connection.id,
                            stream: stream_id(stream),
                        },
                        output,
                    );
                    return;
                }
                Err(ReadError::Blocked) => return,
                Err(ReadError::Reset(code)) => {
                    queue_event(
                        connection,
                        QuicEvent::StreamReset {
                            id: connection.id,
                            stream: stream_id(stream),
                            error_code: code.into_inner(),
                        },
                        output,
                    );
                    return;
                }
            }
        }
    }

    fn flush_writes(&self, connection: &mut QuicConnection, output: &mut DriveOutput) {
        let streams = connection.writes.keys().copied().collect::<Vec<_>>();
        for stream in streams {
            self.flush_stream_writes(connection, stream, output);
        }
    }

    fn flush_stream_writes(
        &self,
        connection: &mut QuicConnection,
        stream: StreamId,
        output: &mut DriveOutput,
    ) {
        while let Some(mut write) = connection
            .writes
            .get_mut(&stream)
            .and_then(VecDeque::pop_front)
        {
            match connection
                .connection
                .send_stream(stream)
                .write(&write.data[write.offset..])
            {
                Ok(written) => {
                    write.offset += written;
                    connection.buffered_bytes = connection.buffered_bytes.saturating_sub(written);
                    if write.offset == write.data.len() {
                        if let Some(pending) = write.pending.take() {
                            output.replies.push((pending, Answer::Done));
                        }
                    } else {
                        connection
                            .writes
                            .entry(stream)
                            .or_default()
                            .push_front(write);
                        break;
                    }
                }
                Err(WriteError::Blocked) => {
                    connection
                        .writes
                        .entry(stream)
                        .or_default()
                        .push_front(write);
                    break;
                }
                Err(error) => {
                    connection.buffered_bytes = connection
                        .buffered_bytes
                        .saturating_sub(write.data.len() - write.offset);
                    if let Some(pending) = write.pending {
                        output
                            .replies
                            .push((pending, Answer::Failed(error.to_string())));
                    }
                }
            }
        }
        if connection
            .writes
            .get(&stream)
            .is_some_and(VecDeque::is_empty)
        {
            connection.writes.remove(&stream);
        }
        if !connection.writes.contains_key(&stream) {
            if let Some(pending) = connection.finishes.remove(&stream) {
                let answer = connection
                    .connection
                    .send_stream(stream)
                    .finish()
                    .map(|()| Answer::Done)
                    .unwrap_or_else(|error| Answer::Failed(error.to_string()));
                output.replies.push((pending, answer));
            }
        }
        if connection.buffered_bytes <= self.config.borrow().write_low_water {
            connection.warned = false;
        }
    }

    async fn emit(&self, ctx: &Ctx, output: DriveOutput) {
        for packet in output.packets {
            if let Err(error) = rskynet_net::udp_send_wait(
                ctx,
                packet.socket,
                Some(packet.destination),
                packet.data,
            )
            .await
            {
                log!(ctx, "QUIC UDP 发送失败：{error}");
            }
        }
        for socket in output.close_sockets {
            let _ = rskynet_net::shutdown(ctx, socket);
        }
        for owned in output.events {
            let _ = ctx.send(owned.owner, MsgType::QUIC, Payload::of(owned.event));
        }
        for (pending, answer) in output.replies {
            reply(ctx, pending, answer);
        }
        if let Some(timer) = output.timer {
            let service = ctx.handle();
            let node = ctx.node();
            let sleeper = ctx.clone();
            ctx.spawn(async move {
                sleeper.sleep(timer.delay_ms).await;
                let _ = node.send(
                    service,
                    service,
                    MsgType::USER,
                    0,
                    Payload::of(Command::Tick {
                        socket: timer.socket,
                        generation: timer.generation,
                    }),
                );
            });
        }
    }
}

impl DriveOutput {
    fn extend(&mut self, mut other: Self) {
        self.packets.append(&mut other.packets);
        self.close_sockets.append(&mut other.close_sockets);
        self.events.append(&mut other.events);
        self.replies.append(&mut other.replies);
        if other.timer.is_some() {
            self.timer = other.timer;
        }
    }
}

fn owned_connection(
    state: &mut State,
    id: QuicConnectionId,
    owner: rskynet_core::Handle,
) -> std::result::Result<&mut QuicConnection, String> {
    let connection = state
        .connections
        .get_mut(&id)
        .ok_or_else(|| format!("{id} 不存在"))?;
    if connection.owner != owner {
        return Err(format!("{id} 不属于调用方"));
    }
    if !connection.started {
        return Err(format!("{id} 尚未 start"));
    }
    Ok(connection)
}

fn operate_send_stream<F>(
    state: &mut State,
    id: QuicConnectionId,
    owner: rskynet_core::Handle,
    stream: QuicStreamId,
    operation: F,
) -> Answer
where
    F: FnOnce(&mut quinn_proto::SendStream<'_>) -> std::result::Result<(), String>,
{
    match (owned_connection(state, id, owner), proto_stream_id(stream)) {
        (Ok(connection), Ok(stream)) => operation(&mut connection.connection.send_stream(stream))
            .map(|()| Answer::Done)
            .unwrap_or_else(Answer::Failed),
        (Err(reason), _) | (_, Err(reason)) => Answer::Failed(reason),
    }
}

fn queue_event(connection: &mut QuicConnection, event: QuicEvent, output: &mut DriveOutput) {
    if connection.started {
        output.events.push(OwnedEvent {
            owner: connection.owner,
            event,
        });
    } else {
        connection.queued_events.push_back(event);
    }
}

fn checked_error_code(error_code: u64) -> std::result::Result<VarInt, String> {
    VarInt::from_u64(error_code).map_err(|_| "QUIC error code 超出范围".into())
}

fn fail_pending_writes(connection: &mut QuicConnection, reason: &str, output: &mut DriveOutput) {
    for queue in connection.writes.values_mut() {
        for write in queue.drain(..) {
            if let Some(pending) = write.pending {
                output
                    .replies
                    .push((pending, Answer::Failed(reason.into())));
            }
        }
    }
    connection.writes.clear();
    for (_, pending) in connection.finishes.drain() {
        output
            .replies
            .push((pending, Answer::Failed(reason.into())));
    }
    connection.buffered_bytes = 0;
    connection.warned = false;
}

fn packet(socket: SocketId, transmit: quinn_proto::Transmit, buffer: &[u8]) -> OutboundPacket {
    OutboundPacket {
        socket,
        destination: transmit.destination,
        data: buffer[..transmit.size].to_vec(),
    }
}

fn stream_id(id: StreamId) -> QuicStreamId {
    QuicStreamId(u64::from(id))
}

fn proto_stream_id(id: QuicStreamId) -> std::result::Result<StreamId, String> {
    VarInt::from_u64(id.0)
        .map(StreamId::from)
        .map_err(|_| format!("{id} 超出 QUIC stream id 范围"))
}

fn direction(dir: Dir) -> StreamDirection {
    match dir {
        Dir::Bi => StreamDirection::Bidirectional,
        Dir::Uni => StreamDirection::Unidirectional,
    }
}

fn datagram_error(error: SendDatagramError) -> String {
    error.to_string()
}

fn duration_ceil_ms(duration: Duration) -> u64 {
    let micros = duration.as_micros();
    micros.div_ceil(1000).max(1).min(u128::from(u64::MAX)) as u64
}

fn reply(ctx: &Ctx, pending: Pending, answer: Answer) {
    if pending.session == 0 {
        return;
    }
    let _ = ctx.node().send(
        ctx.handle(),
        pending.source,
        MsgType::RESPONSE,
        pending.session,
        Payload::of(answer),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_reservations_enforce_both_limits() {
        let config = QuicConfig {
            max_connections: 1,
            max_handshakes: 1,
            ..Default::default()
        };
        let mut state = State::default();
        assert!(state.reserve_connect(&config));
        assert!(!state.reserve_connect(&config));
        state.release_connect();
        assert!(state.reserve_connect(&config));
    }

    #[test]
    fn invalid_application_error_codes_are_rejected_without_panicking() {
        assert!(checked_error_code((1_u64 << 62) - 1).is_ok());
        assert!(checked_error_code(1_u64 << 62).is_err());
        assert!(checked_error_code(u64::MAX).is_err());
    }
}
