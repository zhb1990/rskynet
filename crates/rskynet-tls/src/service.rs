use std::collections::HashMap;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;

use rskynet_core::{
    BoxFuture, Ctx, Error, Message, MsgType, Payload, Result, Service, SvcCell, log,
};
use rskynet_net::{self as net, SocketEvent, SocketId};
use rustls::{Connection, ProtocolVersion};

use crate::NAME;
use crate::command::{Answer, ClientOptions, Command, ServerOptions};
use crate::config::TlsConfig;
use crate::event::{TlsEvent, TlsId, TlsInfo};
use crate::identity::ServerTlsConfig;

#[derive(Clone, Copy)]
struct Pending {
    source: u32,
    session: i32,
}

struct Listener {
    socket: SocketId,
    owner: u32,
    local: Option<SocketAddr>,
    config: ServerTlsConfig,
    started: bool,
}

struct TlsConnection {
    id: TlsId,
    socket: SocketId,
    owner: u32,
    connection: Connection,
    listen: Option<TlsId>,
    local: Option<SocketAddr>,
    peer: SocketAddr,
    announced: bool,
    closing: bool,
    peer_close_notify: bool,
    connect_pending: Option<Pending>,
    close_pending: Option<Pending>,
    paused: bool,
    handshake_timeout_ms: u64,
}

impl TlsConnection {
    fn info(&self) -> TlsInfo {
        TlsInfo {
            id: self.id,
            owner: self.owner,
            kind: if self.listen.is_some() {
                "server"
            } else {
                "client"
            },
            state: if self.closing {
                "closing"
            } else if self.connection.is_handshaking() {
                "handshaking"
            } else {
                "connected"
            },
            paused: self.paused,
            local: self.local,
            peer: Some(self.peer),
            version: version_name(self.connection.protocol_version()),
            cipher_suite: self
                .connection
                .negotiated_cipher_suite()
                .map(|suite| format!("{:?}", suite.suite())),
            alpn: self.connection.alpn_protocol().map(<[u8]>::to_vec),
        }
    }
}

#[derive(Default)]
struct State {
    next_id: u32,
    listeners: HashMap<TlsId, Listener>,
    listener_sockets: HashMap<SocketId, TlsId>,
    connections: HashMap<TlsId, TlsConnection>,
    connection_sockets: HashMap<SocketId, TlsId>,
    handshakes: usize,
}

impl State {
    fn len(&self) -> usize {
        self.listeners.len() + self.connections.len()
    }

    fn allocate(&mut self) -> TlsId {
        loop {
            self.next_id = self.next_id.wrapping_add(1).max(1);
            let id = TlsId(self.next_id);
            if !self.listeners.contains_key(&id) && !self.connections.contains_key(&id) {
                return id;
            }
        }
    }
}

pub struct TlsService {
    state: SvcCell<State>,
    config: SvcCell<TlsConfig>,
}

impl TlsService {
    pub fn new() -> Self {
        Self {
            state: SvcCell::new(State::default()),
            config: SvcCell::new(TlsConfig::default()),
        }
    }
}

impl Default for TlsService {
    fn default() -> Self {
        Self::new()
    }
}

impl Service for TlsService {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let config: TlsConfig = ctx.node().section(NAME)?.unwrap_or_default();
            config.validate()?;
            self.config.replace(config);
            let name = match args.trim() {
                "" => NAME,
                name => name,
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
                    Err(_) => log!(ctx, "TLS 服务收到无法识别的 socket 事件"),
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

impl TlsService {
    async fn on_command(&self, ctx: &Ctx, pending: Pending, command: Command) {
        match command {
            Command::Connect(options) => self.connect(ctx, options, pending).await,
            Command::Listen(options) => self.listen(ctx, options, pending).await,
            Command::Start(id) => self.start(ctx, id, pending).await,
            Command::Pause(id) => {
                let answer = self.pause(ctx, id, pending.source).await;
                reply(ctx, pending, answer);
            }
            Command::Send { id, data, high } => {
                let answer = self.send_plaintext(ctx, id, pending.source, data, high);
                reply(ctx, pending, answer);
            }
            Command::SendWait { id, data, high } => {
                let answer = self
                    .send_plaintext_wait(ctx, id, pending.source, data, high)
                    .await;
                reply(ctx, pending, answer);
            }
            Command::Close(id) => self.close(ctx, id, pending).await,
            Command::Shutdown(id) => {
                let answer = self.shutdown(ctx, id, pending.source);
                reply(ctx, pending, answer);
            }
            Command::Info(id) => {
                let answer = self.info(id, pending.source);
                reply(ctx, pending, answer);
            }
            Command::HandshakeTimeout(id) => self.handshake_timeout(ctx, id),
            Command::CloseFailed { id, reason } => self.close_failed(ctx, id, reason),
        }
    }

    async fn connect(&self, ctx: &Ctx, options: ClientOptions, pending: Pending) {
        if let Some(reason) = self.capacity_error(true) {
            reply(ctx, pending, Answer::Failed(reason));
            return;
        }
        let tls = match rustls::ClientConnection::new(options.config.inner(), options.server_name) {
            Ok(connection) => connection,
            Err(error) => {
                reply(
                    ctx,
                    pending,
                    Answer::Failed(format!("创建 TLS 客户端失败：{error}")),
                );
                return;
            }
        };
        let connected = match options.connect_timeout_ms {
            Some(timeout_ms) => net::connect_timeout(ctx, options.address, timeout_ms).await,
            None => net::connect(ctx, options.address).await,
        };
        let socket = match connected {
            Ok(socket) => socket,
            Err(error) => {
                reply(
                    ctx,
                    pending,
                    Answer::Failed(format!("TCP 连接失败：{error}")),
                );
                return;
            }
        };
        if let Some(reason) = self.capacity_error(true) {
            let _ = net::shutdown(ctx, socket);
            reply(ctx, pending, Answer::Failed(reason));
            return;
        }
        let socket_info = net::info(ctx, socket).await.ok();
        let peer = socket_info
            .as_ref()
            .and_then(|info| info.peer)
            .unwrap_or_else(|| "0.0.0.0:0".parse().expect("固定地址合法"));
        let local = socket_info.and_then(|info| info.local);
        let buffer_limit = self.config.borrow().buffer_limit;
        let handshake_timeout_ms = options
            .handshake_timeout_ms
            .unwrap_or(self.config.borrow().handshake_timeout_ms);
        let mut connection = Connection::Client(tls);
        connection.set_buffer_limit(Some(buffer_limit));
        let id = {
            let mut state = self.state.borrow_mut();
            let id = state.allocate();
            state.connection_sockets.insert(socket, id);
            state.connections.insert(
                id,
                TlsConnection {
                    id,
                    socket,
                    owner: pending.source,
                    connection,
                    listen: None,
                    local,
                    peer,
                    announced: false,
                    closing: false,
                    peer_close_notify: false,
                    connect_pending: Some(pending),
                    close_pending: None,
                    paused: false,
                    handshake_timeout_ms,
                },
            );
            state.handshakes += 1;
            id
        };
        if let Err(reason) = self.flush_tls(ctx, id, true) {
            self.fail(ctx, id, reason);
            return;
        }
        self.arm_timeout(ctx, id);
    }

    async fn listen(&self, ctx: &Ctx, options: ServerOptions, pending: Pending) {
        if let Some(reason) = self.capacity_error(false) {
            reply(ctx, pending, Answer::Failed(reason));
            return;
        }
        let socket = match net::listen(ctx, options.address).await {
            Ok(socket) => socket,
            Err(error) => {
                reply(
                    ctx,
                    pending,
                    Answer::Failed(format!("TCP 监听失败：{error}")),
                );
                return;
            }
        };
        let local = net::info(ctx, socket)
            .await
            .ok()
            .and_then(|info| info.local);
        let id = {
            let mut state = self.state.borrow_mut();
            let id = state.allocate();
            state.listener_sockets.insert(socket, id);
            state.listeners.insert(
                id,
                Listener {
                    socket,
                    owner: pending.source,
                    local,
                    config: options.config,
                    started: false,
                },
            );
            id
        };
        reply(ctx, pending, Answer::Id(id));
    }

    async fn start(&self, ctx: &Ctx, id: TlsId, pending: Pending) {
        let connection = {
            let state = self.state.borrow();
            state
                .connections
                .get(&id)
                .map(|connection| (connection.socket, connection.owner == pending.source))
        };
        if let Some((socket, allowed)) = connection {
            if !allowed {
                reply(ctx, pending, denied(id));
                return;
            }
            match net::start(ctx, socket).await {
                Ok(()) => {
                    if let Some(connection) = self.state.borrow_mut().connections.get_mut(&id) {
                        connection.paused = false;
                    }
                    reply(ctx, pending, Answer::Done);
                }
                Err(error) => reply(ctx, pending, Answer::Failed(error.to_string())),
            }
            return;
        }
        let socket = {
            let state = self.state.borrow();
            let Some(listener) = state.listeners.get(&id) else {
                reply(ctx, pending, missing(id));
                return;
            };
            if listener.owner != pending.source {
                reply(ctx, pending, denied(id));
                return;
            }
            listener.socket
        };
        match net::start(ctx, socket).await {
            Ok(()) => {
                if let Some(listener) = self.state.borrow_mut().listeners.get_mut(&id) {
                    listener.started = true;
                }
                reply(ctx, pending, Answer::Done);
            }
            Err(error) => reply(
                ctx,
                pending,
                Answer::Failed(format!("启动 TLS 监听口失败：{error}")),
            ),
        }
    }

    async fn close(&self, ctx: &Ctx, id: TlsId, pending: Pending) {
        let listener_socket = {
            let state = self.state.borrow();
            state
                .listeners
                .get(&id)
                .map(|listener| (listener.socket, listener.owner == pending.source))
        };
        if let Some((socket, allowed)) = listener_socket {
            if !allowed {
                reply(ctx, pending, denied(id));
                return;
            }
            self.remove_listener(id);
            match net::close(ctx, socket).await {
                Ok(()) => reply(ctx, pending, Answer::Done),
                Err(error) => reply(ctx, pending, Answer::Failed(error.to_string())),
            }
            return;
        }

        let result = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                reply(ctx, pending, missing(id));
                return;
            };
            if connection.owner != pending.source {
                reply(ctx, pending, denied(id));
                return;
            }
            if connection.closing {
                reply(ctx, pending, Answer::Failed(format!("{id} 已在关闭")));
                return;
            }
            connection.closing = true;
            connection.close_pending = Some(pending);
            connection.connection.send_close_notify();
            connection.socket
        };
        if let Err(reason) = self.flush_tls(ctx, id, true) {
            self.fail(ctx, id, reason);
            return;
        }
        let wake = ctx.clone();
        ctx.spawn(async move {
            if let Err(error) = net::close(&wake, result).await {
                let _ = wake.send(
                    wake.handle(),
                    MsgType::USER,
                    Payload::of(Command::CloseFailed {
                        id,
                        reason: error.to_string(),
                    }),
                );
            }
        });
    }

    fn shutdown(&self, ctx: &Ctx, id: TlsId, source: u32) -> Answer {
        if let Some(listener) = self.state.borrow().listeners.get(&id) {
            if listener.owner != source {
                return denied(id);
            }
            let socket = listener.socket;
            self.remove_listener(id);
            let _ = net::shutdown(ctx, socket);
            return Answer::Done;
        }
        let allowed = self
            .state
            .borrow()
            .connections
            .get(&id)
            .map(|connection| connection.owner == source);
        match allowed {
            None => missing(id),
            Some(false) => denied(id),
            Some(true) => {
                let removed = self.remove_connection(id);
                if let Some(connection) = removed {
                    let _ = net::shutdown(ctx, connection.socket);
                    if connection.announced {
                        self.emit(ctx, connection.owner, TlsEvent::Close { id });
                    }
                }
                Answer::Done
            }
        }
    }

    fn info(&self, id: TlsId, source: u32) -> Answer {
        let state = self.state.borrow();
        if let Some(listener) = state.listeners.get(&id) {
            if listener.owner != source {
                return denied(id);
            }
            return Answer::Info(TlsInfo {
                id,
                owner: listener.owner,
                kind: "listener",
                state: if listener.started {
                    "listen"
                } else {
                    "prelisten"
                },
                paused: false,
                local: listener.local,
                peer: None,
                version: None,
                cipher_suite: None,
                alpn: None,
            });
        }
        match state.connections.get(&id) {
            Some(connection) if connection.owner == source => Answer::Info(connection.info()),
            Some(_) => denied(id),
            None => missing(id),
        }
    }

    fn send_plaintext(
        &self,
        ctx: &Ctx,
        id: TlsId,
        source: u32,
        data: Vec<u8>,
        high: bool,
    ) -> Answer {
        let write = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                return missing(id);
            };
            if connection.owner != source {
                return denied(id);
            }
            if !connection.announced || connection.closing {
                return Answer::Failed(format!("{id} 当前不能发送明文"));
            }
            connection.connection.writer().write_all(&data)
        };
        if let Err(error) = write {
            return Answer::Failed(format!("TLS 写入失败：{error}"));
        }
        match self.flush_tls(ctx, id, high) {
            Ok(()) => Answer::Done,
            Err(reason) => {
                self.fail(ctx, id, reason.clone());
                Answer::Failed(reason)
            }
        }
    }

    async fn send_plaintext_wait(
        &self,
        ctx: &Ctx,
        id: TlsId,
        source: u32,
        data: Vec<u8>,
        high: bool,
    ) -> Answer {
        let write = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                return missing(id);
            };
            if connection.owner != source {
                return denied(id);
            }
            if !connection.announced || connection.closing {
                return Answer::Failed(format!("{id} 当前不能发送明文"));
            }
            connection.connection.writer().write_all(&data)
        };
        if let Err(error) = write {
            return Answer::Failed(format!("TLS 写入失败：{error}"));
        }
        match self.flush_tls_wait(ctx, id, high).await {
            Ok(()) => Answer::Done,
            Err(reason) => {
                self.fail(ctx, id, reason.clone());
                Answer::Failed(reason)
            }
        }
    }

    async fn pause(&self, ctx: &Ctx, id: TlsId, source: u32) -> Answer {
        let socket = {
            let state = self.state.borrow();
            let Some(connection) = state.connections.get(&id) else {
                return missing(id);
            };
            if connection.owner != source {
                return denied(id);
            }
            connection.socket
        };
        match net::pause(ctx, socket).await {
            Ok(()) => {
                if let Some(connection) = self.state.borrow_mut().connections.get_mut(&id) {
                    connection.paused = true;
                }
                Answer::Done
            }
            Err(error) => Answer::Failed(error.to_string()),
        }
    }

    async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Accept { id, listen, peer } => self.accept(ctx, id, listen, peer).await,
            SocketEvent::Data { id, data } => self.receive_tls(ctx, id, data),
            SocketEvent::Close { id } => self.socket_closed(ctx, id, None),
            SocketEvent::Error { id, reason } => self.socket_closed(ctx, id, Some(reason)),
            SocketEvent::Warning { id, kilobytes } => {
                let target = {
                    let state = self.state.borrow();
                    state.connection_sockets.get(&id).and_then(|tls_id| {
                        state
                            .connections
                            .get(tls_id)
                            .map(|connection| (*tls_id, connection.owner, connection.announced))
                    })
                };
                if let Some((tls_id, owner, true)) = target {
                    self.emit(
                        ctx,
                        owner,
                        TlsEvent::Warning {
                            id: tls_id,
                            kilobytes,
                        },
                    );
                }
            }
            SocketEvent::Udp { .. } => {}
        }
    }

    async fn accept(&self, ctx: &Ctx, socket: SocketId, listen_socket: SocketId, peer: SocketAddr) {
        let (listen, owner, local, config) = {
            let state = self.state.borrow();
            let Some(listen) = state.listener_sockets.get(&listen_socket).copied() else {
                let _ = net::shutdown(ctx, socket);
                return;
            };
            let Some(listener) = state.listeners.get(&listen) else {
                let _ = net::shutdown(ctx, socket);
                return;
            };
            (
                listen,
                listener.owner,
                listener.local,
                listener.config.clone(),
            )
        };
        if self.capacity_error(true).is_some() {
            let _ = net::shutdown(ctx, socket);
            return;
        }
        let tls = match rustls::ServerConnection::new(config.inner()) {
            Ok(connection) => connection,
            Err(error) => {
                log!(ctx, "创建入站 TLS 连接失败：{error}");
                let _ = net::shutdown(ctx, socket);
                return;
            }
        };
        let buffer_limit = self.config.borrow().buffer_limit;
        let mut connection = Connection::Server(tls);
        connection.set_buffer_limit(Some(buffer_limit));
        let id = {
            let mut state = self.state.borrow_mut();
            let id = state.allocate();
            state.connection_sockets.insert(socket, id);
            state.connections.insert(
                id,
                TlsConnection {
                    id,
                    socket,
                    owner,
                    connection,
                    listen: Some(listen),
                    local,
                    peer,
                    announced: false,
                    closing: false,
                    peer_close_notify: false,
                    connect_pending: None,
                    close_pending: None,
                    paused: false,
                    handshake_timeout_ms: self.config.borrow().handshake_timeout_ms,
                },
            );
            state.handshakes += 1;
            id
        };
        if let Err(error) = net::start(ctx, socket).await {
            self.fail(ctx, id, format!("启动入站 TCP 连接失败：{error}"));
            return;
        }
        self.arm_timeout(ctx, id);
    }

    fn receive_tls(&self, ctx: &Ctx, socket: SocketId, data: Vec<u8>) {
        let id = match self.state.borrow().connection_sockets.get(&socket).copied() {
            Some(id) => id,
            None => return,
        };
        let processed = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                return;
            };
            let mut cursor = Cursor::new(data.as_slice());
            let mut plaintext = Vec::new();
            let mut result = Ok(());
            while (cursor.position() as usize) < data.len() {
                let before = cursor.position();
                match connection.connection.read_tls(&mut cursor) {
                    Ok(0) => {
                        result = Err("接收 TLS 密文时没有取得进展".into());
                        break;
                    }
                    Ok(_) => match connection.connection.process_new_packets() {
                        Ok(io_state) => {
                            connection.peer_close_notify |= io_state.peer_has_closed();
                            let mut buffer = [0u8; 16 * 1024];
                            loop {
                                match connection.connection.reader().read(&mut buffer) {
                                    Ok(0) => break,
                                    Ok(count) => plaintext.extend_from_slice(&buffer[..count]),
                                    Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                                    Err(error) => {
                                        result = Err(format!("读取 TLS 明文失败：{error}"));
                                        break;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            result = Err(format!("TLS 协议错误：{error}"));
                            break;
                        }
                    },
                    Err(error) => {
                        result = Err(format!("接收 TLS 密文失败：{error}"));
                        break;
                    }
                }
                if result.is_err() {
                    break;
                }
                if cursor.position() == before {
                    result = Err("接收 TLS 密文时没有取得进展".into());
                    break;
                }
            }
            result.map(|()| plaintext)
        };
        if let Err(reason) = processed {
            let _ = self.flush_tls(ctx, id, true);
            self.fail(ctx, id, reason);
            return;
        }
        if let Err(reason) = self.flush_tls(ctx, id, true) {
            self.fail(ctx, id, reason);
            return;
        }
        self.announce_if_ready(ctx, id);
        if let Ok(plaintext) = processed {
            if !plaintext.is_empty() {
                let target = self
                    .state
                    .borrow()
                    .connections
                    .get(&id)
                    .and_then(|connection| connection.announced.then_some(connection.owner));
                if let Some(owner) = target {
                    self.emit(
                        ctx,
                        owner,
                        TlsEvent::Data {
                            id,
                            data: plaintext,
                        },
                    );
                }
            }
        }
        let closed = self
            .state
            .borrow()
            .connections
            .get(&id)
            .is_some_and(|connection| connection.peer_close_notify);
        if closed {
            if let Some(socket) = self
                .state
                .borrow()
                .connections
                .get(&id)
                .map(|connection| connection.socket)
            {
                let _ = net::shutdown(ctx, socket);
            }
        }
    }

    fn announce_if_ready(&self, ctx: &Ctx, id: TlsId) {
        let ready = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&id) else {
                return;
            };
            if connection.announced || connection.connection.is_handshaking() {
                return;
            }
            connection.announced = true;
            let pending = connection.connect_pending.take();
            let owner = connection.owner;
            let listen = connection.listen;
            let peer = connection.peer;
            let version = version_name(connection.connection.protocol_version());
            let cipher_suite = connection
                .connection
                .negotiated_cipher_suite()
                .map(|suite| format!("{:?}", suite.suite()));
            let alpn = connection.connection.alpn_protocol().map(<[u8]>::to_vec);
            state.handshakes = state.handshakes.saturating_sub(1);
            (pending, owner, listen, peer, version, cipher_suite, alpn)
        };
        let (pending, owner, listen, peer, version, cipher_suite, alpn) = ready;
        if let Some(pending) = pending {
            reply(ctx, pending, Answer::Id(id));
        }
        self.emit(
            ctx,
            owner,
            TlsEvent::Connected {
                id,
                listen,
                peer,
                version,
                cipher_suite,
                alpn,
            },
        );
    }

    fn flush_tls(&self, ctx: &Ctx, id: TlsId, high: bool) -> std::result::Result<(), String> {
        let (socket, encrypted) = {
            let mut state = self.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&id)
                .ok_or_else(|| format!("{id} 已不存在"))?;
            let mut encrypted = Vec::new();
            while connection.connection.wants_write() {
                connection
                    .connection
                    .write_tls(&mut encrypted)
                    .map_err(|error| format!("生成 TLS 密文失败：{error}"))?;
            }
            (connection.socket, encrypted)
        };
        if encrypted.is_empty() {
            return Ok(());
        }
        let result = if high {
            net::send(ctx, socket, encrypted)
        } else {
            net::send_low(ctx, socket, encrypted)
        };
        result.map_err(|error| format!("发送 TLS 密文失败：{error}"))
    }

    async fn flush_tls_wait(
        &self,
        ctx: &Ctx,
        id: TlsId,
        high: bool,
    ) -> std::result::Result<(), String> {
        let (socket, encrypted) = {
            let mut state = self.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&id)
                .ok_or_else(|| format!("{id} 已不存在"))?;
            let mut encrypted = Vec::new();
            while connection.connection.wants_write() {
                connection
                    .connection
                    .write_tls(&mut encrypted)
                    .map_err(|error| format!("生成 TLS 密文失败：{error}"))?;
            }
            (connection.socket, encrypted)
        };
        if encrypted.is_empty() {
            return Ok(());
        }
        let result = if high {
            net::send_wait(ctx, socket, encrypted).await
        } else {
            net::send_low_wait(ctx, socket, encrypted).await
        };
        result.map_err(|error| format!("发送 TLS 密文失败：{error}"))
    }

    fn socket_closed(&self, ctx: &Ctx, socket: SocketId, error: Option<String>) {
        if let Some(listener) = self.state.borrow().listener_sockets.get(&socket).copied() {
            self.remove_listener(listener);
            return;
        }
        let Some(id) = self.state.borrow().connection_sockets.get(&socket).copied() else {
            return;
        };
        let Some(connection) = self.remove_connection(id) else {
            return;
        };
        if let Some(pending) = connection.close_pending {
            match &error {
                Some(reason) => reply(ctx, pending, Answer::Failed(reason.clone())),
                None => reply(ctx, pending, Answer::Done),
            }
        }
        if let Some(pending) = connection.connect_pending {
            let reason = error.unwrap_or_else(|| "TLS 握手期间 TCP 连接关闭".into());
            reply(ctx, pending, Answer::Failed(reason));
            return;
        }
        if !connection.announced {
            return;
        }
        match error {
            Some(reason) => self.emit(ctx, connection.owner, TlsEvent::Error { id, reason }),
            None if connection.peer_close_notify || connection.closing => {
                self.emit(ctx, connection.owner, TlsEvent::Close { id })
            }
            None => self.emit(
                ctx,
                connection.owner,
                TlsEvent::Error {
                    id,
                    reason: "TCP 在没有 TLS close_notify 的情况下关闭".into(),
                },
            ),
        }
    }

    fn fail(&self, ctx: &Ctx, id: TlsId, reason: String) {
        let Some(connection) = self.remove_connection(id) else {
            return;
        };
        let _ = net::shutdown(ctx, connection.socket);
        if let Some(pending) = connection.connect_pending {
            reply(ctx, pending, Answer::Failed(reason));
        } else if connection.announced {
            self.emit(ctx, connection.owner, TlsEvent::Error { id, reason });
        }
        if let Some(pending) = connection.close_pending {
            reply(ctx, pending, Answer::Failed("TLS 关闭失败".into()));
        }
    }

    fn handshake_timeout(&self, ctx: &Ctx, id: TlsId) {
        let timed_out = self
            .state
            .borrow()
            .connections
            .get(&id)
            .is_some_and(|connection| !connection.announced);
        if timed_out {
            self.fail(ctx, id, "TLS 握手超时".into());
        }
    }

    fn close_failed(&self, ctx: &Ctx, id: TlsId, reason: String) {
        if self.state.borrow().connections.contains_key(&id) {
            self.fail(ctx, id, format!("关闭底层连接失败：{reason}"));
        }
    }

    fn arm_timeout(&self, ctx: &Ctx, id: TlsId) {
        let timeout = self
            .state
            .borrow()
            .connections
            .get(&id)
            .map_or(self.config.borrow().handshake_timeout_ms, |connection| {
                connection.handshake_timeout_ms
            });
        let wake = ctx.clone();
        ctx.spawn(async move {
            wake.sleep_ms(timeout).await;
            let _ = wake.send(
                wake.handle(),
                MsgType::USER,
                Payload::of(Command::HandshakeTimeout(id)),
            );
        });
    }

    fn capacity_error(&self, handshake: bool) -> Option<String> {
        let state = self.state.borrow();
        let config = self.config.borrow();
        if state.len() >= config.max_connections {
            return Some("TLS 连接数已达到上限".into());
        }
        if handshake && state.handshakes >= config.max_handshakes {
            return Some("并发 TLS 握手数已达到上限".into());
        }
        None
    }

    fn remove_listener(&self, id: TlsId) -> Option<Listener> {
        let mut state = self.state.borrow_mut();
        let listener = state.listeners.remove(&id)?;
        state.listener_sockets.remove(&listener.socket);
        Some(listener)
    }

    fn remove_connection(&self, id: TlsId) -> Option<TlsConnection> {
        let mut state = self.state.borrow_mut();
        let connection = state.connections.remove(&id)?;
        state.connection_sockets.remove(&connection.socket);
        if !connection.announced {
            state.handshakes = state.handshakes.saturating_sub(1);
        }
        Some(connection)
    }

    fn emit(&self, ctx: &Ctx, owner: u32, event: TlsEvent) {
        let _ = ctx.send(owner, MsgType::TLS, Payload::of(event));
    }
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

fn missing(id: TlsId) -> Answer {
    Answer::Failed(format!("{id} 不存在或已经关闭"))
}

fn denied(id: TlsId) -> Answer {
    Answer::Failed(format!("调用方不是 {id} 的 owner"))
}

fn version_name(version: Option<ProtocolVersion>) -> Option<&'static str> {
    match version {
        Some(ProtocolVersion::TLSv1_2) => Some("TLSv1.2"),
        Some(ProtocolVersion::TLSv1_3) => Some("TLSv1.3"),
        Some(_) => Some("unknown"),
        None => None,
    }
}
