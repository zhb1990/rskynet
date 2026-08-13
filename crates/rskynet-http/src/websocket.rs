//! 基于 tungstenite、由业务 actor 驱动的 WebSocket 客户端与服务端连接。

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::io::{ErrorKind, Read, Write};
use std::sync::Arc;
use std::task::{Poll, Waker};

use rskynet_core::{Ctx, MsgType, Payload, SvcCell};
use rskynet_net::{SocketEvent, SocketId};
use tungstenite::handshake::client::ClientHandshake;
use tungstenite::handshake::{HandshakeError, MidHandshake};
use tungstenite::protocol::{Role, WebSocket as TungsteniteSocket};

#[cfg(feature = "tls")]
use rskynet_tls::{
    ClientOptions, ClientTlsConfig, ServerName, ServerVerification, TlsEvent, TlsId,
};

use crate::transport::TransportId;
use crate::{HttpError, Result};

pub use tungstenite::client::{ClientRequestBuilder, IntoClientRequest};
pub use tungstenite::protocol::frame::CloseFrame;
pub use tungstenite::protocol::frame::coding::CloseCode;
pub use tungstenite::protocol::{Message, WebSocketConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WebSocketId(pub u64);

impl WebSocketId {
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Default)]
pub(crate) struct EventIo {
    input: VecDeque<u8>,
    output: Vec<u8>,
}

impl EventIo {
    pub(crate) fn push_input(&mut self, data: Vec<u8>) {
        self.input.extend(data);
    }

    pub(crate) fn take_output(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }
}

impl Read for EventIo {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.input.is_empty() {
            return Err(std::io::Error::from(ErrorKind::WouldBlock));
        }
        let used = output.len().min(self.input.len());
        for target in &mut output[..used] {
            *target = self.input.pop_front().expect("input length checked");
        }
        Ok(used)
    }
}

impl Write for EventIo {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        self.output.extend_from_slice(input);
        Ok(input.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn default_protocol_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(8 * 1024)
        .write_buffer_size(0)
        .max_write_buffer_size(1024 * 1024)
        .max_frame_size(Some(2 * 1024 * 1024))
        .max_message_size(Some(8 * 1024 * 1024))
}

#[derive(Debug, Clone)]
pub struct WebSocketUpgradeOptions {
    pub websocket: WebSocketConfig,
    pub input_high_water: usize,
    pub input_low_water: usize,
    pub close_timeout_ms: u64,
    pub protocol: Option<String>,
}

impl WebSocketUpgradeOptions {
    #[must_use]
    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.input_low_water == 0
            || self.input_low_water >= self.input_high_water
            || self.close_timeout_ms == 0
        {
            return Err(HttpError::Config(
                "WebSocket 必须满足 0 < input_low_water < input_high_water，且关闭超时非零".into(),
            ));
        }
        Ok(())
    }
}

impl Default for WebSocketUpgradeOptions {
    fn default() -> Self {
        Self {
            websocket: default_protocol_config(),
            input_high_water: 256 * 1024,
            input_low_water: 128 * 1024,
            close_timeout_ms: 5_000,
            protocol: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WebSocketClientConfig {
    pub connect_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub close_timeout_ms: u64,
    pub input_high_water: usize,
    pub input_low_water: usize,
    pub websocket: WebSocketConfig,
}

impl WebSocketClientConfig {
    fn validate(&self) -> Result<()> {
        if self.connect_timeout_ms == 0
            || self.handshake_timeout_ms == 0
            || self.close_timeout_ms == 0
            || self.input_low_water == 0
            || self.input_low_water >= self.input_high_water
        {
            return Err(HttpError::Config("WebSocket 客户端配置无效".into()));
        }
        Ok(())
    }
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            handshake_timeout_ms: 30_000,
            close_timeout_ms: 5_000,
            input_high_water: 256 * 1024,
            input_low_water: 128 * 1024,
            websocket: default_protocol_config(),
        }
    }
}

struct SocketState {
    socket: TungsteniteSocket<EventIo>,
    messages: VecDeque<Message>,
    queued_bytes: usize,
    recv_waker: Option<Waker>,
    ended: bool,
    error: Option<HttpError>,
    paused: bool,
}

struct SharedSocketOptions {
    protocol: Option<String>,
    input_high_water: usize,
    input_low_water: usize,
    close_timeout_ms: u64,
    cleanup: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(crate) struct SharedSocket {
    id: WebSocketId,
    owner: u32,
    transport: TransportId,
    protocol: Option<String>,
    input_high_water: usize,
    input_low_water: usize,
    close_timeout_ms: u64,
    abort: AbortTransport,
    cleanup: Option<Arc<dyn Fn() + Send + Sync>>,
    state: SvcCell<SocketState>,
}

impl SharedSocket {
    fn from_socket(
        ctx: &Ctx,
        id: WebSocketId,
        transport: TransportId,
        socket: TungsteniteSocket<EventIo>,
        options: SharedSocketOptions,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            id,
            owner: ctx.handle(),
            transport,
            protocol: options.protocol,
            input_high_water: options.input_high_water,
            input_low_water: options.input_low_water,
            close_timeout_ms: options.close_timeout_ms,
            abort: AbortTransport::new(ctx, transport)?,
            cleanup: options.cleanup,
            state: SvcCell::new(SocketState {
                socket,
                messages: VecDeque::new(),
                queued_bytes: 0,
                recv_waker: None,
                ended: false,
                error: None,
                paused: false,
            }),
        }))
    }

    pub(crate) fn server(
        ctx: &Ctx,
        id: WebSocketId,
        transport: TransportId,
        tail: Vec<u8>,
        options: &WebSocketUpgradeOptions,
        cleanup: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let socket = TungsteniteSocket::from_partially_read(
            EventIo::default(),
            tail,
            Role::Server,
            Some(options.websocket),
        );
        Self::from_socket(
            ctx,
            id,
            transport,
            socket,
            SharedSocketOptions {
                protocol: options.protocol.clone(),
                input_high_water: options.input_high_water,
                input_low_water: options.input_low_water,
                close_timeout_ms: options.close_timeout_ms,
                cleanup: Some(cleanup),
            },
        )
    }

    fn check_owner(&self, ctx: &Ctx) -> Result<()> {
        if self.owner != ctx.handle() {
            return Err(HttpError::InvalidState(
                "WebSocket 只能由创建它的业务服务操作",
            ));
        }
        Ok(())
    }

    pub(crate) async fn on_data(&self, ctx: &Ctx, data: Vec<u8>) -> Result<()> {
        self.check_owner(ctx)?;
        let (output, pause, wake, shutdown) = {
            let mut state = self.state.borrow_mut();
            if state.ended {
                return Ok(());
            }
            state.socket.get_mut().push_input(data);
            let mut shutdown = false;
            loop {
                match state.socket.read() {
                    Ok(message) => {
                        state.queued_bytes = state.queued_bytes.saturating_add(message.len());
                        state.messages.push_back(message);
                    }
                    Err(tungstenite::Error::Io(error)) if error.kind() == ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(tungstenite::Error::ConnectionClosed) => {
                        state.ended = true;
                        shutdown = true;
                        break;
                    }
                    Err(error) => {
                        state.error = Some(HttpError::WebSocket(error));
                        state.ended = true;
                        shutdown = true;
                        break;
                    }
                }
            }
            let output = state.socket.get_mut().take_output();
            let pause = !state.paused && state.queued_bytes >= self.input_high_water;
            if pause {
                state.paused = true;
            }
            (output, pause, state.recv_waker.take(), shutdown)
        };
        if !output.is_empty() {
            self.transport.send_wait(ctx, output).await?;
        }
        if pause {
            self.transport.pause(ctx).await?;
        }
        if let Some(waker) = wake {
            waker.wake();
        }
        if shutdown {
            self.cleanup();
            self.transport.shutdown(ctx);
        }
        Ok(())
    }

    pub(crate) fn on_closed(&self, reason: Option<String>) {
        let wake = {
            let mut state = self.state.borrow_mut();
            state.ended = true;
            if let Some(reason) = reason {
                state.error = Some(HttpError::Transport(reason));
            }
            state.recv_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        self.cleanup();
    }

    async fn recv(&self, ctx: &Ctx) -> Result<Option<Message>> {
        self.check_owner(ctx)?;
        let (answer, resume) = poll_fn(|task| {
            let mut state = self.state.borrow_mut();
            if let Some(message) = state.messages.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(message.len());
                let resume = state.paused && state.queued_bytes <= self.input_low_water;
                if resume {
                    state.paused = false;
                }
                return Poll::Ready((Ok(Some(message)), resume));
            }
            if let Some(error) = state.error.take() {
                return Poll::Ready((Err(error), false));
            }
            if state.ended {
                return Poll::Ready((Ok(None), false));
            }
            state.recv_waker = Some(task.waker().clone());
            Poll::Pending
        })
        .await;
        if resume {
            self.transport.resume(ctx).await?;
        }
        answer
    }

    async fn send(&self, ctx: &Ctx, message: Message) -> Result<()> {
        self.check_owner(ctx)?;
        let output = {
            let mut state = self.state.borrow_mut();
            if state.ended {
                return Err(HttpError::BodyClosed);
            }
            state.socket.send(message)?;
            state.socket.get_mut().take_output()
        };
        if !output.is_empty() {
            self.transport.send_wait(ctx, output).await?;
        }
        Ok(())
    }

    async fn close(&self, ctx: &Ctx, frame: Option<CloseFrame>) -> Result<()> {
        self.check_owner(ctx)?;
        let output = {
            let mut state = self.state.borrow_mut();
            if state.ended {
                return Ok(());
            }
            state.socket.close(frame)?;
            state.socket.flush()?;
            state.socket.get_mut().take_output()
        };
        if !output.is_empty() {
            self.transport.send_wait(ctx, output).await?;
        }
        let started = ctx.node().now();
        while !self.state.borrow().ended {
            if ctx.node().now().saturating_sub(started) >= self.close_timeout_ms {
                self.transport.shutdown(ctx);
                self.on_closed(None);
                break;
            }
            ctx.sleep_ms(10).await;
        }
        Ok(())
    }

    pub(crate) fn abort(&self) {
        self.abort.abort();
        self.on_closed(None);
    }

    fn cleanup(&self) {
        if let Some(cleanup) = &self.cleanup {
            cleanup();
        }
    }
}

#[derive(Clone)]
struct AbortTransport {
    node: rskynet_core::NodeRef,
    source: u32,
    dest: u32,
    transport: TransportId,
}

impl AbortTransport {
    fn new(ctx: &Ctx, transport: TransportId) -> Result<Self> {
        let name = match transport {
            TransportId::Plain(_) => rskynet_net::NAME,
            #[cfg(feature = "tls")]
            TransportId::Tls(_) => rskynet_tls::NAME,
        };
        let dest = ctx
            .query_name(name)
            .ok_or(HttpError::InvalidState("传输服务未启动"))?;
        Ok(Self {
            node: ctx.node(),
            source: ctx.handle(),
            dest,
            transport,
        })
    }

    fn abort(&self) {
        let payload = match self.transport {
            TransportId::Plain(id) => Payload::of(rskynet_net::Command::Shutdown(id)),
            #[cfg(feature = "tls")]
            TransportId::Tls(id) => Payload::of(rskynet_tls::Command::Shutdown(id)),
        };
        let _ = self
            .node
            .send(self.source, self.dest, MsgType::USER, 0, payload);
    }
}

pub struct WebSocket {
    shared: Arc<SharedSocket>,
    active: bool,
}

impl WebSocket {
    pub(crate) fn new(shared: Arc<SharedSocket>) -> Self {
        Self {
            shared,
            active: true,
        }
    }

    pub fn id(&self) -> WebSocketId {
        self.shared.id
    }

    pub fn protocol(&self) -> Option<&str> {
        self.shared.protocol.as_deref()
    }

    pub fn sender(&self) -> WebSocketSender {
        WebSocketSender {
            shared: self.shared.clone(),
        }
    }

    pub async fn recv(&mut self, ctx: &Ctx) -> Result<Option<Message>> {
        let answer = self.shared.recv(ctx).await?;
        if answer.is_none() {
            self.active = false;
        }
        Ok(answer)
    }

    pub async fn send(&self, ctx: &Ctx, message: Message) -> Result<()> {
        self.shared.send(ctx, message).await
    }

    pub async fn close(&mut self, ctx: &Ctx, frame: Option<CloseFrame>) -> Result<()> {
        let answer = self.shared.close(ctx, frame).await;
        if answer.is_ok() {
            self.active = false;
        }
        answer
    }
}

impl Drop for WebSocket {
    fn drop(&mut self) {
        if self.active {
            self.shared.abort();
        }
    }
}

#[derive(Clone)]
pub struct WebSocketSender {
    shared: Arc<SharedSocket>,
}

impl WebSocketSender {
    pub fn id(&self) -> WebSocketId {
        self.shared.id
    }

    pub async fn send(&self, ctx: &Ctx, message: Message) -> Result<()> {
        self.shared.send(ctx, message).await
    }

    pub async fn close(&self, ctx: &Ctx, frame: Option<CloseFrame>) -> Result<()> {
        self.shared.close(ctx, frame).await
    }
}

type ClientMidHandshake = MidHandshake<ClientHandshake<EventIo>>;

struct PendingHandshake {
    handshake: Option<ClientMidHandshake>,
    result: Option<Result<(Arc<SharedSocket>, crate::http::Response<()>)>>,
    waiter: Option<Waker>,
    generation: u64,
}

#[derive(Default)]
struct ClientState {
    next_id: u64,
    next_generation: u64,
    pending: HashMap<TransportId, PendingHandshake>,
    connected: HashMap<TransportId, Arc<SharedSocket>>,
}

struct WebSocketClientCore {
    owner: SvcCell<Option<u32>>,
    config: WebSocketClientConfig,
    state: SvcCell<ClientState>,
    #[cfg(feature = "tls")]
    tls: ClientTlsConfig,
}

#[derive(Clone)]
pub struct WebSocketClient {
    core: Arc<WebSocketClientCore>,
}

impl WebSocketClient {
    pub fn new(config: WebSocketClientConfig) -> Self {
        config.validate().expect("WebSocketClientConfig 无效");
        Self {
            core: Arc::new(WebSocketClientCore {
                owner: SvcCell::new(None),
                config,
                state: SvcCell::new(ClientState::default()),
                #[cfg(feature = "tls")]
                tls: ClientTlsConfig::new(
                    ServerVerification::default(),
                    vec![b"http/1.1".to_vec()],
                )
                .expect("默认 WebSocket TLS 配置应有效"),
            }),
        }
    }

    #[cfg(feature = "tls")]
    pub fn with_tls_config(config: WebSocketClientConfig, tls: ClientTlsConfig) -> Self {
        config.validate().expect("WebSocketClientConfig 无效");
        Self {
            core: Arc::new(WebSocketClientCore {
                owner: SvcCell::new(None),
                config,
                state: SvcCell::new(ClientState::default()),
                tls,
            }),
        }
    }

    fn claim_owner(&self, ctx: &Ctx) -> Result<()> {
        let mut owner = self.core.owner.borrow_mut();
        match *owner {
            Some(value) if value != ctx.handle() => Err(HttpError::InvalidState(
                "WebSocketClient 只能由首次使用它的业务服务操作",
            )),
            Some(_) => Ok(()),
            None => {
                *owner = Some(ctx.handle());
                Ok(())
            }
        }
    }

    pub async fn connect<R: IntoClientRequest>(
        &self,
        ctx: &Ctx,
        request: R,
    ) -> Result<(WebSocket, crate::http::Response<()>)> {
        self.claim_owner(ctx)?;
        let request = request.into_client_request()?;
        let uri = request.uri();
        let host = uri
            .host()
            .ok_or_else(|| HttpError::InvalidUri("缺少 host".into()))?;
        let scheme = uri
            .scheme_str()
            .ok_or_else(|| HttpError::InvalidUri("缺少 scheme".into()))?;
        let port = uri.port_u16().unwrap_or(match scheme {
            "ws" => 80,
            "wss" => 443,
            other => return Err(HttpError::UnsupportedScheme(other.into())),
        });
        let address = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let transport = match scheme {
            "ws" => TransportId::Plain(
                rskynet_net::connect_timeout(ctx, address, self.core.config.connect_timeout_ms)
                    .await?,
            ),
            "wss" => {
                #[cfg(feature = "tls")]
                {
                    let server_name = ServerName::try_from(host.to_owned())
                        .map_err(|_| HttpError::InvalidUri("TLS server name 无效".into()))?;
                    let options = ClientOptions::new(address, server_name, self.core.tls.clone())
                        .with_connect_timeout_ms(self.core.config.connect_timeout_ms)
                        .with_handshake_timeout_ms(self.core.config.handshake_timeout_ms);
                    TransportId::Tls(rskynet_tls::connect(ctx, options).await?)
                }
                #[cfg(not(feature = "tls"))]
                {
                    return Err(HttpError::UnsupportedScheme(
                        "wss（未启用 tls feature）".into(),
                    ));
                }
            }
            _ => unreachable!(),
        };
        let started = ClientHandshake::start(
            EventIo::default(),
            request,
            Some(self.core.config.websocket),
        )?;
        let mut handshake = match started.handshake() {
            Err(HandshakeError::Interrupted(value)) => value,
            Err(HandshakeError::Failure(error)) => {
                transport.shutdown(ctx);
                return Err(error.into());
            }
            Ok(_) => unreachable!("空输入不可能完成客户端握手"),
        };
        let output = handshake.get_mut().get_mut().take_output();
        let generation = {
            let mut state = self.core.state.borrow_mut();
            state.next_generation = state.next_generation.wrapping_add(1).max(1);
            let generation = state.next_generation;
            state.pending.insert(
                transport,
                PendingHandshake {
                    handshake: Some(handshake),
                    result: None,
                    waiter: None,
                    generation,
                },
            );
            generation
        };
        if let Err(error) = transport.send_wait(ctx, output).await {
            self.core.state.borrow_mut().pending.remove(&transport);
            transport.shutdown(ctx);
            return Err(error);
        }
        self.arm_handshake_timeout(ctx, transport, generation);
        let result = poll_fn(|task| {
            let mut state = self.core.state.borrow_mut();
            let Some(pending) = state.pending.get_mut(&transport) else {
                return Poll::Ready(Err(HttpError::Canceled));
            };
            if let Some(result) = pending.result.take() {
                state.pending.remove(&transport);
                return Poll::Ready(result);
            }
            pending.waiter = Some(task.waker().clone());
            Poll::Pending
        })
        .await;
        match result {
            Ok((shared, response)) => Ok((WebSocket::new(shared), response)),
            Err(error) => {
                transport.shutdown(ctx);
                Err(error)
            }
        }
    }

    fn arm_handshake_timeout(&self, ctx: &Ctx, transport: TransportId, generation: u64) {
        let core = self.core.clone();
        let wake = ctx.clone();
        let timeout = self.core.config.handshake_timeout_ms;
        ctx.spawn(async move {
            wake.sleep_ms(timeout).await;
            let waiter = {
                let mut state = core.state.borrow_mut();
                let Some(pending) = state.pending.get_mut(&transport) else {
                    return;
                };
                if pending.generation != generation || pending.result.is_some() {
                    return;
                }
                pending.result = Some(Err(HttpError::Timeout("websocket handshake")));
                pending.waiter.take()
            };
            transport.shutdown(&wake);
            if let Some(waiter) = waiter {
                waiter.wake();
            }
        });
    }

    pub fn handles_socket(&self, id: SocketId) -> bool {
        let transport = TransportId::Plain(id);
        let state = self.core.state.borrow();
        state.pending.contains_key(&transport) || state.connected.contains_key(&transport)
    }

    pub async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) -> Result<()> {
        self.claim_owner(ctx)?;
        match event {
            SocketEvent::Data { id, data } => self.on_data(ctx, TransportId::Plain(id), data).await,
            SocketEvent::Close { id } => {
                self.on_closed(TransportId::Plain(id), None);
                Ok(())
            }
            SocketEvent::Error { id, reason } => {
                self.on_closed(TransportId::Plain(id), Some(reason));
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[cfg(feature = "tls")]
    pub fn handles_tls(&self, id: TlsId) -> bool {
        let transport = TransportId::Tls(id);
        let state = self.core.state.borrow();
        state.pending.contains_key(&transport) || state.connected.contains_key(&transport)
    }

    #[cfg(feature = "tls")]
    pub async fn on_tls(&self, ctx: &Ctx, event: TlsEvent) -> Result<()> {
        self.claim_owner(ctx)?;
        match event {
            TlsEvent::Data { id, data } => self.on_data(ctx, TransportId::Tls(id), data).await,
            TlsEvent::Close { id } => {
                self.on_closed(TransportId::Tls(id), None);
                Ok(())
            }
            TlsEvent::Error { id, reason } => {
                self.on_closed(TransportId::Tls(id), Some(reason));
                Ok(())
            }
            _ => Ok(()),
        }
    }

    async fn on_data(&self, ctx: &Ctx, transport: TransportId, data: Vec<u8>) -> Result<()> {
        let shared = self.core.state.borrow().connected.get(&transport).cloned();
        if let Some(shared) = shared {
            return shared.on_data(ctx, data).await;
        }
        let handshake = {
            let mut state = self.core.state.borrow_mut();
            let Some(pending) = state.pending.get_mut(&transport) else {
                return Err(HttpError::InvalidState("WebSocket 客户端连接不存在"));
            };
            let mut handshake = pending
                .handshake
                .take()
                .ok_or(HttpError::InvalidState("WebSocket 握手状态丢失"))?;
            handshake.get_mut().get_mut().push_input(data);
            handshake
        };
        match handshake.handshake() {
            Err(HandshakeError::Interrupted(mut handshake)) => {
                let output = handshake.get_mut().get_mut().take_output();
                self.core
                    .state
                    .borrow_mut()
                    .pending
                    .get_mut(&transport)
                    .expect("pending checked")
                    .handshake = Some(handshake);
                if !output.is_empty() {
                    transport.send_wait(ctx, output).await?;
                }
            }
            Err(HandshakeError::Failure(error)) => {
                self.finish_handshake(transport, Err(error.into()));
            }
            Ok((socket, response)) => {
                let result = if response.headers().contains_key("Sec-WebSocket-Extensions") {
                    Err(HttpError::Protocol(
                        "服务端返回了未支持的 WebSocket 扩展".into(),
                    ))
                } else {
                    let protocol = response
                        .headers()
                        .get("Sec-WebSocket-Protocol")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let id = {
                        let mut state = self.core.state.borrow_mut();
                        state.next_id = state.next_id.wrapping_add(1).max(1);
                        WebSocketId(state.next_id)
                    };
                    SharedSocket::from_socket(
                        ctx,
                        id,
                        transport,
                        socket,
                        SharedSocketOptions {
                            protocol,
                            input_high_water: self.core.config.input_high_water,
                            input_low_water: self.core.config.input_low_water,
                            close_timeout_ms: self.core.config.close_timeout_ms,
                            cleanup: Some({
                                let core = Arc::downgrade(&self.core);
                                Arc::new(move || {
                                    if let Some(core) = core.upgrade() {
                                        core.state.borrow_mut().connected.remove(&transport);
                                    }
                                })
                            }),
                        },
                    )
                    .map(|shared| (shared, response.map(|_| ())))
                };
                if let Ok((shared, _)) = &result {
                    self.core
                        .state
                        .borrow_mut()
                        .connected
                        .insert(transport, shared.clone());
                }
                self.finish_handshake(transport, result);
            }
        }
        Ok(())
    }

    fn finish_handshake(
        &self,
        transport: TransportId,
        result: Result<(Arc<SharedSocket>, crate::http::Response<()>)>,
    ) {
        let waiter = {
            let mut state = self.core.state.borrow_mut();
            let Some(pending) = state.pending.get_mut(&transport) else {
                return;
            };
            pending.result = Some(result);
            pending.waiter.take()
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }

    fn on_closed(&self, transport: TransportId, reason: Option<String>) {
        let (shared, waiter) = {
            let mut state = self.core.state.borrow_mut();
            let shared = state.connected.remove(&transport);
            let waiter = if let Some(pending) = state.pending.get_mut(&transport) {
                pending.result = Some(Err(HttpError::Transport(
                    reason.clone().unwrap_or_else(|| "连接已关闭".into()),
                )));
                pending.waiter.take()
            } else {
                None
            };
            (shared, waiter)
        };
        if let Some(shared) = shared {
            shared.on_closed(reason);
        }
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}

impl Default for WebSocketClient {
    fn default() -> Self {
        Self::new(WebSocketClientConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_event_io_is_nonblocking_not_eof() {
        let error = EventIo::default().read(&mut [0; 8]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::WouldBlock);
    }

    #[test]
    fn partially_read_server_socket_consumes_frame_that_followed_headers() {
        // FIN + text, masked payload "hi" with mask [1, 2, 3, 4].
        let tail = vec![0x81, 0x82, 1, 2, 3, 4, b'h' ^ 1, b'i' ^ 2];
        let mut socket = TungsteniteSocket::from_partially_read(
            EventIo::default(),
            tail,
            Role::Server,
            Some(default_protocol_config()),
        );
        assert_eq!(socket.read().unwrap().to_text().unwrap(), "hi");
        let error = socket.read().unwrap_err();
        assert!(
            matches!(error, tungstenite::Error::Io(ref io) if io.kind() == ErrorKind::WouldBlock)
        );
    }
}
