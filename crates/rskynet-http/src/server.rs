use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;

use rskynet_core::{Ctx, MsgType, Payload, SvcCell};
use rskynet_net::SocketEvent;
use ureq_proto::http::{Request, Response};
use ureq_proto::server::state::{ProvideResponse, RecvBody, RecvRequest, Send100, SendBody};
use ureq_proto::server::{RecvRequestResult, Reply, SendResponseResult};

#[cfg(feature = "tls")]
use rskynet_tls::{ServerOptions, TlsEvent};

use crate::body::{BodySpec, IncomingBody, OutgoingBody};
use crate::config::HttpServerConfig;
use crate::transport::TransportId;
use crate::{HttpError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HttpListenerId(pub u64);

enum ServerProto {
    RecvRequest(Reply<RecvRequest>),
    Await100(Reply<Send100>),
    RecvBody(Reply<RecvBody>),
    Provide(Reply<ProvideResponse>),
    SendBody(Reply<SendBody>),
    Responding,
    #[cfg(feature = "websocket")]
    WebSocket(Arc<crate::websocket::SharedSocket>),
}

struct Connection {
    transport: TransportId,
    listener: HttpListenerId,
    peer: SocketAddr,
    tls: bool,
    proto: Option<ServerProto>,
    input: Vec<u8>,
    chunks: VecDeque<Vec<u8>>,
    body_ended: bool,
    error: Option<String>,
    paused: bool,
    response_expected: Option<u64>,
    response_written: u64,
    abort: AbortTransport,
    header_generation: u64,
}

#[derive(Default)]
struct ServerState {
    next_listener: u64,
    #[cfg(feature = "websocket")]
    next_websocket: u64,
    listeners: HashMap<TransportId, HttpListenerId>,
    connections: HashMap<TransportId, Connection>,
}

struct ServerCore {
    config: HttpServerConfig,
    state: SvcCell<ServerState>,
}

pub struct HttpServer {
    core: Arc<ServerCore>,
}

impl HttpServer {
    pub fn new(config: HttpServerConfig) -> Self {
        config.validate().expect("HttpServerConfig 无效");
        Self {
            core: Arc::new(ServerCore {
                config,
                state: SvcCell::new(ServerState::default()),
            }),
        }
    }

    pub async fn bind_http(&self, ctx: &Ctx, address: impl Into<String>) -> Result<HttpListenerId> {
        let raw = rskynet_net::listen(ctx, address).await?;
        let transport = TransportId::Plain(raw);
        let id = self.insert_listener(transport);
        if let Err(error) = rskynet_net::start(ctx, raw).await {
            self.core.state.borrow_mut().listeners.remove(&transport);
            transport.shutdown(ctx);
            return Err(error.into());
        }
        Ok(id)
    }

    #[cfg(feature = "tls")]
    pub async fn bind_https(&self, ctx: &Ctx, options: ServerOptions) -> Result<HttpListenerId> {
        let raw = rskynet_tls::listen(ctx, options).await?;
        let transport = TransportId::Tls(raw);
        let id = self.insert_listener(transport);
        if let Err(error) = rskynet_tls::start(ctx, raw).await {
            self.core.state.borrow_mut().listeners.remove(&transport);
            transport.shutdown(ctx);
            return Err(error.into());
        }
        Ok(id)
    }

    fn insert_listener(&self, transport: TransportId) -> HttpListenerId {
        let mut state = self.core.state.borrow_mut();
        state.next_listener = state.next_listener.wrapping_add(1).max(1);
        let id = HttpListenerId(state.next_listener);
        state.listeners.insert(transport, id);
        id
    }

    /// 查询监听口的本地地址；使用端口 0 绑定时可由此取得系统分配的端口。
    pub async fn local_addr(&self, ctx: &Ctx, listener: HttpListenerId) -> Result<SocketAddr> {
        let transport = self
            .core
            .state
            .borrow()
            .listeners
            .iter()
            .find_map(|(transport, id)| (*id == listener).then_some(*transport))
            .ok_or(HttpError::InvalidState("HTTP listener 不存在"))?;
        match transport {
            TransportId::Plain(id) => rskynet_net::info(ctx, id)
                .await?
                .local
                .ok_or_else(|| HttpError::Transport("监听口没有本地地址".into())),
            #[cfg(feature = "tls")]
            TransportId::Tls(id) => rskynet_tls::info(ctx, id)
                .await?
                .local
                .ok_or_else(|| HttpError::Transport("TLS 监听口没有本地地址".into())),
        }
    }

    /// 判断一条网络事件是否属于这个 HTTP 服务端。
    ///
    /// 判属只借用事件；返回 `true` 后可继续把事件按值交给 [`Self::on_socket`]。
    /// `Accept` 必须根据原监听口判断，因为新连接此时尚未登记。
    pub fn handles_socket(&self, event: &SocketEvent) -> bool {
        let state = self.core.state.borrow();
        match event {
            SocketEvent::Accept { id, listen, .. } => {
                state.listeners.contains_key(&TransportId::Plain(*listen))
                    || state.connections.contains_key(&TransportId::Plain(*id))
            }
            SocketEvent::Udp { .. } => false,
            event => {
                let transport = TransportId::Plain(event.id());
                state.listeners.contains_key(&transport)
                    || state.connections.contains_key(&transport)
            }
        }
    }

    pub async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) -> Result<Vec<ServerRequest>> {
        match event {
            SocketEvent::Accept { id, listen, peer } => {
                let listener = self
                    .core
                    .state
                    .borrow()
                    .listeners
                    .get(&TransportId::Plain(listen))
                    .copied();
                let Some(listener) = listener else {
                    return Ok(Vec::new());
                };
                let transport = TransportId::Plain(id);
                self.core
                    .insert_connection(ctx, transport, listener, peer, false)?;
                if let Err(error) = rskynet_net::start(ctx, id).await {
                    self.core.remove_connection(transport);
                    return Err(error.into());
                }
                Ok(Vec::new())
            }
            SocketEvent::Data { id, data } => {
                self.core.data(ctx, TransportId::Plain(id), data).await
            }
            SocketEvent::Close { id } => {
                self.core.closed(TransportId::Plain(id), None);
                Ok(Vec::new())
            }
            SocketEvent::Error { id, reason } => {
                self.core.closed(TransportId::Plain(id), Some(reason));
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    #[cfg(feature = "tls")]
    /// 判断一条 TLS 事件是否属于这个 HTTP 服务端。
    ///
    /// 判属只借用事件；返回 `true` 后可继续把事件按值交给 [`Self::on_tls`]。
    /// 入站 `Connected` 事件优先根据 TLS 监听口判断。
    pub fn handles_tls(&self, event: &TlsEvent) -> bool {
        let state = self.core.state.borrow();
        match event {
            TlsEvent::Connected { id, listen, .. } => {
                listen.is_some_and(|listen| state.listeners.contains_key(&TransportId::Tls(listen)))
                    || state.connections.contains_key(&TransportId::Tls(*id))
            }
            event => {
                let transport = TransportId::Tls(event.id());
                state.listeners.contains_key(&transport)
                    || state.connections.contains_key(&transport)
            }
        }
    }

    #[cfg(feature = "tls")]
    pub async fn on_tls(&self, ctx: &Ctx, event: TlsEvent) -> Result<Vec<ServerRequest>> {
        match event {
            TlsEvent::Connected {
                id,
                listen: Some(raw),
                peer,
                ..
            } => {
                let listener = self
                    .core
                    .state
                    .borrow()
                    .listeners
                    .get(&TransportId::Tls(raw))
                    .copied();
                let Some(listener) = listener else {
                    return Ok(Vec::new());
                };
                self.core
                    .insert_connection(ctx, TransportId::Tls(id), listener, peer, true)?;
                Ok(Vec::new())
            }
            TlsEvent::Data { id, data } => self.core.data(ctx, TransportId::Tls(id), data).await,
            TlsEvent::Close { id } => {
                self.core.closed(TransportId::Tls(id), None);
                Ok(Vec::new())
            }
            TlsEvent::Error { id, reason } => {
                self.core.closed(TransportId::Tls(id), Some(reason));
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rskynet_net::SocketId;

    #[test]
    fn handles_only_owned_plain_events() {
        let server = HttpServer::default();
        let listener = SocketId(7);
        server.insert_listener(TransportId::Plain(listener));
        let peer = "127.0.0.1:12345".parse().unwrap();

        assert!(server.handles_socket(&SocketEvent::Accept {
            id: SocketId(8),
            listen: listener,
            peer,
        }));
        assert!(server.handles_socket(&SocketEvent::Close { id: listener }));
        assert!(!server.handles_socket(&SocketEvent::Accept {
            id: SocketId(9),
            listen: SocketId(10),
            peer,
        }));
        assert!(!server.handles_socket(&SocketEvent::Data {
            id: SocketId(10),
            data: Vec::new(),
        }));
        assert!(!server.handles_socket(&SocketEvent::Udp {
            id: listener,
            from: peer,
            data: Vec::new(),
        }));
    }

    #[cfg(feature = "tls")]
    #[test]
    fn handles_only_owned_tls_events() {
        use rskynet_tls::TlsId;

        let server = HttpServer::default();
        let listener = TlsId(11);
        server.insert_listener(TransportId::Tls(listener));
        let peer = "127.0.0.1:12345".parse().unwrap();

        assert!(server.handles_tls(&TlsEvent::Connected {
            id: TlsId(12),
            listen: Some(listener),
            peer,
            version: None,
            cipher_suite: None,
            alpn: None,
        }));
        assert!(server.handles_tls(&TlsEvent::Close { id: listener }));
        assert!(!server.handles_tls(&TlsEvent::Connected {
            id: TlsId(13),
            listen: Some(TlsId(14)),
            peer,
            version: None,
            cipher_suite: None,
            alpn: None,
        }));
        assert!(!server.handles_tls(&TlsEvent::Data {
            id: TlsId(14),
            data: Vec::new(),
        }));
    }
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new(HttpServerConfig::default())
    }
}

pub struct ServerRequest {
    pub request: Request<IncomingBody>,
    pub peer: SocketAddr,
    pub tls: bool,
    pub listener: HttpListenerId,
    pub responder: ServerResponder,
}

#[cfg(feature = "websocket")]
impl ServerRequest {
    /// 验证 WebSocket Upgrade 请求、发送 101，并把连接切换到 WebSocket 协议。
    pub async fn upgrade_websocket(
        self,
        ctx: &Ctx,
        options: crate::websocket::WebSocketUpgradeOptions,
    ) -> Result<crate::websocket::WebSocket> {
        options.validate()?;
        let Self {
            request,
            mut responder,
            ..
        } = self;
        let request = request.map(|_| ());
        if let Some(protocol) = options.protocol.as_deref() {
            let offered = request
                .headers()
                .get_all("Sec-WebSocket-Protocol")
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(','))
                .map(str::trim)
                .any(|value| value == protocol);
            if !offered {
                return Err(HttpError::Protocol(
                    "选中的 WebSocket 子协议不在客户端候选列表中".into(),
                ));
            }
        }
        let mut response = tungstenite::handshake::server::create_response(&request)?;
        if let Some(protocol) = options.protocol.as_deref() {
            response.headers_mut().insert(
                "Sec-WebSocket-Protocol",
                protocol
                    .parse()
                    .map_err(|_| HttpError::Protocol("WebSocket 子协议无效".into()))?,
            );
        }
        let socket = responder
            .handle
            .upgrade_websocket(ctx, response, &options)
            .await?;
        responder.active = false;
        Ok(crate::websocket::WebSocket::new(socket))
    }
}

pub struct ServerResponder {
    handle: ServerBodyHandle,
    active: bool,
}

impl ServerResponder {
    pub async fn respond(
        mut self,
        ctx: &Ctx,
        response: Response<BodySpec>,
    ) -> Result<OutgoingBody> {
        let result = self.handle.respond(ctx, response).await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }
}

impl Drop for ServerResponder {
    fn drop(&mut self) {
        if self.active {
            self.handle.abort();
        }
    }
}

#[derive(Clone)]
struct AbortTransport {
    node: rskynet_core::NodeRef,
    source: rskynet_core::Handle,
    dest: rskynet_core::Handle,
    transport: TransportId,
}

impl AbortTransport {
    fn abort(&self) {
        let payload = match self.transport {
            TransportId::Plain(id) => rskynet_core::Payload::of(rskynet_net::Command::Shutdown(id)),
            #[cfg(feature = "tls")]
            TransportId::Tls(id) => rskynet_core::Payload::of(rskynet_tls::Command::Shutdown(id)),
        };
        let _ = self.node.send(
            self.source,
            self.dest,
            rskynet_core::MsgType::USER,
            0,
            payload,
        );
    }
}

#[derive(Clone)]
pub(crate) struct ServerBodyHandle {
    core: Arc<ServerCore>,
    transport: TransportId,
}

impl ServerBodyHandle {
    #[cfg(feature = "websocket")]
    async fn upgrade_websocket(
        &self,
        ctx: &Ctx,
        response: Response<()>,
        options: &crate::websocket::WebSocketUpgradeOptions,
    ) -> Result<Arc<crate::websocket::SharedSocket>> {
        let (transport, bytes, tail, websocket_id) = {
            let mut state = self.core.state.borrow_mut();
            state.next_websocket = state.next_websocket.wrapping_add(1).max(1);
            let websocket_id = crate::websocket::WebSocketId(state.next_websocket);
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            let proto = connection
                .proto
                .take()
                .ok_or(HttpError::InvalidState("服务端协议状态丢失"))?;
            let provide = match proto {
                ServerProto::Provide(value) => value,
                other => {
                    connection.proto = Some(other);
                    return Err(HttpError::InvalidState("WebSocket 请求体尚未结束"));
                }
            };
            let mut send = provide.provide(response)?;
            let mut bytes = Vec::new();
            while !send.is_finished() {
                let mut output = vec![
                    0;
                    self.core
                        .config
                        .max_header_size
                        .saturating_sub(bytes.len())
                        .max(1)
                ];
                let used = send.write(&mut output)?;
                if used == 0 {
                    return Err(HttpError::Protocol("序列化 101 响应没有进展".into()));
                }
                bytes.extend_from_slice(&output[..used]);
                if bytes.len() > self.core.config.max_header_size {
                    return Err(HttpError::BackpressureLimit {
                        actual: bytes.len(),
                        limit: self.core.config.max_header_size,
                    });
                }
            }
            if !matches!(send.proceed(), SendResponseResult::Cleanup(_)) {
                return Err(HttpError::Protocol("101 响应不应包含 body".into()));
            }
            let tail = std::mem::take(&mut connection.input);
            (connection.transport, bytes, tail, websocket_id)
        };
        let shared =
            crate::websocket::SharedSocket::server(ctx, websocket_id, transport, tail, options, {
                let core = Arc::downgrade(&self.core);
                Arc::new(move || {
                    if let Some(core) = core.upgrade() {
                        core.remove_connection(transport);
                    }
                })
            })?;
        {
            let mut state = self.core.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            connection.proto = Some(ServerProto::WebSocket(shared.clone()));
            connection.body_ended = true;
            connection.chunks.clear();
        }
        if let Err(error) = transport.send_wait(ctx, bytes).await {
            shared.abort();
            self.core.remove_connection(transport);
            return Err(error);
        }
        // `tail` 已由 `from_partially_read` 放入 tungstenite 的读缓冲。即使网络层不再
        // 产生 Data 事件，也要立即驱动一次，避免与请求头同包到达的首帧滞留。
        shared.on_data(ctx, Vec::new()).await?;
        Ok(shared)
    }

    pub(crate) fn abort_incoming(&self) {
        let should_abort = self
            .core
            .state
            .borrow()
            .connections
            .get(&self.transport)
            .is_some_and(|connection| {
                matches!(
                    connection.proto,
                    Some(ServerProto::Await100(_) | ServerProto::RecvBody(_))
                ) && !connection.body_ended
            });
        if should_abort {
            self.abort();
        }
    }

    pub(crate) fn abort(&self) {
        if let Some(connection) = self
            .core
            .state
            .borrow_mut()
            .connections
            .remove(&self.transport)
        {
            connection.abort.abort();
        }
    }

    pub(crate) async fn next_chunk(&self, ctx: &Ctx) -> Result<Option<Vec<u8>>> {
        self.ensure_continue(ctx).await?;
        let started = ctx.now();
        loop {
            let (answer, resume, remove) = {
                let mut state = self.core.state.borrow_mut();
                let connection = state
                    .connections
                    .get_mut(&self.transport)
                    .ok_or(HttpError::BodyClosed)?;
                let (answer, remove) = if let Some(chunk) = connection.chunks.pop_front() {
                    (Some(Ok(Some(chunk))), false)
                } else if let Some(reason) = connection.error.clone() {
                    (Some(Err(HttpError::Transport(reason))), true)
                } else if connection.body_ended {
                    (Some(Ok(None)), false)
                } else {
                    (None, false)
                };
                let buffered =
                    connection.input.len() + connection.chunks.iter().map(Vec::len).sum::<usize>();
                let resume = connection.paused && buffered <= self.core.config.input_low_water;
                if resume {
                    connection.paused = false;
                }
                (answer, resume, remove)
            };
            if resume {
                self.transport.resume(ctx).await?;
            }
            if remove {
                self.core.remove_connection(self.transport);
            }
            if let Some(answer) = answer {
                return answer;
            }
            if ctx.now().saturating_sub(started) >= self.core.config.body_idle_timeout_ms {
                self.transport.shutdown(ctx);
                self.core.remove_connection(self.transport);
                return Err(HttpError::Timeout("server body idle"));
            }
            ctx.sleep_ms(10).await;
        }
    }

    async fn ensure_continue(&self, ctx: &Ctx) -> Result<()> {
        let output = {
            let mut state = self.core.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            let proto = connection.proto.take();
            let call = match proto {
                Some(ServerProto::Await100(call)) => call,
                other => {
                    connection.proto = other;
                    return Ok(());
                }
            };
            let mut output = vec![0; 128];
            let (used, recv) = call.accept(&mut output)?;
            output.truncate(used);
            connection.proto = Some(ServerProto::RecvBody(recv));
            output
        };
        self.transport.send_wait(ctx, output).await?;
        let _ = self.core.process(self.transport)?;
        Ok(())
    }

    async fn respond(&self, ctx: &Ctx, response: Response<BodySpec>) -> Result<OutgoingBody> {
        let (mut parts, spec) = response.into_parts();
        super::client::apply_body_headers(&mut parts.headers, spec)?;
        let expected = match spec {
            BodySpec::Fixed(size) => Some(size),
            BodySpec::Empty => Some(0),
            BodySpec::Chunked => None,
        };
        let (transport, bytes, has_body, close) = {
            let mut state = self.core.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            let proto = connection
                .proto
                .take()
                .ok_or(HttpError::InvalidState("服务端协议状态丢失"))?;
            let provide = match proto {
                ServerProto::Await100(v) => v.reject(),
                ServerProto::Provide(v) => v,
                other => {
                    connection.proto = Some(other);
                    return Err(HttpError::InvalidState("请求体尚未消费完"));
                }
            };
            let mut send = provide.provide(Response::from_parts(parts, ()))?;
            let mut bytes = Vec::new();
            while !send.is_finished() {
                let mut out = vec![
                    0;
                    self.core
                        .config
                        .max_header_size
                        .saturating_sub(bytes.len())
                        .max(1)
                ];
                let used = send.write(&mut out)?;
                if used == 0 {
                    return Err(HttpError::Protocol("序列化响应头没有进展".into()));
                }
                bytes.extend_from_slice(&out[..used]);
                if bytes.len() > self.core.config.max_header_size {
                    return Err(HttpError::BackpressureLimit {
                        actual: bytes.len(),
                        limit: self.core.config.max_header_size,
                    });
                }
            }
            match send.proceed() {
                SendResponseResult::SendBody(v) => {
                    connection.response_expected = expected;
                    connection.response_written = 0;
                    connection.proto = Some(ServerProto::SendBody(v));
                    (connection.transport, bytes, true, false)
                }
                SendResponseResult::Cleanup(v) => {
                    let close = v.must_close_connection();
                    connection.proto = Some(ServerProto::Responding);
                    (connection.transport, bytes, false, close)
                }
            }
        };
        transport.send_wait(ctx, bytes).await?;
        if has_body {
            Ok(OutgoingBody::server(self.clone()))
        } else {
            self.core.finish_response(ctx, transport, close);
            Ok(OutgoingBody::finished())
        }
    }

    pub(crate) async fn write_chunk(&self, ctx: &Ctx, chunk: Vec<u8>) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        if chunk.len() > self.core.config.max_chunk_size {
            return Err(HttpError::BackpressureLimit {
                actual: chunk.len(),
                limit: self.core.config.max_chunk_size,
            });
        }
        let encoded = {
            let mut state = self.core.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            let Some(ServerProto::SendBody(mut call)) = connection.proto.take() else {
                return Err(HttpError::BodyClosed);
            };
            let mut out = vec![0; chunk.len().saturating_add(32).max(64)];
            let (used, written) = call.write(&chunk, &mut out)?;
            if used != chunk.len() {
                connection.proto = Some(ServerProto::SendBody(call));
                return Err(HttpError::Protocol("响应体没有完整编码".into()));
            }
            connection.response_written = connection.response_written.saturating_add(used as u64);
            out.truncate(written);
            connection.proto = Some(ServerProto::SendBody(call));
            out
        };
        self.transport.send_wait(ctx, encoded).await
    }

    pub(crate) async fn finish(&self, ctx: &Ctx) -> Result<()> {
        let (encoded, close) = {
            let mut state = self.core.state.borrow_mut();
            let connection = state
                .connections
                .get_mut(&self.transport)
                .ok_or(HttpError::BodyClosed)?;
            let Some(ServerProto::SendBody(mut call)) = connection.proto.take() else {
                return Err(HttpError::BodyClosed);
            };
            if let Some(expected) = connection.response_expected {
                if connection.response_written != expected {
                    connection.proto = Some(ServerProto::SendBody(call));
                    return Err(HttpError::BodyLengthMismatch {
                        expected,
                        actual: connection.response_written,
                    });
                }
            }
            let mut out = vec![0; 64];
            let (_, written) = call.write(&[], &mut out)?;
            if !call.is_finished() {
                connection.proto = Some(ServerProto::SendBody(call));
                return Err(HttpError::Protocol("响应体尚未完成".into()));
            }
            out.truncate(written);
            let cleanup = call.proceed();
            let close = cleanup.must_close_connection();
            connection.proto = Some(ServerProto::Responding);
            (out, close)
        };
        self.transport.send_wait(ctx, encoded).await?;
        self.core.finish_response(ctx, self.transport, close);
        Ok(())
    }
}

impl ServerCore {
    fn insert_connection(
        self: &Arc<Self>,
        ctx: &Ctx,
        transport: TransportId,
        listener: HttpListenerId,
        peer: SocketAddr,
        tls: bool,
    ) -> Result<()> {
        let proto = Reply::new().map_err(HttpError::from)?;
        let dest = match transport {
            TransportId::Plain(_) => ctx.query_name(rskynet_net::NAME),
            #[cfg(feature = "tls")]
            TransportId::Tls(_) => ctx.query_name(rskynet_tls::NAME),
        }
        .ok_or(HttpError::InvalidState("传输服务未启动"))?;
        let abort = AbortTransport {
            node: ctx.node(),
            source: ctx.handle(),
            dest,
            transport,
        };
        self.state.borrow_mut().connections.insert(
            transport,
            Connection {
                transport,
                listener,
                peer,
                tls,
                proto: Some(ServerProto::RecvRequest(proto)),
                input: Vec::new(),
                chunks: VecDeque::new(),
                body_ended: false,
                error: None,
                paused: false,
                response_expected: None,
                response_written: 0,
                abort,
                header_generation: 1,
            },
        );
        self.arm_header_timeout(ctx, transport, 1);
        Ok(())
    }

    fn remove_connection(&self, transport: TransportId) {
        self.state.borrow_mut().connections.remove(&transport);
    }

    async fn data(
        self: &Arc<Self>,
        ctx: &Ctx,
        transport: TransportId,
        data: Vec<u8>,
    ) -> Result<Vec<ServerRequest>> {
        #[cfg(feature = "websocket")]
        {
            let websocket =
                self.state
                    .borrow()
                    .connections
                    .get(&transport)
                    .and_then(|connection| match connection.proto.as_ref() {
                        Some(ServerProto::WebSocket(shared)) => Some(shared.clone()),
                        _ => None,
                    });
            if let Some(websocket) = websocket {
                websocket.on_data(ctx, data).await?;
                return Ok(Vec::new());
            }
        }
        {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&transport) else {
                return Ok(Vec::new());
            };
            connection.input.extend_from_slice(&data);
        }
        let requests = match self.process(transport) {
            Ok(requests) => requests,
            Err(error) => {
                transport.shutdown(ctx);
                self.remove_connection(transport);
                return Err(error);
            }
        };
        let pause = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&transport) else {
                return Ok(requests);
            };
            let buffered =
                connection.input.len() + connection.chunks.iter().map(Vec::len).sum::<usize>();
            if !connection.body_ended
                && buffered >= self.config.input_high_water
                && !connection.paused
            {
                connection.paused = true;
                true
            } else {
                false
            }
        };
        if pause {
            transport.pause(ctx).await?;
        }
        Ok(requests)
    }

    fn process(self: &Arc<Self>, transport: TransportId) -> Result<Vec<ServerRequest>> {
        let mut result = Vec::new();
        let mut state = self.state.borrow_mut();
        let Some(connection) = state.connections.get_mut(&transport) else {
            return Ok(result);
        };
        loop {
            let Some(proto) = connection.proto.take() else {
                break;
            };
            match proto {
                ServerProto::RecvRequest(mut call) => match call.try_request(&connection.input)? {
                    (used, Some(request)) => {
                        connection.input.drain(..used);
                        let next = call.proceed().expect("request parsed");
                        connection.body_ended = false;
                        connection.proto = Some(match next {
                            RecvRequestResult::Send100(v) => ServerProto::Await100(v),
                            RecvRequestResult::RecvBody(v) => ServerProto::RecvBody(v),
                            RecvRequestResult::ProvideResponse(v) => {
                                connection.body_ended = true;
                                ServerProto::Provide(v)
                            }
                        });
                        let handle = ServerBodyHandle {
                            core: self.clone(),
                            transport,
                        };
                        let (parts, _) = request.into_parts();
                        result.push(ServerRequest {
                            request: Request::from_parts(
                                parts,
                                IncomingBody::server(handle.clone()),
                            ),
                            peer: connection.peer,
                            tls: connection.tls,
                            listener: connection.listener,
                            responder: ServerResponder {
                                handle,
                                active: true,
                            },
                        });
                        continue;
                    }
                    (_, None) => {
                        if connection.input.len() > self.config.max_header_size {
                            return Err(HttpError::BackpressureLimit {
                                actual: connection.input.len(),
                                limit: self.config.max_header_size,
                            });
                        }
                        connection.proto = Some(ServerProto::RecvRequest(call));
                        break;
                    }
                },
                ServerProto::RecvBody(mut call) => {
                    if connection.input.is_empty() {
                        connection.proto = Some(ServerProto::RecvBody(call));
                        break;
                    }
                    let mut out = vec![0; self.config.max_chunk_size];
                    let (used, written) = call.read(&connection.input, &mut out)?;
                    if used > 0 {
                        connection.input.drain(..used);
                    }
                    if written > 0 {
                        out.truncate(written);
                        connection.chunks.push_back(out);
                    }
                    if call.is_ended() {
                        connection.proto = Some(ServerProto::Provide(call.proceed()?));
                        connection.body_ended = true;
                    } else {
                        connection.proto = Some(ServerProto::RecvBody(call));
                    }
                    if used == 0 && written == 0 {
                        break;
                    }
                    continue;
                }
                other => {
                    connection.proto = Some(other);
                    break;
                }
            }
        }
        Ok(result)
    }

    fn closed(&self, transport: TransportId, error: Option<String>) {
        if self.state.borrow().listeners.contains_key(&transport) {
            self.state.borrow_mut().listeners.remove(&transport);
            return;
        }
        #[cfg(feature = "websocket")]
        {
            let websocket =
                self.state
                    .borrow()
                    .connections
                    .get(&transport)
                    .and_then(|connection| match connection.proto.as_ref() {
                        Some(ServerProto::WebSocket(shared)) => Some(shared.clone()),
                        _ => None,
                    });
            if let Some(websocket) = websocket {
                self.state.borrow_mut().connections.remove(&transport);
                websocket.on_closed(error);
                return;
            }
        }
        let idle = self
            .state
            .borrow()
            .connections
            .get(&transport)
            .is_some_and(|connection| {
                matches!(connection.proto, Some(ServerProto::RecvRequest(_)))
            });
        if idle {
            self.remove_connection(transport);
        } else if let Some(connection) = self.state.borrow_mut().connections.get_mut(&transport) {
            connection.error = Some(error.unwrap_or_else(|| "连接已关闭".into()));
            connection.body_ended = true;
        }
    }

    fn finish_response(self: &Arc<Self>, ctx: &Ctx, transport: TransportId, close: bool) {
        if close {
            transport.shutdown(ctx);
            self.remove_connection(transport);
            return;
        }
        let Ok(next) = Reply::new() else {
            transport.shutdown(ctx);
            self.remove_connection(transport);
            return;
        };
        let (generation, buffered) = {
            let mut state = self.state.borrow_mut();
            let Some(connection) = state.connections.get_mut(&transport) else {
                return;
            };
            connection.proto = Some(ServerProto::RecvRequest(next));
            connection.body_ended = false;
            connection.chunks.clear();
            connection.error = None;
            connection.response_expected = None;
            connection.response_written = 0;
            connection.header_generation = connection.header_generation.wrapping_add(1).max(1);
            (connection.header_generation, !connection.input.is_empty())
        };
        self.arm_header_timeout(ctx, transport, generation);
        if buffered {
            // HTTP/1.1 pipelining 可能把下一条请求留在当前 input 中。投一条空数据
            // 事件让嵌入本 HttpServer 的业务服务重新进入公开事件入口并取走请求。
            let event = match transport {
                TransportId::Plain(id) => (
                    MsgType::SOCKET,
                    Payload::of(SocketEvent::Data {
                        id,
                        data: Vec::new(),
                    }),
                ),
                #[cfg(feature = "tls")]
                TransportId::Tls(id) => (
                    MsgType::TLS,
                    Payload::of(TlsEvent::Data {
                        id,
                        data: Vec::new(),
                    }),
                ),
            };
            let _ = ctx.send(ctx.handle(), event.0, event.1);
        }
    }

    fn arm_header_timeout(self: &Arc<Self>, ctx: &Ctx, transport: TransportId, generation: u64) {
        let core = self.clone();
        let wake = ctx.clone();
        let timeout = self.config.header_timeout_ms;
        ctx.spawn(async move {
            wake.sleep_ms(timeout).await;
            let waiting =
                core.state
                    .borrow()
                    .connections
                    .get(&transport)
                    .is_some_and(|connection| {
                        connection.header_generation == generation
                            && matches!(
                                connection.proto.as_ref(),
                                Some(ServerProto::RecvRequest(_))
                            )
                    });
            if waiting {
                transport.shutdown(&wake);
                core.remove_connection(transport);
            }
        });
    }
}
