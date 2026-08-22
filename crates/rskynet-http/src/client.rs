use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use rskynet_core::{BoxFuture, Ctx, Message, MsgType, Payload, Service, SvcCell, boxed_payload};
use rskynet_net::SocketEvent;
use ureq_proto::BodyMode;
use ureq_proto::client::state::{Await100, RecvBody, RecvResponse, SendBody, SendRequest};
use ureq_proto::client::{
    Await100Result, Call, RecvBodyResult, RecvResponseResult, SendRequestResult,
};
use ureq_proto::http::{Request, Response, Uri, Version, header};

#[cfg(feature = "tls")]
use rskynet_tls::{ClientOptions, ClientTlsConfig, ServerName, ServerVerification, TlsEvent};

use crate::body::{BodySpec, IncomingBody};
use crate::config::HttpClientConfig;
use crate::transport::TransportId;
use crate::{HttpError, Result};

pub const NAME: &str = "http-client";
static NEXT_START_TOKEN: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
enum Command {
    Start {
        token: u64,
        alive: Weak<()>,
        request: Request<BodySpec>,
    },
    Write {
        id: u64,
        chunk: Vec<u8>,
    },
    Finish(u64),
    Response(u64),
    Next(u64),
    Cancel(u64),
    CancelStart(u64),
    ConfirmStart(u64),
    ContinueTimeout(u64),
    WaitTimeout {
        id: u64,
        session: u64,
        body: bool,
    },
    IdleTimeout {
        transport: TransportId,
        generation: u64,
    },
    ResumeStart {
        token: u64,
        alive: Weak<()>,
        pending: Pending,
        request: Request<BodySpec>,
    },
}
boxed_payload!(Command);

#[derive(Debug)]
enum Answer {
    Id(u64),
    Done,
    Response(Response<IncomingBody>),
    Chunk(Option<Vec<u8>>),
    Failed(HttpError),
}
boxed_payload!(Answer);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Origin {
    scheme: &'static str,
    host: String,
    port: u16,
}

impl Origin {
    fn from_uri(uri: &Uri) -> Result<Self> {
        let scheme = match uri.scheme_str() {
            Some("http") => "http",
            Some("https") => {
                #[cfg(feature = "tls")]
                {
                    "https"
                }
                #[cfg(not(feature = "tls"))]
                {
                    return Err(HttpError::UnsupportedScheme(
                        "https（未启用 tls feature）".into(),
                    ));
                }
            }
            Some(other) => return Err(HttpError::UnsupportedScheme(other.into())),
            None => return Err(HttpError::InvalidUri("缺少 scheme".into())),
        };
        let host = uri
            .host()
            .ok_or_else(|| HttpError::InvalidUri("缺少 host".into()))?;
        let port = uri
            .port_u16()
            .unwrap_or(if scheme == "https" { 443 } else { 80 });
        Ok(Self {
            scheme,
            host: host.to_ascii_lowercase(),
            port,
        })
    }

    fn address(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    source: rskynet_core::Handle,
    session: u64,
}

enum Proto {
    SendBody(Call<SendBody>),
    Await100(Call<Await100>),
    RecvResponse(Call<RecvResponse>),
    RecvBody(Call<RecvBody>),
    Done,
}

struct Exchange {
    transport: TransportId,
    origin: Origin,
    proto: Option<Proto>,
    input: Vec<u8>,
    response: Option<Response<()>>,
    response_taken: bool,
    response_waiter: Option<Pending>,
    /// `Expect: 100-continue` 阶段挂起的唯一一次 body 写入。
    write_waiter: Option<(Pending, Vec<u8>)>,
    chunks: VecDeque<Vec<u8>>,
    body_waiter: Option<Pending>,
    body_ended: bool,
    error: Option<String>,
    paused: bool,
    request_finished: bool,
    request_expected: Option<u64>,
    request_written: u64,
    reusable: Option<bool>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    exchanges: HashMap<u64, Exchange>,
    active: HashMap<TransportId, u64>,
    origins: HashMap<TransportId, Origin>,
    idle: HashMap<Origin, VecDeque<TransportId>>,
    pending_starts: VecDeque<(u64, Weak<()>, Pending, Request<BodySpec>)>,
    start_tokens: HashMap<u64, u64>,
    connecting: HashMap<Origin, usize>,
    idle_generation: HashMap<TransportId, u64>,
    next_idle_generation: u64,
}

pub struct HttpClientService {
    state: SvcCell<State>,
    config: SvcCell<HttpClientConfig>,
    #[cfg(feature = "tls")]
    tls: ClientTlsConfig,
}

impl HttpClientService {
    pub fn new() -> Self {
        Self {
            state: SvcCell::new(State::default()),
            config: SvcCell::new(HttpClientConfig::default()),
            #[cfg(feature = "tls")]
            tls: ClientTlsConfig::new(ServerVerification::default(), vec![b"http/1.1".to_vec()])
                .expect("默认 TLS 配置应有效"),
        }
    }

    #[cfg(feature = "tls")]
    pub fn with_tls_config(config: ClientTlsConfig) -> Self {
        Self {
            tls: config,
            ..Self::new()
        }
    }
}

impl Default for HttpClientService {
    fn default() -> Self {
        Self::new()
    }
}

impl Service for HttpClientService {
    fn init(
        self: Arc<Self>,
        ctx: Ctx,
        _args: String,
    ) -> BoxFuture<'static, rskynet_core::Result<()>> {
        Box::pin(async move {
            let config: HttpClientConfig = ctx.node().section(NAME)?.unwrap_or_default();
            config
                .validate()
                .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
            self.config.replace(config);
            if !ctx.register_name(NAME) {
                return Err(rskynet_core::Error::Service(format!(
                    "名字 `.{NAME}` 已被占用"
                )));
            }
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if msg.mtype == MsgType::SOCKET {
                if let Ok(event) = msg.take_payload().downcast::<SocketEvent>() {
                    self.on_socket(&ctx, *event).await;
                }
                return;
            }
            #[cfg(feature = "tls")]
            if msg.mtype == MsgType::TLS {
                if let Ok(event) = msg.take_payload().downcast::<TlsEvent>() {
                    self.on_tls(&ctx, *event).await;
                }
                return;
            }
            let pending = Pending {
                source: msg.source,
                session: msg.session,
            };
            let Ok(command) = msg.take_payload().downcast::<Command>() else {
                let _ = ctx.reply_error(&msg);
                return;
            };
            self.on_command(&ctx, pending, *command).await;
        })
    }
}

impl HttpClientService {
    async fn on_command(&self, ctx: &Ctx, pending: Pending, command: Command) {
        match command {
            Command::Start {
                token,
                alive,
                request,
            } => {
                self.start_exchange(ctx, token, alive, pending, request)
                    .await
            }
            Command::Write { id, chunk } => {
                let Some(chunk) = self.defer_continue_write(ctx, pending, id, chunk).await else {
                    return;
                };
                let answer = self.write_request(ctx, id, chunk).await;
                reply(ctx, pending, answer);
            }
            Command::Finish(id) => {
                let answer = self.finish_request(ctx, id).await;
                reply(ctx, pending, answer);
            }
            Command::Response(id) => self.wait_response(ctx, pending, id),
            Command::Next(id) => self.next_body(ctx, pending, id).await,
            Command::Cancel(id) => {
                self.cancel(ctx, id, "exchange 已取消");
                reply(ctx, pending, Answer::Done);
            }
            Command::CancelStart(token) => self.cancel_start(ctx, token),
            Command::ConfirmStart(token) => {
                self.state.borrow_mut().start_tokens.remove(&token);
            }
            Command::ContinueTimeout(id) => self.continue_timeout(ctx, id).await,
            Command::WaitTimeout { id, session, body } => self.wait_timeout(ctx, id, session, body),
            Command::IdleTimeout {
                transport,
                generation,
            } => self.idle_timeout(ctx, transport, generation),
            Command::ResumeStart {
                token,
                alive,
                pending,
                request,
            } => {
                self.start_exchange(ctx, token, alive, pending, request)
                    .await
            }
        }
    }

    async fn start_exchange(
        &self,
        ctx: &Ctx,
        token: u64,
        alive: Weak<()>,
        pending: Pending,
        request: Request<BodySpec>,
    ) {
        if alive.upgrade().is_none() {
            self.wake_queued(ctx);
            return;
        }
        let origin_hint = match Origin::from_uri(request.uri()) {
            Ok(value) => value,
            Err(error) => {
                reply(ctx, pending, Answer::Failed(error));
                self.wake_queued(ctx);
                return;
            }
        };
        if self.should_queue(&origin_hint) {
            self.state
                .borrow_mut()
                .pending_starts
                .push_back((token, alive, pending, request));
            return;
        }
        let (mut parts, spec) = request.into_parts();
        parts.version = Version::HTTP_11;
        if let Err(error) = apply_body_headers(&mut parts.headers, spec) {
            reply(ctx, pending, Answer::Failed(error));
            self.wake_queued(ctx);
            return;
        }
        let origin = match Origin::from_uri(&parts.uri) {
            Ok(v) => v,
            Err(e) => {
                reply(ctx, pending, Answer::Failed(e));
                self.wake_queued(ctx);
                return;
            }
        };
        let request = Request::from_parts(parts, ());
        let call = match Call::new(request) {
            Ok(v) => v,
            Err(e) => {
                reply(ctx, pending, Answer::Failed(e.into()));
                self.wake_queued(ctx);
                return;
            }
        };
        let transport = match self.acquire(ctx, &origin).await {
            Ok(v) => v,
            Err(e) => {
                reply(ctx, pending, Answer::Failed(e));
                self.wake_queued(ctx);
                return;
            }
        };
        if alive.upgrade().is_none() {
            transport.shutdown(ctx);
            self.forget_transport(transport);
            self.wake_queued(ctx);
            return;
        }
        let send = match serialize_request(call.proceed(), self.config.borrow().max_header_size) {
            Ok(v) => v,
            Err(e) => {
                transport.shutdown(ctx);
                self.forget_transport(transport);
                reply(ctx, pending, Answer::Failed(e));
                self.wake_queued(ctx);
                return;
            }
        };
        let id = {
            let mut state = self.state.borrow_mut();
            state.next_id = state.next_id.wrapping_add(1).max(1);
            let id = state.next_id;
            state.active.insert(transport, id);
            state.start_tokens.insert(token, id);
            state.exchanges.insert(
                id,
                Exchange {
                    transport,
                    origin,
                    proto: Some(send.1),
                    input: Vec::new(),
                    response: None,
                    response_taken: false,
                    response_waiter: None,
                    write_waiter: None,
                    chunks: VecDeque::new(),
                    body_waiter: None,
                    body_ended: false,
                    error: None,
                    paused: false,
                    request_finished: matches!(spec, BodySpec::Empty),
                    request_expected: match spec {
                        BodySpec::Fixed(size) => Some(size),
                        BodySpec::Empty => Some(0),
                        BodySpec::Chunked => None,
                    },
                    request_written: 0,
                    reusable: None,
                },
            );
            id
        };
        if let Err(error) = transport.send_wait(ctx, send.0).await {
            self.cancel(ctx, id, &error.to_string());
            reply(ctx, pending, Answer::Failed(error));
            return;
        }
        if alive.upgrade().is_none() {
            self.cancel(ctx, id, "exchange 启动已取消");
            return;
        }
        if matches!(
            self.state
                .borrow()
                .exchanges
                .get(&id)
                .and_then(|e| e.proto.as_ref()),
            Some(Proto::Await100(_))
        ) {
            let wake = ctx.clone();
            let timeout = self.config.borrow().continue_timeout_ms;
            ctx.spawn(async move {
                wake.sleep_ms(timeout).await;
                let _ = wake.send(
                    NAME,
                    MsgType::USER,
                    Payload::of(Command::ContinueTimeout(id)),
                );
            });
        }
        reply(ctx, pending, Answer::Id(id));
    }

    fn cancel_start(&self, ctx: &Ctx, token: u64) {
        let id = {
            let mut state = self.state.borrow_mut();
            state
                .pending_starts
                .retain(|(queued, _, _, _)| *queued != token);
            state.start_tokens.remove(&token)
        };
        if let Some(id) = id {
            self.cancel(ctx, id, "exchange 启动已取消");
        } else {
            self.wake_queued(ctx);
        }
    }

    async fn acquire(&self, ctx: &Ctx, origin: &Origin) -> Result<TransportId> {
        let idle = {
            let mut state = self.state.borrow_mut();
            let id = state.idle.get_mut(origin).and_then(VecDeque::pop_front);
            if let Some(id) = id {
                state.idle_generation.remove(&id);
            }
            id
        };
        if let Some(id) = idle {
            return Ok(id);
        }
        {
            let mut state = self.state.borrow_mut();
            *state.connecting.entry(origin.clone()).or_default() += 1;
        }
        let connect_timeout_ms = self.config.borrow().connect_timeout_ms;
        let connected: Result<TransportId> = if origin.scheme == "http" {
            rskynet_net::connect_timeout(ctx, origin.address(), connect_timeout_ms)
                .await
                .map(TransportId::Plain)
                .map_err(Into::into)
        } else {
            #[cfg(feature = "tls")]
            {
                match ServerName::try_from(origin.host.clone()) {
                    Ok(name) => rskynet_tls::connect(
                        ctx,
                        ClientOptions::new(origin.address(), name, self.tls.clone())
                            .with_connect_timeout_ms(connect_timeout_ms)
                            .with_handshake_timeout_ms(connect_timeout_ms),
                    )
                    .await
                    .map(TransportId::Tls)
                    .map_err(Into::into),
                    Err(error) => Err(HttpError::Tls(error.to_string())),
                }
            }
            #[cfg(not(feature = "tls"))]
            {
                Err(HttpError::UnsupportedScheme("https".into()))
            }
        };
        {
            let mut state = self.state.borrow_mut();
            if let Some(count) = state.connecting.get_mut(origin) {
                *count -= 1;
                if *count == 0 {
                    state.connecting.remove(origin);
                }
            }
        }
        let id = connected?;
        self.state.borrow_mut().origins.insert(id, origin.clone());
        Ok(id)
    }

    async fn write_request(&self, ctx: &Ctx, id: u64, chunk: Vec<u8>) -> Answer {
        if chunk.is_empty() {
            return Answer::Done;
        }
        let limit = self.config.borrow().max_chunk_size;
        if chunk.len() > limit {
            return Answer::Failed(HttpError::BackpressureLimit {
                actual: chunk.len(),
                limit,
            });
        }
        let (transport, encoded) = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return Answer::Failed(HttpError::BodyClosed);
            };
            if exchange.request_finished {
                return Answer::Failed(HttpError::BodyClosed);
            }
            let proto = exchange.proto.take();
            let mut call = match proto {
                Some(Proto::SendBody(call)) => call,
                other => {
                    exchange.proto = other;
                    return Answer::Failed(if exchange.response.is_some() {
                        HttpError::RequestBodyRejected
                    } else {
                        HttpError::InvalidState("请求体尚不可写")
                    });
                }
            };
            let mut output = vec![0; chunk.len().saturating_add(32).max(64)];
            let result = call.write(&chunk, &mut output);
            match result {
                Ok((used, written)) if used == chunk.len() => {
                    output.truncate(written);
                    exchange.proto = Some(Proto::SendBody(call));
                    exchange.request_written = exchange.request_written.saturating_add(used as u64);
                    (exchange.transport, Ok(output))
                }
                Ok(_) => {
                    exchange.proto = Some(Proto::SendBody(call));
                    (
                        exchange.transport,
                        Err(HttpError::Protocol("请求体没有完整编码".into())),
                    )
                }
                Err(e) => {
                    exchange.proto = Some(Proto::SendBody(call));
                    (exchange.transport, Err(e.into()))
                }
            }
        };
        match encoded {
            Ok(data) => match transport.send_wait(ctx, data).await {
                Ok(()) => Answer::Done,
                Err(e) => Answer::Failed(e),
            },
            Err(e) => Answer::Failed(e),
        }
    }

    /// `100 Continue` 尚未决定时保留调用 session；协议进入可写或拒绝状态后再应答。
    async fn defer_continue_write(
        &self,
        ctx: &Ctx,
        pending: Pending,
        id: u64,
        chunk: Vec<u8>,
    ) -> Option<Vec<u8>> {
        if chunk.is_empty() {
            return Some(chunk);
        }
        let (answer, chunk) = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return Some(chunk);
            };
            if !matches!(exchange.proto, Some(Proto::Await100(_))) {
                return Some(chunk);
            }
            if exchange.write_waiter.is_some() {
                (
                    Some(Answer::Failed(HttpError::InvalidState(
                        "已有请求体写入在等待 100 Continue",
                    ))),
                    None,
                )
            } else {
                exchange.write_waiter = Some((pending, chunk));
                (None, None)
            }
        };
        if let Some(answer) = answer {
            reply(ctx, pending, answer);
        }
        chunk
    }

    async fn finish_request(&self, ctx: &Ctx, id: u64) -> Answer {
        let (transport, encoded) = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return Answer::Failed(HttpError::BodyClosed);
            };
            if exchange.request_finished {
                return Answer::Done;
            }
            if let Some(expected) = exchange.request_expected {
                if exchange.request_written != expected {
                    return Answer::Failed(HttpError::BodyLengthMismatch {
                        expected,
                        actual: exchange.request_written,
                    });
                }
            }
            let Some(proto) = exchange.proto.take() else {
                return Answer::Failed(HttpError::InvalidState("请求状态丢失"));
            };
            match proto {
                Proto::SendBody(mut call) => {
                    let mut output = vec![0; 64];
                    match call.write(&[], &mut output) {
                        Ok((_, written)) if call.can_proceed() => {
                            output.truncate(written);
                            exchange.proto =
                                Some(Proto::RecvResponse(call.proceed().expect("can_proceed")));
                            exchange.request_finished = true;
                            (exchange.transport, Ok(output))
                        }
                        Ok(_) => {
                            exchange.proto = Some(Proto::SendBody(call));
                            (
                                exchange.transport,
                                Err(HttpError::Protocol("请求体尚未完成".into())),
                            )
                        }
                        Err(e) => {
                            exchange.proto = Some(Proto::SendBody(call));
                            (exchange.transport, Err(e.into()))
                        }
                    }
                }
                Proto::Await100(call) => {
                    exchange.proto = Some(Proto::Await100(call));
                    (
                        exchange.transport,
                        Err(HttpError::InvalidState("仍在等待 100 Continue")),
                    )
                }
                other => {
                    exchange.proto = Some(other);
                    exchange.request_finished = true;
                    return Answer::Done;
                }
            }
        };
        match encoded {
            Ok(data) => match transport.send_wait(ctx, data).await {
                Ok(()) => Answer::Done,
                Err(e) => Answer::Failed(e),
            },
            Err(e) => Answer::Failed(e),
        }
    }

    fn wait_response(&self, ctx: &Ctx, pending: Pending, id: u64) {
        let answer = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                reply(ctx, pending, Answer::Failed(HttpError::BodyClosed));
                return;
            };
            if exchange.response_taken {
                Some(Answer::Failed(HttpError::InvalidState("响应已取得")))
            } else if let Some(reason) = exchange.error.clone() {
                Some(Answer::Failed(HttpError::Transport(reason)))
            } else if let Some(response) = exchange.response.take() {
                exchange.response_taken = true;
                let handle = ClientBodyHandle::new(id, ctx, pending.source);
                Some(Answer::Response(
                    response.map(|_| IncomingBody::client(handle)),
                ))
            } else if exchange.response_waiter.is_some() {
                Some(Answer::Failed(HttpError::InvalidState(
                    "已有 response 调用在等待",
                )))
            } else {
                exchange.response_waiter = Some(pending);
                None
            }
        };
        if let Some(answer) = answer {
            reply(ctx, pending, answer);
        } else {
            self.arm_wait_timeout(
                ctx,
                id,
                pending.session,
                false,
                self.config.borrow().response_head_timeout_ms,
            );
        }
    }

    async fn next_body(&self, ctx: &Ctx, pending: Pending, id: u64) {
        let (answer, resume, cleanup) = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                reply(ctx, pending, Answer::Failed(HttpError::BodyClosed));
                return;
            };
            let answer = if let Some(chunk) = exchange.chunks.pop_front() {
                Some(Answer::Chunk(Some(chunk)))
            } else if let Some(reason) = exchange.error.clone() {
                Some(Answer::Failed(HttpError::Transport(reason)))
            } else if exchange.body_ended {
                Some(Answer::Chunk(None))
            } else if exchange.body_waiter.is_some() {
                Some(Answer::Failed(HttpError::InvalidState(
                    "已有 next_chunk 调用在等待",
                )))
            } else {
                exchange.body_waiter = Some(pending);
                None
            };
            let buffered =
                exchange.input.len() + exchange.chunks.iter().map(Vec::len).sum::<usize>();
            let resume = exchange.paused && buffered <= self.config.borrow().input_low_water;
            if resume {
                exchange.paused = false;
            }
            let cleanup = if matches!(answer, Some(Answer::Chunk(None))) {
                exchange
                    .reusable
                    .take()
                    .map(|reusable| (exchange.transport, exchange.origin.clone(), reusable))
            } else {
                None
            };
            (answer, resume.then_some(exchange.transport), cleanup)
        };
        if let Some(transport) = resume {
            if let Err(e) = transport.resume(ctx).await {
                self.cancel(ctx, id, &e.to_string());
            }
        }
        if let Some(answer) = answer {
            reply(ctx, pending, answer);
        }
        if let Some((transport, origin, reusable)) = cleanup {
            self.recycle(ctx, id, transport, origin, reusable);
            self.state.borrow_mut().exchanges.remove(&id);
        } else {
            self.arm_wait_timeout(
                ctx,
                id,
                pending.session,
                true,
                self.config.borrow().body_idle_timeout_ms,
            );
        }
    }

    async fn continue_timeout(&self, ctx: &Ctx, id: u64) {
        let (transition, write_job, rejected) = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return;
            };
            let Some(Proto::Await100(call)) = exchange.proto.take() else {
                return;
            };
            match call.proceed() {
                Ok(Await100Result::SendBody(call)) => {
                    exchange.proto = Some(Proto::SendBody(call));
                    (None, exchange.write_waiter.take(), None)
                }
                Ok(Await100Result::RecvResponse(call)) => {
                    exchange.proto = Some(Proto::RecvResponse(call));
                    (
                        Some(exchange.transport),
                        None,
                        exchange.write_waiter.take().map(|(pending, _)| pending),
                    )
                }
                Err(e) => {
                    exchange.error = Some(e.to_string());
                    (
                        None,
                        None,
                        exchange.write_waiter.take().map(|(pending, _)| pending),
                    )
                }
            }
        };
        if let Some(pending) = rejected {
            reply(ctx, pending, Answer::Failed(HttpError::RequestBodyRejected));
        }
        if let Some((pending, chunk)) = write_job {
            let answer = self.write_request(ctx, id, chunk).await;
            reply(ctx, pending, answer);
        }
        if transition.is_some() {
            self.drive_input(ctx, id).await;
        }
    }

    fn arm_wait_timeout(&self, ctx: &Ctx, id: u64, session: u64, body: bool, timeout: u64) {
        let wake = ctx.clone();
        ctx.spawn(async move {
            wake.sleep_ms(timeout).await;
            let _ = wake.send(
                NAME,
                MsgType::USER,
                Payload::of(Command::WaitTimeout { id, session, body }),
            );
        });
    }

    fn wait_timeout(&self, ctx: &Ctx, id: u64, session: u64, body: bool) {
        let pending = {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return;
            };
            let slot = if body {
                &mut exchange.body_waiter
            } else {
                &mut exchange.response_waiter
            };
            if slot.is_some_and(|pending| pending.session == session) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(pending) = pending {
            reply(
                ctx,
                pending,
                Answer::Failed(HttpError::Timeout(if body {
                    "body idle"
                } else {
                    "response head"
                })),
            );
            self.cancel(ctx, id, "HTTP 等待超时");
        }
    }

    fn idle_timeout(&self, ctx: &Ctx, transport: TransportId, generation: u64) {
        if self.state.borrow().idle_generation.get(&transport).copied() != Some(generation) {
            return;
        }
        let removed = {
            let mut state = self.state.borrow_mut();
            let mut found = false;
            for idle in state.idle.values_mut() {
                let before = idle.len();
                idle.retain(|id| *id != transport);
                found |= idle.len() != before;
            }
            found
        };
        if removed {
            self.state.borrow_mut().idle_generation.remove(&transport);
            transport.shutdown(ctx);
            self.forget_transport(transport);
            self.wake_queued(ctx);
        }
    }

    async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Data { id, data } => self.on_data(ctx, TransportId::Plain(id), data).await,
            SocketEvent::Close { id } => self.on_closed(ctx, TransportId::Plain(id), None).await,
            SocketEvent::Error { id, reason } => {
                self.on_closed(ctx, TransportId::Plain(id), Some(reason))
                    .await
            }
            _ => {}
        }
    }

    #[cfg(feature = "tls")]
    async fn on_tls(&self, ctx: &Ctx, event: TlsEvent) {
        match event {
            TlsEvent::Data { id, data } => self.on_data(ctx, TransportId::Tls(id), data).await,
            TlsEvent::Close { id } => self.on_closed(ctx, TransportId::Tls(id), None).await,
            TlsEvent::Error { id, reason } => {
                self.on_closed(ctx, TransportId::Tls(id), Some(reason))
                    .await
            }
            _ => {}
        }
    }

    async fn on_data(&self, ctx: &Ctx, transport: TransportId, data: Vec<u8>) {
        let id = self.state.borrow().active.get(&transport).copied();
        let Some(id) = id else {
            transport.shutdown(ctx);
            self.forget_transport(transport);
            return;
        };
        {
            let mut state = self.state.borrow_mut();
            if let Some(exchange) = state.exchanges.get_mut(&id) {
                exchange.input.extend_from_slice(&data);
            }
        }
        self.drive_input(ctx, id).await;
    }

    async fn drive_input(&self, ctx: &Ctx, id: u64) {
        let mut replies = Vec::new();
        let mut write_job = None;
        let mut pause = None;
        {
            let mut state = self.state.borrow_mut();
            let Some(exchange) = state.exchanges.get_mut(&id) else {
                return;
            };
            loop {
                let Some(proto) = exchange.proto.take() else {
                    break;
                };
                match proto {
                    Proto::Await100(mut call) => match call.try_read_100(&exchange.input) {
                        Ok(used) => {
                            if used > 0 {
                                exchange.input.drain(..used);
                            }
                            if call.can_keep_await_100() {
                                exchange.proto = Some(Proto::Await100(call));
                                break;
                            }
                            match call.proceed() {
                                Ok(Await100Result::SendBody(v)) => {
                                    exchange.proto = Some(Proto::SendBody(v));
                                    write_job = exchange.write_waiter.take();
                                }
                                Ok(Await100Result::RecvResponse(v)) => {
                                    exchange.request_finished = true;
                                    exchange.proto = Some(Proto::RecvResponse(v));
                                    if let Some((waiting, _)) = exchange.write_waiter.take() {
                                        replies.push((
                                            waiting,
                                            Answer::Failed(HttpError::RequestBodyRejected),
                                        ));
                                    }
                                    continue;
                                }
                                Err(e) => {
                                    exchange.error = Some(e.to_string());
                                    break;
                                }
                            }
                            break;
                        }
                        Err(e) => {
                            exchange.error = Some(e.to_string());
                            break;
                        }
                    },
                    Proto::RecvResponse(mut call) => {
                        match call.try_response(&exchange.input, false) {
                            Ok((used, Some(response))) => {
                                exchange.input.drain(..used);
                                exchange.response = Some(response);
                                match call.proceed().expect("response parsed") {
                                    RecvResponseResult::RecvBody(v) => {
                                        exchange.proto = Some(Proto::RecvBody(v))
                                    }
                                    RecvResponseResult::Redirect(v) => {
                                        let cleanup = v.proceed();
                                        exchange.reusable = Some(!cleanup.must_close_connection());
                                        exchange.proto = Some(Proto::Done);
                                        exchange.body_ended = true;
                                    }
                                    RecvResponseResult::Cleanup(v) => {
                                        exchange.reusable = Some(!v.must_close_connection());
                                        exchange.proto = Some(Proto::Done);
                                        exchange.body_ended = true;
                                    }
                                }
                                if let Some(waiting) = exchange.response_waiter.take() {
                                    exchange.response_taken = true;
                                    let handle = ClientBodyHandle::new(id, ctx, waiting.source);
                                    let response = exchange
                                        .response
                                        .take()
                                        .expect("response present")
                                        .map(|_| IncomingBody::client(handle));
                                    replies.push((waiting, Answer::Response(response)));
                                }
                                continue;
                            }
                            Ok(_) => {
                                if exchange.input.len() > self.config.borrow().max_header_size {
                                    exchange.error = Some("响应头超过上限".into());
                                } else {
                                    exchange.proto = Some(Proto::RecvResponse(call));
                                }
                                break;
                            }
                            Err(e) => {
                                exchange.error = Some(e.to_string());
                                break;
                            }
                        }
                    }
                    Proto::RecvBody(mut call) => {
                        if exchange.input.is_empty() {
                            exchange.proto = Some(Proto::RecvBody(call));
                            break;
                        }
                        let mut output = vec![0; self.config.borrow().max_chunk_size];
                        match call.read(&exchange.input, &mut output) {
                            Ok((used, written)) => {
                                if used > 0 {
                                    exchange.input.drain(..used);
                                }
                                if written > 0 {
                                    output.truncate(written);
                                    exchange.chunks.push_back(output);
                                }
                                if call.can_proceed()
                                    && call.body_mode() != BodyMode::CloseDelimited
                                {
                                    match call.proceed().expect("body complete") {
                                        RecvBodyResult::Cleanup(v) => {
                                            exchange.reusable = Some(!v.must_close_connection())
                                        }
                                        RecvBodyResult::Redirect(v) => {
                                            let v = v.proceed();
                                            exchange.reusable = Some(!v.must_close_connection());
                                        }
                                    }
                                    exchange.proto = Some(Proto::Done);
                                    exchange.body_ended = true;
                                } else {
                                    exchange.proto = Some(Proto::RecvBody(call));
                                }
                                if used == 0 && written == 0 {
                                    break;
                                }
                                continue;
                            }
                            Err(e) => {
                                exchange.error = Some(e.to_string());
                                break;
                            }
                        }
                    }
                    other => {
                        exchange.proto = Some(other);
                        break;
                    }
                }
            }
            if let Some(waiting) = exchange.body_waiter.take() {
                let answer = if let Some(chunk) = exchange.chunks.pop_front() {
                    Some(Answer::Chunk(Some(chunk)))
                } else if let Some(reason) = exchange.error.clone() {
                    Some(Answer::Failed(HttpError::Transport(reason)))
                } else if exchange.body_ended {
                    Some(Answer::Chunk(None))
                } else {
                    None
                };
                if let Some(answer) = answer {
                    replies.push((waiting, answer));
                } else {
                    exchange.body_waiter = Some(waiting);
                }
            }
            let buffered =
                exchange.input.len() + exchange.chunks.iter().map(Vec::len).sum::<usize>();
            if !exchange.body_ended
                && buffered >= self.config.borrow().input_high_water
                && !exchange.paused
            {
                exchange.paused = true;
                pause = Some(exchange.transport);
            }
            if let Some(reason) = exchange.error.clone() {
                if let Some(waiting) = exchange.response_waiter.take() {
                    replies.push((waiting, Answer::Failed(HttpError::Protocol(reason.clone()))));
                }
            }
        }
        for (waiting, answer) in replies {
            reply(ctx, waiting, answer);
        }
        if let Some((waiting, chunk)) = write_job {
            let answer = self.write_request(ctx, id, chunk).await;
            reply(ctx, waiting, answer);
        }
        if let Some(transport) = pause {
            if let Err(e) = transport.pause(ctx).await {
                self.cancel(ctx, id, &e.to_string());
            }
        }
    }

    async fn on_closed(&self, ctx: &Ctx, transport: TransportId, error: Option<String>) {
        let id = self.state.borrow_mut().active.remove(&transport);
        self.forget_transport(transport);
        let Some(id) = id else {
            self.wake_queued(ctx);
            return;
        };
        let close_delimited = {
            let state = self.state.borrow();
            state.exchanges.get(&id).is_some_and(|e| matches!(e.proto.as_ref(), Some(Proto::RecvBody(v)) if v.body_mode() == BodyMode::CloseDelimited))
        };
        if close_delimited && error.is_none() {
            {
                let mut state = self.state.borrow_mut();
                if let Some(exchange) = state.exchanges.get_mut(&id) {
                    exchange.body_ended = true;
                    exchange.proto = Some(Proto::Done);
                    exchange.reusable = Some(false);
                }
            }
            self.drive_input(ctx, id).await;
        } else {
            self.cancel(ctx, id, &error.unwrap_or_else(|| "连接提前关闭".into()));
        }
    }

    fn recycle(&self, ctx: &Ctx, id: u64, transport: TransportId, origin: Origin, reusable: bool) {
        self.state.borrow_mut().active.remove(&transport);
        if !reusable {
            transport.shutdown(ctx);
            self.forget_transport(transport);
            self.wake_queued(ctx);
            return;
        }
        let mut state = self.state.borrow_mut();
        let config = self.config.borrow();
        let total_idle = state.idle.values().map(VecDeque::len).sum::<usize>();
        let same = state.idle.get(&origin).map_or(0, VecDeque::len);
        if total_idle >= config.max_idle_connections
            || same >= config.max_idle_connections_per_origin
        {
            drop(state);
            transport.shutdown(ctx);
            self.forget_transport(transport);
        } else {
            state.idle.entry(origin).or_default().push_back(transport);
            state.next_idle_generation = state.next_idle_generation.wrapping_add(1).max(1);
            let generation = state.next_idle_generation;
            state.idle_generation.insert(transport, generation);
            let wake = ctx.clone();
            let timeout = config.pool_idle_timeout_ms;
            drop(config);
            drop(state);
            ctx.spawn(async move {
                wake.sleep_ms(timeout).await;
                let _ = wake.send(
                    NAME,
                    MsgType::USER,
                    Payload::of(Command::IdleTimeout {
                        transport,
                        generation,
                    }),
                );
            });
            self.wake_queued(ctx);
            return;
        }
        let _ = id;
        self.wake_queued(ctx);
    }

    fn forget_transport(&self, transport: TransportId) {
        let mut state = self.state.borrow_mut();
        state.origins.remove(&transport);
        state.active.remove(&transport);
        state.idle_generation.remove(&transport);
        for idle in state.idle.values_mut() {
            idle.retain(|id| *id != transport);
        }
    }

    fn cancel(&self, ctx: &Ctx, id: u64, reason: &str) {
        let (transport, waiters) = {
            let mut state = self.state.borrow_mut();
            let Some(mut exchange) = state.exchanges.remove(&id) else {
                return;
            };
            state
                .start_tokens
                .retain(|_, exchange_id| *exchange_id != id);
            exchange.error = Some(reason.into());
            exchange.body_ended = true;
            (
                exchange.transport,
                (
                    exchange.response_waiter.take(),
                    exchange.body_waiter.take(),
                    exchange.write_waiter.take().map(|(pending, _)| pending),
                ),
            )
        };
        transport.shutdown(ctx);
        self.forget_transport(transport);
        if let Some(v) = waiters.0 {
            reply(ctx, v, Answer::Failed(HttpError::Canceled));
        }
        if let Some(v) = waiters.1 {
            reply(ctx, v, Answer::Failed(HttpError::Canceled));
        }
        if let Some(v) = waiters.2 {
            reply(ctx, v, Answer::Failed(HttpError::Canceled));
        }
        self.wake_queued(ctx);
    }

    fn should_queue(&self, origin: &Origin) -> bool {
        let state = self.state.borrow();
        if state.idle.get(origin).is_some_and(|idle| !idle.is_empty()) {
            return false;
        }
        let connecting = state.connecting.values().sum::<usize>();
        let total = state.origins.len() + connecting;
        let same = state
            .origins
            .values()
            .filter(|value| *value == origin)
            .count()
            + state.connecting.get(origin).copied().unwrap_or(0);
        let config = self.config.borrow();
        total >= config.max_connections || same >= config.max_connections_per_origin
    }

    fn wake_queued(&self, ctx: &Ctx) {
        let job = {
            let mut state = self.state.borrow_mut();
            state
                .pending_starts
                .retain(|(_, alive, _, _)| alive.upgrade().is_some());
            let index = state.pending_starts.iter().position(|(_, _, _, request)| {
                Origin::from_uri(request.uri()).is_ok_and(|origin| {
                    if state.idle.get(&origin).is_some_and(|idle| !idle.is_empty()) {
                        return true;
                    }
                    let total = state.origins.len() + state.connecting.values().sum::<usize>();
                    let same = state
                        .origins
                        .values()
                        .filter(|value| **value == origin)
                        .count()
                        + state.connecting.get(&origin).copied().unwrap_or(0);
                    let config = self.config.borrow();
                    total < config.max_connections && same < config.max_connections_per_origin
                })
            });
            index.and_then(|index| state.pending_starts.remove(index))
        };
        if let Some((token, alive, pending, request)) = job {
            let _ = ctx.send(
                ctx.handle(),
                MsgType::USER,
                Payload::of(Command::ResumeStart {
                    token,
                    alive,
                    pending,
                    request,
                }),
            );
        }
    }
}

pub(crate) fn apply_body_headers(
    headers: &mut ureq_proto::http::HeaderMap,
    spec: BodySpec,
) -> Result<()> {
    if headers.contains_key(header::CONTENT_LENGTH)
        && headers.contains_key(header::TRANSFER_ENCODING)
    {
        return Err(HttpError::Protocol(
            "Content-Length 与 Transfer-Encoding 不能并存".into(),
        ));
    }
    match spec {
        BodySpec::Empty => {
            if headers
                .get(header::CONTENT_LENGTH)
                .is_some_and(|v| v != "0")
                || headers.contains_key(header::TRANSFER_ENCODING)
            {
                return Err(HttpError::Protocol(
                    "Empty body 与 framing header 不一致".into(),
                ));
            }
        }
        BodySpec::Fixed(size) => {
            if let Some(v) = headers.get(header::CONTENT_LENGTH) {
                if v.to_str().ok().and_then(|v| v.parse::<u64>().ok()) != Some(size) {
                    return Err(HttpError::Protocol(
                        "Content-Length 与 BodySpec 不一致".into(),
                    ));
                }
            } else {
                headers.insert(header::CONTENT_LENGTH, size.into());
            }
        }
        BodySpec::Chunked => {
            if headers.contains_key(header::CONTENT_LENGTH) {
                return Err(HttpError::Protocol(
                    "Chunked body 不能带 Content-Length".into(),
                ));
            }
            if let Some(value) = headers.get(header::TRANSFER_ENCODING) {
                if !value.as_bytes().eq_ignore_ascii_case(b"chunked") {
                    return Err(HttpError::Protocol(
                        "Transfer-Encoding 与 Chunked BodySpec 不一致".into(),
                    ));
                }
            } else {
                headers.insert(
                    header::TRANSFER_ENCODING,
                    "chunked".parse().expect("static header"),
                );
            }
        }
    }
    Ok(())
}

fn serialize_request(mut call: Call<SendRequest>, limit: usize) -> Result<(Vec<u8>, Proto)> {
    let mut all = Vec::new();
    while !call.can_proceed() {
        let mut output = vec![0; limit.saturating_sub(all.len()).max(1)];
        let used = call.write(&mut output)?;
        if used == 0 {
            return Err(HttpError::Protocol("序列化请求头没有进展".into()));
        }
        all.extend_from_slice(&output[..used]);
        if all.len() > limit {
            return Err(HttpError::BackpressureLimit {
                actual: all.len(),
                limit,
            });
        }
    }
    let proto = match call
        .proceed()?
        .ok_or(HttpError::Protocol("请求头状态无法推进".into()))?
    {
        SendRequestResult::Await100(v) => Proto::Await100(v),
        SendRequestResult::SendBody(v) => Proto::SendBody(v),
        SendRequestResult::RecvResponse(v) => Proto::RecvResponse(v),
    };
    Ok((all, proto))
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

async fn ask(ctx: &Ctx, command: Command) -> Result<Answer> {
    let value = ctx.call(NAME, MsgType::USER, Payload::of(command)).await?;
    match *value
        .downcast::<Answer>()
        .map_err(|_| HttpError::ServiceUnavailable("应答类型错误".into()))?
    {
        Answer::Failed(e) => Err(e),
        answer => Ok(answer),
    }
}

pub(crate) struct ClientBodyHandle {
    id: u64,
    node: rskynet_core::NodeRef,
    source: rskynet_core::Handle,
    dest: rskynet_core::Handle,
    active: bool,
}

impl ClientBodyHandle {
    fn new(id: u64, ctx: &Ctx, source: rskynet_core::Handle) -> Self {
        Self {
            id,
            node: ctx.node(),
            source,
            dest: ctx.handle(),
            active: true,
        }
    }

    pub(crate) async fn next_chunk(&mut self, ctx: &Ctx) -> Result<Option<Vec<u8>>> {
        let result = next_chunk(ctx, self.id).await?;
        if result.is_none() {
            self.active = false;
        }
        Ok(result)
    }
}

impl Drop for ClientBodyHandle {
    fn drop(&mut self) {
        if self.active {
            let _ = self.node.send(
                self.source,
                self.dest,
                MsgType::USER,
                0,
                Payload::of(Command::Cancel(self.id)),
            );
        }
    }
}

pub struct ClientExchange {
    id: u64,
    node: rskynet_core::NodeRef,
    source: rskynet_core::Handle,
    dest: rskynet_core::Handle,
    active: bool,
}

struct StartGuard {
    token: u64,
    node: rskynet_core::NodeRef,
    source: rskynet_core::Handle,
    dest: rskynet_core::Handle,
    active: bool,
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.node.send(
                self.source,
                self.dest,
                MsgType::USER,
                0,
                Payload::of(Command::CancelStart(self.token)),
            );
        }
    }
}

impl ClientExchange {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub async fn write_chunk(&mut self, ctx: &Ctx, chunk: Vec<u8>) -> Result<()> {
        write_chunk(ctx, self.id, chunk).await
    }
    pub async fn finish_request(&mut self, ctx: &Ctx) -> Result<()> {
        finish_request(ctx, self.id).await
    }
    pub async fn response(&mut self, ctx: &Ctx) -> Result<Response<IncomingBody>> {
        match ask(ctx, Command::Response(self.id)).await? {
            Answer::Response(v) => {
                self.active = false;
                Ok(v)
            }
            _ => Err(HttpError::ServiceUnavailable("响应类型错误".into())),
        }
    }
}

pub async fn start(ctx: &Ctx, request: Request<BodySpec>) -> Result<ClientExchange> {
    let dest = ctx
        .query_name(NAME)
        .ok_or_else(|| HttpError::ServiceUnavailable(NAME.into()))?;
    let alive = Arc::new(());
    let mut token = NEXT_START_TOKEN.fetch_add(1, Ordering::Relaxed);
    if token == 0 {
        token = NEXT_START_TOKEN.fetch_add(1, Ordering::Relaxed);
    }
    let mut guard = StartGuard {
        token,
        node: ctx.node(),
        source: ctx.handle(),
        dest,
        active: true,
    };
    match ask(
        ctx,
        Command::Start {
            token,
            alive: Arc::downgrade(&alive),
            request,
        },
    )
    .await?
    {
        Answer::Id(id) => {
            ctx.send(
                dest,
                MsgType::USER,
                Payload::of(Command::ConfirmStart(token)),
            )?;
            guard.active = false;
            Ok(ClientExchange {
                id,
                node: ctx.node(),
                source: ctx.handle(),
                dest,
                active: true,
            })
        }
        _ => Err(HttpError::ServiceUnavailable("启动应答错误".into())),
    }
}

pub async fn request(ctx: &Ctx, request: Request<Vec<u8>>) -> Result<Response<IncomingBody>> {
    let (parts, bytes) = request.into_parts();
    let mut exchange = start(
        ctx,
        Request::from_parts(parts, BodySpec::Fixed(bytes.len() as u64)),
    )
    .await?;
    if !bytes.is_empty() {
        exchange.write_chunk(ctx, bytes).await?;
    }
    exchange.finish_request(ctx).await?;
    exchange.response(ctx).await
}

pub(crate) async fn write_chunk(ctx: &Ctx, id: u64, chunk: Vec<u8>) -> Result<()> {
    match ask(ctx, Command::Write { id, chunk }).await? {
        Answer::Done => Ok(()),
        _ => Err(HttpError::ServiceUnavailable("写应答错误".into())),
    }
}
pub(crate) async fn finish_request(ctx: &Ctx, id: u64) -> Result<()> {
    match ask(ctx, Command::Finish(id)).await? {
        Answer::Done => Ok(()),
        _ => Err(HttpError::ServiceUnavailable("结束应答错误".into())),
    }
}
pub(crate) async fn next_chunk(ctx: &Ctx, id: u64) -> Result<Option<Vec<u8>>> {
    match ask(ctx, Command::Next(id)).await? {
        Answer::Chunk(v) => Ok(v),
        _ => Err(HttpError::ServiceUnavailable("body 应答错误".into())),
    }
}

impl Drop for ClientExchange {
    fn drop(&mut self) {
        if self.active {
            let _ = self.node.send(
                self.source,
                self.dest,
                MsgType::USER,
                0,
                Payload::of(Command::Cancel(self.id)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ureq_proto::http::HeaderMap;

    #[test]
    fn body_spec_controls_framing_headers() {
        let mut fixed = HeaderMap::new();
        apply_body_headers(&mut fixed, BodySpec::Fixed(7)).unwrap();
        assert_eq!(fixed[header::CONTENT_LENGTH], "7");

        let mut chunked = HeaderMap::new();
        apply_body_headers(&mut chunked, BodySpec::Chunked).unwrap();
        assert_eq!(chunked[header::TRANSFER_ENCODING], "chunked");

        let mut conflict = HeaderMap::new();
        conflict.insert(header::CONTENT_LENGTH, "3".parse().unwrap());
        assert!(apply_body_headers(&mut conflict, BodySpec::Fixed(4)).is_err());
    }

    #[test]
    fn request_headers_are_encoded_by_ureq_proto() {
        let request = Request::post("http://example.test/upload")
            .header("x-test", "yes")
            .body(())
            .unwrap();
        let mut prepare = Call::new(request).unwrap();
        prepare.header(header::CONTENT_LENGTH, "0").unwrap();
        let (bytes, proto) = serialize_request(prepare.proceed(), 4096).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.starts_with("POST /upload HTTP/1.1\r\n"));
        assert!(text.contains("host: example.test\r\n"));
        assert!(matches!(proto, Proto::SendBody(_) | Proto::RecvResponse(_)));
    }
}
