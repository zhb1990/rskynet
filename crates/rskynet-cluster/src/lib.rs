//! rskynet 的可选跨节点通信层。
//!
//! 本地 actor 继续直接传递 `Payload::Boxed`；只有实现 [`ClusterMessage`]
//! 的 Protobuf 类型才能进入这条网络路径。
//!
//! 通常用 derive 声明稳定 TYPE_ID，再用 [`handler`] 把自由 async 函数提交到
//! 自动注册表。顶层 `rskynet` 发现 `[cluster]` 配置段时会自动收集并启动。

mod wire;

use std::any::TypeId;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use prost::Message as ProstMessage;
use rskynet_core::{
    Addr, Ctx, Error as CoreError, FromPayload, Message, MsgType, Payload, Registry,
    Result as CoreResult, Service, SvcCell, boxed_payload,
};
use rskynet_net::{self as net, SocketEvent, SocketId};
use serde::{Deserialize, Serialize};
use wire::{Envelope, Kind, PROTOCOL_VERSION, Target, target};

pub use rskynet_macros::{ClusterMessage, cluster_handler as handler};

pub const NAME: &str = "cluster";
pub const DEFAULT_MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(value: u32) -> Result<Self, ClusterError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or(ClusterError::InvalidNodeId)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterAddr {
    pub node: NodeId,
    pub service: Addr,
}

impl ClusterAddr {
    pub fn new(node: NodeId, service: impl Into<Addr>) -> Self {
        Self {
            node,
            service: service.into(),
        }
    }
}

pub trait ClusterMessage: ProstMessage + Default + Send + 'static {
    /// 全集群稳定且唯一的非零类型号。
    const TYPE_ID: u32;
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    #[error("NodeId 必须是非零 u32")]
    InvalidNodeId,
    #[error("节点 {0} 尚未连接")]
    NodeUnavailable(u32),
    #[error("跨节点请求超时")]
    Timeout,
    #[error("远端错误：{0}")]
    Remote(String),
    #[error("协议错误：{0}")]
    Protocol(String),
    #[error("重复的 cluster handler：target={target}, type_id={type_id}")]
    DuplicateHandler { target: String, type_id: u32 },
    #[error("ClusterMessage TYPE_ID {0} 已被另一个 Rust 类型占用")]
    DuplicateTypeId(u32),
    #[error("ClusterMessage TYPE_ID 不能为 0")]
    InvalidTypeId,
    #[error("自动 cluster handler `{location}` 注册失败：{reason}")]
    AutoRegistration {
        location: &'static str,
        reason: String,
    },
    #[error("本地 cluster 服务错误：{0}")]
    Local(#[from] CoreError),
    #[error("Protobuf 解码失败：{0}")]
    Decode(#[from] prost::DecodeError),
}

#[derive(Clone)]
pub struct RemoteContext {
    pub source_node: NodeId,
    pub request_id: Option<u64>,
    target: String,
    type_id: u32,
    ctx: Ctx,
}

impl std::fmt::Debug for RemoteContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteContext")
            .field("source_node", &self.source_node)
            .field("request_id", &self.request_id)
            .field("target", &self.target)
            .field("type_id", &self.type_id)
            .finish_non_exhaustive()
    }
}

impl RemoteContext {
    /// 当前 handler 的 cluster 路由名或本地 handle。
    pub fn target(&self) -> &str {
        &self.target
    }

    /// 当前请求消息的 [`ClusterMessage::TYPE_ID`]。
    pub const fn type_id(&self) -> u32 {
        self.type_id
    }

    /// 写入节点 logger，并自动附带当前 cluster 请求的诊断字段。
    pub fn log(&self, text: impl Into<String>) {
        self.ctx.log(format!(
            "cluster target={} type_id={} source_node={} request_id={} | {}",
            self.target,
            self.type_id,
            self.source_node.get(),
            self.request_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string()),
            text.into()
        ));
    }

    /// 向本地 actor 投递消息，不等待应答。
    pub fn post(&self, dest: impl Into<Addr>, payload: Payload) -> CoreResult<()> {
        self.ctx.post(dest, payload)
    }

    /// 请求本地 actor 并等待应答。
    pub async fn request(&self, dest: impl Into<Addr>, payload: Payload) -> CoreResult<Payload> {
        self.ctx.request(dest, payload).await
    }

    /// 节点启动至今的毫秒数。
    pub fn now(&self) -> u64 {
        self.ctx.now()
    }

    /// 当前 Unix 时间，单位毫秒。
    pub fn time(&self) -> u64 {
        self.ctx.time()
    }

    /// 节点启动时刻的 Unix 时间，单位毫秒。
    pub fn start_time(&self) -> u64 {
        self.ctx.start_time()
    }

    /// 挂起指定毫秒数，不阻塞其他 cluster 请求。
    pub async fn sleep(&self, millis: u32) {
        self.ctx.sleep(millis).await;
    }

    /// 挂起指定毫秒数，不阻塞其他 cluster 请求。
    pub async fn sleep_ms(&self, millis: u64) {
        self.ctx.sleep_ms(millis).await;
    }

    /// 让出一次当前服务的调度。
    pub async fn yield_now(&self) {
        self.ctx.yield_now().await;
    }
}

type HandlerFn = Arc<
    dyn Fn(RemoteContext, Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, String>> + Send + Sync,
>;

#[derive(Clone)]
struct Handler {
    response_type: u32,
    call: HandlerFn,
}

#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum AutoTarget {
    Name(&'static str),
    Handle(rskynet_core::Handle),
}

#[doc(hidden)]
pub struct AutoHandler {
    target: AutoTarget,
    source: &'static str,
    register: fn(&mut HandlerRegistry) -> Result<(), ClusterError>,
}

impl AutoHandler {
    pub const fn name(
        target: &'static str,
        source: &'static str,
        register: fn(&mut HandlerRegistry) -> Result<(), ClusterError>,
    ) -> Self {
        Self {
            target: AutoTarget::Name(target),
            source,
            register,
        }
    }

    pub const fn handle(
        target: rskynet_core::Handle,
        source: &'static str,
        register: fn(&mut HandlerRegistry) -> Result<(), ClusterError>,
    ) -> Self {
        Self {
            target: AutoTarget::Handle(target),
            source,
            register,
        }
    }

    fn sort_key(&self) -> (u8, String, &'static str) {
        match self.target {
            AutoTarget::Name(name) => (0, name.to_string(), self.source),
            AutoTarget::Handle(handle) => (1, format!("{handle:016x}"), self.source),
        }
    }
}

inventory::collect!(AutoHandler);

#[derive(Clone, Default)]
pub struct HandlerRegistry {
    handlers: HashMap<(ServiceKey, u32), Handler>,
    message_types: HashMap<u32, TypeId>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_auto() -> Result<Self, ClusterError> {
        let mut handlers: Vec<&'static AutoHandler> =
            inventory::iter::<AutoHandler>.into_iter().collect();
        handlers.sort_unstable_by_key(|handler| handler.sort_key());
        let mut registry = Self::new();
        for handler in handlers {
            (handler.register)(&mut registry).map_err(|error| ClusterError::AutoRegistration {
                location: handler.source,
                reason: error.to_string(),
            })?;
        }
        Ok(registry)
    }

    pub fn register<Req, Resp, F, Fut>(
        &mut self,
        service: impl Into<Addr>,
        handler: F,
    ) -> Result<&mut Self, ClusterError>
    where
        Req: ClusterMessage,
        Resp: ClusterMessage,
        F: Fn(RemoteContext, Req) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Resp, String>> + Send + 'static,
    {
        let key = (ServiceKey::from_addr(&service.into()), Req::TYPE_ID);
        let display = key.0.to_string();
        if self.handlers.contains_key(&key) {
            return Err(ClusterError::DuplicateHandler {
                target: display,
                type_id: Req::TYPE_ID,
            });
        }
        self.check_type::<Req>()?;
        self.check_type::<Resp>()?;
        if Req::TYPE_ID == Resp::TYPE_ID && TypeId::of::<Req>() != TypeId::of::<Resp>() {
            return Err(ClusterError::DuplicateTypeId(Req::TYPE_ID));
        }
        let call: HandlerFn = Arc::new(move |remote, bytes| {
            let decoded = Req::decode(bytes.as_slice()).map_err(|e| e.to_string());
            let future = match decoded {
                Ok(message) => Some(handler(remote, message)),
                Err(error) => return Box::pin(async move { Err(error) }),
            };
            Box::pin(async move {
                let response = future.unwrap().await?;
                Ok(response.encode_to_vec())
            })
        });
        let wrapped = Handler {
            response_type: Resp::TYPE_ID,
            call,
        };
        self.message_types.insert(Req::TYPE_ID, TypeId::of::<Req>());
        self.message_types
            .insert(Resp::TYPE_ID, TypeId::of::<Resp>());
        self.handlers.insert(key, wrapped);
        Ok(self)
    }

    pub fn register_send<M, F, Fut>(
        &mut self,
        service: impl Into<Addr>,
        handler: F,
    ) -> Result<&mut Self, ClusterError>
    where
        M: ClusterMessage,
        F: Fn(RemoteContext, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let key = (ServiceKey::from_addr(&service.into()), M::TYPE_ID);
        let display = key.0.to_string();
        if self.handlers.contains_key(&key) {
            return Err(ClusterError::DuplicateHandler {
                target: display,
                type_id: M::TYPE_ID,
            });
        }
        self.check_type::<M>()?;
        let call: HandlerFn = Arc::new(move |remote, bytes| {
            let decoded = M::decode(bytes.as_slice()).map_err(|e| e.to_string());
            let future = match decoded {
                Ok(message) => Some(handler(remote, message)),
                Err(error) => return Box::pin(async move { Err(error) }),
            };
            Box::pin(async move { future.unwrap().await.map(|()| Vec::new()) })
        });
        self.message_types.insert(M::TYPE_ID, TypeId::of::<M>());
        self.handlers.insert(
            key,
            Handler {
                response_type: 0,
                call,
            },
        );
        Ok(self)
    }

    fn check_type<M: ClusterMessage>(&self) -> Result<(), ClusterError> {
        if M::TYPE_ID == 0 {
            return Err(ClusterError::InvalidTypeId);
        }
        match self.message_types.get(&M::TYPE_ID) {
            Some(existing) if *existing != TypeId::of::<M>() => {
                Err(ClusterError::DuplicateTypeId(M::TYPE_ID))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Seed {
    pub node_id: u32,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub node_id: u32,
    pub listen: String,
    pub advertise: Option<String>,
    pub seeds: Vec<Seed>,
    pub request_timeout_ms: u64,
    pub max_frame_len: usize,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            listen: "127.0.0.1:0".into(),
            advertise: None,
            seeds: Vec::new(),
            request_timeout_ms: 5_000,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        }
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<NodeId, ClusterError> {
        let node = NodeId::new(self.node_id)?;
        if self.request_timeout_ms == 0 {
            return Err(ClusterError::Protocol(
                "request_timeout_ms 必须大于 0".into(),
            ));
        }
        if self.max_frame_len == 0 || self.max_frame_len > u32::MAX as usize {
            return Err(ClusterError::Protocol(
                "max_frame_len 必须在 1..=u32::MAX".into(),
            ));
        }
        let mut ids = HashSet::new();
        for seed in &self.seeds {
            let id = NodeId::new(seed.node_id)?;
            if id == node {
                return Err(ClusterError::Protocol("不能把本节点列为 seed".into()));
            }
            if !ids.insert(id) {
                return Err(ClusterError::Protocol(format!(
                    "seed NodeId {} 重复",
                    id.get()
                )));
            }
        }
        Ok(node)
    }
}

#[derive(Debug)]
enum Command {
    Send {
        addr: ClusterAddr,
        type_id: u32,
        payload: Vec<u8>,
    },
    Request {
        addr: ClusterAddr,
        type_id: u32,
        response_type: u32,
        payload: Vec<u8>,
    },
    Timeout(u64),
    Connect {
        node: NodeId,
        address: String,
    },
}
boxed_payload!(Command);

#[derive(Debug)]
enum Answer {
    Done,
    Payload(Vec<u8>),
    Failed(String),
}
boxed_payload!(Answer);

#[derive(Clone, Hash, PartialEq, Eq)]
enum ServiceKey {
    Handle(rskynet_core::Handle),
    Name(String),
}

impl ServiceKey {
    fn from_addr(addr: &Addr) -> Self {
        match addr {
            Addr::Handle(v) => Self::Handle(*v),
            Addr::Name(v) => Self::Name(v.clone()),
        }
    }
    fn from_target(target: Target) -> Option<Self> {
        match target.value? {
            target::Value::Handle(v) => Some(Self::Handle(v)),
            target::Value::Name(v) => Some(Self::Name(v)),
        }
    }
    fn target(&self) -> Target {
        Target {
            value: Some(match self {
                Self::Handle(v) => target::Value::Handle(*v),
                Self::Name(v) => target::Value::Name(v.clone()),
            }),
        }
    }
}

impl std::fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handle(v) => write!(f, ":{v:08x}"),
            Self::Name(v) => write!(f, ".{v}"),
        }
    }
}

struct Pending {
    source: rskynet_core::Handle,
    session: u64,
    node: NodeId,
    response_type: u32,
}
struct Peer {
    node: Option<NodeId>,
    expected: Option<NodeId>,
    buffer: Vec<u8>,
}

#[derive(Default)]
struct State {
    next_request: u64,
    nodes: HashMap<NodeId, SocketId>,
    peers: HashMap<SocketId, Peer>,
    pending: HashMap<u64, Pending>,
}

pub struct ClusterService {
    handlers: HandlerRegistry,
    state: SvcCell<State>,
    config: SvcCell<ClusterConfig>,
    node_id: SvcCell<Option<NodeId>>,
}

impl ClusterService {
    pub fn new(handlers: HandlerRegistry) -> Self {
        Self {
            handlers,
            state: SvcCell::new(State::default()),
            config: SvcCell::new(ClusterConfig::default()),
            node_id: SvcCell::new(None),
        }
    }

    fn hello(&self) -> Envelope {
        Envelope {
            version: PROTOCOL_VERSION,
            kind: Kind::Hello as i32,
            source_node: self.node_id.borrow().unwrap().get(),
            advertise: self.config.borrow().advertise.clone().unwrap_or_default(),
            ..Default::default()
        }
    }

    fn transmit(
        &self,
        ctx: &Ctx,
        socket: SocketId,
        envelope: &Envelope,
    ) -> Result<(), ClusterError> {
        let bytes = wire::frame(envelope, self.config.borrow().max_frame_len)
            .map_err(|e| ClusterError::Protocol(e.into()))?;
        net::send(ctx, socket, bytes)?;
        Ok(())
    }

    fn fail(&self, ctx: &Ctx, msg: &Message, reason: impl Into<String>) {
        let _ = ctx.reply(msg, Payload::of(Answer::Failed(reason.into())));
    }
}

impl Service for ClusterService {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, CoreResult<()>> {
        Box::pin(async move {
            let config: ClusterConfig = ctx.node().section(NAME)?.unwrap_or_default();
            let node = config
                .validate()
                .map_err(|e| CoreError::service(e.to_string()))?;
            self.config.replace(config.clone());
            self.node_id.replace(Some(node));
            if !ctx.register_name(NAME) {
                return Err(CoreError::service("名字 `.cluster` 已经被占用"));
            }
            let listener = net::listen(&ctx, config.listen.clone()).await?;
            net::start(&ctx, listener).await?;
            for seed in config.seeds {
                let node =
                    NodeId::new(seed.node_id).map_err(|e| CoreError::service(e.to_string()))?;
                ctx.send(
                    ctx.handle(),
                    MsgType::USER,
                    Payload::of(Command::Connect {
                        node,
                        address: seed.address,
                    }),
                )?;
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
            let Ok(command) = msg.take_payload().downcast::<Command>() else {
                let _ = ctx.reply_error(&msg);
                return;
            };
            match *command {
                Command::Connect { node, address } => self.connect(&ctx, node, address).await,
                Command::Timeout(id) => self.timeout(&ctx, id),
                Command::Send {
                    addr,
                    type_id,
                    payload,
                } => match self.outbound(&ctx, addr, type_id, 0, Kind::Send, payload) {
                    Ok(()) => {
                        let _ = ctx.reply(&msg, Payload::of(Answer::Done));
                    }
                    Err(e) => self.fail(&ctx, &msg, e.to_string()),
                },
                Command::Request {
                    addr,
                    type_id,
                    response_type,
                    payload,
                } => {
                    let mut state = self.state.borrow_mut();
                    state.next_request = state.next_request.wrapping_add(1).max(1);
                    let id = state.next_request;
                    state.pending.insert(
                        id,
                        Pending {
                            source: msg.source,
                            session: msg.session,
                            node: addr.node,
                            response_type,
                        },
                    );
                    drop(state);
                    if let Err(e) = self.outbound(&ctx, addr, type_id, id, Kind::Request, payload) {
                        self.state.borrow_mut().pending.remove(&id);
                        self.fail(&ctx, &msg, e.to_string());
                        return;
                    }
                    let timeout = self.config.borrow().request_timeout_ms;
                    let wake = ctx.clone();
                    ctx.spawn(async move {
                        wake.sleep_ms(timeout).await;
                        let _ = wake.send(
                            wake.handle(),
                            MsgType::USER,
                            Payload::of(Command::Timeout(id)),
                        );
                    });
                }
            }
        })
    }
}

impl ClusterService {
    async fn connect(&self, ctx: &Ctx, expected: NodeId, address: String) {
        if self.state.borrow().nodes.contains_key(&expected) {
            return;
        }
        match net::connect(ctx, address.clone()).await {
            Ok(socket) => {
                self.state.borrow_mut().peers.insert(
                    socket,
                    Peer {
                        node: None,
                        expected: Some(expected),
                        buffer: Vec::new(),
                    },
                );
                if self.transmit(ctx, socket, &self.hello()).is_err() {
                    let _ = net::shutdown(ctx, socket);
                }
            }
            Err(error) => {
                ctx.log(format!("cluster 连接节点 {} 失败：{error}", expected.get()));
                Self::retry(ctx, expected, address);
            }
        }
    }

    fn retry(ctx: &Ctx, node: NodeId, address: String) {
        let wake = ctx.clone();
        ctx.spawn(async move {
            wake.sleep_ms(500).await;
            let _ = wake.send(
                wake.handle(),
                MsgType::USER,
                Payload::of(Command::Connect { node, address }),
            );
        });
    }

    fn timeout(&self, ctx: &Ctx, id: u64) {
        if let Some(pending) = self.state.borrow_mut().pending.remove(&id) {
            let _ = ctx.node().send(
                ctx.handle(),
                pending.source,
                MsgType::RESPONSE,
                pending.session,
                Payload::of(Answer::Failed(ClusterError::Timeout.to_string())),
            );
        }
    }

    fn outbound(
        &self,
        ctx: &Ctx,
        addr: ClusterAddr,
        type_id: u32,
        request_id: u64,
        kind: Kind,
        payload: Vec<u8>,
    ) -> Result<(), ClusterError> {
        let socket = self
            .state
            .borrow()
            .nodes
            .get(&addr.node)
            .copied()
            .ok_or(ClusterError::NodeUnavailable(addr.node.get()))?;
        let envelope = Envelope {
            version: PROTOCOL_VERSION,
            kind: kind as i32,
            source_node: self.node_id.borrow().unwrap().get(),
            request_id,
            type_id,
            target: Some(ServiceKey::from_addr(&addr.service).target()),
            payload,
            error: String::new(),
            advertise: String::new(),
        };
        self.transmit(ctx, socket, &envelope)
    }

    async fn on_socket(&self, ctx: &Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Accept { id, .. } => {
                self.state.borrow_mut().peers.insert(
                    id,
                    Peer {
                        node: None,
                        expected: None,
                        buffer: Vec::new(),
                    },
                );
                if net::start(ctx, id).await.is_ok() {
                    let _ = self.transmit(ctx, id, &self.hello());
                }
            }
            SocketEvent::Data { id, data } => {
                let frames = {
                    let mut state = self.state.borrow_mut();
                    let Some(peer) = state.peers.get_mut(&id) else {
                        return;
                    };
                    peer.buffer.extend(data);
                    wire::drain(&mut peer.buffer, self.config.borrow().max_frame_len)
                };
                match frames {
                    Ok(frames) => {
                        for frame in frames {
                            self.on_envelope(ctx, id, frame).await;
                        }
                    }
                    Err(reason) => {
                        ctx.log(format!("cluster 丢弃非法连接 {id}：{reason}"));
                        let _ = net::shutdown(ctx, id);
                    }
                }
            }
            SocketEvent::Close { id } | SocketEvent::Error { id, .. } => self.disconnected(ctx, id),
            _ => {}
        }
    }

    async fn on_envelope(&self, ctx: &Ctx, socket: SocketId, envelope: Envelope) {
        if envelope.version != PROTOCOL_VERSION {
            let _ = net::shutdown(ctx, socket);
            return;
        }
        let Ok(source) = NodeId::new(envelope.source_node) else {
            let _ = net::shutdown(ctx, socket);
            return;
        };
        let Some(kind) = Kind::try_from(envelope.kind).ok() else {
            let _ = net::shutdown(ctx, socket);
            return;
        };
        if kind == Kind::Hello {
            self.accept_hello(ctx, socket, source);
            return;
        }
        if self.state.borrow().peers.get(&socket).and_then(|p| p.node) != Some(source) {
            let _ = net::shutdown(ctx, socket);
            return;
        }
        match kind {
            Kind::Send | Kind::Request => {
                self.invoke(ctx, socket, source, envelope, kind == Kind::Request)
                    .await
            }
            Kind::Response | Kind::Error => {
                self.complete(ctx, source, envelope, kind == Kind::Error)
            }
            Kind::Hello => {}
        }
    }

    fn accept_hello(&self, ctx: &Ctx, socket: SocketId, node: NodeId) {
        if node == self.node_id.borrow().unwrap() {
            let _ = net::shutdown(ctx, socket);
            return;
        }
        let mut state = self.state.borrow_mut();
        let Some(expected) = state.peers.get(&socket).map(|peer| peer.expected) else {
            return;
        };
        if expected.is_some_and(|expected| expected != node) {
            drop(state);
            let _ = net::shutdown(ctx, socket);
            return;
        }
        // 两边同时建连时，统一保留由较小 NodeId 发起的那条；只有一条时
        // 无论谁发起都接受。这样两端不会各自关掉对方选中的连接。
        let local = self.node_id.borrow().unwrap();
        let new_initiator = if expected.is_some() { local } else { node };
        if let Some(old) = state.nodes.get(&node).copied() {
            let old_initiator = state
                .peers
                .get(&old)
                .map(|old_peer| {
                    if old_peer.expected.is_some() {
                        local
                    } else {
                        node
                    }
                })
                .unwrap_or(node);
            if old_initiator <= new_initiator {
                drop(state);
                let _ = net::shutdown(ctx, socket);
                return;
            }
        }
        state.peers.get_mut(&socket).unwrap().node = Some(node);
        if let Some(old) = state.nodes.insert(node, socket) {
            if old != socket {
                state.peers.remove(&old);
                drop(state);
                let _ = net::shutdown(ctx, old);
            }
        }
    }

    async fn invoke(
        &self,
        ctx: &Ctx,
        socket: SocketId,
        source: NodeId,
        envelope: Envelope,
        wants_reply: bool,
    ) {
        let target = envelope.target.and_then(ServiceKey::from_target);
        let handler = target
            .as_ref()
            .and_then(|target| {
                self.handlers
                    .handlers
                    .get(&(target.clone(), envelope.type_id))
            })
            .cloned();
        let result = match handler {
            Some(ref handler) => {
                (handler.call)(
                    RemoteContext {
                        source_node: source,
                        request_id: wants_reply.then_some(envelope.request_id),
                        target: target
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "<missing>".into()),
                        type_id: envelope.type_id,
                        ctx: ctx.clone(),
                    },
                    envelope.payload,
                )
                .await
            }
            None => Err(format!(
                "未注册的 cluster handler：target={}, type_id={}",
                target
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "<missing>".into()),
                envelope.type_id
            )),
        };
        if !wants_reply {
            return;
        }
        let (kind, payload, error) = match result {
            Ok(payload) => (Kind::Response, payload, String::new()),
            Err(error) => (Kind::Error, Vec::new(), error),
        };
        let response_type = handler.map(|handler| handler.response_type).unwrap_or(0);
        let response = Envelope {
            version: PROTOCOL_VERSION,
            kind: kind as i32,
            source_node: self.node_id.borrow().unwrap().get(),
            request_id: envelope.request_id,
            type_id: response_type,
            payload,
            error,
            ..Default::default()
        };
        let _ = self.transmit(ctx, socket, &response);
    }

    fn complete(&self, ctx: &Ctx, source: NodeId, envelope: Envelope, failed: bool) {
        let Some(pending) = self.state.borrow_mut().pending.remove(&envelope.request_id) else {
            return;
        };
        let answer = if pending.node != source {
            Answer::Failed("应答节点与请求目标不一致".into())
        } else if failed {
            Answer::Failed(envelope.error)
        } else if pending.response_type != envelope.type_id {
            Answer::Failed(format!(
                "应答类型不符：期待 {}，收到 {}",
                pending.response_type, envelope.type_id
            ))
        } else {
            Answer::Payload(envelope.payload)
        };
        let _ = ctx.node().send(
            ctx.handle(),
            pending.source,
            MsgType::RESPONSE,
            pending.session,
            Payload::of(answer),
        );
    }

    fn disconnected(&self, ctx: &Ctx, socket: SocketId) {
        let mut state = self.state.borrow_mut();
        let node = state.peers.remove(&socket).and_then(|peer| peer.node);
        if let Some(node) = node {
            if state.nodes.get(&node) == Some(&socket) {
                state.nodes.remove(&node);
            }
            let ids: Vec<u64> = state
                .pending
                .iter()
                .filter_map(|(id, p)| (p.node == node).then_some(*id))
                .collect();
            for id in ids {
                if let Some(p) = state.pending.remove(&id) {
                    let _ = ctx.node().send(
                        ctx.handle(),
                        p.source,
                        MsgType::RESPONSE,
                        p.session,
                        Payload::of(Answer::Failed(format!("节点 {} 连接已断开", node.get()))),
                    );
                }
            }
            if let Some(seed) = self
                .config
                .borrow()
                .seeds
                .iter()
                .find(|seed| seed.node_id == node.get())
            {
                Self::retry(ctx, node, seed.address.clone());
            }
        }
    }
}

pub async fn send<M: ClusterMessage>(
    ctx: &Ctx,
    addr: ClusterAddr,
    message: M,
) -> Result<(), ClusterError> {
    if M::TYPE_ID == 0 {
        return Err(ClusterError::InvalidTypeId);
    }
    match ask(
        ctx,
        Command::Send {
            addr,
            type_id: M::TYPE_ID,
            payload: message.encode_to_vec(),
        },
    )
    .await?
    {
        Answer::Done => Ok(()),
        Answer::Failed(e) => Err(ClusterError::Remote(e)),
        Answer::Payload(_) => Err(ClusterError::Protocol("未期待的 payload 应答".into())),
    }
}

pub async fn request<Req: ClusterMessage, Resp: ClusterMessage>(
    ctx: &Ctx,
    addr: ClusterAddr,
    message: Req,
) -> Result<Resp, ClusterError> {
    if Req::TYPE_ID == 0 || Resp::TYPE_ID == 0 {
        return Err(ClusterError::InvalidTypeId);
    }
    match ask(
        ctx,
        Command::Request {
            addr,
            type_id: Req::TYPE_ID,
            response_type: Resp::TYPE_ID,
            payload: message.encode_to_vec(),
        },
    )
    .await?
    {
        Answer::Payload(bytes) => Ok(Resp::decode(bytes.as_slice())?),
        Answer::Failed(e) => Err(ClusterError::Remote(e)),
        Answer::Done => Err(ClusterError::Protocol("未期待的空应答".into())),
    }
}

async fn ask(ctx: &Ctx, command: Command) -> Result<Answer, ClusterError> {
    let payload = ctx.call(NAME, MsgType::USER, Payload::of(command)).await?;
    Answer::from_payload(payload).map_err(ClusterError::Local)
}

pub trait RegistryExt {
    fn with_cluster(self, handlers: HandlerRegistry) -> Self;
}

impl RegistryExt for Registry {
    fn with_cluster(self, handlers: HandlerRegistry) -> Self {
        self.with(NAME, move || ClusterService::new(handlers.clone()))
    }
}

#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, ProstMessage)]
    struct Ping {
        #[prost(uint32, tag = "1")]
        value: u32,
    }
    impl ClusterMessage for Ping {
        const TYPE_ID: u32 = 10;
    }
    #[derive(Clone, PartialEq, ProstMessage)]
    struct Pong {
        #[prost(uint32, tag = "1")]
        value: u32,
    }
    impl ClusterMessage for Pong {
        const TYPE_ID: u32 = 11;
    }
    #[derive(Clone, PartialEq, ProstMessage)]
    struct Colliding {}
    impl ClusterMessage for Colliding {
        const TYPE_ID: u32 = 10;
    }

    #[derive(Clone, PartialEq, ProstMessage, crate::ClusterMessage)]
    #[cluster(type_id = 20, crate = crate)]
    struct DirectPing {}
    #[derive(Clone, PartialEq, ProstMessage, crate::ClusterMessage)]
    #[cluster(type_id = 21, crate = crate)]
    struct DirectPong {}
    #[derive(Clone, PartialEq, ProstMessage, crate::ClusterMessage)]
    #[cluster(type_id = 22, crate = crate)]
    struct DirectNotice {}

    #[crate::handler("direct", crate = crate)]
    async fn direct_handler(
        _remote: RemoteContext,
        _request: DirectPing,
    ) -> std::result::Result<DirectPong, String> {
        Ok(DirectPong {})
    }

    #[crate::handler("direct-send", crate = crate)]
    async fn direct_send_handler(
        _remote: RemoteContext,
        _request: DirectNotice,
    ) -> std::result::Result<(), String> {
        Ok(())
    }

    #[test]
    fn ids_and_duplicate_handlers_are_checked() {
        assert!(NodeId::new(0).is_err());
        let mut registry = HandlerRegistry::new();
        registry
            .register::<Ping, Pong, _, _>("echo", |_, ping| async move {
                Ok(Pong { value: ping.value })
            })
            .unwrap();
        assert!(matches!(
            registry.register::<Ping, Pong, _, _>("echo", |_, _| async { Ok(Pong::default()) }),
            Err(ClusterError::DuplicateHandler { .. })
        ));
        assert!(matches!(
            registry.register_send::<Colliding, _, _>("other", |_, _| async { Ok(()) }),
            Err(ClusterError::DuplicateTypeId(10))
        ));
    }

    #[test]
    fn protobuf_schema_accepts_unknown_fields() {
        let mut bytes = Ping { value: 7 }.encode_to_vec();
        bytes.extend_from_slice(&[0x10, 0x2a]);
        assert_eq!(Ping::decode(bytes.as_slice()).unwrap().value, 7);
    }

    #[test]
    fn direct_dependency_macros_register_handlers() {
        let registry = HandlerRegistry::from_auto().unwrap();
        assert!(
            registry
                .handlers
                .contains_key(&(ServiceKey::Name("direct".into()), DirectPing::TYPE_ID,))
        );
        assert!(registry.handlers.contains_key(&(
            ServiceKey::Name("direct-send".into()),
            DirectNotice::TYPE_ID,
        )));
    }
}
