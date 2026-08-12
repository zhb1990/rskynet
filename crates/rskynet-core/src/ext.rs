//! 内核的对外扩展接口。C 版没有对应文件——skynet 的 socket 线程与内核同住
//! 一个编译单元，`skynet_socket_*` 直接碰内部结构；这里网络层住在独立 crate
//! （`rskynet-net`）里，就必须有一套公开接口才进得来。
//!
//! 三件事凑成一套完整的扩展能力：
//!
//! - [`NodeRef`]：从外部线程往服务邮箱投消息，也就是 `skynet_context_push` 那条路。
//! - [`Plugin`]：跟着节点一起起落的自有线程，对照 C 版的 `thread_socket`。
//! - [`ReplyToken`]：让外部线程能给一次 `call` 回包，于是服务侧能把「向 socket
//!   线程下个命令并等结果」写成一句 `await`。

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::message::{Addr, MsgType, Payload};
use crate::server::Node;
use crate::start::Config;

/// 扩展槽的内容：插件在 [`Plugin::init`] 里交出来，之后全节点只读。
pub(crate) type Extensions = HashMap<TypeId, Arc<dyn Any + Send + Sync>>;

/// 节点的对外把手，扩展代码访问内核的唯一入口。
///
/// 刻意做成 newtype 而不是把 `Node` 本身公开：`Node` 的字段是邮箱、调度器、
/// handle 表这些内部结构，一旦公开就再也改不动了。这里只开放外部真正需要的
/// 那几件事。
///
/// 克隆代价是一次 `Arc::clone`，可以随手交给自己起的线程。
#[derive(Clone)]
pub struct NodeRef {
    node: Arc<Node>,
}

impl NodeRef {
    pub(crate) fn new(node: Arc<Node>) -> Self {
        Self { node }
    }

    /// 往服务邮箱投一条消息，对照 `skynet_send`。
    ///
    /// `source` 填 0 表示「来自内核」，定时器与 socket 事件都用它。
    /// `session` 非 0 且 `mtype` 是 `RESPONSE` / `ERROR` 时，收方那个挂起的
    /// `call` 会被直接唤醒，不经过 `dispatch`。
    pub fn send(
        &self,
        source: u32,
        dest: u32,
        mtype: MsgType,
        session: i32,
        payload: Payload,
    ) -> Result<()> {
        self.node.send_raw(source, dest, mtype, session, payload)
    }

    /// 地址解析，对照 `skynet_queryname`。名字表是快照读，不抢锁。
    pub fn resolve(&self, addr: &Addr) -> Result<u32> {
        self.node.resolve(addr)
    }

    /// 节点是否已经收工。插件线程的主循环应当盯着它。
    pub fn is_quit(&self) -> bool {
        self.node.sched.is_quit()
    }

    /// 节点启动至今的厘秒数，对照 `skynet.now`。
    pub fn now(&self) -> u64 {
        self.node.timer.now()
    }

    /// 写一条日志，走的是发给 logger 服务的那条路。
    pub fn log(&self, source: u32, text: impl Into<String>) {
        self.node.log(source, text.into());
    }

    /// 取某个插件在 [`Plugin::init`] 里登记的那个对象。
    ///
    /// 这是扩展 trait 拿自己那份状态的标准姿势：网络层的
    /// `SocketExt for Ctx` 就靠它从 `Ctx` 摸到 socket 线程的句柄。
    pub fn extension<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        self.node
            .extensions()?
            .get(&TypeId::of::<T>())?
            .clone()
            .downcast::<T>()
            .ok()
    }
}

impl std::fmt::Debug for NodeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRef").finish_non_exhaustive()
    }
}

/// 一条跟着节点起落的自有线程，对照 C 版 `skynet_start.c` 里的 `thread_socket`。
///
/// 三个钩子的时序：
///
/// 1. [`Plugin::init`]：worker 与任何服务都还没起来，用来建资源。返回值进扩展槽，
///    随后服务就能在自己的 `init` 里通过 [`NodeRef::extension`] 取到它。
/// 2. [`Plugin::run`]：在 [`crate::start`] 的线程作用域里独占一条线程。
/// 3. [`Plugin::shutdown`]：worker 与定时器线程都已收工，用来把 `run` 那条线程
///    从阻塞里叫醒（网络层就是敲 mio 的 `Waker`）。**只有它返回后 `run` 那条
///    线程才会被 join**，所以这里必须真的把线程叫醒，否则 `start` 永远不返回。
///
/// `init` 返回 `Err` 会让 `start` 当场带着这个错误返回，`run` 与 `shutdown` 都不
/// 会再被调用——那时插件线程还没起来，没有需要收的尾。
pub trait Plugin: Send + Sync + 'static {
    /// 插件名，用于线程名与日志。
    fn name(&self) -> &str;

    /// 建资源。返回 `Some(x)` 就把 `x` 放进扩展槽，键是它的具体类型。
    fn init(&self, node: &NodeRef, config: &Config) -> Result<Option<Arc<dyn Any + Send + Sync>>> {
        let _ = (node, config);
        Ok(None)
    }

    /// 插件自己的主循环，跑在独占线程上。应当以 [`NodeRef::is_quit`] 为退出条件。
    fn run(&self, node: NodeRef) {
        let _ = node;
    }

    /// 叫醒 [`Plugin::run`] 那条线程，让它看到 `is_quit` 并返回。
    fn shutdown(&self, node: &NodeRef) {
        let _ = node;
    }
}

/// 一次「等外部线程回话」的凭据，由 [`crate::Ctx::call_external`] 交出来。
///
/// 它就是 skynet 里 socket 线程回一条带 session 的消息给发起方那件事，只是把
/// 「谁在等、等的是哪个 session」打成了一个可以跨线程搬的对象。
///
/// 析构时若还没回过话，会自动补一个错误回包——外部线程半路撂挑子（连接线程
/// panic、命令队列被清空）时，发起方那个 `await` 不至于永久挂着。
pub struct ReplyToken {
    node: Arc<Node>,
    dest: u32,
    /// 回过话就置 0，兼作「已完成」标记，好让析构时不再补发。
    session: i32,
}

impl ReplyToken {
    pub(crate) fn new(node: Arc<Node>, dest: u32, session: i32) -> Self {
        Self {
            node,
            dest,
            session,
        }
    }

    /// 等待方的地址，日志与索引用。
    pub fn dest(&self) -> u32 {
        self.dest
    }

    /// 回一个正常结果，对方的 `call_external` 得到 `Ok(payload)`。
    pub fn reply(mut self, payload: Payload) {
        self.finish(MsgType::RESPONSE, payload);
    }

    /// 回一个错误，对方得到 [`crate::Error::CallFailed`]。
    pub fn fail(mut self) {
        self.finish(MsgType::ERROR, Payload::None);
    }

    fn finish(&mut self, mtype: MsgType, payload: Payload) {
        let session = std::mem::replace(&mut self.session, 0);
        if session == 0 {
            return;
        }
        // source 填 0：这条回包来自内核之外，与定时器回包同一个身份
        let _ = self.node.send_raw(0, self.dest, mtype, session, payload);
    }
}

impl Drop for ReplyToken {
    fn drop(&mut self) {
        self.finish(MsgType::ERROR, Payload::None);
    }
}

impl std::fmt::Debug for ReplyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReplyToken(:{:08x} #{})", self.dest, self.session)
    }
}
