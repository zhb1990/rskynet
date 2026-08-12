//! 内核的对外扩展接口。C 版没有对应文件——skynet 的 socket 线程与内核同住
//! 一个编译单元，`skynet_socket_*` 直接碰内部结构；这里网络层住在独立 crate
//! （`rskynet-net`）里，就必须有一套公开接口才进得来。
//!
//! 剩下的只有两件事，因为「跟着节点起落的线程」这件事已经由
//! [独占线程服务][crate::Exclusive] 承担了——它就是个服务，不需要另开扩展点：
//!
//! - [`NodeRef`]：从内核之外的线程往服务邮箱投消息，也就是 `skynet_context_push`
//!   那条路。独占服务自己起的子线程（连接线程、阻塞线程池）靠它回话。
//! - [`ReplyToken`]：让内核之外的线程能给一次 `call` 回包，于是服务侧能把
//!   「把活交给别的线程并等结果」写成一句 `await`。

use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::message::{Addr, MsgType, Payload};
use crate::server::Node;

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

    /// 节点是否已经收工。自己起的线程该盯着它决定何时退出。
    pub fn is_quit(&self) -> bool {
        self.node.sched.is_quit()
    }

    /// 宣布节点收工，对照 C 版的 `CHECK_ABORT`：置上收工标记并叫醒所有服务。
    ///
    /// 与 [`crate::Ctx::abort`] 不同，这里不摘除任何服务，只是让 worker 与独占
    /// 线程看到「该收摊了」。定时器实现用它来落实「最后一个服务退出即节点退出」。
    pub fn quit(&self) {
        self.node.quit();
    }

    /// 兜底唤醒：叫醒一个睡着的 worker 去运行队列里找活。
    ///
    /// 投递方漏叫的情况极少，但按节拍醒来的服务顺手 poke 一下，最坏也就是让
    /// 睡着的 worker 早一个 tick 睁眼，代价可以忽略。
    pub fn poke(&self) {
        self.node.sched.poke();
    }

    /// 本节点当前活着的服务数，对照 `skynet_context_total`。
    ///
    /// 保留服务（日志、定时器）不计入，所以这个数归零就意味着业务全退场了。
    pub fn service_count(&self) -> i64 {
        self.node.total()
    }

    /// 系统服务是否都已拉起。
    ///
    /// 「服务数为 0」有两个截然不同的时刻：引导还没出场，和一切都已收场。想据此
    /// 判断节点该收工的服务（定时器就是）必须先问一句这个，否则会把前者当成后者。
    pub fn is_booted(&self) -> bool {
        self.node.is_booted()
    }

    /// 节点启动至今的厘秒数，对照 `skynet.now`。
    pub fn now(&self) -> u64 {
        self.node.timer.now()
    }

    /// 写一条日志，走的是发给 logger 服务的那条路。
    pub fn log(&self, source: u32, text: impl Into<String>) {
        self.node.log(source, text.into());
    }

    /// 取属于自己的那一段配置，例如 `ctx.node().section::<NetConfig>("net")`。
    ///
    /// 段不存在时返回 `Ok(None)`，由服务自己决定是走默认值还是报错。服务的 `init`
    /// 只收得到一个字符串参数，成段的配置从这里来。
    pub fn section<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        self.node.config().section(name)
    }
}

impl std::fmt::Debug for NodeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRef").finish_non_exhaustive()
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
