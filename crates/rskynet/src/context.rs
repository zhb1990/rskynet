//! 服务视角的 API，对照 `lualib/skynet.lua`。
//!
//! skynet 用一张字符串命令表（`skynet_command` 的 `"LAUNCH"` / `"REG"` / `"EXIT"`…）
//! 让 Lua 层驱动内核，Rust 直接用 [`Ctx`] 上的类型化方法，编译期就能查错。
//!
//! 最关键的是 [`Ctx::call`]：它把「发请求 - 挂起 - 等回包 - 继续」写成一句
//! `await`，这正是 skynet 用协程做到的事，只是承载体换成了 `Future`。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;

use crate::error::Result;
use crate::message::{Addr, Message, MsgType, Payload};
use crate::server::ServiceContext;

/// 一个服务的实现。
///
/// 对照 skynet 里由 `skynet.start` 注册的初始化协程与 `skynet.dispatch` 注册的
/// 消息处理函数：[`Service::init`] 对应前者，[`Service::dispatch`] 对应后者。
///
/// 两个方法都返回 `BoxFuture` 而不是写成 `async fn`，因为内核需要
/// `dyn Service`（trait 里的 `async fn` 目前还不能做成 trait object）。
/// 实现时把函数体裹进 `Box::pin(async move { ... })` 即可。
///
/// ```ignore
/// struct Echo;
///
/// impl Service for Echo {
///     fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()> {
///         Box::pin(async move {
///             let _ = ctx.reply(&msg, msg.payload);
///         })
///     }
/// }
/// ```
pub trait Service: Send + Sync + 'static {
    /// 服务启动时调用一次。允许 `await`（比如去 `call` 别的服务），
    /// 挂起后 `launch` 就会返回，剩下的初始化在后台继续。
    /// 返回 `Err` 表示启动失败，服务会被立即销毁。
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        let _ = (ctx, args);
        Box::pin(async { Ok(()) })
    }

    /// 收到一条非应答消息时调用。每条消息都会开一个独立任务，
    /// 因此这里可以放心 `await`，不会挡住同服务的其它消息。
    fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()>;
}

/// 服务句柄：既是自己的身份，也是访问内核的唯一入口。
#[derive(Clone)]
pub struct Ctx {
    pub(crate) inner: Arc<ServiceContext>,
}

impl Ctx {
    pub(crate) fn new(inner: Arc<ServiceContext>) -> Self {
        Self { inner }
    }

    /// 自己的地址。
    pub fn handle(&self) -> u32 {
        self.inner.handle
    }

    /// 自己的服务类型名。
    pub fn kind(&self) -> &str {
        &self.inner.kind
    }

    /// 本节点的 harbor 编号，即所有 handle 高 8 位的那个值。
    pub fn harbor(&self) -> u32 {
        self.inner.node.handles.harbor()
    }

    fn resolve(&self, addr: impl Into<Addr>) -> Result<u32> {
        self.inner.node.resolve(&addr.into())
    }

    /// 发一条消息，不等应答，对照 `skynet.send`。
    pub fn send(&self, dest: impl Into<Addr>, mtype: MsgType, payload: Payload) -> Result<()> {
        let dest = self.resolve(dest)?;
        self.inner
            .node
            .send_raw(self.handle(), dest, mtype, 0, payload)
    }

    /// 发一条 `MsgType::USER` 消息，业务代码最常用的那种。
    pub fn post(&self, dest: impl Into<Addr>, payload: Payload) -> Result<()> {
        self.send(dest, MsgType::USER, payload)
    }

    /// 发请求并等应答，对照 `skynet.call`。
    ///
    /// 挂起期间本服务照常处理其它消息——这正是 skynet 的核心手法：
    /// 阻塞的是当前这个「协程」，不是整个服务。
    pub async fn call(
        &self,
        dest: impl Into<Addr>,
        mtype: MsgType,
        payload: Payload,
    ) -> Result<Payload> {
        let dest = self.resolve(dest)?;
        let session = self.inner.sessions.alloc();
        if let Err(err) = self
            .inner
            .node
            .send_raw(self.handle(), dest, mtype, session, payload)
        {
            self.inner.sessions.abandon(session);
            return Err(err);
        }
        Call {
            ctx: &self.inner,
            session,
            finished: false,
        }
        .await
    }

    /// `call` 的 `MsgType::USER` 快捷写法。
    pub async fn request(&self, dest: impl Into<Addr>, payload: Payload) -> Result<Payload> {
        self.call(dest, MsgType::USER, payload).await
    }

    /// 应答一条请求，对照 `skynet.ret`。
    ///
    /// 请求方没带 session（即 `send` 而非 `call`）时静默忽略。
    pub fn reply(&self, request: &Message, payload: Payload) -> Result<()> {
        if !request.needs_reply() {
            return Ok(());
        }
        self.inner.node.send_raw(
            self.handle(),
            request.source,
            MsgType::RESPONSE,
            request.session,
            payload,
        )
    }

    /// 告诉请求方「这活儿办不了」，对方的 `call` 会拿到 [`crate::Error::CallFailed`]。
    pub fn reply_error(&self, request: &Message) -> Result<()> {
        if !request.needs_reply() {
            return Ok(());
        }
        self.inner.node.send_raw(
            self.handle(),
            request.source,
            MsgType::ERROR,
            request.session,
            Payload::None,
        )
    }

    /// 在本服务内起一个并发任务，对照 `skynet.fork`。
    ///
    /// 任务与消息处理共享同一套调度，彼此之间仍然是单线程的，
    /// 所以服务状态不需要加锁（用 [`crate::SvcCell`] 就够）。
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.inner.spawn(Box::pin(future));
    }

    /// 挂起 `ticks` 厘秒（10ms 一格），对照 `skynet.sleep`。
    pub async fn sleep(&self, ticks: u32) {
        let session = self.inner.sessions.alloc();
        self.inner.node.timeout(self.handle(), ticks, session);
        let _ = Call {
            ctx: &self.inner,
            session,
            finished: false,
        }
        .await;
    }

    /// 按毫秒挂起，内部换算成厘秒（向上取整，至少一格）。
    pub async fn sleep_ms(&self, millis: u64) {
        let ticks = millis.div_ceil(10).min(u32::MAX as u64) as u32;
        self.sleep(ticks).await;
    }

    /// 让出一次调度，对照 `skynet.yield`：把后面的活儿排到当前就绪队列尾部。
    pub async fn yield_now(&self) {
        self.sleep(0).await;
    }

    /// 启动一个新服务，对照 `skynet.newservice`。
    ///
    /// 新服务的 `init` 会在当前线程上先跑到第一次挂起，之后本调用返回。
    pub async fn launch(&self, kind: &str, args: impl AsRef<str>) -> Result<u32> {
        self.inner.node.new_service(kind, args.as_ref())
    }

    /// 给自己注册一个本地名字，对照 `skynet.register`。名字已被占用时返回 false。
    pub fn register_name(&self, name: &str) -> bool {
        self.inner
            .node
            .handles
            .register_name(self.handle(), name.trim_start_matches('.'))
    }

    /// 按名字查地址，对照 `skynet.localname`。
    pub fn query_name(&self, name: &str) -> Option<u32> {
        self.inner
            .node
            .handles
            .find_name(name.trim_start_matches('.'))
    }

    /// 退出自己，对照 `skynet.exit`。
    ///
    /// 返回后当前任务仍会继续跑到下一次挂起点，真正的资源释放发生在那之后。
    pub fn exit(&self) {
        self.inner.node.retire(self.handle());
    }

    /// 干掉别的服务，对照 `skynet.kill`。
    pub fn kill(&self, dest: impl Into<Addr>) -> bool {
        match self.resolve(dest) {
            Ok(handle) => self.inner.node.retire(handle),
            Err(_) => false,
        }
    }

    /// 关停整个节点，对照 `skynet.abort`：干掉所有服务，`start` 随之返回。
    pub fn abort(&self) {
        self.inner.node.retire_all();
    }

    /// 节点启动至今的厘秒数，对照 `skynet.now`。
    pub fn now(&self) -> u64 {
        self.inner.node.timer.now()
    }

    /// 当前 unix 时间，单位秒，对照 `skynet.time`。
    pub fn time(&self) -> f64 {
        self.inner.node.timer.wall_clock() as f64 / 100.0
    }

    /// 节点启动时刻的 unix 时间（秒），对照 `skynet.starttime`。
    pub fn start_time(&self) -> u64 {
        self.inner.node.timer.start_seconds()
    }

    /// 写一条日志，对照 `skynet.error`：日志本身也是发给 logger 服务的消息。
    pub fn log(&self, text: impl Into<String>) {
        self.inner.node.log(self.handle(), text.into());
    }

    /// 自己邮箱里积压的消息数，对照 `skynet.stat("mqlen")`。
    pub fn mailbox_len(&self) -> usize {
        self.inner.mailbox.len()
    }

    /// 自己处理过的消息总数，对照 `skynet.stat("message")`。
    pub fn message_count(&self) -> u64 {
        self.inner.message_count()
    }

    /// 服务内当前活着的任务数（含正在等应答的），用于观察并发情况。
    pub fn task_count(&self) -> usize {
        self.inner.task_count()
    }

    /// 自己发出去、还没收到回包的请求数。
    pub fn pending_calls(&self) -> usize {
        self.inner.sessions.pending()
    }

    /// 本服务累计占用 worker 的时长，对照 `skynet.stat("cpu")`。
    /// 需要配置里打开 `profile`（默认开）。
    pub fn cpu_cost(&self) -> std::time::Duration {
        self.inner.cpu_cost()
    }

    /// 本节点当前活着的服务数，对照 `skynet_context_total`。
    pub fn service_count(&self) -> i64 {
        self.inner.node.total()
    }

    /// 全局队列里排队的服务数，观察负载用。
    pub fn runnable_services(&self) -> usize {
        self.inner.node.global.len()
    }
}

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ctx(:{:08x} {})", self.handle(), self.kind())
    }
}

/// 等一个 session 的回包。
///
/// 对照 skynet 里 `session_id_coroutine[session] = co` 之后的那次 `coroutine.yield`。
/// 被取消（外层任务提前销毁）时在 Drop 里注销 session，迟到的回包会被直接丢弃，
/// 对应 skynet 把表项置成 `false` 的做法。
struct Call<'a> {
    ctx: &'a ServiceContext,
    session: i32,
    finished: bool,
}

impl Future for Call<'_> {
    type Output = Result<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.ctx.sessions.poll(self.session, cx.waker()) {
            Poll::Ready(result) => {
                self.finished = true;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for Call<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.ctx.sessions.abandon(self.session);
        }
    }
}
