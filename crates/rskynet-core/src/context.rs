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

use crate::error::{Error, Result};
use crate::ext::{NodeRef, ReplyToken};
use crate::message::{Addr, Message, MsgType, Payload};
use crate::server::ServiceContext;

/// 一个服务的实现。
///
/// 对照 skynet 里由 `skynet.start` 注册的初始化协程与 `skynet.dispatch` 注册的
/// 消息处理函数：[`Service::init`] 对应前者，[`Service::dispatch`] 对应后者。
///
/// 两个方法都返回 `BoxFuture` 而不是写成 `async fn`，因为内核需要
/// `dyn Service`（trait 里的 `async fn` 目前还不能做成 trait object）。
/// 应用代码通常使用 `#[rskynet::service]`，由宏生成这层底层实现。
///
/// ```ignore
/// struct Echo;
///
/// #[rskynet::service]
/// impl Echo {
///     async fn dispatch(&self, ctx: Ctx, msg: Message) {
///         let _ = ctx.reply(&msg, msg.payload);
///     }
/// }
/// ```
pub trait Service: Send + Sync + 'static {
    /// 服务启动时调用一次。允许 `await`（比如去 `call` 别的服务），公开的
    /// [`Ctx::launch`] 会等待本 Future 完整返回。
    /// 返回 `Err` 表示启动失败，服务会被立即销毁。
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        let _ = (ctx, args);
        Box::pin(async { Ok(()) })
    }

    /// 收到一条非应答消息时调用。每条消息都会开一个独立任务，
    /// 因此这里可以放心 `await`，不会挡住同服务的其它消息。
    fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()>;
}

/// 当前 service 的执行上下文。
///
/// 它必须是 `Send`，以便 service Future 在 worker 间迁移。`launch`、`kill`、
/// `abort`、名字与时间查询只是线程安全的 node 代理，可以跨线程调用；`call`、
/// `sleep`、`spawn`、`send`、`reply`、`exit` 等 service 本地接口则只能在运行时
/// 当前正执行这个 service 时调用。需要长期交给外部线程时应导出 [`NodeRef`]。
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

    /// 节点的线程安全把手。节点管理、时间、名字查询和观测接口都在这里。
    pub fn node(&self) -> NodeRef {
        NodeRef::new(self.inner.node.clone())
    }

    fn resolve(&self, addr: impl Into<Addr>) -> Result<u32> {
        self.inner.node.resolve(&addr.into())
    }

    /// 发一条消息，不等应答，对照 `skynet.send`。
    pub fn send(&self, dest: impl Into<Addr>, mtype: MsgType, payload: Payload) -> Result<()> {
        self.inner.assert_ownership();
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
        self.inner.assert_ownership();

        // 应答类型会直接唤醒收方等待中的 session，根本不进 dispatch；
        // 把它当请求发出去，session 数值碰撞时可能串到别人的回包上。
        // 因此必须在分配 session 之前拒绝。
        if mtype.is_reply() {
            return Err(Error::service(format!(
                "call 不能使用应答消息类型 {mtype:?}"
            )));
        }

        let dest = self.resolve(dest)?;
        let session = self.inner.sessions.alloc();
        let call = Call {
            ctx: &self.inner,
            session,
            finished: false,
        };
        // 先建 Call 再发送：发送失败或 send_raw 意外 panic 时，Call 的 Drop
        // 都会注销 session，不会在表里留下孤儿。
        self.inner
            .node
            .send_raw(self.handle(), dest, mtype, session, payload)?;
        call.await
    }

    /// `call` 的 `MsgType::USER` 快捷写法。
    pub async fn request(&self, dest: impl Into<Addr>, payload: Payload) -> Result<Payload> {
        self.call(dest, MsgType::USER, payload).await
    }

    /// 向内核之外的线程发起一次请求并等它回话。
    ///
    /// 与 [`Ctx::call`] 的区别只在对端不是服务：闭包收到一个
    /// [`ReplyToken`]，把它交给别的线程（socket 线程、线程池、C 库的回调……），
    /// 那边办完事调 [`ReplyToken::reply`]，这里的 `await` 就醒过来。
    /// 挂起期间本服务照常处理其它消息，与 `call` 一模一样。
    ///
    /// 这是网络层「下一条命令给 socket 线程，等它返回结果」的底座。
    ///
    /// ```ignore
    /// let reply = ctx.call_external(|token| {
    ///     commands.send(Command::Listen { addr, token });
    /// }).await?;
    /// ```
    ///
    /// 闭包在返回前就被调用，所以对端抢先回话也不会丢——回包会存在 session 表里
    /// 等第一次 poll 来取。token 被丢弃而没回话时，这里会得到
    /// [`crate::Error::CallFailed`] 而不是永久挂起。
    pub async fn call_external<F>(&self, f: F) -> Result<Payload>
    where
        F: FnOnce(ReplyToken),
    {
        self.inner.assert_ownership();
        let session = self.inner.sessions.alloc();
        let call = Call {
            ctx: &self.inner,
            session,
            finished: false,
        };
        // Call 在 f 之前建立：f panic 时 Drop 会注销 session。
        f(ReplyToken::new(
            self.inner.node.clone(),
            self.handle(),
            session,
        ));
        call.await
    }

    /// 应答一条请求，对照 `skynet.ret`。
    ///
    /// 请求方没带 session（即 `send` 而非 `call`）时静默忽略。
    pub fn reply(&self, request: &Message, payload: Payload) -> Result<()> {
        self.inner.assert_ownership();
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
        self.inner.assert_ownership();
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
        self.inner.assert_ownership();
        self.inner.spawn(Box::pin(future));
    }

    /// 挂起指定毫秒数。具体时间源可以采用更粗的内部格度，但不得提前唤醒。
    ///
    /// 挂表交给注入的 [`crate::Timer`]，到期时它投一条 `RESPONSE` 回来。挂表本身
    /// 从节点建起来那一刻就可用，哪怕推刻度的那条线程还没上线。
    pub async fn sleep(&self, millis: u32) {
        self.inner.assert_ownership();
        let session = self.inner.sessions.alloc();
        let call = Call {
            ctx: &self.inner,
            session,
            finished: false,
        };
        // Timer::timeout 由外部实现，panic 时同样不能留下孤儿 session。
        self.inner.node.timeout(self.handle(), millis, session);
        let _ = call.await;
    }

    /// 按毫秒挂起，超过 `u32` 可表达范围时饱和到最大值。
    pub async fn sleep_ms(&self, millis: u64) {
        self.sleep(millis.min(u32::MAX as u64) as u32).await;
    }

    /// 让出一次调度，对照 `skynet.yield`：把后面的活儿排到当前就绪队列尾部。
    pub async fn yield_now(&self) {
        self.sleep(0).await;
    }

    /// 启动一个新 service 并等待其 init Future 完整成功。
    ///
    /// 这是 [`NodeRef::launch`] 的便捷代理，可跨线程调用。
    pub async fn launch(&self, kind: &str, args: impl AsRef<str>) -> Result<u32> {
        let service = self.inner.node.new_service(kind, args.as_ref())?;
        service.init.await?;
        Ok(service.handle)
    }

    /// 给自己注册一个本地名字，对照 `skynet.register`。名字已被占用时返回 false。
    pub fn register_name(&self, name: &str) -> bool {
        self.inner.assert_ownership();
        self.inner
            .node
            .handles
            .register_name(self.handle(), name.trim_start_matches('.'))
    }

    /// 按本地注册名查 handle，是 [`NodeRef::query_name`] 的便捷代理。
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
        self.inner.assert_ownership();
        self.inner.request_exit();
    }

    /// 摘除一个 service，是 [`NodeRef::kill`] 的便捷代理。
    pub fn kill(&self, dest: impl Into<Addr>) -> bool {
        match self.inner.node.resolve(&dest.into()) {
            Ok(handle) => self.inner.node.retire(handle),
            Err(_) => false,
        }
    }

    /// 摘除全部非 reserved service，是 [`NodeRef::abort`] 的便捷代理。
    pub fn abort(&self) {
        self.inner.node.retire_all();
    }

    /// 节点启动至今的毫秒数，是 [`NodeRef::now`] 的便捷代理。
    pub fn now(&self) -> u64 {
        self.inner.node.timer.now()
    }

    /// 当前 unix 时间，单位毫秒，是 [`NodeRef::time`] 的便捷代理。
    pub fn time(&self) -> u64 {
        self.inner.node.timer.wall_clock()
    }

    /// 节点启动时刻的 unix 时间，单位毫秒，是 [`NodeRef::start_time`] 的便捷代理。
    pub fn start_time(&self) -> u64 {
        self.inner.node.timer.start_time()
    }

    /// 写一条日志，对照 `skynet.error`：日志本身也是发给 logger 服务的消息。
    pub fn log(&self, text: impl Into<String>) {
        self.inner.assert_ownership();
        self.inner.node.log(self.handle(), text.into());
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
/// 被取消（外层任务提前销毁）时在 Drop 里直接注销 session；session 是 u64
/// 单调编号，迟到回包只会查无此号并被丢弃。
struct Call<'a> {
    ctx: &'a ServiceContext,
    session: u64,
    finished: bool,
}

impl Future for Call<'_> {
    type Output = Result<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ctx.assert_ownership();
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
            if std::thread::panicking() && !self.ctx.owns_current_thread() {
                return;
            }
            self.ctx.assert_ownership();
            self.ctx.sessions.abandon(self.session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::{dummy_context_on, test_node};
    use futures_util::task::noop_waker;

    /// `call` 只能发请求类型：RESPONSE / ERROR 会在收端直接命中 session 表，
    /// 不能带着新分配的 session 冒充业务请求。
    #[test]
    fn call_rejects_reply_message_types() {
        let node = test_node();
        let ctx = node
            .handles
            .register_with(|handle| dummy_context_on(node.clone(), handle));
        let cx = Ctx::new(ctx.clone());

        ctx.with_ownership(|| {
            for mtype in [MsgType::RESPONSE, MsgType::ERROR] {
                let future = cx.call(ctx.handle, mtype, Payload::None);
                let error = futures_util::FutureExt::now_or_never(future)
                    .expect("拒绝应同步完成，不需要等待")
                    .expect_err("应答类型必须被拒绝");
                assert!(matches!(error, Error::Service(_)));
            }

            // USER 不受影响：应该正常分配 session 并把消息发出去
            let mut future = Box::pin(cx.call(ctx.handle, MsgType::USER, Payload::None));
            let waker = noop_waker();
            let mut task_cx = Context::from_waker(&waker);
            assert!(future.as_mut().poll(&mut task_cx).is_pending());

            let sent = ctx.mailbox.drain();
            assert_eq!(sent.len(), 1, "USER call 应已发出消息");
            let msg = &sent[0];
            assert_eq!(msg.mtype, MsgType::USER);
            assert_ne!(msg.session, 0, "call 应分配 session");

            // 回包到达后，call 应正常返回
            ctx.sessions
                .complete(msg.session, Ok(Payload::text("pong")));
            match future.as_mut().poll(&mut task_cx) {
                Poll::Ready(Ok(payload)) => assert_eq!(payload.as_str(), Some("pong")),
                other => panic!("USER call 应正常收到回包，实际 {other:?}"),
            }
        });
    }
}
