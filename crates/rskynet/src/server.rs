//! 服务上下文与节点，对照 `skynet-src/skynet_server.c`。
//!
//! [`ServiceContext`] 等价于 C 版的 `struct skynet_context`，[`Node`] 等价于
//! `struct skynet_node` 加上那几个全局单例（handle 表、全局队列、定时器）。
//! C 版用文件级静态变量，这里收进一个 `Arc<Node>`，于是同进程可以跑多个互不
//! 干扰的节点，单元测试也就能并行跑了。
//!
//! # 生命周期与循环引用
//!
//! `ServiceContext` -> `TaskSet` -> `Future` -> `Ctx` -> `ServiceContext` 是一条
//! 真实存在的循环引用。它由销毁流程显式打断：服务被 kill 后，
//! [`Node::destroy`] 会清空任务集，循环随之断开，`Arc` 计数归零。
//! 这与 skynet 里 `delete_context` 释放 Lua 虚拟机、连带干掉所有协程是一个道理。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use futures_util::future::BoxFuture;
use parking_lot::Mutex;

use crate::context::{Ctx, Service};
use crate::error::{Error, Result};
use crate::handle::HandleStorage;
use crate::message::{Addr, Message, MsgType, Payload};
use crate::module::Registry;
use crate::mq::{GlobalQueue, Mailbox, Work};
use crate::session::SessionTable;
use crate::start::Config;
use crate::task::TaskSet;
use crate::timer::Timer;

/// 一个服务的运行时上下文。
pub(crate) struct ServiceContext {
    pub(crate) handle: u32,
    /// 服务类型名，即注册表里的键，只用于日志。
    pub(crate) kind: String,
    pub(crate) node: Arc<Node>,
    pub(crate) mailbox: Mailbox,
    pub(crate) sessions: SessionTable,
    service: Arc<dyn Service>,
    tasks: TaskSet,
    /// 正在处理中的请求：任务 id -> (请求方, session)。
    /// 服务半途退出时要给这些请求方回一个错误，否则对方的 `call` 永远挂着。
    /// 对照 `lualib/skynet.lua` 里 `skynet.exit` 遍历 `session_coroutine_id` 的那段。
    inflight: Mutex<HashMap<usize, (u32, i32)>>,
    /// 已被摘出 handle 表，等待某个 worker 领走做销毁。
    dead: AtomicBool,
    /// 保留服务：不计入退出条件，且要留到最后才释放。
    /// 对照 `skynet_context_reserve`（C 版用它保住 harbor 服务），logger 用的就是它
    /// ——否则关停时它邮箱里积压的日志会跟着一起被丢掉。
    reserved: AtomicBool,
    /// 销毁记账是否已完成，保证 `total` 只减一次。
    destroyed: AtomicBool,
    message_count: AtomicU64,
    /// 累计占用 worker 的时间，单位微秒；仅在 `profile` 打开时统计。
    cpu_cost: AtomicU64,
    /// 指向自己，用于把自己推进全局队列、以及给任务 waker 用。
    me: Weak<ServiceContext>,
}

impl ServiceContext {
    fn arc(&self) -> Arc<ServiceContext> {
        self.me.upgrade().expect("服务上下文正在被释放")
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Acquire)
    }

    pub(crate) fn is_reserved(&self) -> bool {
        self.reserved.load(Ordering::Acquire)
    }

    fn mark_dead(&self) {
        self.dead.store(true, Ordering::Release);
    }

    /// 投递一条消息。这是所有消息进入服务的唯一入口。
    pub(crate) fn push(&self, msg: Message) {
        if self.mailbox.push_message(msg) {
            self.node.global.push(self.arc());
        }
    }

    /// 唤醒服务内的某个任务，与消息投递走同一条「进入全局队列」的路径。
    pub(crate) fn wake_task(&self, task: usize) {
        if self.mailbox.push_ready(task) {
            self.node.global.push(self.arc());
        }
    }

    /// 起一个服务内任务，等价于 skynet 的 `skynet.fork`。
    pub(crate) fn spawn(&self, future: BoxFuture<'static, ()>) -> usize {
        let task = self.tasks.insert(&self.me, future);
        self.wake_task(task);
        task
    }

    pub(crate) fn task_count(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) fn message_count(&self) -> u64 {
        self.message_count.load(Ordering::Relaxed)
    }

    /// 累计占用 worker 的时长，对照 `skynet.stat("cpu")`。
    pub(crate) fn cpu_cost(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.cpu_cost.load(Ordering::Relaxed))
    }

    /// 处理一条消息，对照 C 版 `dispatch_message` 与 skynet.lua 的
    /// `raw_dispatch_message`：应答消息唤醒挂起的 session，其余消息开一个新任务。
    fn handle_message(&self, mut msg: Message) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
        if msg.mtype.is_reply() && msg.session != 0 {
            let result = if msg.mtype == MsgType::ERROR {
                Err(Error::CallFailed(msg.source))
            } else {
                Ok(msg.take_payload())
            };
            self.sessions.complete(msg.session, result);
            return;
        }
        let (source, session) = (msg.source, msg.session);
        let ctx = Ctx::new(self.arc());
        // 这里只是把任务排进就绪队列，真正的 poll 在本函数返回之后，
        // 所以登记 inflight 不会漏掉已经完成的任务
        let task = self.spawn(self.service.clone().dispatch(ctx, msg));
        if session != 0 && source != 0 {
            self.inflight.lock().insert(task, (source, session));
        }
    }

    /// poll 一个就绪任务。
    ///
    /// Future 在 poll 期间被移出槽位，这样任务在 poll 中再 `spawn` 新任务也不会
    /// 撞上嵌套借用。
    fn poll_task(&self, task: usize) {
        let Some((mut future, waker)) = self.tasks.take(task) else {
            // 任务已完成，唤醒迟到了；Future 契约允许这种无害的多余唤醒
            return;
        };
        let mut cx = std::task::Context::from_waker(&waker);
        let started = self.node.profile().then(std::time::Instant::now);
        let result = future.as_mut().poll(&mut cx);
        if let Some(started) = started {
            self.cpu_cost
                .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        match result {
            std::task::Poll::Ready(()) => {
                self.inflight.lock().remove(&task);
                self.tasks.remove(task);
            }
            std::task::Poll::Pending => self.tasks.restore(task, future),
        }
    }

    /// 只把就绪任务推进到全部挂起，不碰邮箱。服务初始化阶段用它。
    fn drain_ready(&self) {
        while let Some(task) = self.mailbox.take_ready() {
            self.poll_task(task);
        }
    }

    /// 给所有「正在处理中」的请求回错误，对照 `skynet.exit` 里遍历
    /// `session_coroutine_id` 逐个 `PTYPE_ERROR` 的那段。
    ///
    /// 必须在退出的**那一刻**就发，不能拖到销毁时：服务往往是在自己的 dispatch
    /// 任务里调 `exit` 的，那个任务随后会正常返回，inflight 记录也就跟着被当成
    /// 「已办完」清掉了，届时再想通知请求方已经晚了。
    ///
    /// 即使该请求其实已经应答过也无妨：请求方的 session 早已销毁，迟到的错误包
    /// 会被直接丢弃。
    pub(crate) fn fail_inflight(&self) {
        let inflight: Vec<(u32, i32)> = self
            .inflight
            .lock()
            .drain()
            .map(|(_, request)| request)
            .collect();
        for (source, session) in inflight {
            let _ = self
                .node
                .send_raw(self.handle, source, MsgType::ERROR, session, Payload::None);
        }
    }

    /// 销毁前的清理，幂等。
    fn cleanup(&self) {
        self.fail_inflight();
        // 邮箱里没来得及处理的请求同样要回个错误
        for msg in self.mailbox.drain() {
            if msg.needs_reply() && msg.source != 0 && !msg.mtype.is_reply() {
                let _ = self.node.send_raw(
                    self.handle,
                    msg.source,
                    MsgType::ERROR,
                    msg.session,
                    Payload::None,
                );
            }
        }
        // 先丢任务再清 session 表：任务里的 `Call` 析构时还会来注销 session
        let tasks = self.tasks.drain();
        drop(tasks);
        self.sessions.clear();
        // 让清理之后迟到的消息还能把自己重新推进全局队列，从而被再清一次
        self.mailbox.reset_in_global();
    }
}

/// 一个 rskynet 节点。
pub(crate) struct Node {
    pub(crate) global: GlobalQueue,
    pub(crate) handles: HandleStorage,
    pub(crate) timer: Timer,
    modules: Registry,
    /// 活着的服务数，归零即整个节点退出，对照 `skynet_context_total`。
    total: AtomicI64,
    /// logger 服务的 handle，0 表示还没起来。
    logger: AtomicU32,
    profile: bool,
}

impl Node {
    pub(crate) fn new(config: &Config, modules: Registry) -> Arc<Node> {
        Arc::new(Node {
            global: GlobalQueue::new(),
            handles: HandleStorage::new(config.harbor),
            timer: Timer::new(),
            modules,
            total: AtomicI64::new(0),
            logger: AtomicU32::new(0),
            profile: config.profile,
        })
    }

    pub(crate) fn total(&self) -> i64 {
        self.total.load(Ordering::Acquire)
    }

    pub(crate) fn profile(&self) -> bool {
        self.profile
    }

    pub(crate) fn set_logger(&self, handle: u32) {
        self.logger.store(handle, Ordering::Release);
    }

    /// 把服务标成保留服务，对照 `skynet_context_reserve`：
    /// 不计入退出条件（否则节点永远等不到服务数归零），并且留到最后才释放。
    pub(crate) fn reserve(&self, handle: u32) {
        if let Some(ctx) = self.handles.grab(handle) {
            if !ctx.reserved.swap(true, Ordering::AcqRel) {
                self.total.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// 地址解析，对照 `skynet_queryname`。
    pub(crate) fn resolve(&self, addr: &Addr) -> Result<u32> {
        match addr {
            Addr::Handle(handle) => Ok(*handle),
            Addr::Name(name) => self
                .handles
                .find_name(name)
                .ok_or_else(|| Error::NameNotFound(name.clone())),
        }
    }

    /// 发一条消息，对照 `skynet_send`。
    pub(crate) fn send_raw(
        &self,
        source: u32,
        dest: u32,
        mtype: MsgType,
        session: i32,
        payload: Payload,
    ) -> Result<()> {
        let ctx = self.handles.grab(dest).ok_or(Error::NoService(dest))?;
        ctx.push(Message::new(source, session, mtype, payload));
        Ok(())
    }

    /// 创建服务，对照 `skynet_context_new`。
    ///
    /// 初始化被当成服务的第一个任务，并在调用者线程上就地推进到它第一次挂起
    /// ——此时 `in_global` 自创建起就是置位的，没有任何 worker 能同时碰到这个
    /// 服务，所以就地 poll 不会破坏「一服务一线程」的不变量。
    /// 这也复刻了 skynet 的行为：`skynet.start` 注册的初始化协程会先跑到第一次
    /// yield，`launch` 才返回。
    pub(crate) fn new_service(self: &Arc<Self>, kind: &str, args: &str) -> Result<u32> {
        let factory = self
            .modules
            .get(kind)
            .ok_or_else(|| Error::UnknownService(kind.to_string()))?;
        let service = factory();

        let node = self.clone();
        let kind_name = kind.to_string();
        let instance = service.clone();
        let ctx = self.handles.register_with(move |handle| {
            Arc::new_cyclic(|me| ServiceContext {
                handle,
                kind: kind_name,
                node,
                mailbox: Mailbox::new(),
                sessions: SessionTable::new(),
                service: instance,
                tasks: TaskSet::new(),
                inflight: Mutex::new(HashMap::new()),
                dead: AtomicBool::new(false),
                reserved: AtomicBool::new(false),
                destroyed: AtomicBool::new(false),
                message_count: AtomicU64::new(0),
                cpu_cost: AtomicU64::new(0),
                me: me.clone(),
            })
        });
        let handle = ctx.handle;
        self.total.fetch_add(1, Ordering::AcqRel);

        // 初始化失败的原因要能同步带回给 launch 的调用方
        let failure: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));
        {
            let slot = failure.clone();
            let cx = Ctx::new(ctx.clone());
            let args = args.to_string();
            ctx.spawn(Box::pin(async move {
                if let Err(err) = service.init(cx.clone(), args).await {
                    cx.log(format!("error: 初始化失败：{err}"));
                    *slot.lock() = Some(err);
                    // 初始化失败的服务不该留在世上；同步阶段失败时
                    // new_service 会看到 dead 标志并接手善后
                    cx.exit();
                }
            }));
        }
        ctx.drain_ready();

        // 注意：init 里主动 `exit` 是合法的（bootstrap 就是干完活立刻退场），
        // 所以只有拿到明确的错误才算启动失败
        if let Some(err) = failure.lock().take() {
            self.handles.retire(handle);
            ctx.mark_dead();
            self.destroy(&ctx);
            return Err(Error::Init {
                kind: kind.to_string(),
                reason: err.to_string(),
            });
        }

        self.log(handle, format!("LAUNCH {kind} {args}"));
        // in_global 自创建起就是置位的，这里补上真正的入队，服务开始接受调度
        self.global.push(ctx);
        Ok(handle)
    }

    /// 摘除服务，对照 `skynet_handle_retire`。
    ///
    /// 真正的资源释放交给 worker：这里只把服务从地址表里摘掉并保证它一定会被
    /// 领走一次。若此刻正有 worker 在跑这个服务，它会在本批消息处理完后看到
    /// `dead` 标志并完成销毁。
    pub(crate) fn retire(&self, handle: u32) -> bool {
        match self.handles.retire(handle) {
            None => false,
            Some(ctx) => {
                ctx.mark_dead();
                // 趁请求还记在账上，立刻通知所有等着本服务回话的人
                ctx.fail_inflight();
                if ctx.mailbox.mark_in_global() {
                    self.global.push(ctx);
                }
                true
            }
        }
    }

    /// 干掉所有非保留服务，对照 `skynet_handle_retireall`（`ABORT` 命令）。
    ///
    /// 保留服务（logger）留给 [`Node::retire_reserved`]，这样关停过程中产生的日志
    /// 还有人接收。
    pub(crate) fn retire_all(&self) {
        loop {
            let mut remaining = 0;
            for handle in self.handles.handles() {
                match self.handles.grab(handle) {
                    Some(ctx) if ctx.is_reserved() => continue,
                    _ => {}
                }
                self.retire(handle);
                remaining += 1;
            }
            if remaining == 0 {
                return;
            }
        }
    }

    /// 送走保留服务，节点收尾的最后一步。
    pub(crate) fn retire_reserved(&self) {
        for handle in self.handles.handles() {
            self.retire(handle);
        }
    }

    /// 真正释放服务资源，只会由持有该服务的 worker 调用。
    pub(crate) fn destroy(&self, ctx: &Arc<ServiceContext>) {
        // cleanup 幂等：清理之后迟到的消息会把服务再推一次全局队列，在这里被再清一遍
        ctx.cleanup();
        if ctx.destroyed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.log(ctx.handle, format!("KILL {}", ctx.kind));
        if ctx.is_reserved() {
            return;
        }
        if self.total.fetch_sub(1, Ordering::AcqRel) <= 1 {
            // 最后一个服务也走了，通知 worker 收工，对照 C 版的 CHECK_ABORT
            self.global.set_quit();
        }
    }

    /// worker 的一轮调度，对照 `skynet_context_message_dispatch`。
    ///
    /// `hold` 是上一轮留在手里的服务；返回值是下一轮要接着跑的服务。
    pub(crate) fn dispatch(
        &self,
        hold: Option<Arc<ServiceContext>>,
        weight: i32,
    ) -> Option<Arc<ServiceContext>> {
        let ctx = match hold {
            Some(ctx) => ctx,
            None => self.global.pop()?,
        };
        if ctx.is_dead() {
            self.destroy(&ctx);
            return self.global.pop();
        }

        // 权重批处理：weight 为负表示一次只处理一条消息，否则处理 len >> weight 条
        let mut budget = 1usize;
        let mut handled = 0usize;
        loop {
            match ctx.mailbox.take_work() {
                // 邮箱和就绪队列都空了，in_global 已被清掉，把这个服务放生
                None => return self.global.pop(),
                Some(Work::Task(task)) => ctx.poll_task(task),
                Some(Work::Message(msg)) => {
                    if handled == 0 && weight >= 0 {
                        budget = (ctx.mailbox.len() >> weight).max(1);
                    }
                    let overload = ctx.mailbox.take_overload();
                    if overload > 0 {
                        self.log(
                            ctx.handle,
                            format!("error: 消息队列可能过载，长度 {overload}"),
                        );
                    }
                    ctx.handle_message(msg);
                    handled += 1;
                }
            }
            if ctx.is_dead() {
                self.destroy(&ctx);
                return self.global.pop();
            }
            if handled >= budget {
                break;
            }
        }

        // 让渡：全局队列里还有别的服务在等，就把自己排到队尾去
        match self.global.pop() {
            Some(next) => {
                self.global.push(ctx);
                Some(next)
            }
            None => Some(ctx),
        }
    }

    /// 挂定时器，对照 `skynet_timeout`。
    pub(crate) fn timeout(&self, handle: u32, ticks: u32, session: i32) {
        if ticks == 0 {
            let _ = self.send_raw(0, handle, MsgType::RESPONSE, session, Payload::None);
        } else {
            self.timer.add(handle, session, ticks);
        }
    }

    /// 派发到期的定时器，由定时器线程调用。
    pub(crate) fn fire_timers(&self) {
        for event in self.timer.update() {
            let _ = self.send_raw(
                0,
                event.handle,
                MsgType::RESPONSE,
                event.session,
                Payload::None,
            );
        }
    }

    /// 写日志，对照 `skynet_error`：日志本身也是一条消息，发给 logger 服务。
    pub(crate) fn log(&self, source: u32, text: String) {
        let logger = self.logger.load(Ordering::Acquire);
        if logger != 0 {
            if let Some(ctx) = self.handles.grab(logger) {
                if !ctx.is_dead() {
                    ctx.push(Message::new(
                        source,
                        0,
                        MsgType::TEXT,
                        Payload::Bytes(text.into_bytes()),
                    ));
                    return;
                }
            }
        }
        // logger 还没起来或已经退出，退回到 stderr
        eprintln!("[:{source:08x}] {text}");
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// 什么都不做的服务，测试里用来占个位置。
    #[derive(Default)]
    pub(crate) struct NullService;

    impl Service for NullService {
        fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    pub(crate) fn test_node() -> Arc<Node> {
        Node::new(
            &Config::default(),
            Registry::new().with("null", NullService::default),
        )
    }

    /// 给 handle 表的单元测试用：造一个不参与调度的空壳上下文。
    pub(crate) fn dummy_context(handle: u32) -> Arc<ServiceContext> {
        let node = test_node();
        Arc::new_cyclic(|me| ServiceContext {
            handle,
            kind: "null".to_string(),
            node,
            mailbox: Mailbox::new(),
            sessions: SessionTable::new(),
            service: Arc::new(NullService),
            tasks: TaskSet::new(),
            inflight: Mutex::new(HashMap::new()),
            dead: AtomicBool::new(false),
            reserved: AtomicBool::new(false),
            destroyed: AtomicBool::new(false),
            message_count: AtomicU64::new(0),
            cpu_cost: AtomicU64::new(0),
            me: me.clone(),
        })
    }

    /// 服务创建之后应当能按地址找回来
    #[test]
    fn new_service_is_addressable() {
        let node = test_node();
        let handle = node.new_service("null", "").expect("应创建成功");
        assert_eq!(node.total(), 1);
        assert!(node.handles.grab(handle).is_some());
        assert_eq!(node.resolve(&Addr::Handle(handle)).unwrap(), handle);
    }

    /// 没注册过的服务类型创建时就该失败，且不留下痕迹
    #[test]
    fn unknown_service_kind_fails() {
        let node = test_node();
        let err = node.new_service("并不存在", "").expect_err("应失败");
        assert!(matches!(err, Error::UnknownService(_)));
        assert_eq!(node.total(), 0);
    }

    /// 摘除服务后地址即刻失效，服务计数归零并通知 worker 收工
    #[test]
    fn retired_service_becomes_unreachable() {
        let node = test_node();
        let handle = node.new_service("null", "").unwrap();
        assert!(node.retire(handle));
        assert!(!node.retire(handle), "重复摘除应返回 false");
        assert!(node.handles.grab(handle).is_none());

        // 销毁由 worker 完成：跑一轮调度即可
        assert!(node.dispatch(None, 0).is_none());
        assert_eq!(node.total(), 0);
        assert!(node.global.is_quit(), "服务数归零应通知 worker 收工");
    }

    /// 发给不存在的地址应当报错而不是悄悄丢弃
    #[test]
    fn send_to_missing_service_errors() {
        let node = test_node();
        let err = node
            .send_raw(0, 0xdead, MsgType::USER, 0, Payload::None)
            .expect_err("应失败");
        assert!(matches!(err, Error::NoService(0xdead)));
    }
}
