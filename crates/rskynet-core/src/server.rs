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

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle, Thread};

use arc_swap::ArcSwapOption;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;

use crate::clock::Timer;
use crate::context::{Ctx, Service};
use crate::error::{Error, Result};
use crate::exclusive::Exclusive;
use crate::handle::HandleStorage;
use crate::message::{Addr, Message, MsgType, Payload};
use crate::module::Registry;
use crate::mq::{Mailbox, Ready, Scheduler, Work};
use crate::session::SessionTable;
use crate::start::Config;
use crate::task::TaskSet;

/// 一次访问最多干多少件活，就回头看一眼运行队列里有没有别人在等。
///
/// **消息与就绪任务各算一件。** 只按消息计数的话，一个在 poll 中自唤醒的任务
/// （`yield_now` 那类 future 就是这么实现的）会让 [`Node::run_service`] 永不返回：
/// 就绪队列始终非空，邮箱永远等不到空，那个 worker 也就再看不到收工信号。
///
/// 取 64 是在「让渡开销」与「换服务之后重新热起工作集」之间折中：开销占比大致是
/// `切换成本 / (64 × 单件活成本)`，同时也把 `pending` 那条被所有 worker 反复写的
/// 热缓存行的读取频率压到每 64 件一次。一条消息通常对应两件活——消息本身，
/// 以及它开出来的那个任务随后那次 poll。
const YIELD_INTERVAL: usize = 64;

/// 一个服务的运行时上下文。
pub(crate) struct ServiceContext {
    pub(crate) handle: u32,
    /// 服务类型名，即注册表里的键，只用于日志。
    pub(crate) kind: String,
    pub(crate) node: Arc<Node>,
    pub(crate) mailbox: Mailbox,
    pub(crate) sessions: SessionTable,
    service: Arc<dyn Service>,
    /// 独占线程服务的另一副面孔，与 `service` 是同一个对象；普通服务是 `None`。
    /// 它同时兼作「本服务由自己那条线程执行，不进运行队列」的标记。
    exclusive: Option<Arc<dyn Exclusive>>,
    /// 独占线程的句柄，投递方靠它把那条线程从 park 里叫醒。
    thread: ArcSwapOption<Thread>,
    tasks: TaskSet,
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
    /// 指向自己，给任务 waker 用：waker 可能被外部长期扣着，不该吊住整个服务。
    me: Weak<ServiceContext>,
}

thread_local! {
    /// 当前线程正在执行哪个服务，存的是 `ServiceContext` 的地址，0 表示不在服务里。
    ///
    /// 服务内部那些不加锁的容器（任务集、用户自己的 [`crate::SvcCell`]）都以
    /// 「只有正在执行本服务的线程会碰」为前提，这个量就是用来当场判定这一点的。
    static CURRENT_SERVICE: Cell<usize> = const { Cell::new(0) };
}

/// 标记「本线程正在执行某个服务」，析构时还原成先前那个。
///
/// 需要还原而不是清零，是因为会嵌套：服务 A 的任务里调 `launch` 起服务 B 时，
/// B 的 init 会就地在 A 的线程上跑到第一次挂起。
struct Running(usize);

impl Running {
    fn enter(ctx: &ServiceContext) -> Self {
        let me = ctx as *const ServiceContext as usize;
        Running(CURRENT_SERVICE.with(|cell| cell.replace(me)))
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        CURRENT_SERVICE.with(|cell| cell.set(self.0));
    }
}

impl ServiceContext {
    /// 本线程此刻是否正在执行本服务，也就是能不能直接动服务内部那些不加锁的容器。
    fn owns_current_thread(&self) -> bool {
        let me = self as *const ServiceContext as usize;
        CURRENT_SERVICE.with(|cell| cell.get()) == me
    }

    /// 顶着「本线程正在执行本服务」的标记跑一段同步代码。
    ///
    /// 独占线程的 [`Exclusive::idle`] 用它，于是那个钩子里也能直接 `ctx.spawn`、
    /// 碰服务自己的 [`crate::SvcCell`]，与在 `dispatch` 里一样。
    pub(crate) fn with_ownership<R>(&self, f: impl FnOnce() -> R) -> R {
        let _running = Running::enter(self);
        f()
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

    /// 本服务是否独占一条线程。
    pub(crate) fn is_exclusive(&self) -> bool {
        self.exclusive.is_some()
    }

    /// 有活干了，把服务交给它的执行者。
    ///
    /// `queued` 是邮箱状态机的回话：`true` 表示这件活刚把服务从「没人管」推成
    /// 「有人管」，该由投递方负责递交。普通服务的执行者是运行队列上的某个
    /// worker，独占服务的执行者是它自己那条线程。
    ///
    /// 独占分支同样只在 `queued` 为真时才叫人，这一点是对的也是必须的：状态机里
    /// 进入 `QUEUED` 的路径只有「投递方 CAS 成功」这一条（服务刚创建那次除外，
    /// 而那次的接手方正是即将启动的专属线程），所以每一次「线程可能正睡着」都
    /// 恰好对应一次 `queued`。反过来若每条消息都叫一次，忙起来就是每条消息一次
    /// 系统调用。
    fn wake(self: &Arc<Self>, queued: bool) {
        if !queued {
            return;
        }
        match &self.exclusive {
            None => self.node.sched.push(self.clone()),
            Some(service) => {
                self.unpark();
                service.interrupt();
            }
        }
    }

    /// 无条件把独占线程从阻塞里敲出来，普通服务无事可做。节点收工时用它。
    pub(crate) fn interrupt(&self) {
        if let Some(service) = &self.exclusive {
            self.unpark();
            service.interrupt();
        }
    }

    fn unpark(&self) {
        if let Some(thread) = self.thread.load_full() {
            thread.unpark();
        }
    }

    /// 登记专属线程的句柄。必须在服务可能被投递之前做掉，见 [`crate::Idler`]。
    pub(crate) fn bind_thread(&self) {
        self.thread.store(Some(Arc::new(thread::current())));
    }

    /// 投递一条消息。这是所有消息进入服务的唯一入口。
    ///
    /// 收 `&Arc<Self>` 而不是 `&self`：投递方手上本来就有 `Arc`，这样入队时只需
    /// 一次普通 `Arc::clone`，省掉每条消息一次 `Weak::upgrade`（那是个 CAS 循环）。
    pub(crate) fn push(self: &Arc<Self>, msg: Message) {
        let queued = self.mailbox.push_message(msg);
        self.wake(queued);
    }

    /// 唤醒服务内的某个任务，与消息投递走同一条「交给执行者」的路径。
    pub(crate) fn wake_task(self: &Arc<Self>, task: usize) {
        let queued = self.mailbox.push_ready(Ready::Task(task));
        self.wake(queued);
    }

    /// 起一个服务内任务，等价于 skynet 的 `skynet.fork`。
    ///
    /// `Ctx` 是 `Send` 的，用户完全可以从自己起的 OS 线程调它，而那个线程碰不得
    /// 服务的任务集（见 [`crate::task`] 的安全契约）。所以这里分两条路：本线程
    /// 正执行本服务就直接插入，否则把 future 当成一件活塞进邮箱，由持有者插入。
    pub(crate) fn spawn(self: &Arc<Self>, future: BoxFuture<'static, ()>) {
        if self.owns_current_thread() {
            self.install_task(future, None);
        } else {
            let queued = self.mailbox.push_ready(Ready::Spawn(future));
            self.wake(queued);
        }
    }

    /// 把一个 future 放进任务集并排进就绪队列。只有持有本服务的线程能调用。
    fn install_task(
        self: &Arc<Self>,
        future: BoxFuture<'static, ()>,
        request: Option<(u32, i32)>,
    ) -> usize {
        let task = self.tasks.insert(&self.me, future, request);
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
    fn handle_message(self: &Arc<Self>, mut msg: Message) {
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
        let request = (session != 0 && source != 0).then_some((source, session));
        let ctx = Ctx::new(self.clone());
        self.install_task(self.service.clone().dispatch(ctx, msg), request);
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
        // 用户代码就在下面这次 poll 里跑，`Ctx::spawn`、`Ctx::exit` 都靠这个标记
        // 认出「调用者正是本服务的持有者」，从而敢直接动服务内部那些不加锁的容器
        let _running = Running::enter(self);
        let mut cx = std::task::Context::from_waker(&waker);
        let started = self.node.profile().then(std::time::Instant::now);
        let result = future.as_mut().poll(&mut cx);
        if let Some(started) = started {
            self.cpu_cost
                .fetch_add(started.elapsed().as_micros() as u64, Ordering::Relaxed);
        }
        match result {
            // 任务正常跑完，它手上那个请求也就算办完了，不必再回错误
            std::task::Poll::Ready(()) => self.tasks.remove(task),
            std::task::Poll::Pending => self.tasks.restore(task, future),
        }
    }

    /// 只把就绪队列推进到全部挂起，不碰邮箱。服务初始化阶段用它。
    fn drain_ready(self: &Arc<Self>) {
        while let Some(ready) = self.mailbox.take_ready() {
            self.run_ready(ready);
        }
    }

    /// 处理就绪队列里的一件活。
    fn run_ready(self: &Arc<Self>, ready: Ready) {
        match ready {
            Ready::Task(task) => self.poll_task(task),
            // 别的线程托我们插的任务：插进去就会排到就绪队列尾部，随后被 poll
            Ready::Spawn(future) => {
                self.install_task(future, None);
            }
        }
    }

    /// 给所有「正在处理中」的请求回错误，对照 `skynet.exit` 里遍历
    /// `session_coroutine_id` 逐个 `PTYPE_ERROR` 的那段。
    ///
    /// 服务自己调 `exit` 时必须在**那一刻**就发，不能拖到销毁时：那个任务随后会
    /// 正常返回，记录也就跟着被当成「已办完」清掉了，届时再想通知请求方已经晚了。
    ///
    /// 即使该请求其实已经应答过也无妨：请求方的 session 早已销毁，迟到的错误包
    /// 会被直接丢弃。
    ///
    /// 非持有线程（跨线程 `kill`）直接跳过：那种情况下请求与任务完成本来就在竞态，
    /// 交给销毁它的那个 worker 在 [`ServiceContext::cleanup`] 里补上更清楚。
    pub(crate) fn fail_inflight(&self) {
        if !self.owns_current_thread() {
            return;
        }
        for (source, session) in self.tasks.take_requests() {
            let _ = self
                .node
                .send_raw(self.handle, source, MsgType::ERROR, session, Payload::None);
        }
    }

    /// 销毁前的清理，幂等。
    ///
    /// 「放生邮箱」放在最后一步，而且只有确认清理期间没有新活进来才算成功——
    /// 因为服务一旦放生就可能被别的 worker 重新领走、再清一次，而清理动的都是
    /// 服务内部那些「只有持有者会碰」的结构，两个 worker 同时清同一个服务是不行的。
    fn cleanup(&self) {
        // 清理动的全是「只有持有者会碰」的东西，而调用方正是持有者
        let _running = Running::enter(self);
        loop {
            self.fail_inflight();
            // 先丢任务再清 session 表：任务里的 `Call` 析构时还会来注销 session
            let tasks = self.tasks.drain();
            drop(tasks);
            self.sessions.clear();
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
            if self.mailbox.release() {
                return;
            }
        }
    }
}

/// 一轮 [`Node::run_service`] 的结局。
pub(crate) enum Ran {
    /// 邮箱与就绪队列都空了，服务已经放生（状态落回 `IDLE`），调用方不再持有它。
    Idle,
    /// 该让渡了——运行队列里有别人在等，或者节点要收工。服务仍归调用方持有。
    Yielded,
    /// 服务已被摘除，仍归调用方持有，等着被销毁。
    Dead,
}

impl Ran {
    pub(crate) fn is_dead(&self) -> bool {
        matches!(self, Ran::Dead)
    }
}

/// join 一批线程，返回其中第一个没接住的 panic。
fn join_all(threads: Vec<JoinHandle<()>>) -> Option<Box<dyn Any + Send + 'static>> {
    let mut panic = None;
    for thread in threads {
        if let Err(payload) = thread.join() {
            panic = panic.or(Some(payload));
        }
    }
    panic
}

/// 一个 rskynet 节点。
pub(crate) struct Node {
    pub(crate) sched: Scheduler,
    pub(crate) handles: HandleStorage,
    /// 时间来源，启动方注入，实现在内核之外，见 [`crate::Timer`]。
    pub(crate) timer: Arc<dyn Timer>,
    modules: Registry,
    /// 独占服务的线程句柄，节点收尾时按它逐个 join。
    ///
    /// 不能用 `thread::scope` 那种 scoped 线程：独占服务可以在运行期被 `launch`，
    /// 那时启动阶段的作用域早就建好了。所以这里是普通线程加一张表，代价是收尾
    /// 得自己 join（见 [`Node::join_exclusives`]）。
    exclusives: Mutex<HashMap<u32, JoinHandle<()>>>,
    /// 活着的服务数，归零即整个节点退出，对照 `skynet_context_total`。
    total: AtomicI64,
    /// 系统服务是否都已拉起。
    ///
    /// 定时器要靠它分辨两个同样是「服务数为 0」的时刻：引导还没出场，和一切都已
    /// 收场。它自己排在引导之前起，光看服务数会把前者当成后者。
    booted: AtomicBool,
    /// logger 服务的 handle，0 表示还没起来。
    logger: AtomicU32,
    /// 节点配置原样留一份：服务在自己的 `init` 里靠 [`crate::NodeRef::section`]
    /// 读属于自己的那一段（网络层的 `[net]` 之类）。`init` 只收得到一个字符串
    /// 参数，成段的配置只能从这里来。
    config: Config,
    profile: bool,
}

impl Node {
    pub(crate) fn new(config: &Config, modules: Registry, timer: Arc<dyn Timer>) -> Arc<Node> {
        Arc::new(Node {
            sched: Scheduler::new(config.thread),
            handles: HandleStorage::new(config.harbor),
            timer,
            modules,
            exclusives: Mutex::new(HashMap::new()),
            total: AtomicI64::new(0),
            booted: AtomicBool::new(false),
            logger: AtomicU32::new(0),
            config: config.clone(),
            profile: config.profile,
        })
    }

    pub(crate) fn total(&self) -> i64 {
        self.total.load(Ordering::Acquire)
    }

    /// 系统服务都拉起来了，见 [`Node::is_booted`]。
    pub(crate) fn mark_booted(&self) {
        self.booted.store(true, Ordering::Release);
    }

    pub(crate) fn is_booted(&self) -> bool {
        self.booted.load(Ordering::Acquire)
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
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
        let instance = factory();
        let service = instance.service.clone();
        let exclusive = instance.exclusive.clone();

        let node = self.clone();
        let kind_name = kind.to_string();
        let ctx = self.handles.register_with(move |handle| {
            Arc::new_cyclic(|me| ServiceContext {
                handle,
                kind: kind_name,
                node,
                mailbox: Mailbox::new(),
                sessions: SessionTable::new(),
                service: instance.service,
                exclusive: instance.exclusive,
                thread: ArcSwapOption::empty(),
                tasks: TaskSet::new(),
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
            // 这个服务还没进过运行队列，本线程独占它，可以直接插任务
            ctx.install_task(
                Box::pin(async move {
                    if let Err(err) = service.init(cx.clone(), args).await {
                        cx.log(format!("error: 初始化失败：{err}"));
                        *slot.lock() = Some(err);
                        // 初始化失败的服务不该留在世上；同步阶段失败时
                        // new_service 会看到 dead 标志并接手善后
                        cx.exit();
                    }
                }),
                None,
            );
        }
        ctx.drain_ready();

        // 注意：init 里主动 `exit` 是合法的（bootstrap 就是干完活立刻退场），
        // 所以只有拿到明确的错误才算启动失败
        if let Some(err) = failure.lock().take() {
            self.handles.retire(handle);
            ctx.mark_dead();
            // 这个服务从创建起就没进过运行队列，一直被本线程独占；把状态挪到
            // RUNNING 是为了让销毁流程的「放生」一步与正常路径走同一套 CAS
            ctx.mailbox.mark_running();
            self.destroy(&ctx);
            return Err(Error::Init {
                kind: kind.to_string(),
                reason: err.to_string(),
            });
        }

        self.log(handle, format!("LAUNCH {kind} {args}"));
        match exclusive {
            // in_global 自创建起就是置位的，这里补上真正的入队，服务开始接受调度
            None => self.sched.push(ctx),
            // 独占服务的执行者是它自己那条线程，起来就接管
            Some(service) => self.spawn_exclusive(ctx, service),
        }
        Ok(handle)
    }

    /// 给一个独占服务起专属线程，见 [`crate::exclusive`]。
    fn spawn_exclusive(self: &Arc<Self>, ctx: Arc<ServiceContext>, service: Arc<dyn Exclusive>) {
        let handle = ctx.handle;
        let name = format!("rskynet-{}-{handle:08x}", ctx.kind);
        let thread = thread::Builder::new()
            .name(name)
            .spawn(move || crate::exclusive::exclusive_loop(ctx, service))
            .expect("独占服务的线程创建失败");
        let mut table = self.exclusives.lock();
        // 顺手清掉已经收工的：反复 launch / kill 独占服务时别让这张表一直长
        table.retain(|_, thread| !thread.is_finished());
        table.insert(handle, thread);
    }

    /// join 掉那些服务已经被摘除的独占线程。
    ///
    /// 摘除即意味着那条线程一定会收工（`retire` 会把它叫醒），所以这一步不会
    /// 等到天荒地老。返回线程里没接住的 panic，由调用方重抛。
    pub(crate) fn join_retired_exclusives(&self) -> Option<Box<dyn Any + Send + 'static>> {
        let retired: Vec<_> = {
            let mut table = self.exclusives.lock();
            let gone: Vec<u32> = table
                .keys()
                .copied()
                .filter(|handle| self.handles.grab(*handle).is_none())
                .collect();
            // join 必须在锁外做：那条线程收尾时可能还要 launch / kill，会来抢这把锁
            gone.iter()
                .filter_map(|handle| table.remove(handle))
                .collect()
        };
        join_all(retired)
    }

    /// join 掉全部独占线程。
    ///
    /// **调用前必须保证它们的服务都已被摘除**，否则那些线程还在等活，这里会等死。
    pub(crate) fn join_exclusives(&self) -> Option<Box<dyn Any + Send + 'static>> {
        let all: Vec<_> = self
            .exclusives
            .lock()
            .drain()
            .map(|(_, thread)| thread)
            .collect();
        join_all(all)
    }

    /// 宣布节点收工：叫醒所有 worker，也把独占线程从阻塞里敲出来。
    ///
    /// 对照 C 版的 `CHECK_ABORT`。独占线程那一下不是非有不可（它们真正的退出
    /// 条件是自己被摘除，而摘除自带唤醒），但让 `idle` 里那些盯着 `is_quit` 的
    /// 服务早一点看到收工信号总是好的。
    pub(crate) fn quit(&self) {
        self.sched.set_quit();
        for ctx in self.handles.contexts() {
            ctx.interrupt();
        }
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
                // 服务自己调 exit 时，趁请求还记在账上立刻通知所有等着回话的人；
                // 跨线程 kill 则什么都不做，由销毁它的那个 worker 在 cleanup 里补
                ctx.fail_inflight();
                // 与投递一件活同一条路：保证这个服务一定会被执行者再领走一次，
                // 好让它把销毁做掉
                let queued = ctx.mailbox.notify();
                ctx.wake(queued);
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
    ///
    /// `keep_logger` 为真时把 logger 留着：别人销毁时那句「KILL」还要指着它写，
    /// logger 先走的话就只能退回 stderr 了。
    pub(crate) fn retire_reserved(&self, keep_logger: bool) {
        let logger = self.logger.load(Ordering::Acquire);
        for handle in self.handles.handles() {
            if keep_logger && handle == logger {
                continue;
            }
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
            // 最后一个服务也走了，通知所有线程收工
            self.quit();
        }
    }

    /// worker 的一轮调度，对照 `skynet_context_message_dispatch`。
    ///
    /// `hold` 是上一轮留在手里的服务；返回值是下一轮要接着跑的服务。
    pub(crate) fn dispatch(
        &self,
        hold: Option<Arc<ServiceContext>>,
    ) -> Option<Arc<ServiceContext>> {
        let ctx = match hold {
            Some(ctx) => ctx,
            None => self.sched.pop()?,
        };
        match self.run_service(&ctx, true) {
            Ran::Dead => {
                self.destroy(&ctx);
                self.sched.pop()
            }
            Ran::Idle => {
                // 放生的那一瞬间可能正好有人在 kill 它：对方的 notify 撞上我们
                // 的「落回 IDLE」，于是谁都不会再把它推进运行队列，销毁也就没人
                // 做了。这里补一次入队，让它一定被某个 worker 领走并销毁。
                if ctx.is_dead() && ctx.mailbox.notify() {
                    self.sched.push(ctx);
                }
                self.sched.pop()
            }
            // 让渡：运行队列里还有别的服务在等，就把自己交回去
            Ran::Yielded => match self.sched.pop() {
                Some(next) => {
                    self.sched.push(ctx);
                    Some(next)
                }
                None => Some(ctx),
            },
        }
    }

    /// 在当前线程上执行一个自己持有的服务，直到没活干、该让渡了或者服务已死。
    ///
    /// worker 与独占线程共用这一段：区别只在让不让渡与「干完之后怎么处置」，
    /// 都由调用方决定。
    ///
    /// `preemptible` 为真（worker）时每干满 [`YIELD_INTERVAL`] 件活就问一次
    /// [`Scheduler::should_yield`]：确实有人在等才交回运行队列，没人等就重新
    /// 计数接着跑，于是空闲时的行为与「一口气干到邮箱空」完全一致。为假
    /// （独占线程）时一次把活干完——那条线程只有这一个服务，让渡给谁都没有意义。
    pub(crate) fn run_service(&self, ctx: &Arc<ServiceContext>, preemptible: bool) -> Ran {
        if ctx.is_dead() {
            return Ran::Dead;
        }
        let mut done = 0usize;
        loop {
            match ctx.mailbox.take_work() {
                // 邮箱和就绪队列都空了，状态已落回 IDLE，服务就此放生
                None => return Ran::Idle,
                Some(Work::Ready(ready)) => ctx.run_ready(ready),
                Some(Work::Message(msg)) => {
                    let overload = ctx.mailbox.take_overload();
                    if overload > 0 {
                        self.log(
                            ctx.handle,
                            format!("error: 消息队列可能过载，长度 {overload}"),
                        );
                    }
                    ctx.handle_message(msg);
                }
            }
            if ctx.is_dead() {
                return Ran::Dead;
            }
            done += 1;
            if preemptible && done >= YIELD_INTERVAL {
                if self.sched.should_yield() {
                    return Ran::Yielded;
                }
                // 没人在等，那就没有让渡的理由：重新计数，接着把这个服务跑下去
                done = 0;
            }
        }
    }

    /// 把邮箱里已经积压的活干完，**忽略服务已死这件事**。
    ///
    /// 独占线程收尾时用它：`retire` 之后邮箱里往往还剩着消息，而销毁流程只会把
    /// 它们丢掉（顶多给请求方回个错误）。日志服务全靠这一步把最后几行写出去。
    ///
    /// 预算按进来时的积压量给：清理期间别人可能还在往里投（比如别的服务还在写
    /// 日志），无上限地跟下去就退不出来了。
    pub(crate) fn drain_service(&self, ctx: &Arc<ServiceContext>) {
        // 每条消息都会开一个任务，所以留够「一条消息一次 poll」的份额，另外
        // 多给一点余量，好让 init 那种还没跑完的任务有机会收尾
        let mut budget = ctx.mailbox.len() * 2 + 16;
        while budget > 0 {
            budget -= 1;
            match ctx.mailbox.take_work() {
                None => return,
                Some(Work::Ready(ready)) => ctx.run_ready(ready),
                Some(Work::Message(msg)) => ctx.handle_message(msg),
            }
        }
    }

    /// 挂定时器，对照 `skynet_timeout`。
    ///
    /// 零延迟不必惊动时间来源：直接回包，语义上等价于「本刻度就到期」，
    /// 也让 `ctx.yield_now()` 这条最常走的路少绕一圈。
    pub(crate) fn timeout(&self, handle: u32, ticks: u32, session: i32) {
        if ticks == 0 {
            let _ = self.send_raw(0, handle, MsgType::RESPONSE, session, Payload::None);
        } else {
            self.timer.timeout(handle, session, ticks);
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

    #[rskynet_macros::service(crate = ::rskynet_core, name = "null")]
    impl NullService {}

    /// 同上，但独占一条线程：起来就阻塞在 park 上，等着被摘除。
    #[derive(Default)]
    pub(crate) struct NullExclusive;

    #[rskynet_macros::exclusive(crate = ::rskynet_core, name = "solo")]
    impl NullExclusive {}

    /// 测试用的时间来源：刻度从不前进，挂上的表也不会到期。内核的单元测试只关心
    /// 消息与调度，真正的时间轮在 `rskynet-timer` 里自测。
    #[derive(Default)]
    pub(crate) struct StubTimer;

    impl Timer for StubTimer {
        fn timeout(&self, _handle: u32, _session: i32, _ticks: u32) {}

        fn now(&self) -> u64 {
            0
        }

        fn wall_clock(&self) -> u64 {
            0
        }

        fn start_seconds(&self) -> u64 {
            0
        }
    }

    pub(crate) fn test_node() -> Arc<Node> {
        test_node_with(Config::default())
    }

    pub(crate) fn test_node_with(config: Config) -> Arc<Node> {
        let registry = Registry::from_auto().expect("测试服务的自动注册表应当有效");
        assert!(registry.contains("null"), "共享测试服务应被自动注册");
        assert!(registry.contains("solo"), "独占测试服务应被自动注册");
        Node::new(&config, registry, Arc::new(StubTimer))
    }

    /// 给 handle 表的单元测试用：造一个不参与调度的空壳上下文。
    pub(crate) fn dummy_context(handle: u32) -> Arc<ServiceContext> {
        dummy_context_on(test_node(), handle)
    }

    /// 同上，但挂在指定节点下，好让一批上下文共用一个调度器。
    pub(crate) fn dummy_context_on(node: Arc<Node>, handle: u32) -> Arc<ServiceContext> {
        Arc::new_cyclic(|me| ServiceContext {
            handle,
            kind: "null".to_string(),
            node,
            mailbox: Mailbox::new(),
            sessions: SessionTable::new(),
            service: Arc::new(NullService),
            exclusive: None,
            thread: ArcSwapOption::empty(),
            tasks: TaskSet::new(),
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
        assert!(node.dispatch(None).is_none());
        assert_eq!(node.total(), 0);
        assert!(node.sched.is_quit(), "服务数归零应通知 worker 收工");
    }

    /// 服务被 worker 领走之后才被 kill，也必须被销毁
    ///
    /// 这时 `retire` 的 notify 只会把邮箱状态推到 `NOTIFIED`，谁都不会把这个服务
    /// 重新推进运行队列——销毁只能由持有它的那个 worker 自己负责。
    #[test]
    fn a_service_killed_while_held_is_still_destroyed() {
        let node = test_node();
        let handle = node.new_service("null", "").unwrap();
        // 扮成 worker 把它领走，此刻它的状态是 RUNNING
        let ctx = node.sched.pop().expect("新服务应当排在运行队列里");
        assert_eq!(ctx.handle, handle);

        assert!(node.retire(handle));
        assert_eq!(node.sched.len(), 0, "没有人会把它重新入队");

        // 持有者接着跑一轮就该发现 dead 标志并善后
        assert!(node.dispatch(Some(ctx)).is_none());
        assert_eq!(node.total(), 0, "服务必须被销毁，否则节点永远等不到退出");
    }

    /// 一条测试用消息：不带 source 也不带 session，于是不会牵扯回包那套记账
    fn work_message() -> Message {
        Message::new(0, 0, MsgType::USER, Payload::None)
    }

    /// 没人在等就不让渡：一口气把邮箱干空，`YIELD_INTERVAL` 只是个检查点
    #[test]
    fn a_lone_service_runs_to_empty() {
        let node = test_node();
        let ctx = dummy_context_on(node.clone(), 1);
        // 邮箱新建时就是 QUEUED，所以这些投递都不会真的去动运行队列
        let total = YIELD_INTERVAL * 4;
        for _ in 0..total {
            ctx.push(work_message());
        }
        ctx.mailbox.mark_running();

        assert_eq!(node.sched.len(), 0, "运行队列空着，没人可让");
        assert!(matches!(node.run_service(&ctx, true), Ran::Idle));
        assert_eq!(ctx.message_count(), total as u64, "该一条不剩地干完");
    }

    /// 有别人在等，干满一批就得把自己交回运行队列
    #[test]
    fn a_busy_service_yields_when_others_are_waiting() {
        let node = test_node();
        // 运行队列里排一个别人，`pending` 于是不为 0
        node.sched.push(dummy_context_on(node.clone(), 2));

        let ctx = dummy_context_on(node.clone(), 1);
        for _ in 0..YIELD_INTERVAL {
            ctx.push(work_message());
        }
        ctx.mailbox.mark_running();

        assert!(matches!(node.run_service(&ctx, true), Ran::Yielded));
        // 一条消息是两件活：消息本身，加上它开出来那个任务的第一次 poll
        assert_eq!(ctx.message_count(), (YIELD_INTERVAL / 2) as u64);
    }

    /// 在 poll 里自唤醒的任务不能把 worker 永久扣住
    ///
    /// 这种任务的就绪队列永远不空，邮箱也就永远等不到空，`run_service` 走不到
    /// `Ran::Idle`。只按消息计数的话它连一次让渡检查都触发不了，worker 从此
    /// 看不到收工信号，节点关不掉——这条用例守的就是那个出口。
    #[test]
    fn a_self_waking_task_cannot_pin_the_worker() {
        #[derive(Default)]
        struct SelfWaker;

        #[rskynet_macros::service(crate = ::rskynet_core)]
        impl SelfWaker {
            async fn dispatch(&self) {
                std::future::poll_fn(|cx| {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                })
                .await
            }
        }

        let registry = Registry::new().with("self-waker", SelfWaker::default);
        let node = Node::new(&Config::default(), registry, Arc::new(StubTimer));
        node.new_service("self-waker", "").expect("应创建成功");
        // 自己把它领走，运行队列于是空了：能救场的只剩收工信号
        let ctx = node.sched.pop().expect("新服务应当排在运行队列里");
        ctx.push(work_message());
        assert_eq!(node.sched.len(), 0);

        node.sched.set_quit();
        assert!(
            matches!(node.run_service(&ctx, true), Ran::Yielded),
            "收工信号该把它换下来，而不是在里面转到天荒地老"
        );
    }

    /// 独占服务不进运行队列，它自带一条线程；摘除之后那条线程要自己收工
    #[test]
    fn an_exclusive_service_brings_its_own_thread() {
        let node = test_node();
        let handle = node.new_service("solo", "").expect("应创建成功");
        let ctx = node.handles.grab(handle).expect("应当能按地址找回来");
        assert!(ctx.is_exclusive());
        assert_eq!(node.sched.len(), 0, "独占服务不该进运行队列");
        assert_eq!(node.total(), 1);

        // 摘除就是通知：那条线程会被叫醒、看到 dead、自己把销毁做掉
        assert!(node.retire(handle));
        assert!(node.join_exclusives().is_none(), "线程不该 panic");
        assert_eq!(node.total(), 0, "销毁记账要由那条线程自己做掉");
        assert!(node.sched.is_quit(), "服务数归零应通知所有线程收工");
    }

    /// 投给独占服务的消息要能把它那条线程叫起来
    #[test]
    fn a_message_wakes_the_exclusive_thread() {
        let node = test_node();
        let handle = node.new_service("solo", "").expect("应创建成功");
        let ctx = node.handles.grab(handle).unwrap();

        // 等它睡下：状态落回 IDLE 就说明邮箱已经取空、线程正阻塞在 park 上
        while ctx.mailbox.len() > 0 || !ctx.mailbox.is_idle() {
            std::hint::spin_loop();
        }
        node.send_raw(0, handle, MsgType::USER, 0, Payload::None)
            .expect("服务还活着");
        // 被叫醒之后它会把这条消息处理掉，邮箱重新变空
        while ctx.message_count() == 0 {
            std::hint::spin_loop();
        }
        assert_eq!(ctx.mailbox.len(), 0);

        node.retire(handle);
        assert!(node.join_exclusives().is_none());
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
