//! 两级消息队列，对照 `skynet-src/skynet_mq.c`。
//!
//! - [`Mailbox`]：每个服务一个，等价于 C 版的 `struct message_queue`。
//!   C 版手写环形缓冲加倍扩容，这里用 `VecDeque`（本身就是按需扩容的环形缓冲），
//!   语义一致；`in_global` 标志与过载检测原样保留。
//! - [`Scheduler`]：存放「有活干的服务」，对应 `struct global_queue`——
//!   但不再是一条被所有线程争抢的队列，见下。
//!
//! 相比 C 版本，邮箱里多了一条 `ready` 队列存放服务内部被唤醒的 Future 任务，
//! 于是「服务可被调度」的条件从「邮箱非空」推广为「邮箱非空或有就绪任务」。
//! `in_global` 依然是整套调度的核心不变量：
//! **它为 true 时表示该服务要么排在某条运行队列里，要么正被某个 worker 持有，
//! 因此同一服务绝不会被两个 worker 同时执行。**
//!
//! # 为什么全局队列变成了每 worker 一条
//!
//! C 版的全局队列是一把大锁护着的 `VecDeque`，worker 越多争得越凶。这里换成
//! 每个 worker 一条 [`crate::bwos`] 的 BWoS 队列：owner 在自己的队列上无锁
//! push/pop，闲下来才去别人队列头部窃取。
//!
//! 但 BWoS 的 owner 侧操作只允许绑定线程调用，而投递方是任意线程（定时器线程、
//! 外部唤醒的 waker 都可能），所以还留了一条加锁的 injector 队列兜底：
//! 非 worker 线程的投递、本地队列写满的溢出，都落到 injector，谁都能从里面取。

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::bwos::{self, CachePad, Owner, Stealer};
use crate::message::Message;
use crate::server::ServiceContext;

/// 过载报警的初始阈值，对照 C 版 `MQ_OVERLOAD`。
pub(crate) const OVERLOAD_THRESHOLD: usize = 1024;

/// 一次调度取到的活儿：要么是一条新消息，要么是一个被唤醒的服务内任务。
pub(crate) enum Work {
    Message(Message),
    Task(usize),
}

struct MailboxInner {
    queue: VecDeque<Message>,
    /// 服务内被唤醒、等待 poll 的任务 id（相当于 skynet 里可以 resume 的协程）。
    ready: VecDeque<usize>,
    in_global: bool,
    overload: usize,
    overload_threshold: usize,
}

pub(crate) struct Mailbox {
    inner: Mutex<MailboxInner>,
}

impl Mailbox {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(MailboxInner {
                queue: VecDeque::with_capacity(64),
                ready: VecDeque::new(),
                // 与 C 版一致：创建时就置位，保证服务初始化完成前不会被 worker 领走
                in_global: true,
                overload: 0,
                overload_threshold: OVERLOAD_THRESHOLD,
            }),
        }
    }

    /// 投递消息。返回 true 表示调用方需要把该服务推入全局队列。
    pub(crate) fn push_message(&self, msg: Message) -> bool {
        let mut inner = self.inner.lock();
        inner.queue.push_back(msg);
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 标记某个服务内任务就绪。返回 true 表示调用方需要把该服务推入全局队列。
    pub(crate) fn push_ready(&self, task: usize) -> bool {
        let mut inner = self.inner.lock();
        inner.ready.push_back(task);
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 取一件活干。返回 `None` 表示服务彻底空闲，此时会**原子地**清掉
    /// `in_global`，让后续的投递方负责把服务重新推进全局队列。
    ///
    /// 就绪任务优先于新消息：对应 skynet 里被 resume 的协程会一路跑到下一次
    /// yield，之后才轮到下一条消息。
    pub(crate) fn take_work(&self) -> Option<Work> {
        let mut inner = self.inner.lock();
        if let Some(task) = inner.ready.pop_front() {
            return Some(Work::Task(task));
        }
        match inner.queue.pop_front() {
            Some(msg) => {
                let length = inner.queue.len();
                while length > inner.overload_threshold {
                    inner.overload = length;
                    inner.overload_threshold *= 2;
                }
                Some(Work::Message(msg))
            }
            None => {
                inner.in_global = false;
                inner.overload_threshold = OVERLOAD_THRESHOLD;
                None
            }
        }
    }

    /// 只取就绪任务，不碰 `in_global`。服务初始化阶段用它把 init 推进到第一次挂起。
    pub(crate) fn take_ready(&self) -> Option<usize> {
        self.inner.lock().ready.pop_front()
    }

    /// 强行置位 `in_global`。返回 true 表示调用方需要入队。
    /// 服务被 kill 时用它保证一定会被某个 worker 领走一次以完成销毁。
    pub(crate) fn mark_in_global(&self) -> bool {
        let mut inner = self.inner.lock();
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 清掉 `in_global`。服务清理完毕后调用，好让清理之后迟到的消息还能
    /// 把这个服务重新推进全局队列、再被清一次。
    pub(crate) fn reset_in_global(&self) {
        self.inner.lock().in_global = false;
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().queue.len()
    }

    /// 取出并清零过载读数，对照 `skynet_mq_overload`。
    pub(crate) fn take_overload(&self) -> usize {
        std::mem::take(&mut self.inner.lock().overload)
    }

    /// 服务销毁时清空邮箱，未处理的消息交回调用方以便给发起者回错误。
    pub(crate) fn drain(&self) -> Vec<Message> {
        let mut inner = self.inner.lock();
        inner.ready.clear();
        inner.queue.drain(..).collect()
    }
}

/// 连续取活多少次之后回头看一眼 injector，必须是 2 的幂。
///
/// 本地队列取空之前不会主动去碰 injector，这个周期性回看是为了防止 injector 里的
/// 活被本地队列饿死，对应 Go 调度器每 61 次查一遍全局队列的做法。
const INJECT_INTERVAL: usize = 64;

thread_local! {
    /// 当前线程绑定到了哪个调度器的哪号 worker。
    ///
    /// 存调度器地址是因为同进程可以跑多个节点，得认准自己那一个；非 worker 线程
    /// （主线程、定时器线程、外部唤醒线程）这里始终是 `None`。
    static CURRENT_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

/// 每个 worker 的私有记账，独占一条 cache line。
#[derive(Default)]
struct WorkerSlot {
    /// 取活次数，用来触发 [`INJECT_INTERVAL`] 的回看。
    tick: AtomicUsize,
    /// 挑窃取目标用的 xorshift 状态。
    rng: AtomicU64,
}

impl WorkerSlot {
    fn new(id: usize) -> CachePad<Self> {
        CachePad(Self {
            // xorshift 不能从 0 起步
            rng: AtomicU64::new((id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            ..Self::default()
        })
    }

    fn next_random(&self) -> u64 {
        let mut x = self.rng.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng.store(x, Ordering::Relaxed);
        x
    }
}

/// 运行队列的集合：每 worker 一条 BWoS 本地队列，外加一条谁都能用的 injector。
pub(crate) struct Scheduler {
    /// 下标即 worker 编号。owner 侧方法只允许对应的 worker 线程调用。
    locals: Vec<Owner<Arc<ServiceContext>>>,
    stealers: Vec<Stealer<Arc<ServiceContext>>>,
    slots: Vec<CachePad<WorkerSlot>>,
    /// 非 worker 线程的投递、以及本地队列写满时的溢出，都落在这里。
    injector: Mutex<VecDeque<Arc<ServiceContext>>>,
    /// 所有队列加起来排着多少个服务。
    ///
    /// 它存在的理由是让「全都空着」这个最常见的情况便宜到只剩一次原子读：
    /// 权重为负的 worker 每处理一条消息就要让渡一次，也就是每条消息都要问一遍
    /// 「还有别人在等吗」。没有它的话每次都要走一遍「遍历本地块 + 锁 injector +
    /// 扫一圈别人的队列」，空转成本比原来那把大锁还高。
    pending: CachePad<AtomicUsize>,
    /// injector 里排着多少个，用来在它空着时省掉一次加锁。
    injector_len: CachePad<AtomicUsize>,
    /// 睡在条件变量上的 worker 数，对照 C 版 `struct monitor` 的 `sleep`。
    sleepers: AtomicUsize,
    /// 每次投递自增，睡前用它确认「我扫队列之后没人投过新活」。
    notify_seq: AtomicU64,
    /// 只用来跟 `idle` 配对，不保护任何数据。
    gate: Mutex<()>,
    idle: Condvar,
    quit: AtomicBool,
}

impl Scheduler {
    pub(crate) fn new(workers: usize) -> Self {
        let workers = workers.max(1);
        let mut locals = Vec::with_capacity(workers);
        let mut stealers = Vec::with_capacity(workers);
        let mut slots = Vec::with_capacity(workers);
        for id in 0..workers {
            let (owner, stealer) = bwos::queue(bwos::NUM_BLOCKS, bwos::BLOCK_SIZE);
            locals.push(owner);
            stealers.push(stealer);
            slots.push(WorkerSlot::new(id));
        }
        Self {
            locals,
            stealers,
            slots,
            injector: Mutex::new(VecDeque::new()),
            pending: CachePad(AtomicUsize::new(0)),
            injector_len: CachePad(AtomicUsize::new(0)),
            sleepers: AtomicUsize::new(0),
            notify_seq: AtomicU64::new(0),
            gate: Mutex::new(()),
            idle: Condvar::new(),
            quit: AtomicBool::new(false),
        }
    }

    /// 把当前线程绑定成 `id` 号 worker，从此它的投递与取活优先走本地队列。
    /// 返回的守卫析构时解绑。
    pub(crate) fn register_worker(&self, id: usize) -> WorkerGuard {
        assert!(id < self.locals.len(), "worker 编号越界");
        CURRENT_WORKER.with(|slot| slot.set(Some((self.identity(), id))));
        WorkerGuard
    }

    /// 用自身地址当身份证。调度器建好之后就一直待在 `Arc<Node>` 里不会再挪窝，
    /// 而绑定关系由 [`WorkerGuard`] 保证不会活过注册它的那段作用域，
    /// 所以不存在「地址被新节点复用、旧记录张冠李戴」的问题。
    fn identity(&self) -> usize {
        std::ptr::from_ref(self) as usize
    }

    /// 当前线程在本调度器里的 worker 编号，非 worker 线程返回 `None`。
    fn worker_id(&self) -> Option<usize> {
        CURRENT_WORKER.with(|slot| match slot.get() {
            Some((sched, id)) if sched == self.identity() => Some(id),
            _ => None,
        })
    }

    /// 把一个有活干的服务放进运行队列。
    pub(crate) fn push(&self, ctx: Arc<ServiceContext>) {
        // 有 worker 在睡就绕开本地队列：BWoS 里 owner 当前正在写的那一块对窃贼是
        // 隐形的，活攒不满一块就塞本地，被唤醒的 worker 会空手而归。系统忙起来
        // （无人睡）之后自然回到全速的本地路径。
        let target = match self.worker_id() {
            Some(id) if self.sleepers.load(Ordering::SeqCst) == 0 => Some(id),
            _ => None,
        };
        let overflow = match target {
            // 安全性：worker 编号来自线程局部变量，同一条本地队列只有绑定它的
            // 那个线程会走到这里，满足 BWoS 的 owner 独占契约。
            Some(id) => unsafe { self.locals[id].push_back(ctx) }.err(),
            None => Some(ctx),
        };
        if let Some(ctx) = overflow {
            self.push_injector(ctx);
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
        self.notify();
    }

    /// 取一个有活干的服务。worker 线程走「本地 -> injector -> 窃取」，
    /// 其它线程只能取 injector。
    pub(crate) fn pop(&self) -> Option<Arc<ServiceContext>> {
        let Some(id) = self.worker_id() else {
            return self.pop_injector();
        };
        if self.pending.load(Ordering::Relaxed) == 0 {
            // 全都空着，这是让渡路径上最常走的一条，到此为止
            return None;
        }
        let slot = &self.slots[id];
        if slot.tick.fetch_add(1, Ordering::Relaxed) & (INJECT_INTERVAL - 1) == 0 {
            if let Some(ctx) = self.pop_injector() {
                return Some(ctx);
            }
        }
        // 安全性：同上，owner 侧操作只由绑定线程发起。
        if let Some(ctx) = unsafe { self.locals[id].pop_back() } {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            return Some(ctx);
        }
        self.pop_injector().or_else(|| self.steal(id))
    }

    fn push_injector(&self, ctx: Arc<ServiceContext>) {
        let mut injector = self.injector.lock();
        injector.push_back(ctx);
        self.injector_len.store(injector.len(), Ordering::Release);
    }

    fn pop_injector(&self) -> Option<Arc<ServiceContext>> {
        if self.injector_len.load(Ordering::Acquire) == 0 {
            return None;
        }
        let mut injector = self.injector.lock();
        // 可能被别的线程抢先取空了，这时什么都不该记账
        let ctx = injector.pop_front()?;
        self.injector_len.store(injector.len(), Ordering::Release);
        self.pending.fetch_sub(1, Ordering::Relaxed);
        Some(ctx)
    }

    /// 从随机一个起点扫一圈别人的队列。偷不到不代表别人真闲着——
    /// 对方正在写的那一块本来就偷不动。
    fn steal(&self, id: usize) -> Option<Arc<ServiceContext>> {
        let count = self.stealers.len();
        if count < 2 {
            return None;
        }
        let start = (self.slots[id].next_random() % count as u64) as usize;
        for offset in 0..count {
            let victim = (start + offset) % count;
            if victim == id {
                continue;
            }
            if let Some(ctx) = self.stealers[victim].steal_front() {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                return Some(ctx);
            }
        }
        None
    }

    /// worker 收工前把本地队列倒进 injector。
    ///
    /// 收尾阶段主线程还要把剩下的活干完，而它取不到别人的本地队列——窃取允许伪
    /// 失败，靠不住。只有 owner 自己的 `pop_back` 能确定性地排空。
    pub(crate) fn flush_local(&self) {
        let Some(id) = self.worker_id() else {
            return;
        };
        let mut injector = self.injector.lock();
        // 安全性：同上，owner 侧操作只由绑定线程发起。
        while let Some(ctx) = unsafe { self.locals[id].pop_back() } {
            injector.push_back(ctx);
        }
        self.injector_len.store(injector.len(), Ordering::Release);
    }

    /// 取一次投递序列号快照。worker 要在**扫队列之前**取，才能识别出
    /// 「扫完到睡下这段时间里有人投了新活」。
    pub(crate) fn notify_seq(&self) -> u64 {
        self.notify_seq.load(Ordering::SeqCst)
    }

    /// 投递之后叫醒一个睡着的 worker。
    ///
    /// C 版的 `skynet_globalmq_push` 并不唤醒 worker，靠定时器线程每 2.5ms 顺手
    /// 唤醒，代价是消息在所有 worker 都睡着时最坏要等一个 tick。这里改成有人睡就
    /// 直接唤一个，延迟更低；没人睡时只花两个原子操作，连锁都不用碰。
    fn notify(&self) {
        self.notify_seq.fetch_add(1, Ordering::SeqCst);
        if self.sleepers.load(Ordering::SeqCst) > 0 {
            // 借 gate 排一次队：确保对方要么已经睡稳（收得到通知），
            // 要么还没进入 wait（会自己复查序列号后放弃睡觉）
            drop(self.gate.lock());
            self.idle.notify_one();
        }
    }

    /// worker 空闲时睡在这里。`seq` 必须是**开始找活之前**取的快照。
    pub(crate) fn wait_for_work(&self, seq: u64) {
        // 先声明「我要睡了」，再复查序列号。这两步与 notify 里「先自增序列号、
        // 再看有没有人睡」构成 Dekker 模式：SeqCst 全序保证两边至少一方看到对方，
        // 因此不会出现「投递方以为没人睡、睡眠方以为没新活」的丢唤醒。
        self.sleepers.fetch_add(1, Ordering::SeqCst);
        let mut gate = self.gate.lock();
        if self.notify_seq.load(Ordering::SeqCst) == seq && !self.is_quit() {
            self.idle.wait(&mut gate);
        }
        drop(gate);
        self.sleepers.fetch_sub(1, Ordering::SeqCst);
    }

    /// 定时器线程的兜底唤醒，对照 C 版 `wakeup(m, m->count - 1)`。
    pub(crate) fn poke(&self) {
        if self.len() > 0 && self.sleepers.load(Ordering::SeqCst) > 0 {
            self.notify();
        }
    }

    pub(crate) fn set_quit(&self) {
        self.quit.store(true, Ordering::Release);
        self.notify_seq.fetch_add(1, Ordering::SeqCst);
        drop(self.gate.lock());
        self.idle.notify_all();
    }

    pub(crate) fn is_quit(&self) -> bool {
        self.quit.load(Ordering::Acquire)
    }

    /// 正在睡觉的 worker 数，仅供测试等待「对方确实睡下了」。
    #[cfg(test)]
    pub(crate) fn sleeping(&self) -> usize {
        self.sleepers.load(Ordering::SeqCst)
    }

    /// 排队中的服务总数，观测用，允许短暂不精确。
    pub(crate) fn len(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

/// [`Scheduler::register_worker`] 的守卫，析构时解除线程与 worker 编号的绑定。
pub(crate) struct WorkerGuard;

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        CURRENT_WORKER.with(|slot| slot.set(None));
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::message::{MsgType, Payload};
    use crate::server::tests::{dummy_context_on, test_node_with};
    use crate::start::Config;

    fn message(session: i32) -> Message {
        Message::new(1, session, MsgType::USER, Payload::None)
    }

    /// in_global 标志只有在邮箱确实空了之后才清掉
    #[test]
    fn in_global_cleared_only_when_empty() {
        let mailbox = Mailbox::new();
        // 新建时 in_global 已置位，第一次投递不需要入队
        assert!(!mailbox.push_message(message(1)));
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
        // 队列空 -> 清标志
        assert!(mailbox.take_work().is_none());
        // 清掉之后再投递就需要调用方入队了
        assert!(mailbox.push_message(message(2)));
        assert!(!mailbox.push_message(message(3)));
    }

    /// 就绪任务要排在新消息前面，对应协程被 resume 后跑到下次 yield 才轮到下条消息
    #[test]
    fn ready_tasks_come_before_messages() {
        let mailbox = Mailbox::new();
        mailbox.push_message(message(1));
        mailbox.push_ready(7);
        match mailbox.take_work() {
            Some(Work::Task(id)) => assert_eq!(id, 7),
            other => panic!("应先取到就绪任务，实际是 {:?}", other.is_some()),
        }
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
    }

    /// 报警一次之后阈值翻倍，免得持续过载时把日志刷爆
    #[test]
    fn overload_threshold_doubles() {
        let mailbox = Mailbox::new();
        for i in 0..(OVERLOAD_THRESHOLD * 2 + 2) {
            mailbox.push_message(message(i as i32));
        }
        // 弹出一条后剩余长度超过 1024，触发一次报警，阈值抬到 2048
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), OVERLOAD_THRESHOLD * 2 + 1);
        // 读过即清零，且没有再次越过新阈值
        assert_eq!(mailbox.take_overload(), 0);
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), 0);

        // 队列排空后阈值复位
        while mailbox.take_work().is_some() {}
        for i in 0..(OVERLOAD_THRESHOLD + 2) {
            mailbox.push_message(message(i as i32));
        }
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), OVERLOAD_THRESHOLD + 1);
    }

    /// 销毁时要能把未处理的消息全部取出来，好给发送方回错误
    #[test]
    fn drain_returns_all_pending_messages() {
        let mailbox = Mailbox::new();
        mailbox.push_message(message(1));
        mailbox.push_message(message(2));
        mailbox.push_ready(3);
        let left = mailbox.drain();
        assert_eq!(left.len(), 2);
        assert!(mailbox.take_ready().is_none());
    }

    /// 没绑定 worker 的线程（主线程、定时器线程）只走 injector，收发都得通
    #[test]
    fn non_worker_threads_use_the_injector() {
        let node = test_node_with(Config::default().with_thread(2));
        node.sched.push(dummy_context_on(node.clone(), 7));
        assert_eq!(node.sched.len(), 1);
        assert_eq!(node.sched.pop().map(|ctx| ctx.handle), Some(7));
        assert!(node.sched.pop().is_none());
    }

    /// worker 的投递落在自己的本地队列里，别人只能靠窃取拿走
    #[test]
    fn worker_pushes_land_in_its_own_queue() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(0);
                // 攒够两整块，好让 0 号把它们交给窃贼
                for handle in 0..(bwos::BLOCK_SIZE as u32 * 2 + 8) {
                    sched.push(dummy_context_on(node.clone(), handle));
                }
            });
        });
        assert_eq!(sched.len(), bwos::BLOCK_SIZE * 2 + 8);
        assert!(sched.pop().is_none(), "非 worker 线程看不到别人的本地队列");

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(1);
                // 窃取是从头部按先进先出拿，所以拿到的是最早那个
                assert_eq!(sched.pop().map(|ctx| ctx.handle), Some(0));
            });
        });
    }

    /// 本地队列写满之后要溢出到 injector，一件活都不能丢
    #[test]
    fn local_overflow_falls_back_to_the_injector() {
        let node = test_node_with(Config::default().with_thread(1));
        let sched = &node.sched;
        let capacity = bwos::NUM_BLOCKS * bwos::BLOCK_SIZE;

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(0);
                for handle in 0..(capacity as u32 + 8) {
                    sched.push(dummy_context_on(node.clone(), handle));
                }
            });
        });
        assert_eq!(sched.len(), capacity + 8);
        // 溢出的部分谁都够得着
        let mut overflow = 0;
        while sched.pop().is_some() {
            overflow += 1;
        }
        assert_eq!(overflow, 8, "只有装不下的那几个才该落到 injector");
    }

    /// 睡着的 worker 必须被叫醒，而且醒来要真的找得到活
    #[test]
    fn a_sleeping_worker_wakes_up_and_finds_work() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;
        let found = AtomicBool::new(false);

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(0);
                let seq = sched.notify_seq();
                assert!(sched.pop().is_none());
                sched.wait_for_work(seq);
                found.store(sched.pop().is_some(), Ordering::SeqCst);
            });

            // 等 0 号确实睡下，这条用例才不依赖时序
            while sched.sleeping() == 0 {
                std::hint::spin_loop();
            }
            // 明知有人在睡，1 号就该把活放进谁都够得着的 injector 而不是自己的本地队列
            let _worker = sched.register_worker(1);
            sched.push(dummy_context_on(node.clone(), 1));
        });

        assert!(found.load(Ordering::SeqCst), "被唤醒的 worker 应当能取到那件活");
    }
}
