//! 两级消息队列，对照 `skynet-src/skynet_mq.c`。
//!
//! - [`Mailbox`]：每个服务一个，等价于 C 版的 `struct message_queue`。
//!   C 版是「大锁 + 手写环形缓冲加倍扩容」，这里换成两条无锁的
//!   [`SegQueue`] 加一个原子状态机，投递方与消费方全程不加锁。
//! - [`Scheduler`]：存放「有活干的服务」，对应 `struct global_queue`——
//!   但不再是一条被所有线程争抢的队列，见下。
//!
//! 相比 C 版本，邮箱里多了一条 `ready` 队列存放服务内部被唤醒的 Future 任务，
//! 于是「服务可被调度」的条件从「邮箱非空」推广为「邮箱非空或有就绪任务」。
//!
//! # in_global 布尔量为什么升级成状态机
//!
//! C 版靠 `in_global` 一个布尔量维持整套调度的核心不变量：
//! **它为 true 时表示该服务要么排在某条运行队列里，要么正被某个 worker 持有，
//! 因此同一服务绝不会被两个 worker 同时执行。**
//!
//! 但这个布尔量必须与「队列已空」**原子地**绑定，否则会丢活：消费方看到两条
//! 队列都空 → 投递方压入新消息、看到 `in_global` 仍为 true 于是不入队 →
//! 消费方清掉标志放生服务。那条消息就此再也无人处理。C 版靠邮箱那把锁把两件事
//! 圈在一起，无锁化之后改用四态状态机，把「持有期间来了新活」显式记下来：
//!
//! ```text
//! Idle     ──投递──> Queued     投递方 CAS 成功，由它负责入运行队列
//! Queued   ──取走──> Running    worker 从运行队列取到，开始独占执行
//! Running  ──投递──> Notified   持有期间又来了新活，投递方无需入队
//! Notified ──复位──> Running    消费方发现有人投过活，复位后重扫队列
//! Running  ──取空──> Idle       CAS 成功才算放生，失败说明刚被 Notified
//! Running  ──让渡──> Queued     交回运行队列，必须先改状态再入队
//! ```
//!
//! # 为什么全局队列变成了每 worker 一条
//!
//! C 版的全局队列是一把大锁护着的 `VecDeque`，worker 越多争得越凶。这里换成
//! 每个 worker 一条 [`crate::bwos`] 的 BWoS 队列：owner 在自己的队列上无锁
//! push/pop，闲下来才去别人队列头部窃取。
//!
//! 但 BWoS 的 owner 侧操作只允许绑定线程调用，而投递方是任意线程（独占服务那些
//! 线程、外部唤醒的 waker 都可能），所以还留了一条 injector 队列兜底：
//! 非 worker 线程的投递、本地队列写满的溢出，都落到 injector，谁都能从里面取。

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, Thread};

use crate::bwos::{self, CachePad, Owner, Stealer};
use crate::message::Message;
use crate::server::ServiceContext;
use arc_swap::ArcSwapOption;
use crossbeam_queue::SegQueue;

/// 过载报警的初始阈值，对照 C 版 `MQ_OVERLOAD`。
pub(crate) const OVERLOAD_THRESHOLD: usize = 1024;

/// 就绪队列里的一件活。
pub(crate) enum Ready {
    /// 服务内某个任务被唤醒了，去 poll 它。
    Task(usize),
}

/// 一次调度取到的活儿：要么是一条新消息，要么是就绪队列里的一件活。
pub(crate) enum Work {
    Message(Message),
    Ready(Ready),
}

/// 邮箱状态，取代 C 版的 `in_global` 布尔量，取值含义见模块头。
mod state {
    pub(super) const IDLE: u8 = 0;
    pub(super) const QUEUED: u8 = 1;
    pub(super) const RUNNING: u8 = 2;
    pub(super) const NOTIFIED: u8 = 3;
}

pub(crate) struct Mailbox {
    queue: SegQueue<Message>,
    /// 服务内被唤醒、等待 poll 的任务（相当于 skynet 里可以 resume 的协程）。
    ready: SegQueue<Ready>,
    /// 积压的消息条数。过载检测与 `mqlen` 统计都只需要近似值，Relaxed 足够。
    len: AtomicUsize,
    state: AtomicU8,
    /// 下面两个只有「当前持有本服务的那个 worker」会碰，天然无竞争。
    overload: AtomicUsize,
    overload_threshold: AtomicUsize,
}

impl Mailbox {
    #[cfg(test)]
    pub(crate) fn take_ready(&self) -> Option<Ready> {
        self.ready.pop()
    }

    pub(crate) fn new() -> Self {
        Self {
            queue: SegQueue::new(),
            ready: SegQueue::new(),
            len: AtomicUsize::new(0),
            // 与 C 版一致：创建时就置位，保证服务初始化完成前不会被 worker 领走
            state: AtomicU8::new(state::QUEUED),
            overload: AtomicUsize::new(0),
            overload_threshold: AtomicUsize::new(OVERLOAD_THRESHOLD),
        }
    }

    /// 投递消息。返回 true 表示调用方需要把该服务推入运行队列。
    pub(crate) fn push_message(&self, msg: Message) -> bool {
        self.len.fetch_add(1, Ordering::Relaxed);
        self.queue.push(msg);
        self.notify()
    }

    /// 往就绪队列里放一件活。返回 true 表示调用方需要把该服务推入运行队列。
    pub(crate) fn push_ready(&self, ready: Ready) -> bool {
        self.ready.push(ready);
        self.notify()
    }

    /// 宣告「这个服务有活干了」，返回 true 表示调用方需要把它推入运行队列。
    ///
    /// 必须在活**已经**压进队列之后调用：消费方一旦看到 `NOTIFIED`，就必须能
    /// 捞到东西，否则它复位重扫仍是空的，会白转一圈。
    ///
    /// 服务被 kill 时也用它，保证一定会被某个 worker 领走一次以完成销毁。
    pub(crate) fn notify(&self) -> bool {
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            let next = match current {
                state::IDLE => state::QUEUED,
                state::RUNNING => state::NOTIFIED,
                // 已经排在运行队列里，或者持有者已经知道有新活，都无需再管
                _ => return false,
            };
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next == state::QUEUED,
                Err(actual) => current = actual,
            }
        }
    }

    /// 取一件活干。返回 `None` 表示服务彻底空闲，此时状态已经落回 `IDLE`，
    /// 后续的投递方会负责把服务重新推进运行队列。
    ///
    /// 就绪任务优先于新消息：对应 skynet 里被 resume 的协程会一路跑到下一次
    /// yield，之后才轮到下一条消息。
    pub(crate) fn take_work(&self) -> Option<Work> {
        loop {
            if let Some(ready) = self.ready.pop() {
                return Some(Work::Ready(ready));
            }
            if let Some(msg) = self.queue.pop() {
                self.check_overload();
                return Some(Work::Message(msg));
            }
            // 两条队列都看空了。只有 CAS 成功才算真的放生：失败说明就在刚才这段
            // 空隙里有人投了新活，复位成 RUNNING 再扫一遍即可——投递方是「先压队列
            // 再改状态」，所以这一遍必定有收获，循环不会没完没了。
            match self.state.compare_exchange(
                state::RUNNING,
                state::IDLE,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.overload_threshold
                        .store(OVERLOAD_THRESHOLD, Ordering::Relaxed);
                    return None;
                }
                Err(state::NOTIFIED) => self.state.store(state::RUNNING, Ordering::Release),
                Err(other) => {
                    // 走到这里说明有人在服务不属于自己的时候调了 take_work
                    debug_assert!(false, "取活时的邮箱状态不该是 {other}");
                    return None;
                }
            }
        }
    }

    /// 积压超过阈值就记一笔，并把阈值翻倍，免得持续过载时把日志刷爆。
    /// 只有持有本服务的 worker 会调用，因此这里的读改写不必是原子的。
    fn check_overload(&self) {
        let length = self.len.fetch_sub(1, Ordering::Relaxed) - 1;
        let mut threshold = self.overload_threshold.load(Ordering::Relaxed);
        if length <= threshold {
            return;
        }
        while length > threshold {
            threshold *= 2;
        }
        self.overload.store(length, Ordering::Relaxed);
        self.overload_threshold.store(threshold, Ordering::Relaxed);
    }

    /// 状态置为「已排进运行队列」。**必须在真正入队之前调用**：反过来的话，
    /// 别的 worker 可能已经把服务取走并置成 `RUNNING`，我们再一覆盖就变成了
    /// 「标记为在队列里，实际谁也没拿着」，这个服务从此不会再被唤醒。
    pub(crate) fn mark_queued(&self) {
        self.state.store(state::QUEUED, Ordering::Release);
    }

    /// 状态置为「已被某个 worker 独占」，由运行队列的取出方调用。
    pub(crate) fn mark_running(&self) {
        self.state.store(state::RUNNING, Ordering::Release);
    }

    /// 销毁流程的收尾：把状态放回 `IDLE`，让清理之后迟到的消息还能把这个服务
    /// 重新推进运行队列、再被清一次。
    ///
    /// 返回 false 表示清理期间又有活进来了，此时状态仍归调用方所有，
    /// 它必须再清一遍再来放生。**这是「两个 worker 不会同时清理同一个服务」的
    /// 依据**：只有这一次 CAS 成功之后，服务才可能被别人重新领走。
    pub(crate) fn release(&self) -> bool {
        match self.state.compare_exchange(
            state::RUNNING,
            state::IDLE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(state::NOTIFIED) => {
                self.state.store(state::RUNNING, Ordering::Release);
                false
            }
            Err(other) => {
                debug_assert!(false, "放生时的邮箱状态不该是 {other}");
                true
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// 服务此刻是否「没人管」：既不在运行队列里，也没被谁持有。
    ///
    /// 只是一瞬间的观测，仅供测试等待「对方确实空转下来了」。
    #[cfg(test)]
    pub(crate) fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == state::IDLE
    }

    /// 取出并清零过载读数，对照 `skynet_mq_overload`。
    pub(crate) fn take_overload(&self) -> usize {
        self.overload.swap(0, Ordering::Relaxed)
    }

    /// 服务销毁时清空邮箱，未处理的消息交回调用方以便给发起者回错误。
    pub(crate) fn drain(&self) -> Vec<Message> {
        while self.ready.pop().is_some() {}
        let mut left = Vec::new();
        while let Some(msg) = self.queue.pop() {
            self.len.fetch_sub(1, Ordering::Relaxed);
            left.push(msg);
        }
        left
    }
}

/// 连续取活多少次之后回头看一眼 injector，必须是 2 的幂。
///
/// 本地队列取空之前不会主动去碰 injector，这个周期性回看是为了防止 injector 里的
/// 活被本地队列饿死，对应 Go 调度器每 61 次查一遍全局队列的做法。
const INJECT_INTERVAL: usize = 64;

/// 找不到活时先空转几轮再挂起。
///
/// 活是一阵一阵来的，而 Windows 上一次「挂起 + 唤醒」往返要 1~10µs，白挨一次很
/// 不划算。自旋一会儿就能接住那些只差一点点就赶上的投递，代价是几百纳秒的空转。
///
/// 256 是量出来的：「16 worker / 4 服务」那条压测在 3 / 64 / 256 / 1024 轮下分别是
/// 70 / 131 / 206 / 194 万次每秒——再往上，空转的 worker 开始抢真正干活那几个的
/// CPU，另外两条压测也跟着掉。
const SPIN_ROUNDS: usize = 256;

/// 一轮窃取最多试几个受害者。
///
/// BWoS 允许伪失败（对方正在写的那一块偷不动），所以「扫完一圈没偷到」并不说明
/// 别人真闲着；与其把 N-1 条队列全读一遍，不如试两下就回去看 injector 再睡。
const STEAL_ATTEMPTS: usize = 2;

thread_local! {
    /// 当前线程绑定到了哪个调度器的哪号 worker。
    ///
    /// 存调度器地址是因为同进程可以跑多个节点，得认准自己那一个；非 worker 线程
    /// （主线程、独占服务的线程、外部唤醒线程）这里始终是 `None`。
    static CURRENT_WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

/// 每个 worker 的私有记账，独占一条 cache line。
#[derive(Default)]
struct WorkerSlot {
    /// 取活次数，用来触发 [`INJECT_INTERVAL`] 的回看。
    tick: AtomicUsize,
    /// 挑窃取目标用的 xorshift 状态。
    rng: AtomicU64,
    /// 绑定线程的句柄，[`Scheduler::register_worker`] 时填入，用于定向唤醒。
    thread: ArcSwapOption<Thread>,
    /// 定向递交的单槽，对照 ltask 的 `worker->service_ready`。
    ///
    /// 空指针表示没有；非空时它是一个「寄存」在这里的 `Arc` 强引用，
    /// 取出方负责用 [`Arc::from_raw`] 收回，因此计数不会失衡。
    handoff: AtomicPtr<ServiceContext>,
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

    /// 把服务直接递到这个 worker 手上。槽已被占时原样交还调用方。
    fn offer(&self, ctx: Arc<ServiceContext>) -> Result<(), Arc<ServiceContext>> {
        let ptr = Arc::into_raw(ctx).cast_mut();
        match self.handoff.compare_exchange(
            std::ptr::null_mut(),
            ptr,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => Ok(()),
            // 安全性：CAS 失败说明这个 Arc 没交出去，所有权仍在我们手里
            Err(_) => Err(unsafe { Arc::from_raw(ptr) }),
        }
    }

    /// 取走别人递过来的服务。
    fn take(&self) -> Option<Arc<ServiceContext>> {
        let ptr = self.handoff.swap(std::ptr::null_mut(), Ordering::AcqRel);
        // 安全性：swap 保证同一个指针只会被一个线程取到，收回的正是 offer 寄存的那个引用
        (!ptr.is_null()).then(|| unsafe { Arc::from_raw(ptr) })
    }

    fn wake(&self) {
        if let Some(thread) = self.thread.load_full() {
            thread.unpark();
        }
    }
}

/// 运行队列的集合：每 worker 一条 BWoS 本地队列，外加一条谁都能用的 injector。
pub(crate) struct Scheduler {
    /// 下标即 worker 编号。owner 侧方法只允许对应的 worker 线程调用。
    locals: Vec<Owner<Arc<ServiceContext>>>,
    stealers: Vec<Stealer<Arc<ServiceContext>>>,
    slots: Vec<CachePad<WorkerSlot>>,
    /// 非 worker 线程的投递、以及本地队列写满时的溢出，都落在这里。
    injector: SegQueue<Arc<ServiceContext>>,
    /// 所有队列加起来排着多少个服务。
    ///
    /// 它存在的理由是让「全都空着」这个最常见的情况便宜到只剩一次原子读：
    /// worker 每干满一批活就要问一遍「还有别人在等吗」（见
    /// [`Scheduler::should_yield`]），取活时也要先判一次空。没有它的话每次都要
    /// 走一遍「遍历本地块 + 锁 injector + 扫一圈别人的队列」，空转成本比原来那把
    /// 大锁还高。
    ///
    /// 反过来，它是被每次 push/pop 反复写的热缓存行，所以问的一方要按批问，
    /// 别按件问。
    pending: CachePad<AtomicUsize>,
    /// 空闲 worker 位图，每 64 个 worker 一个字：位 i 为 1 表示 i 号 worker 已经
    /// 登记「我要睡了」。对照 C 版 `struct monitor` 的 `sleep` 计数，
    /// 但记的是「谁」而不只是「几个」，这样才能定向唤醒。
    idle: Box<[CachePad<AtomicU64>]>,
    /// 醒着、手上没活、正在扫队列的 worker 数。
    ///
    /// 有人在找活时投递方就不必再叫人起来——反正那位马上会扫到这件活。
    /// 少了这一条，一次扇出风暴会叫醒 O(消息数) 个线程，全都白起。
    searching: CachePad<AtomicUsize>,
    /// 同时找活的 worker 数上限，超了就直接去睡。
    ///
    /// 没有这道闸，N 个空闲 worker 会一起去扫全场，跨核缓存流量是 O(N²)，
    /// 而它们本来就是因为没活干才在扫。
    max_searchers: usize,
    /// 「哪条本地队列有货可偷」的提示位图，每 64 个 worker 一个字。
    ///
    /// 有了它，挑受害者从「挨个去碰 N 条别人的队列」变成「读一个字、挑一位」。
    /// 它只是提示，允许滞后：owner 跨块时置位（每 `BLOCK_SIZE` 个元素一次，
    /// 摊薄到可忽略），窃贼白跑一趟就把它清掉，于是错报最多浪费一次尝试。
    stealable: Box<[CachePad<AtomicU64>]>,
    quit: AtomicBool,
    /// 测试钩子：让 worker 停在「登记 searching」之后，便于把 push 的竞态窗口
    /// 放大成确定性用例。
    #[cfg(test)]
    pause_after_searching_inc: AtomicBool,
    /// 测试钩子：让 push 停在「看到 searcher」之后、真正塞进 injector 之前。
    #[cfg(test)]
    pause_before_injector_push: AtomicBool,
    /// 测试钩子：push 已经看到 searcher、正停在 injector 之前。
    #[cfg(test)]
    injector_push_paused: AtomicBool,
    /// 测试钩子：push 已确认没有 searcher 和空闲 worker，停在真正入队之前。
    #[cfg(test)]
    pause_before_place: AtomicBool,
    /// 测试钩子：push 正停在「无人可唤醒」与真正入队之间。
    #[cfg(test)]
    place_paused: AtomicBool,
    /// 测试钩子：worker 已完成睡前最后一次扫描，停在 park 之前。
    #[cfg(test)]
    pause_before_park: AtomicBool,
    /// 测试钩子：worker 正停在最后一次扫描与 park 之间。
    #[cfg(test)]
    park_paused: AtomicBool,
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
            injector: SegQueue::new(),
            pending: CachePad(AtomicUsize::new(0)),
            idle: (0..workers.div_ceil(64))
                .map(|_| CachePad(AtomicU64::new(0)))
                .collect(),
            searching: CachePad(AtomicUsize::new(0)),
            max_searchers: (workers / 2).max(1),
            stealable: (0..workers.div_ceil(64))
                .map(|_| CachePad(AtomicU64::new(0)))
                .collect(),
            quit: AtomicBool::new(false),
            #[cfg(test)]
            pause_after_searching_inc: AtomicBool::new(false),
            #[cfg(test)]
            pause_before_injector_push: AtomicBool::new(false),
            #[cfg(test)]
            injector_push_paused: AtomicBool::new(false),
            #[cfg(test)]
            pause_before_place: AtomicBool::new(false),
            #[cfg(test)]
            place_paused: AtomicBool::new(false),
            #[cfg(test)]
            pause_before_park: AtomicBool::new(false),
            #[cfg(test)]
            park_paused: AtomicBool::new(false),
        }
    }

    /// 把当前线程绑定成 `id` 号 worker，从此它的投递与取活优先走本地队列，
    /// 并且可以被定向唤醒。返回的守卫析构时解绑。
    pub(crate) fn register_worker(&self, id: usize) -> WorkerGuard {
        assert!(id < self.locals.len(), "worker 编号越界");
        self.slots[id]
            .thread
            .store(Some(Arc::new(thread::current())));
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
        // 独占服务的执行者是它自己那条线程，进了运行队列就等于允许两条线程同时
        // 执行同一个服务——那条不变量是 SvcCell 的立身之本，这里守一道
        debug_assert!(!ctx.is_exclusive(), "独占服务不该进运行队列");
        // 顺序要紧：先宣告「我在队列里」再真的入队。反过来的话，别的 worker 可能
        // 已经把它取走并置成 RUNNING，我们这一记 store 就把它标成了「在队列里，
        // 实际谁也没拿着」，这个服务从此不会再被唤醒。
        ctx.mailbox.mark_queued();
        self.pending.fetch_add(1, Ordering::Relaxed);

        // 这道屏障与 worker 睡前的那道配对，见 [`Scheduler::find_work_or_park`]：
        // 两侧各有一道 SeqCst 全序点，才能保证「投递方以为没人要睡、
        // 待睡方以为没有新活」这种双向失明不会发生。
        std::sync::atomic::fence(Ordering::SeqCst);
        if self.searching.load(Ordering::Relaxed) > 0 {
            // 已经有人醒着在扫队列，就把活放进谁都够得着的 injector 让他顺手捞走，
            // 省下一次挂起唤醒往返（Windows 上 1~10µs，乒乓型负载里全部开销都在这）。
            // 这里**不能**图快放自己的本地队列：BWoS 里 owner 正在写的那一块对窃贼
            // 是隐形的，那位扫一圈空手而归就去睡了，下一跳又得叫人。
            #[cfg(test)]
            {
                self.injector_push_paused.store(true, Ordering::Release);
                while self.pause_before_injector_push.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                self.injector_push_paused.store(false, Ordering::Release);
            }
            self.injector.push(ctx);
            // 复查一次：searcher 可能正好在我们看它与真正入队之间全部睡下。
            // 这时 searching 已经减到 0，而睡下的人一定先登记了空闲位；
            // 补叫一个，别让这件 injector 里的活陪着它一起睡过去。
            if self.searching.load(Ordering::SeqCst) == 0 {
                if let Some(id) = self.claim_idle() {
                    self.slots[id].wake();
                }
            }
            return;
        }
        if let Some(id) = self.claim_idle() {
            // 定向递交：既省掉一趟 injector，也同样绕开了「当前块隐形」的问题
            let slot = &self.slots[id];
            if let Err(ctx) = slot.offer(ctx) {
                self.place(ctx);
            }
            // 位已经被我们摘掉了，不管递交成不成，这一下都必须叫：
            // 否则没人再会来叫它，它就带着活睡过去了
            slot.wake();
            return;
        }
        // 大家都在忙，没人等着接活：进自己的本地队列，这条路最快
        // worker 投到本地队列后自己仍在运行，稍后能亲自取走；只有外部/独占线程
        // 投 injector 时，才可能出现「唯一能取活的 worker 正好在此刻睡下」。
        let needs_wakeup_recheck = self.worker_id().is_none();
        #[cfg(test)]
        {
            self.place_paused.store(true, Ordering::Release);
            while self.pause_before_place.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            self.place_paused.store(false, Ordering::Release);
        }
        self.place(ctx);
        // 上面的第一次检查与真正入队之间，可能恰好有 worker 从 searching 转成
        // idle 并睡下。任务此刻已经对取活方可见，再按与睡眠侧相同的 SeqCst
        // 顺序复查一次：仍有人在找就让它捞走，否则叫醒刚登记的空闲 worker。
        if needs_wakeup_recheck && self.searching.load(Ordering::SeqCst) == 0 {
            if let Some(id) = self.claim_idle() {
                self.slots[id].wake();
            }
        }
    }

    /// 把服务安置进某条运行队列：worker 线程优先放自己的本地队列（无锁且缓存热），
    /// 其余线程与本地队列写满的溢出都落到 injector。
    fn place(&self, ctx: Arc<ServiceContext>) {
        let overflow = match self.worker_id() {
            // 安全性：worker 编号来自线程局部变量，同一条本地队列只有绑定它的
            // 那个线程会走到这里，满足 BWoS 的 owner 独占契约。
            Some(id) => match unsafe { self.locals[id].push_back(ctx) } {
                // 让出了一整块，告诉窃贼这里有货
                Ok(bwos::Pushed::Granted) => {
                    self.mark_stealable(id);
                    None
                }
                Ok(bwos::Pushed::Local) => None,
                Err(ctx) => Some(ctx),
            },
            None => Some(ctx),
        };
        if let Some(ctx) = overflow {
            self.injector.push(ctx);
        }
    }

    /// 取一个有活干的服务。worker 线程走「本地 -> injector -> 窃取」，
    /// 其它线程只能取 injector。
    ///
    /// 取到的服务状态一律置成 `RUNNING`，也就是「已被本线程独占」——
    /// 这是「同一服务绝不会被两个 worker 同时执行」这条不变量的落点。
    pub(crate) fn pop(&self) -> Option<Arc<ServiceContext>> {
        let ctx = self.pop_runnable()?;
        ctx.mailbox.mark_running();
        Some(ctx)
    }

    fn pop_runnable(&self) -> Option<Arc<ServiceContext>> {
        let Some(id) = self.worker_id() else {
            return self.pop_injector();
        };
        // 别人指名递到手上的活最优先，而且这一步不能被下面的 pending 快速判空绕过
        if let Some(ctx) = self.slots[id].take() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            return Some(ctx);
        }
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

    fn pop_injector(&self) -> Option<Arc<ServiceContext>> {
        // 可能被别的线程抢先取空了，这时什么都不该记账
        let ctx = self.injector.pop()?;
        self.pending.fetch_sub(1, Ordering::Relaxed);
        Some(ctx)
    }

    /// 按 `stealable` 位图挑受害者下手，最多试 [`STEAL_ATTEMPTS`] 个。
    ///
    /// 位图之外的队列一概不碰：读一个字就能筛掉全场，省下的正是「一个空闲 worker
    /// 挨个去读 N-1 条别人的队列头」那份跨核缓存流量。起点仍然随机，免得所有窃贼
    /// 一起盯着编号最小的那个受害者。
    ///
    /// 偷不到不代表对方真闲着——对方正在写的那一块本来就偷不动，所以这里失败即
    /// 收手，回去看 injector、自旋、然后睡，比固执地扫满一圈划算。
    fn steal(&self, id: usize) -> Option<Arc<ServiceContext>> {
        let count = self.stealers.len();
        if count < 2 {
            return None;
        }
        let start = (self.slots[id].next_random() % count as u64) as usize;
        let mut attempts = 0;
        for offset in 0..count {
            let victim = (start + offset) % count;
            if victim == id || !self.is_stealable(victim) {
                continue;
            }
            if let Some(ctx) = self.stealers[victim].steal_front() {
                self.pending.fetch_sub(1, Ordering::Relaxed);
                return Some(ctx);
            }
            // 白跑一趟：把提示位清掉，别的窃贼就不必再来一遍。
            // owner 下次跨块会重新置位，所以清早了也不会漏活
            self.clear_stealable(victim);
            attempts += 1;
            if attempts >= STEAL_ATTEMPTS {
                break;
            }
        }
        None
    }

    fn is_stealable(&self, id: usize) -> bool {
        self.stealable[id / 64].load(Ordering::Relaxed) & (1 << (id % 64)) != 0
    }

    fn mark_stealable(&self, id: usize) {
        self.stealable[id / 64].fetch_or(1 << (id % 64), Ordering::Release);
    }

    fn clear_stealable(&self, id: usize) {
        self.stealable[id / 64].fetch_and(!(1 << (id % 64)), Ordering::Relaxed);
    }

    /// worker 收工前把手上的队列倒进 injector。
    ///
    /// 收尾阶段主线程还要把剩下的活干完，而它取不到别人的本地队列——窃取允许伪
    /// 失败，靠不住。只有 owner 自己的 `pop_back` 能确定性地排空。
    pub(crate) fn flush_local(&self) {
        let Some(id) = self.worker_id() else {
            return;
        };
        if let Some(ctx) = self.slots[id].take() {
            self.injector.push(ctx);
        }
        // 安全性：同上，owner 侧操作只由绑定线程发起。
        while let Some(ctx) = unsafe { self.locals[id].pop_back() } {
            self.injector.push(ctx);
        }
        self.clear_stealable(id);
    }

    /// 找活：先连扫几轮，扫不到就登记空闲并挂起。返回自旋期间捞到的活。
    ///
    /// # 为什么不会丢唤醒
    ///
    /// 投递方是「压队列 → 屏障 → 看有没有人在找活 / 有没有人睡着」，
    /// 待睡方是「登记空闲位 → 屏障 → 再扫一遍队列」。两边各有一道 SeqCst 全序点，
    /// 于是至少有一方能看见对方：要么投递方看到空闲位并把我们叫起来，
    /// 要么我们那最后一遍扫描能看到它投的活。这就是原来那套「投递序列号 + 条件
    /// 变量」的无锁版本，代价从「共享行上的 SeqCst 读改写」降到「本地屏障 +
    /// 读多写少的普通读」，而且唤醒从「随便叫一个」变成了定向叫。
    ///
    /// `thread::park` 自带唤醒令牌，早到的 `unpark` 不会丢，所以这里不需要
    /// 「睡前复查」之外的任何配合。
    pub(crate) fn find_work_or_park(&self) -> Option<Arc<ServiceContext>> {
        let id = self.worker_id().expect("只有 worker 线程会来找活");
        // 找活的人已经够多了就别再添乱，直接去睡：真有活时投递方会点名叫我们
        let searching = self.searching.fetch_add(1, Ordering::SeqCst);
        #[cfg(test)]
        while self.pause_after_searching_inc.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let rounds = if searching < self.max_searchers {
            SPIN_ROUNDS
        } else {
            0
        };
        for _ in 0..rounds {
            if let Some(ctx) = self.pop() {
                self.searching.fetch_sub(1, Ordering::Relaxed);
                return Some(ctx);
            }
            if self.is_quit() {
                self.searching.fetch_sub(1, Ordering::Relaxed);
                return None;
            }
            std::hint::spin_loop();
        }

        // 登记空闲位必须排在 searching 减回去之前：中间那一瞬要是「既没人在找活、
        // 也没人登记空闲」，投递方就谁都不叫了，而我们正准备睡下
        self.mark_idle(id);
        self.searching.fetch_sub(1, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);

        if let Some(ctx) = self.pop() {
            self.clear_idle(id);
            return Some(ctx);
        }
        if !self.is_quit() {
            #[cfg(test)]
            {
                self.park_paused.store(true, Ordering::Release);
                while self.pause_before_park.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                self.park_paused.store(false, Ordering::Release);
            }
            thread::park();
        }
        self.clear_idle(id);
        None
    }

    /// 摘一个已登记空闲的 worker 出来，摘到就意味着「叫它起来是我的责任」。
    fn claim_idle(&self) -> Option<usize> {
        for (index, word) in self.idle.iter().enumerate() {
            let mut bits = word.load(Ordering::Relaxed);
            while bits != 0 {
                let bit = bits.trailing_zeros();
                match word.compare_exchange_weak(
                    bits,
                    bits & !(1 << bit),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(index * 64 + bit as usize),
                    // 被别的投递方抢先摘走了，看看这个字里还有没有别人
                    Err(actual) => bits = actual,
                }
            }
        }
        None
    }

    fn mark_idle(&self, id: usize) {
        self.idle[id / 64].fetch_or(1 << (id % 64), Ordering::SeqCst);
    }

    fn clear_idle(&self, id: usize) {
        self.idle[id / 64].fetch_and(!(1 << (id % 64)), Ordering::Relaxed);
    }

    /// 定时器服务的兜底唤醒，对照 C 版 `wakeup(m, m->count - 1)`。
    pub(crate) fn poke(&self) {
        if self.len() > 0 {
            if let Some(id) = self.claim_idle() {
                self.slots[id].wake();
            }
        }
    }

    /// 手上这个服务该交回运行队列了吗：有别人在等，或者节点要收工。
    ///
    /// 收工信号折进来是因为它是自唤醒任务唯一的出口：那种服务的就绪队列永远
    /// 非空，`run_service` 走不到 `Ran::Idle`，而 worker 的退出条件在它外面。
    /// 少了这一句，一个 poll 里自唤醒的任务就能把 worker 永久扣住——主线程正
    /// 卡在 join 上，`retire_all` 排在 join 之后，于是没人会去把那个服务标成
    /// 死的，节点再也关不掉。
    ///
    /// `pending` 是热的写行，所以调用方必须按批问而不是按件问。
    pub(crate) fn should_yield(&self) -> bool {
        self.pending.load(Ordering::Relaxed) > 0 || self.is_quit()
    }

    /// 只管 worker。要宣布整个节点收工请走 `Node::quit`，独占线程还得单独敲。
    pub(crate) fn set_quit(&self) {
        self.quit.store(true, Ordering::Release);
        // 收工信号要让所有睡着的 worker 都醒过来，一个都不能落下
        for slot in &self.slots {
            slot.wake();
        }
    }

    pub(crate) fn is_quit(&self) -> bool {
        self.quit.load(Ordering::Acquire)
    }

    /// 已登记空闲的 worker 数，仅供测试等待「对方确实睡下了」。
    #[cfg(test)]
    pub(crate) fn sleeping(&self) -> usize {
        self.idle
            .iter()
            .map(|word| word.load(Ordering::SeqCst).count_ones() as usize)
            .sum()
    }

    /// 排队中的服务总数，观测用，允许短暂不精确。
    pub(crate) fn len(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        // handoff 槽里寄存着 Arc 强引用，正常收尾时 flush_local 会取空，
        // 这里兜住异常路径（比如 worker 线程 panic）以免泄漏
        for slot in &self.slots {
            drop(slot.take());
        }
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

    /// 造一个「已被 worker 领走」的邮箱，即 take_work 的正常前提
    fn running_mailbox() -> Mailbox {
        let mailbox = Mailbox::new();
        mailbox.mark_running();
        mailbox
    }

    /// 状态只有在邮箱确实空了之后才落回 IDLE
    #[test]
    fn state_falls_back_to_idle_only_when_empty() {
        let mailbox = Mailbox::new();
        // 新建时状态已是 QUEUED，第一次投递不需要入队
        assert!(!mailbox.push_message(message(1)));
        mailbox.mark_running();
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
        // 队列空 -> 落回 IDLE
        assert!(mailbox.take_work().is_none());
        // 落回之后再投递就需要调用方入队了
        assert!(mailbox.push_message(message(2)));
        assert!(!mailbox.push_message(message(3)));
    }

    /// 持有期间来的活不会丢：消费方判空之后才被通知，也必须重扫一遍
    #[test]
    fn work_arriving_during_the_empty_check_is_not_lost() {
        let mailbox = running_mailbox();
        // 投递方压进一条活，并把状态从 RUNNING 推到 NOTIFIED
        assert!(
            !mailbox.push_message(message(1)),
            "持有者还在，投递方不该自己入队"
        );
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));

        // 再模拟最刁的那一刻：消费方已经看完两条空队列、正要落回 IDLE 时被通知。
        // 此时状态是 NOTIFIED 而队列是空的，消费方必须复位重扫一遍才能放生
        assert!(!mailbox.notify());
        assert!(
            mailbox.take_work().is_none(),
            "重扫确实没活，才允许落回 IDLE"
        );
        // 落回之后的投递方就该自己负责入队了
        assert!(mailbox.push_message(message(2)));
    }

    /// 清理期间又来了活就不许放生：否则服务会被别的 worker 领走，两个人同时清一个
    #[test]
    fn release_refuses_while_new_work_arrives() {
        let mailbox = running_mailbox();
        assert!(mailbox.release(), "队列空着，放生");

        mailbox.mark_running();
        assert!(!mailbox.push_message(message(1)), "持有者还在，不必入队");
        assert!(!mailbox.release(), "清理期间来了新活，不许放生");
        // 状态仍归调用方所有，再清一遍才放得掉
        mailbox.drain();
        assert!(mailbox.release());
    }

    /// 就绪任务要排在新消息前面，对应协程被 resume 后跑到下次 yield 才轮到下条消息
    #[test]
    fn ready_tasks_come_before_messages() {
        let mailbox = running_mailbox();
        mailbox.push_message(message(1));
        mailbox.push_ready(Ready::Task(7));
        match mailbox.take_work() {
            Some(Work::Ready(Ready::Task(id))) => assert_eq!(id, 7),
            other => panic!("应先取到就绪任务，实际是 {:?}", other.is_some()),
        }
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
    }

    /// 报警一次之后阈值翻倍，免得持续过载时把日志刷爆
    #[test]
    fn overload_threshold_doubles() {
        let mailbox = running_mailbox();
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
        mailbox.mark_running();
        for i in 0..(OVERLOAD_THRESHOLD + 2) {
            mailbox.push_message(message(i as i32));
        }
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), OVERLOAD_THRESHOLD + 1);
    }

    /// 销毁时要能把未处理的消息全部取出来，好给发送方回错误
    #[test]
    fn drain_returns_all_pending_messages() {
        let mailbox = running_mailbox();
        mailbox.push_message(message(1));
        mailbox.push_message(message(2));
        mailbox.push_ready(Ready::Task(3));
        let left = mailbox.drain();
        assert_eq!(left.len(), 2);
        assert_eq!(mailbox.len(), 0, "长度计数要跟着清干净");
        assert!(mailbox.take_ready().is_none());
    }

    /// 多个投递方与一个消费方并发时，一件活都不能丢、也不能被处理两次
    #[test]
    fn concurrent_push_and_take_lose_nothing() {
        const SENDERS: usize = 4;
        const PER_SENDER: i32 = 5_000;

        let mailbox = Mailbox::new();
        // 用一个布尔量冒充运行队列：true 表示这个服务正排在里面等人来取
        let queued = AtomicBool::new(true);
        let mut taken = 0usize;

        thread::scope(|scope| {
            for _ in 0..SENDERS {
                scope.spawn(|| {
                    for i in 0..PER_SENDER {
                        if mailbox.push_message(message(i)) {
                            queued.store(true, Ordering::Release);
                        }
                    }
                });
            }

            let total = SENDERS * PER_SENDER as usize;
            while taken < total {
                // 取走服务这一步与真实的 Scheduler::pop 一致：先出队，再置 RUNNING
                if !queued.swap(false, Ordering::AcqRel) {
                    std::hint::spin_loop();
                    continue;
                }
                mailbox.mark_running();
                while mailbox.take_work().is_some() {
                    taken += 1;
                }
            }
        });

        assert_eq!(taken, SENDERS * PER_SENDER as usize, "一条都不该丢");
        assert_eq!(mailbox.len(), 0);
    }

    /// 没绑定 worker 的线程（主线程、独占服务的线程）只走 injector，收发都得通
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

    /// 可偷位图允许滞后，但不能长期错位：攒满一块就置位，窃贼白跑一趟就清位
    #[test]
    fn the_stealable_hint_follows_the_queues() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(0);
                // 不满一块时对窃贼是隐形的，位图也不该置位
                for handle in 0..(bwos::BLOCK_SIZE as u32) {
                    sched.push(dummy_context_on(node.clone(), handle));
                }
                assert!(!sched.is_stealable(0), "还在当前块里，没什么可偷");
                // 再压一个就跨块了，前一块整块交出去
                sched.push(dummy_context_on(node.clone(), 999));
                assert!(sched.is_stealable(0));
            });
        });

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(1);
                // 位图指到哪偷到哪，把交出来的那一块掏空
                for _ in 0..bwos::BLOCK_SIZE {
                    assert!(sched.pop().is_some());
                }
                assert!(sched.is_stealable(0), "位图滞后是允许的");
                // 白跑这一趟之后位就该清掉，别的窃贼不必再来一遍
                assert!(sched.pop().is_none());
                assert!(!sched.is_stealable(0));
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
                assert!(sched.pop().is_none());
                // 自旋几轮后登记空闲位并挂起，被叫醒后从 handoff 槽里取到那件活
                sched.find_work_or_park();
                found.store(sched.pop().is_some(), Ordering::SeqCst);
            });

            // 等 0 号确实睡下，这条用例才不依赖时序
            while sched.sleeping() == 0 {
                std::hint::spin_loop();
            }
            // 明知有人在睡，1 号就该把活直接递到它手上而不是塞自己的本地队列
            let _worker = sched.register_worker(1);
            sched.push(dummy_context_on(node.clone(), 1));
        });

        assert!(
            found.load(Ordering::SeqCst),
            "被唤醒的 worker 应当能取到那件活"
        );
    }

    /// 递到手上的活优先于本地队列里的：定向唤醒的语义就是「这件活归你」
    #[test]
    fn a_handed_off_service_comes_first() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;

        thread::scope(|scope| {
            scope.spawn(|| {
                let _worker = sched.register_worker(0);
                // 先往自己的本地队列里塞一个，此时无人空闲，走的是本地路径
                sched.push(dummy_context_on(node.clone(), 7));
                // 再登记成空闲，于是下一次投递会走「摘一个空闲 worker 定向递交」
                sched.mark_idle(0);
                sched.push(dummy_context_on(node.clone(), 9));
                assert_eq!(sched.sleeping(), 0, "空闲位应当被投递方摘走");
                assert_eq!(sched.pop().map(|ctx| ctx.handle), Some(9), "递到手上的优先");
                assert_eq!(sched.pop().map(|ctx| ctx.handle), Some(7));
            });
        });
    }

    /// handoff 槽被占着时投递要退回队列，不能把活丢了
    #[test]
    fn a_taken_handoff_slot_falls_back_to_the_queue() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;

        // 第一件活递进 0 号的槽里
        sched.mark_idle(0);
        sched.push(dummy_context_on(node.clone(), 1));
        // 第二件活来的时候槽已经占着了，只能退回队列
        sched.mark_idle(0);
        sched.push(dummy_context_on(node.clone(), 2));

        assert_eq!(sched.len(), 2, "两件活都得在账上");
        // 递到 0 号手上那件只有它自己拿得到，非 worker 线程只够得着 injector
        assert_eq!(sched.pop().map(|ctx| ctx.handle), Some(2));
        assert!(sched.pop().is_none());
    }

    /// 已经有 worker 醒着在找活时，投递方不该再叫醒别人
    #[test]
    fn a_searching_worker_absorbs_the_wakeup() {
        let node = test_node_with(Config::default().with_thread(2));
        let sched = &node.sched;

        // 0 号登记为空闲，同时假装有人正在找活
        sched.mark_idle(0);
        sched.searching.fetch_add(1, Ordering::SeqCst);
        sched.push(dummy_context_on(node.clone(), 1));
        assert_eq!(sched.sleeping(), 1, "有人在找活，空闲位不该被摘掉");

        // 找活的那位走了，下一次投递才轮到叫人
        sched.searching.fetch_sub(1, Ordering::SeqCst);
        sched.push(dummy_context_on(node.clone(), 2));
        assert_eq!(sched.sleeping(), 0, "这次该把睡着的 0 号摘出来叫醒了");
    }

    /// push 看到 searcher 后，那位却正好在 push 真正入队前睡下：
    /// 这是「searcher 吸收唤醒」路径上最窄的竞态窗口。用测试钩子把窗口撑到
    /// 确定性可复现，push 必须在 injector 入队后复查一遍，把刚睡下的人叫醒。
    #[test]
    fn a_searcher_that_parks_before_the_push_lands_is_woken_directly() {
        let node = test_node_with(Config::default().with_thread(1));
        let sched = &node.sched;

        thread::scope(|scope| {
            // 无论用例在哪一步失败，都要先把测试钩子解开并把睡着的 worker 叫醒，
            // 否则 scoped thread 会陪着用例一起挂死。守卫必须放在 scope 闭包内，
            // 这样 panic 时它会先于「等待所有线程结束」执行。
            struct ReleaseHooks<'a>(&'a Scheduler);
            impl Drop for ReleaseHooks<'_> {
                fn drop(&mut self) {
                    self.0
                        .pause_after_searching_inc
                        .store(false, Ordering::SeqCst);
                    self.0
                        .pause_before_injector_push
                        .store(false, Ordering::SeqCst);
                    for slot in &self.0.slots {
                        slot.wake();
                    }
                }
            }
            let _release = ReleaseHooks(sched);
            let wait = |flag: &AtomicBool, expected: bool| {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while flag.load(Ordering::Acquire) != expected
                    && std::time::Instant::now() < deadline
                {
                    std::hint::spin_loop();
                }
                flag.load(Ordering::Acquire) == expected
            };

            sched
                .pause_after_searching_inc
                .store(true, Ordering::SeqCst);
            let worker = scope.spawn(|| {
                let _worker = sched.register_worker(0);
                sched.find_work_or_park();
                sched.pop().is_some()
            });

            // worker 已经登记 searching，正被钩子停在扫描之前
            {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while sched.searching.load(Ordering::SeqCst) == 0
                    && std::time::Instant::now() < deadline
                {
                    std::hint::spin_loop();
                }
                assert_eq!(
                    sched.searching.load(Ordering::SeqCst),
                    1,
                    "worker 应先登记 searching"
                );
            }

            // 让 push 看到 searching 之后也停在 injector 之前
            sched
                .pause_before_injector_push
                .store(true, Ordering::SeqCst);
            let pusher = scope.spawn(|| sched.push(dummy_context_on(node.clone(), 7)));
            assert!(
                wait(&sched.injector_push_paused, true),
                "push 应当看到 searcher 并停在 injector 之前"
            );

            // 放走 worker：它扫不到尚未入队的活，只能登记空闲并睡下
            sched
                .pause_after_searching_inc
                .store(false, Ordering::SeqCst);
            {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while sched.sleeping() == 0 && std::time::Instant::now() < deadline {
                    std::hint::spin_loop();
                }
                assert_eq!(sched.sleeping(), 1, "worker 应当已经睡下");
            }

            // 现在放 push 入队。若复查逻辑缺失，worker 会带着这件活一直睡；
            // 这里只短暂等待，之后用 poke 兜底，保证用例失败时也能收拾干净。
            sched
                .pause_before_injector_push
                .store(false, Ordering::SeqCst);
            pusher.join().unwrap();

            let woken_directly = {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
                while sched.sleeping() != 0 && std::time::Instant::now() < deadline {
                    std::hint::spin_loop();
                }
                sched.sleeping() == 0
            };
            if !woken_directly {
                sched.poke();
            }

            let got_work = worker.join().unwrap();
            assert!(
                woken_directly,
                "push 在 injector 入队后必须复查并叫醒刚睡下的 worker"
            );
            assert!(got_work, "worker 醒来后应当从 injector 拿到这件活");
        });

        assert_eq!(sched.len(), 0, "活被取走后 pending 计数要归零");
    }

    /// push 已确认「没有 searcher，也没有空闲 worker」之后，新 worker 才开始找活：
    /// pending 已经加一，但任务尚未真正入队，worker 会扫描失败并睡下。随后 push
    /// 入队时若不复查空闲位，就会留下「有活排队、唯一 worker 却睡着」的状态。
    #[test]
    fn a_worker_parking_between_wakeup_check_and_queue_insert_is_not_stranded() {
        let node = test_node_with(Config::default().with_thread(1));
        let sched = &node.sched;

        sched.pause_before_place.store(true, Ordering::SeqCst);
        sched.pause_before_park.store(true, Ordering::SeqCst);
        thread::scope(|scope| {
            struct ReleaseHook<'a>(&'a Scheduler);
            impl Drop for ReleaseHook<'_> {
                fn drop(&mut self) {
                    self.0.pause_before_place.store(false, Ordering::SeqCst);
                    self.0.pause_before_park.store(false, Ordering::SeqCst);
                    self.0.poke();
                }
            }
            let _release = ReleaseHook(sched);

            let pusher = scope.spawn(|| sched.push(dummy_context_on(node.clone(), 7)));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !sched.place_paused.load(Ordering::Acquire)
                && std::time::Instant::now() < deadline
            {
                std::hint::spin_loop();
            }
            assert!(
                sched.place_paused.load(Ordering::Acquire),
                "push 应停在唤醒检查结束、真正入队之前"
            );

            let worker = scope.spawn(|| {
                let _worker = sched.register_worker(0);
                let found_while_searching = sched.find_work_or_park().is_some();
                let found_after_wakeup = sched.pop().map(|ctx| ctx.handle);
                (found_while_searching, found_after_wakeup)
            });

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            while !sched.park_paused.load(Ordering::Acquire) && std::time::Instant::now() < deadline
            {
                std::hint::spin_loop();
            }
            assert!(
                sched.park_paused.load(Ordering::Acquire),
                "worker 应完成睡前最后一次扫描并停在 park 之前"
            );
            assert_eq!(sched.sleeping(), 1, "worker 应已登记为空闲");

            sched.pause_before_place.store(false, Ordering::SeqCst);
            pusher.join().unwrap();
            sched.pause_before_park.store(false, Ordering::SeqCst);

            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
            while sched.sleeping() != 0 && std::time::Instant::now() < deadline {
                std::hint::spin_loop();
            }
            let woken_by_push = sched.sleeping() == 0;
            let pending_while_stranded = sched.len();

            // 失败时用公开的定时器兜底路径收拾线程，避免测试挂死。
            if !woken_by_push {
                assert_eq!(pending_while_stranded, 1, "worker 睡着时任务仍在运行队列");
                sched.poke();
            }
            let (found_while_searching, found_after_wakeup) = worker.join().unwrap();

            assert!(!found_while_searching, "入队前不可能提前取到任务");
            assert_eq!(found_after_wakeup, Some(7), "worker 唤醒后任务应可取");
            assert!(
                woken_by_push,
                "push 入队后没有叫醒在唤醒检查之后才睡下的 worker"
            );
        });
    }

    /// 多个 worker 边投递边取活：本地队列、injector 与窃取路径一起压，
    /// 最终必须一件不丢、pending 归零。
    #[test]
    fn many_workers_pushing_and_popping_lose_nothing() {
        const WORKERS: usize = 4;
        const PER_WORKER: u32 = 2_000;
        const TOTAL: usize = WORKERS * PER_WORKER as usize;

        let node = test_node_with(Config::default().with_thread(WORKERS));
        let sched = &node.sched;
        let popped = AtomicUsize::new(0);

        thread::scope(|scope| {
            for id in 0..WORKERS {
                let popped = &popped;
                let node = node.clone();
                scope.spawn(move || {
                    let _worker = sched.register_worker(id);
                    let base = id as u32 * PER_WORKER;
                    for n in 0..PER_WORKER {
                        sched.push(dummy_context_on(node.clone(), base + n));
                    }
                    // 不 park：所有 worker 一直扫到全部活取空为止，专测队列本身。
                    // 给个宽限期，失败时别让 scoped thread 无限空转。
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    while popped.load(Ordering::SeqCst) < TOTAL
                        && std::time::Instant::now() < deadline
                    {
                        if sched.pop().is_some() {
                            popped.fetch_add(1, Ordering::SeqCst);
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });
            }
        });

        assert_eq!(popped.load(Ordering::SeqCst), TOTAL, "一件都不能丢");
        assert_eq!(sched.len(), 0, "取空后运行队列计数必须归零");
    }
}
