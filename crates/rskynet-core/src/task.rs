//! 服务内部的迷你 executor，取代 skynet 里 Lua 的协程池。
//!
//! # 安全契约
//!
//! 调度器保证「**同一个服务在任意时刻只会被一个 worker 线程执行**」——这一点由
//! [`crate::mq::Mailbox`] 的状态机维持：状态不为 `IDLE` 期间，服务要么排在运行
//! 队列里，要么被唯一一个 worker 持有。因此服务内部的状态天生是单线程访问的，
//! 只是「哪个线程」会随调度变化。
//!
//! [`SvcCell`] 正是建立在这条不变量上：它就是 `RefCell`，只额外声明了 `Sync`，
//! 好让 `Arc<MyService>` 满足 `Send`、服务能在 worker 之间迁移。
//! 用它而不用 `Mutex` 的理由是：跨 `await` 持有 `Mutex` 会真的死锁，而 `SvcCell`
//! 只会在借用冲突时 panic，能第一时间把 bug 暴露出来。

use std::cell::{Ref, RefCell, RefMut};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::task::Waker;

use futures_util::future::BoxFuture;
use futures_util::task::{ArcWake, waker};
use slab::Slab;

use crate::server::ServiceContext;

/// 服务状态容器：语义等同 `RefCell`，但可以放进 `Arc` 里跨 worker 线程迁移。
///
/// ```ignore
/// struct Counter { hits: SvcCell<u64> }
/// *counter.hits.borrow_mut() += 1;
/// ```
///
/// 注意不要跨 `await` 持有借用守卫，否则同一服务的另一个任务访问同一 cell 时会 panic。
pub struct SvcCell<T> {
    inner: RefCell<T>,
    /// `RefCell` 的借用计数不是原子的；公开的 `Sync` 实现必须先把跨线程访问挡在
    /// 它外面。运行时正常路径不会争用，误从两条线程同时访问时则当场 panic。
    borrowed: AtomicBool,
}

// 安全性：所有共享访问都先独占 `borrowed`，并由返回的守卫持有到借用结束，因此
// `RefCell` 的非原子借用计数和内部的 `T` 永远不会被两条线程同时触碰。T 只需 Send，
// 因为守卫刻意不允许并发共享读。
unsafe impl<T: Send> Sync for SvcCell<T> {}

struct BorrowLease<'a>(&'a AtomicBool);

impl Drop for BorrowLease<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// [`SvcCell`] 的只读借用守卫。
pub struct SvcRef<'a, T> {
    inner: Ref<'a, T>,
    _lease: BorrowLease<'a>,
}

impl<T> Deref for SvcRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for SvcRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for SvcRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

/// [`SvcCell`] 的可变借用守卫。
pub struct SvcRefMut<'a, T> {
    inner: RefMut<'a, T>,
    _lease: BorrowLease<'a>,
}

impl<T> Deref for SvcRefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for SvcRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T: fmt::Debug> fmt::Debug for SvcRefMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for SvcRefMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> SvcCell<T> {
    pub const fn new(value: T) -> Self {
        Self {
            inner: RefCell::new(value),
            borrowed: AtomicBool::new(false),
        }
    }

    fn lease(&self) -> BorrowLease<'_> {
        assert!(
            self.borrowed
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok(),
            "SvcCell 已被借用，不能重入或从多条线程同时访问"
        );
        BorrowLease(&self.borrowed)
    }

    pub fn borrow(&self) -> SvcRef<'_, T> {
        let lease = self.lease();
        SvcRef {
            inner: self.inner.borrow(),
            _lease: lease,
        }
    }

    pub fn borrow_mut(&self) -> SvcRefMut<'_, T> {
        let lease = self.lease();
        SvcRefMut {
            inner: self.inner.borrow_mut(),
            _lease: lease,
        }
    }

    pub fn replace(&self, value: T) -> T {
        std::mem::replace(&mut *self.borrow_mut(), value)
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut()
    }

    pub fn into_inner(self) -> T {
        self.inner.into_inner()
    }
}

impl<T: Copy> SvcCell<T> {
    pub fn get(&self) -> T {
        *self.borrow()
    }

    pub fn set(&self, value: T) {
        *self.borrow_mut() = value;
    }
}

impl<T: Default> Default for SvcCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for SvcCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.borrow().fmt(f)
    }
}

/// 服务内任务的身份：slab 槽位号加一个单调 generation。
///
/// 槽位号在任务删除后会被 slab 复用；generation 让扣在外部很久的旧 Waker
/// 只能唤醒它出生时的那个任务，不会误唤醒后来复用同一槽位的新任务。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TaskId {
    pub(crate) index: usize,
    pub(crate) generation: u64,
}

/// 服务内的一个任务，相当于 skynet 里的一个协程。
struct TaskSlot {
    /// poll 期间会被取走，避免任务在 poll 中再 spawn 新任务时形成嵌套借用。
    future: Option<BoxFuture<'static, ()>>,
    waker: Waker,
    generation: u64,
    /// 这个任务正在处理谁的请求：(请求方, session)。
    ///
    /// 服务半途退出时要给这些请求方回一个错误，否则对方的 `call` 永远挂着；
    /// 对照 `lualib/skynet.lua` 里 `skynet.exit` 遍历 `session_coroutine_id` 的那段。
    /// 记在任务槽里而不是另开一张表，是因为它的生命周期与任务严丝合缝。
    request: Option<(u32, u64)>,
    /// 最初开出这个任务的消息来源，给 worker monitor 报告死循环用。
    source: u32,
}

/// 任务 waker：被唤醒时把任务 id 塞回服务的就绪队列，并让服务重新进入全局队列。
///
/// 持的是 `Weak`，因为 waker 可能被外部（比如别的线程的 channel）长期扣着，
/// 不该因此吊住整个服务的生命周期。
struct TaskWaker {
    ctx: Weak<ServiceContext>,
    task: TaskId,
}

impl ArcWake for TaskWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        if let Some(ctx) = arc_self.ctx.upgrade() {
            ctx.wake_task(arc_self.task);
        }
    }
}

/// 服务持有的任务集合。
///
/// 这里用 [`SvcCell`] 而不是 `Mutex`：所有访问点都在「当前执行本服务的那个线程」
/// 上，加锁只是白付代价。外部线程不能直接调用 `Ctx::spawn`，只能通过邮箱投消息。
pub(crate) struct TaskSet {
    slots: SvcCell<Slab<TaskSlot>>,
    /// 任务 generation 来源。只有持有者会改，原子量只是为了凑 Sync。
    next_generation: AtomicU64,
    /// 任务数。只有持有者会改，但 `NodeRef::service_stats` 允许别的线程看一眼，
    /// 所以单独记一个原子量，免得为了读个数字就得去借 cell。
    count: AtomicUsize,
}

impl TaskSet {
    pub(crate) fn new() -> Self {
        Self {
            slots: SvcCell::new(Slab::new()),
            next_generation: AtomicU64::new(0),
            count: AtomicUsize::new(0),
        }
    }

    /// 放入一个新任务，返回任务 id。`request` 是它正在处理的那个请求（如果有）。
    pub(crate) fn insert(
        &self,
        owner: &Weak<ServiceContext>,
        future: BoxFuture<'static, ()>,
        request: Option<(u32, u64)>,
        source: u32,
    ) -> TaskId {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let mut slots = self.slots.borrow_mut();
        let entry = slots.vacant_entry();
        let task = TaskId {
            index: entry.key(),
            generation,
        };
        let waker = waker(Arc::new(TaskWaker {
            ctx: owner.clone(),
            task,
        }));
        entry.insert(TaskSlot {
            future: Some(future),
            waker,
            generation,
            request,
            source,
        });
        self.count.store(slots.len(), Ordering::Relaxed);
        task
    }

    /// 取出任务准备 poll。任务已完成、generation 不匹配或正被 poll 时返回 `None`。
    pub(crate) fn take(&self, task: TaskId) -> Option<(BoxFuture<'static, ()>, Waker, u32)> {
        let mut slots = self.slots.borrow_mut();
        let slot = slots.get_mut(task.index)?;
        if slot.generation != task.generation {
            return None;
        }
        let future = slot.future.take()?;
        Some((future, slot.waker.clone(), slot.source))
    }

    /// poll 返回 `Pending`，把 Future 放回原槽位。
    pub(crate) fn restore(&self, task: TaskId, future: BoxFuture<'static, ()>) {
        let mut slots = self.slots.borrow_mut();
        let Some(slot) = slots.get_mut(task.index) else {
            drop(future);
            return;
        };
        if slot.generation != task.generation {
            drop(future);
            return;
        }
        slot.future = Some(future);
    }

    /// 删除任务。槽位 generation 不匹配时无事发生。
    pub(crate) fn remove(&self, task: TaskId) {
        let mut slots = self.slots.borrow_mut();
        if !slots
            .get(task.index)
            .is_some_and(|slot| slot.generation == task.generation)
        {
            return;
        }
        slots.remove(task.index);
        self.count.store(slots.len(), Ordering::Relaxed);
    }

    pub(crate) fn len(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// 摘走所有「正在处理中的请求」，摘过就不会再报一遍。
    pub(crate) fn take_requests(&self) -> Vec<(u32, u64)> {
        self.slots
            .borrow_mut()
            .iter_mut()
            .filter_map(|(_, slot)| slot.request.take())
            .collect()
    }

    /// 服务销毁时清空任务。Future 的析构可能回调本服务（比如 `Call` 的 Drop 要动
    /// session 表），所以只在借用期内把它们搬出来，实际释放留给调用方在借用外做。
    pub(crate) fn drain(&self) -> Vec<BoxFuture<'static, ()>> {
        let mut slots = self.slots.borrow_mut();
        let taken: Vec<_> = slots
            .iter_mut()
            .filter_map(|(_, slot)| slot.future.take())
            .collect();
        slots.clear();
        self.count.store(0, Ordering::Relaxed);
        taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 借用冲突时要当场 panic 把 bug 暴露出来，而不是像 Mutex 那样死锁
    #[test]
    fn svc_cell_panics_on_conflict_instead_of_deadlock() {
        let cell = SvcCell::new(1u32);
        let guard = cell.borrow_mut();

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let conflict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = cell.borrow_mut();
        }));
        std::panic::set_hook(previous);
        assert!(conflict.is_err(), "重复可变借用应当 panic");

        drop(guard);
        *cell.borrow_mut() += 1;
        assert_eq!(cell.get(), 2);
    }

    /// 服务要能在 worker 线程之间迁移，所以 `Arc<SvcCell<T>>` 必须是 Send
    #[test]
    fn svc_cell_can_move_across_threads() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        let cell = Arc::new(SvcCell::new(String::from("x")));
        assert_send_sync(&cell);
    }

    #[test]
    fn concurrent_access_is_rejected_before_touching_refcell() {
        let cell = Arc::new(SvcCell::new(1u32));
        let held = cell.borrow_mut();
        let other = cell.clone();
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let conflict = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| other.borrow_mut())).is_err()
        })
        .join()
        .expect("测试线程不应逃逸 panic");
        std::panic::set_hook(previous);
        assert!(conflict, "跨线程借用必须在访问 RefCell 之前被拒绝");
        drop(held);
        assert_eq!(*cell.borrow(), 1);
    }

    #[test]
    fn task_keeps_the_source_that_created_it() {
        let tasks = TaskSet::new();
        let owner = Weak::<ServiceContext>::new();
        let task = tasks.insert(&owner, Box::pin(async {}), None, 0x1234);
        let (_, _, source) = tasks.take(task).expect("任务应当存在");
        assert_eq!(source, 0x1234);
    }

    #[test]
    fn stale_task_id_cannot_wake_a_reused_slot() {
        let tasks = TaskSet::new();
        let owner = Weak::<ServiceContext>::new();

        let first = tasks.insert(&owner, Box::pin(async {}), None, 0);
        let _ = tasks.take(first).expect("第一个任务应当存在");
        tasks.remove(first);

        let second = tasks.insert(&owner, Box::pin(async {}), None, 0);
        assert_eq!(first.index, second.index, "slab 确实复用了槽位");
        assert_ne!(first.generation, second.generation, "generation 必须变化");

        assert!(
            tasks.take(first).is_none(),
            "旧 id 不能取走复用槽位上的新任务"
        );
        assert!(tasks.take(second).is_some(), "新 id 应当正常取到自己的任务");
    }
}
