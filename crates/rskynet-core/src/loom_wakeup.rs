//! Scheduler lost-wakeup 协议的抽象状态机。
//!
//! 对应当前 [`crate::mq`] 的 lost-wakeup 协议。只模拟
//! `pending` / `visible_work` / `searching` / `idle` / `wake` token /
//! 每 worker 一个 `Notify`，不模拟 `thread::park`、`SegQueue`、BWoS、`ArcSwap`。
//!
//! | 模型 | `Scheduler` |
//! |---|---|
//! | `pending.fetch_add` + `fence(SeqCst)` + 读 `searching` | `pending.fetch_add` + `fence` + `searching.load` |
//! | `visible_work.fetch_add` | `injector.push` / `slot.offer` / `place` |
//! | `searching==0` 时 claim idle + `wake` bit + `Notify` | `claim_idle` + `Thread::unpark` |
//! | 可见发布后再读 `searching==0` 则补叫 | injector / place 后的复查 |
//! | worker：`searching++`，扫 `visible_work`，`idle`，`searching--`，`fence`，再扫，否则 `Notify::wait` | `find_work_or_park` |
//!
//! 三层加 quit：
//!
//! ```text
//! idle bitmap   = 谁已声明「准备睡」
//! wake bitmap   = 协议产生的逻辑工作 token
//! Notify[id]    = 该 worker 的 OS park/unpark 模拟
//! quit          = 退出条件；只能 unblock，不能制造 work token
//! ```
//!
//! `Notify` 只负责阻塞；`wait()` 会探索 spurious wakeup，因此返回后必须先看
//! 自己的 wake bit，绝不能直接 `take()`。shutdown 对每个 worker 无条件
//! `notify()`，对齐 `set_quit()`，但不改 wake bitmap。
//!
//! 核心不变量：`visible_work > 0 ⇒ work 不得因 lost-wakeup 永久 stranded`。
//! `pending++` 只表示 producer 已宣告工作；在 `visible_work++` 之前，
//! worker 的 `take()` 必须失败。真正 lost-wakeup 时 worker 会在
//! `Notify::wait` 上阻塞，Loom 报 deadlock。
//!
//! Loom 把普通 `SeqCst` 访问按较弱的 `AcqRel` 处理；`fence(SeqCst)` 是支持的，
//! 但本模型全绿不是形式化证明。真实协议仍靠
//! `a_searcher_that_parks_before_the_push_lands_is_woken_directly` 与
//! `a_worker_parking_between_wakeup_check_and_queue_insert_is_not_stranded`。
//!
//! ThreadSanitizer 不支持 `atomic::fence`，因此 TSAN 也不能单独为这条协议背书。

#![cfg(all(test, loom))]

use crate::sync::{Arc, AtomicBool, AtomicU64, AtomicUsize, Ordering, fence, thread};
use loom::sync::Notify;

struct WakeModel {
    /// declaration counter only; intentionally never consumed
    pending: AtomicUsize,
    /// 已真正进入可取位置。不模拟 queue payload。
    visible_work: AtomicUsize,
    searching: AtomicUsize,
    idle: AtomicU64,
    wake: AtomicU64,
    sleeping: AtomicU64,
    quit: AtomicBool,
    notifies: Vec<Notify>,
}

impl WakeModel {
    fn new(workers: usize) -> Self {
        Self {
            pending: AtomicUsize::new(0),
            visible_work: AtomicUsize::new(0),
            searching: AtomicUsize::new(0),
            idle: AtomicU64::new(0),
            wake: AtomicU64::new(0),
            sleeping: AtomicU64::new(0),
            quit: AtomicBool::new(false),
            notifies: (0..workers).map(|_| Notify::new()).collect(),
        }
    }

    fn publish(&self) {
        self.pending.fetch_add(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);

        if self.searching.load(Ordering::Relaxed) > 0 {
            self.publish_visible();
            if self.searching.load(Ordering::SeqCst) == 0 {
                self.wake_idle();
            }
            return;
        }

        if let Some(id) = self.claim_idle() {
            self.publish_visible();
            self.notify_worker(id);
            return;
        }

        self.publish_visible();
        if self.searching.load(Ordering::SeqCst) == 0 {
            self.wake_idle();
        }
    }

    /// `visible_work++` 抽象 queue publication。
    ///
    /// 后面的 SeqCst fence 是 Loom-only 建模补偿：Loom 会把普通
    /// SeqCst atomic access 按 AcqRel 处理，因此在 post-publish
    /// searching recheck 前补一个受支持的 SeqCst 全序点。
    /// 它不对应生产 Scheduler 中额外的一道 fence。
    fn publish_visible(&self) {
        self.visible_work.fetch_add(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
    }

    fn claim_idle(&self) -> Option<usize> {
        let mut bits = self.idle.load(Ordering::Relaxed);
        while bits != 0 {
            let id = bits.trailing_zeros() as usize;
            let bit = 1u64 << id;
            match self.idle.compare_exchange_weak(
                bits,
                bits & !bit,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(id),
                Err(actual) => bits = actual,
            }
        }
        None
    }

    fn wake_idle(&self) -> bool {
        let Some(id) = self.claim_idle() else {
            return false;
        };
        self.notify_worker(id);
        true
    }

    /// 工作唤醒：必须先写 protocol token，再 Notify。
    fn notify_worker(&self, id: usize) {
        let bit = 1u64 << id;
        self.wake.fetch_or(bit, Ordering::Release);
        self.notifies[id].notify();
    }

    /// 对齐 `set_quit`：先置 quit，再对每个 worker unpark。不改 wake bitmap。
    fn notify_all_for_shutdown(&self) {
        self.quit.store(true, Ordering::Release);
        for notify in &self.notifies {
            notify.notify();
        }
    }

    fn take(&self) -> bool {
        let mut current = self.visible_work.load(Ordering::Relaxed);
        while current > 0 {
            match self.visible_work.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Test-harness termination only：最后一件可见工作被消费后叫醒 waiter，
                    // 让它们复查 quit / 空队列并完成 join。
                    // 不改 wake bitmap，也不属于 Scheduler lost-wakeup protocol。
                    if current == 1 {
                        for notify in &self.notifies {
                            notify.notify();
                        }
                    }
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
        false
    }

    fn should_exit(&self) -> bool {
        self.quit.load(Ordering::Acquire) && self.visible_work.load(Ordering::Relaxed) == 0
    }

    fn worker(&self, id: usize) -> bool {
        let bit = 1u64 << id;
        loop {
            if self.should_exit() {
                return false;
            }

            self.searching.fetch_add(1, Ordering::SeqCst);
            if self.take() {
                self.searching.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
            self.idle.fetch_or(bit, Ordering::SeqCst);
            self.searching.fetch_sub(1, Ordering::SeqCst);
            fence(Ordering::SeqCst);
            if self.take() {
                self.idle.fetch_and(!bit, Ordering::Relaxed);
                return true;
            }

            self.sleeping.fetch_or(bit, Ordering::Release);
            loop {
                if self.should_exit() {
                    self.sleeping.fetch_and(!bit, Ordering::Relaxed);
                    self.idle.fetch_and(!bit, Ordering::Relaxed);
                    return false;
                }
                self.notifies[id].wait();
                let has_token = self.wake.fetch_and(!bit, Ordering::AcqRel) & bit != 0;
                if has_token {
                    self.sleeping.fetch_and(!bit, Ordering::Relaxed);
                    self.idle.fetch_and(!bit, Ordering::Relaxed);
                    if self.take() {
                        return true;
                    }
                    if self.should_exit() {
                        return false;
                    }
                    // token 只表示有资格再找活；take 失败则重新走协议
                    break;
                }
                if self.should_exit() {
                    self.sleeping.fetch_and(!bit, Ordering::Relaxed);
                    self.idle.fetch_and(!bit, Ordering::Relaxed);
                    return false;
                }
            }
        }
    }
}

fn check(workers: usize) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.max_threads = 6;
    builder.check(move || {
        let model = Arc::new(WakeModel::new(workers));
        let producer = {
            let model = model.clone();
            thread::spawn(move || model.publish())
        };
        let handles: Vec<_> = (0..workers)
            .map(|id| {
                let model = model.clone();
                thread::spawn(move || model.worker(id))
            })
            .collect();

        producer.join().unwrap();
        model.notify_all_for_shutdown();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(
            model.visible_work.load(Ordering::SeqCst),
            0,
            "published work must be consumed"
        );
    });
}

/// 1 producer × 1 worker：覆盖 searcher 吸收唤醒，以及复查补叫。
#[test]
fn one_producer_one_worker_never_strands_work() {
    check(1);
}

/// 1 producer × 2 worker：一人取走后另一人因 quit 退出；无 token 不得偷吃。
#[test]
fn one_producer_two_workers_never_strands_work() {
    check(2);
}
