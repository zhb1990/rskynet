//! Scheduler lost-wakeup 协议的抽象状态机。
//!
//! 只模拟 `work` / `searching` / `idle` / `wake_token`，不模拟 `thread::park`、
//! `SegQueue`、BWoS、`ArcSwap`。
//!
//! 对应 `mq.rs` 在 `e6a049c` 的路径：
//!
//! | 模型 | `Scheduler` |
//! |---|---|
//! | `work.fetch_add` + `fence(SeqCst)` + 读 `searching` | `pending.fetch_add` + `fence(SeqCst)` + `searching.load` |
//! | `searching==0` 时 claim idle + `wake=true` | `claim_idle` + `Thread::unpark` |
//! | 发布后再读 `searching.load(SeqCst)==0` 则补 wake | injector / place 后的复查 |
//! | worker：`searching++`，扫 work，`idle=true`，`searching--`，`fence`，再扫，否则睡 | `find_work_or_park` |
//!
//! 核心不变量：`work 已发布 ⇒ worker 能看见 work OR 持有 wake token`。
//! 禁止 `work && sleeping && !wake && searching==0`。
//!
//! Loom 把普通 `SeqCst` 访问按较弱的 `AcqRel` 处理；`fence(SeqCst)` 是支持的，
//! 但本模型全绿不是形式化证明。真实协议仍靠
//! `a_searcher_that_parks_before_the_push_lands_is_woken_directly` 与
//! `a_worker_parking_between_wakeup_check_and_queue_insert_is_not_stranded`。
//!
//! ThreadSanitizer 不支持 `atomic::fence`，因此 TSAN 也不能单独为这条协议背书。

#![cfg(all(test, loom))]

use crate::sync::{AtomicU64, AtomicUsize, Ordering, fence, thread};

struct WakeModel {
    work: AtomicUsize,
    searching: AtomicUsize,
    idle: AtomicU64,
    wake: AtomicU64,
    sleeping: AtomicU64,
}

impl WakeModel {
    fn new() -> Self {
        Self {
            work: AtomicUsize::new(0),
            searching: AtomicUsize::new(0),
            idle: AtomicU64::new(0),
            wake: AtomicU64::new(0),
            sleeping: AtomicU64::new(0),
        }
    }

    fn publish(&self) {
        self.work.fetch_add(1, Ordering::Relaxed);
        fence(Ordering::SeqCst);
        if self.searching.load(Ordering::Relaxed) > 0 {
            if self.searching.load(Ordering::SeqCst) == 0 {
                self.wake_idle();
            }
            return;
        }
        if self.wake_idle() {
            return;
        }
        if self.searching.load(Ordering::SeqCst) == 0 {
            self.wake_idle();
        }
    }

    fn wake_idle(&self) -> bool {
        let mut bits = self.idle.load(Ordering::Relaxed);
        while bits != 0 {
            let bit = 1u64 << bits.trailing_zeros();
            match self.idle.compare_exchange_weak(
                bits,
                bits & !bit,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.wake.fetch_or(bit, Ordering::Release);
                    return true;
                }
                Err(actual) => bits = actual,
            }
        }
        false
    }

    fn take(&self) -> bool {
        let mut current = self.work.load(Ordering::Relaxed);
        while current > 0 {
            match self.work.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
        false
    }

    fn worker(&self, id: usize) -> bool {
        let bit = 1u64 << id;
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
        let woken = self.wake.fetch_and(!bit, Ordering::AcqRel) & bit != 0;
        if woken {
            self.idle.fetch_and(!bit, Ordering::Relaxed);
            return self.take();
        }
        self.sleeping.fetch_or(bit, Ordering::Release);
        false
    }

    fn assert_no_lost_wakeup(&self, got: bool) {
        let work = self.work.load(Ordering::SeqCst);
        let sleeping = self.sleeping.load(Ordering::SeqCst);
        let wake = self.wake.load(Ordering::SeqCst);
        let searching = self.searching.load(Ordering::SeqCst);
        assert!(
            !(work > 0 && sleeping != 0 && wake == 0 && searching == 0),
            "lost wakeup: work={work} sleeping={sleeping} wake={wake} searching={searching} got={got}"
        );
        assert!(
            got || work == 0 || wake != 0 || sleeping != 0,
            "work vanished without a consumer or wake token"
        );
    }
}

/// 1 producer × 1 worker：覆盖 searcher 吸收唤醒，以及复查补叫。
#[test]
fn one_producer_one_worker_never_strands_work() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let model = std::sync::Arc::new(WakeModel::new());
        let producer = {
            let model = model.clone();
            thread::spawn(move || model.publish())
        };
        let worker = {
            let model = model.clone();
            thread::spawn(move || model.worker(0))
        };
        producer.join().unwrap();
        let got = worker.join().unwrap();
        model.assert_no_lost_wakeup(got);
    });
}

/// 1 producer × 2 worker：一个在找、一个可能已闲，work 不得和双睡一起出现。
#[test]
fn one_producer_two_workers_never_strands_work() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let model = std::sync::Arc::new(WakeModel::new());
        let producer = {
            let model = model.clone();
            thread::spawn(move || model.publish())
        };
        let w0 = {
            let model = model.clone();
            thread::spawn(move || model.worker(0))
        };
        let w1 = {
            let model = model.clone();
            thread::spawn(move || model.worker(1))
        };
        producer.join().unwrap();
        let got0 = w0.join().unwrap();
        let got1 = w1.join().unwrap();
        model.assert_no_lost_wakeup(got0 || got1);
    });
}
