//! BWoS 的 Loom 模型。一律用 `queue(2, 1)` / `queue(2, 2)`，
//! 不要把生产常量 `NUM_BLOCKS=64` / `BLOCK_SIZE=16` 拉进状态空间。
//!
//! 不测 generation 数值回绕：逻辑 sequence 单调递增，越界 fail-fast。
//! 要测的是旧 thief 持有旧 generation + spos 时，reclaim 之后 CAS 必须失败。

#![cfg(all(test, loom))]

use crate::bwos::{Pushed, queue};
use crate::sync::{Arc, AtomicUsize, Ordering, thread};

fn check(f: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(f);
}

struct Counts {
    consumed: Vec<AtomicUsize>,
    dropped: Vec<AtomicUsize>,
}

struct DropProbe {
    id: usize,
    counts: Arc<Counts>,
}

impl DropProbe {
    fn new(id: usize, counts: &Arc<Counts>) -> Self {
        Self {
            id,
            counts: counts.clone(),
        }
    }

    fn consume(self) -> usize {
        self.counts.consumed[self.id].fetch_add(1, Ordering::SeqCst);
        self.id
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.counts.dropped[self.id].fetch_add(1, Ordering::SeqCst);
    }
}

fn counts(n: usize) -> Arc<Counts> {
    Arc::new(Counts {
        consumed: (0..n).map(|_| AtomicUsize::new(0)).collect(),
        dropped: (0..n).map(|_| AtomicUsize::new(0)).collect(),
    })
}

fn assert_once(counts: &Counts, n: usize) {
    for id in 0..n {
        assert_eq!(
            counts.consumed[id].load(Ordering::SeqCst),
            1,
            "value {id} consumed count"
        );
        assert_eq!(
            counts.dropped[id].load(Ordering::SeqCst),
            1,
            "value {id} dropped count"
        );
    }
}

/// P0：owner 写入、grant、thief steal、随后物理块复用。
/// Loom 的 `UnsafeCell` 会在同一 slot 上非法重叠读写时直接失败。
#[test]
fn slot_lifecycle_owner_put_thief_steal() {
    check(|| {
        let (owner, stealer) = queue::<DropProbe>(2, 1);
        let counts = counts(3);
        let thief = thread::spawn(move || {
            let mut got = Vec::new();
            if let Some(v) = stealer.steal_front() {
                got.push(v.consume());
            }
            if let Some(v) = stealer.steal_front() {
                got.push(v.consume());
            }
            got
        });

        unsafe {
            let _ = owner
                .push_back(DropProbe::new(0, &counts))
                .map_err(DropProbe::consume);
            let _ = owner
                .push_back(DropProbe::new(1, &counts))
                .map_err(DropProbe::consume);
            let _ = owner
                .push_back(DropProbe::new(2, &counts))
                .map_err(DropProbe::consume);
        }

        let _stolen = thief.join().unwrap();
        while let Some(v) = unsafe { owner.pop_back() } {
            v.consume();
        }
        drop(owner);
        assert_once(&counts, 3);
    });
}

/// P0：验证 `e6a049c` 注释里的 release sequence。
///
/// `grant` Release store 之后，steal_tail 上连续成功的 RMW 延续同一条
/// release sequence；第二个 thief 即使读到的是前一个 thief 写入的
/// `steal_tail`，Acquire CAS 仍必须看见本轮 grant 之前的槽位写入。
#[test]
fn release_sequence_second_thief_sees_grant() {
    check(|| {
        let (owner, stealer) = queue::<DropProbe>(2, 2);
        let counts = counts(3);

        unsafe {
            assert_eq!(
                owner.push_back(DropProbe::new(0, &counts)).ok(),
                Some(Pushed::Local)
            );
            assert_eq!(
                owner.push_back(DropProbe::new(1, &counts)).ok(),
                Some(Pushed::Local)
            );
            assert_eq!(
                owner.push_back(DropProbe::new(2, &counts)).ok(),
                Some(Pushed::Granted)
            );
        }

        let s1 = stealer.clone();
        let t1 = thread::spawn(move || s1.steal_front().map(DropProbe::consume));
        let t2 = thread::spawn(move || stealer.steal_front().map(DropProbe::consume));
        let a = t1.join().unwrap();
        let b = t2.join().unwrap();

        let mut stolen = [a, b];
        stolen.sort();
        assert_eq!(stolen, [Some(0), Some(1)], "A、B 必须各被偷走一次");

        let leftover = unsafe { owner.pop_back() }.map(DropProbe::consume);
        assert_eq!(leftover, Some(2), "C 仍在 owner 侧");
        drop(owner);
        assert_once(&counts, 3);
    });
}

/// P1：owner `pop_back`（含 takeover）与 thief `steal_front` 并发。
#[test]
fn owner_pop_back_vs_thief_steal_front() {
    check(|| {
        let (owner, stealer) = queue::<DropProbe>(2, 2);
        let counts = counts(3);
        unsafe {
            let _ = owner.push_back(DropProbe::new(0, &counts));
            let _ = owner.push_back(DropProbe::new(1, &counts));
            let _ = owner.push_back(DropProbe::new(2, &counts));
        }

        let thief = thread::spawn(move || {
            let mut got = Vec::new();
            while let Some(v) = stealer.steal_front() {
                got.push(v.consume());
            }
            got
        });

        while let Some(v) = unsafe { owner.pop_back() } {
            v.consume();
        }
        let _stolen = thief.join().unwrap();
        drop(owner);
        assert_once(&counts, 3);
    });
}

/// P1：2×1 队列高频走 grant / steal / reclaim / 物理 slot 复用。
/// 不缩小 generation bit，不测数值回绕。
#[test]
fn physical_block_reuse_with_concurrent_thief() {
    check(|| {
        let (owner, stealer) = queue::<DropProbe>(2, 1);
        const N: usize = 3;
        let counts = counts(N);

        let thief = thread::spawn(move || {
            let mut got = Vec::new();
            for _ in 0..N {
                if let Some(v) = stealer.steal_front() {
                    got.push(v.consume());
                }
            }
            got
        });

        for id in 0..N {
            match unsafe { owner.push_back(DropProbe::new(id, &counts)) } {
                Ok(_) => {}
                Err(v) => {
                    v.consume();
                }
            }
            if id % 2 == 1 {
                if let Some(v) = unsafe { owner.pop_back() } {
                    v.consume();
                }
            }
        }

        let _stolen = thief.join().unwrap();
        while let Some(v) = unsafe { owner.pop_back() } {
            v.consume();
        }
        drop(owner);
        assert_once(&counts, N);
    });
}
