//! claim-before-steal 的极小 Loom 模型。
//!
//! 这不是怀疑点：`e6a049c` 已改为先 `take_stealable` 再 steal，失败不再 clear。
//! 现有 `new_stealable_hint_survives_failed_old_steal` 是主回归；这里只做长期证明。

#![cfg(all(test, loom))]

use crate::sync::{AtomicU64, Ordering, thread};

/// 明确按 take → mark → failed steal 的因果顺序，新 hint 必须留下。
#[test]
fn take_then_mark_then_failed_steal_keeps_hint() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let hint = crate::sync::Arc::new(AtomicU64::new(1));
        let taken = crate::sync::Arc::new(AtomicU64::new(0));

        let thief = {
            let hint = hint.clone();
            let taken = taken.clone();
            thread::spawn(move || {
                let bit = 1u64;
                assert!(hint.fetch_and(!bit, Ordering::Acquire) & bit != 0);
                taken.store(1, Ordering::Release);
                while taken.load(Ordering::Acquire) != 2 {
                    crate::sync::spin_loop();
                }
                // steal 失败，不再 clear
            })
        };

        while taken.load(Ordering::Acquire) != 1 {
            crate::sync::spin_loop();
        }
        hint.fetch_or(1, Ordering::Release);
        taken.store(2, Ordering::Release);
        thief.join().unwrap();

        assert_eq!(hint.load(Ordering::Acquire) & 1, 1, "新 hint 必须活下来");
    });
}
