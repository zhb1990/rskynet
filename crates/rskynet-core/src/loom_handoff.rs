//! `Handoff` 单槽 CAS 的 Loom 模型。
//!
//! 不实例化 `Scheduler` / `ServiceContext`。生产路径继续用 `std::sync::Arc`，
//! 这里用 `crate::sync::Arc`（Loom 下即 `loom::sync::Arc`）包同一套指针 CAS。

#![cfg(all(test, loom))]

use crate::handoff::Handoff;
use crate::sync::{Arc, AtomicUsize, Ordering, thread};

struct Token {
    id: usize,
    from_raw: Arc<[AtomicUsize; 2]>,
    dropped: Arc<[AtomicUsize; 2]>,
}

impl Drop for Token {
    fn drop(&mut self) {
        self.dropped[self.id].fetch_add(1, Ordering::SeqCst);
    }
}

fn take_arc(ptr: *mut Token) -> Option<Arc<Token>> {
    if ptr.is_null() {
        return None;
    }
    // 同一个指针只允许 from_raw 一次。
    let token = unsafe { &*ptr };
    let prev = token.from_raw[token.id].fetch_add(1, Ordering::SeqCst);
    assert_eq!(prev, 0, "Arc 只能 from_raw 一次");
    Some(unsafe { Arc::from_raw(ptr) })
}

#[test]
fn two_producers_one_consumer_from_raw_once() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let from_raw = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let dropped = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let handoff = Arc::new(Handoff::<Token>::new());

        let spawn_offer = |id: usize| {
            let handoff = handoff.clone();
            let from_raw = from_raw.clone();
            let dropped = dropped.clone();
            thread::spawn(move || {
                let arc = Arc::new(Token {
                    id,
                    from_raw,
                    dropped,
                });
                let ptr = Arc::into_raw(arc).cast_mut();
                match handoff.offer_raw(ptr) {
                    Ok(()) => {}
                    Err(ptr) => {
                        // 失败必须把所有权退还，随后在本线程 drop。
                        drop(take_arc(ptr).expect("failed offer returns the pointer"));
                    }
                }
            })
        };

        let p0 = spawn_offer(0);
        let p1 = spawn_offer(1);
        let consumer = {
            let handoff = handoff.clone();
            thread::spawn(move || {
                if let Some(arc) = take_arc(handoff.take_raw()) {
                    drop(arc);
                }
            })
        };

        p0.join().unwrap();
        p1.join().unwrap();
        consumer.join().unwrap();

        // 残留必须收走，否则 drop Handoff 会泄漏 into_raw 的引用。
        if let Some(arc) = take_arc(handoff.take_raw()) {
            drop(arc);
        }
        drop(handoff);

        for id in 0..2 {
            assert_eq!(
                dropped[id].load(Ordering::SeqCst),
                1,
                "token {id} must drop exactly once"
            );
            assert!(
                from_raw[id].load(Ordering::SeqCst) <= 1,
                "token {id} from_raw at most once"
            );
        }
    });
}
