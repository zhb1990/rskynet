//! 并发原语门面。默认 std；`RUSTFLAGS='--cfg loom'` 切到 loom。
//!
//! 只有需要被 Loom 建模的模块（[`crate::bwos`]、[`crate::handoff`]）走这里。
//! Scheduler 本体继续用 std，避免把 `SegQueue` / `ArcSwap` / `park` 卷进模型。
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p rskynet-core --lib loom_ -- --test-threads=1
//!
//! RUSTFLAGS="-Zsanitizer=thread" RUSTDOCFLAGS="-Zsanitizer=thread" \
//!   cargo +nightly test -Zbuild-std --target aarch64-apple-darwin -p rskynet-core
//! ```
//!
//! ThreadSanitizer 不支持 `atomic::fence`。lost-wakeup 协议依赖 `fence(SeqCst)`，
//! 因此 TSAN 全绿不能单独为那条协议背书。

#[cfg(not(loom))]
pub(crate) use std::cell::UnsafeCell;
#[cfg(not(loom))]
pub(crate) use std::sync::Arc;
#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::sync::atomic::{
    AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::thread;

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;
#[cfg(loom)]
pub(crate) use loom::sync::Arc;
#[cfg(loom)]
#[allow(unused_imports)]
pub(crate) use loom::sync::atomic::{
    AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering, fence,
};
#[cfg(loom)]
pub(crate) use loom::thread;

/// 生产路径是 `spin_loop`；Loom 下必须 `yield_now`，否则探索器看不到
/// 另一线程的 Release 存储，自旋等待会假死。
#[inline]
pub(crate) fn spin_loop() {
    #[cfg(not(loom))]
    std::hint::spin_loop();
    #[cfg(loom)]
    loom::thread::yield_now();
}
