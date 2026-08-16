//! 并发原语门面。默认 std；`RUSTFLAGS='--cfg loom'` 切到 loom。
//!
//! 需要被 Loom 建模的底层模块和 `loom_*` 抽象模型走这里；
//! Scheduler 本体仍使用 std，不整体 Loom 化。
//!
//! ```text
//! Loom（必须过滤 loom_，否则其它单测会在 loom::model 外碰到 loom 原子量）：
//!
//!   RUSTFLAGS="--cfg loom" cargo test -p rskynet-core --lib loom_ -- --test-threads=1
//!
//! TSAN（nightly + rust-src；下列为已验证的 Apple Silicon /
//! aarch64-apple-darwin 命令。
//! `-Zsanitizer` / `-Zbuild-std` 未稳定，不是 crate MSRV。
//! 为隔离普通构建与 TSAN 构建产物使用独立 `CARGO_TARGET_DIR`；
//! test harness 本身要求 unwind，下面的 `CARGO_PROFILE_*_PANIC=unwind`
//! 作为显式防御保留）：
//!
//!   rustup component add rust-src --toolchain nightly-aarch64-apple-darwin
//!
//!   CARGO_TARGET_DIR=target/tsan \
//!   CARGO_INCREMENTAL=0 \
//!   CARGO_PROFILE_DEV_PANIC=unwind \
//!   CARGO_PROFILE_RELEASE_PANIC=unwind \
//!   RUSTFLAGS="-Zsanitizer=thread" \
//!   RUSTDOCFLAGS="-Zsanitizer=thread" \
//!     cargo +nightly test -Zbuild-std --target aarch64-apple-darwin -p rskynet-core
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
