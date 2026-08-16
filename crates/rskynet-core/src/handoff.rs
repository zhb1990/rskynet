//! 单槽定向递交：`WorkerSlot` 用来把一个 `Arc` 交给指定 worker。
//!
//! 只抽象指针 CAS。`Arc` 的 `into_raw` / `from_raw` 仍由调用方持有，
//! 这样生产路径继续用 `std::sync::Arc<ServiceContext>`，Loom 侧可以用
//! `loom::sync::Arc` 包同一套协议，而不必把整个 Scheduler 换成 loom Arc。

use crate::sync::{AtomicPtr, Ordering};

pub(crate) struct Handoff<T> {
    slot: AtomicPtr<T>,
}

impl<T> Default for Handoff<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Handoff<T> {
    pub(crate) fn new() -> Self {
        Self {
            slot: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// 空槽则放入 `ptr`；已被占用则把 `ptr` 原样交还。
    pub(crate) fn offer_raw(&self, ptr: *mut T) -> Result<(), *mut T> {
        self.slot
            .compare_exchange(
                std::ptr::null_mut(),
                ptr,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .map(|_| ())
            .map_err(|_| ptr)
    }

    /// 取出当前指针；空槽返回 null。
    pub(crate) fn take_raw(&self) -> *mut T {
        self.slot.swap(std::ptr::null_mut(), Ordering::AcqRel)
    }
}
