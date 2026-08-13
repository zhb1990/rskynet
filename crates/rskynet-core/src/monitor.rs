//! Worker 死循环监测，对照 `skynet-src/skynet_monitor.c`。
//!
//! 每个共享 worker 各有一个 [`Monitor`]。worker 在 poll Future 的前后推进
//! 版本，监测线程每 5 秒看一遍：若两次看到同一版本且目标服务非 0，
//! 说明这次 poll 一直没有返回。

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 一个 worker 的进度。
pub(crate) struct Monitor {
    version: AtomicU64,
    checked: AtomicU64,
    /// 高 32 位是 source，低 32 位是 destination，保证路由成对更新。
    route: AtomicU64,
}

impl Monitor {
    pub(crate) fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            checked: AtomicU64::new(0),
            route: AtomicU64::new(0),
        }
    }

    fn begin(&self, source: u32, destination: u32) {
        // 先推进版本再发布路由。check 在读路由前后各读一次版本，
        // 因此撞在这个边界上只会重建基线，不会把刚开始的 poll 误报。
        self.version.fetch_add(1, Ordering::AcqRel);
        self.route
            .store(pack(source, destination), Ordering::Release);
    }

    fn finish(&self) {
        // 先清目标，边界竞态最多漏报一轮，不会把已返回的 poll 误报成死循环。
        self.route.store(0, Ordering::Release);
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    /// 返回持续未前进的 `(source, destination, version)`。
    pub(crate) fn check(&self) -> Option<(u32, u32, u64)> {
        let before = self.version.load(Ordering::Acquire);
        let route = self.route.load(Ordering::Acquire);
        let after = self.version.load(Ordering::Acquire);
        if before != after {
            self.checked.store(after, Ordering::Release);
            return None;
        }
        let checked = self.checked.load(Ordering::Acquire);
        if after != checked {
            self.checked.store(after, Ordering::Release);
            return None;
        }
        let (source, destination) = unpack(route);
        (destination != 0).then_some((source, destination, after))
    }
}

fn pack(source: u32, destination: u32) -> u64 {
    (u64::from(source) << 32) | u64::from(destination)
}

fn unpack(route: u64) -> (u32, u32) {
    ((route >> 32) as u32, route as u32)
}

thread_local! {
    /// 只有共享 worker 会绑定；主线程收尾和独占服务不受监测。
    static CURRENT: RefCell<Option<Arc<Monitor>>> = const { RefCell::new(None) };
}

/// 把 monitor 绑到当前 worker，析构时解绑。
pub(crate) struct Binding;

impl Binding {
    pub(crate) fn install(monitor: Arc<Monitor>) -> Self {
        CURRENT.with(|current| {
            let previous = current.replace(Some(monitor));
            assert!(previous.is_none(), "一条 worker 只能绑定一个 monitor");
        });
        Self
    }
}

impl Drop for Binding {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(None);
        });
    }
}

/// 标记当前 worker 开始 poll 一个任务；析构即表示 poll 已返回。
pub(crate) struct Running;

impl Running {
    pub(crate) fn enter(source: u32, destination: u32) -> Self {
        CURRENT.with(|current| {
            if let Some(monitor) = current.borrow().as_ref() {
                monitor.begin(source, destination);
            }
        });
        Self
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            if let Some(monitor) = current.borrow().as_ref() {
                monitor.finish();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_monitor_never_reports() {
        let monitor = Monitor::new();
        assert_eq!(monitor.check(), None);
        assert_eq!(monitor.check(), None);
    }

    #[test]
    fn unchanged_work_reports_on_the_second_check() {
        let monitor = Monitor::new();
        monitor.begin(0x12, 0x34);
        assert_eq!(monitor.check(), None, "第一次只建立基线");
        assert_eq!(monitor.check(), Some((0x12, 0x34, 1)));
        assert_eq!(monitor.check(), Some((0x12, 0x34, 1)), "持续卡住要重复报警");
    }

    #[test]
    fn completion_and_new_work_advance_the_baseline() {
        let monitor = Monitor::new();
        monitor.begin(1, 2);
        assert_eq!(monitor.check(), None);
        monitor.finish();
        assert_eq!(monitor.check(), None);
        assert_eq!(monitor.check(), None, "已完成的任务不该报警");

        monitor.begin(3, 4);
        assert_eq!(monitor.check(), None, "新任务应重新建立基线");
        assert_eq!(monitor.check(), Some((3, 4, 3)));
    }

    #[test]
    fn binding_routes_running_work_to_the_current_monitor() {
        let monitor = Arc::new(Monitor::new());
        let _binding = Binding::install(monitor.clone());
        let running = Running::enter(0xab, 0xcd);
        assert_eq!(monitor.check(), None);
        assert_eq!(monitor.check(), Some((0xab, 0xcd, 1)));
        drop(running);
        assert_eq!(monitor.check(), None);
        assert_eq!(monitor.check(), None);
    }
}
