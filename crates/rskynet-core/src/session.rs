//! session 表：把「等应答」的任务挂起来，等回包到了再唤醒。
//!
//! 对照 `lualib/skynet.lua` 里的 `session_id_coroutine`：
//! skynet 用 `session -> coroutine` 的映射，回包到达时 resume 对应协程；
//! 这里换成 `session -> Waker`，回包到达时 wake 对应任务。
//! `sleep` 与 `call` 共用同一张表，因为定时器到期也是以 `RESPONSE` 消息回来的。
//!
//! session 使用 `u64` 单调递增编号：等待方放弃时直接删除表项，不需要墓碑。
//! 编号在服务生命周期内不会复用，因此迟到的回包只会命中「查无此 session」，
//! 不会错误唤醒后来占用同一编号的新请求。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

use crate::error::{Error, Result};
use crate::message::Payload;
use crate::task::SvcCell;

enum Slot {
    /// 已登记，等待回包。`Waker` 要等任务第一次被 poll 才拿得到。
    Waiting(Option<Waker>),
    /// 回包已到，等任务来取。
    Done(Result<Payload>),
}

struct Inner {
    /// 下一个 session，从 1 开始单调递增；0 保留给「不需要应答」的消息。
    next: u64,
    slots: HashMap<u64, Slot>,
}

pub(crate) struct SessionTable {
    /// 只有当前执行本 service 的线程可以修改；跨线程观测只能读取 `active`。
    inner: SvcCell<Inner>,
    active: AtomicUsize,
}

impl SessionTable {
    pub(crate) fn new() -> Self {
        Self {
            inner: SvcCell::new(Inner {
                next: 0,
                slots: HashMap::new(),
            }),
            active: AtomicUsize::new(0),
        }
    }

    /// 分配一个新的 session 并登记为等待中。
    ///
    /// u64 空间下编号回绕实际不可达；即使发生回绕也会跳过仍存活的编号，
    /// 因此不需要 Abandoned 墓碑来防复用。
    pub(crate) fn alloc(&self) -> u64 {
        let mut inner = self.inner.borrow_mut();
        loop {
            inner.next = inner.next.wrapping_add(1);
            if inner.next == 0 {
                inner.next = 1;
            }
            let session = inner.next;
            if let std::collections::hash_map::Entry::Vacant(slot) = inner.slots.entry(session) {
                slot.insert(Slot::Waiting(None));
                self.active.fetch_add(1, Ordering::Relaxed);
                return session;
            }
        }
    }

    /// 回包到达。返回 false 表示没人在等（迟到、已放弃或重复回包），消息应当丢弃。
    pub(crate) fn complete(&self, session: u64, result: Result<Payload>) -> bool {
        let mut inner = self.inner.borrow_mut();
        match inner.slots.get(&session) {
            // 丢弃回包 Payload 前先释放表借用，否则用户 Drop 重入 session 表会撞车。
            // 析构 panic 与其它用户代码 panic 一样直接向上传播。
            None => {
                drop(inner);
                drop(result);
                return false;
            }
            Some(Slot::Done(_)) => {
                drop(inner);
                drop(result);
                return false;
            }
            Some(Slot::Waiting(_)) => {}
        }
        let previous = inner.slots.insert(session, Slot::Done(result));
        // 先释放借用再唤醒：wake 可能同步回调到本服务，避免重入这张表
        drop(inner);
        if let Some(Slot::Waiting(Some(waker))) = previous {
            waker.wake();
        }
        true
    }

    pub(crate) fn poll(&self, session: u64, waker: &Waker) -> Poll<Result<Payload>> {
        let mut inner = self.inner.borrow_mut();
        match inner.slots.get_mut(&session) {
            Some(Slot::Waiting(slot)) => {
                if !slot.as_ref().is_some_and(|old| old.will_wake(waker)) {
                    *slot = Some(waker.clone());
                }
                return Poll::Pending;
            }
            Some(Slot::Done(_)) => {}
            // 表项已被放弃或随服务销毁清掉
            _ => return Poll::Ready(Err(Error::Canceled)),
        }
        match inner.slots.remove(&session) {
            Some(Slot::Done(result)) => {
                self.active.fetch_sub(1, Ordering::Relaxed);
                Poll::Ready(result)
            }
            _ => unreachable!("刚刚确认过是 Done"),
        }
    }

    /// 等待方放弃等待。直接删除表项；迟到的回包会因查无此 session 被丢弃。
    pub(crate) fn abandon(&self, session: u64) {
        // 先搬出借用再析构：`Done` 里的 Payload Drop 可能重入 session 表。
        // 析构 panic 与其它用户代码 panic 一样直接向上传播。
        let removed = {
            let mut inner = self.inner.borrow_mut();
            inner.slots.remove(&session)
        };
        if removed.is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
        drop(removed);
    }

    /// 服务销毁时清空。此时所有等待中的任务马上会被一起丢弃，无需逐个唤醒。
    pub(crate) fn clear(&self) {
        // 整张表搬出来，释放 SvcCell 借用后再析构槽位。
        // `Slot::Done` 里可能装着用户 `Payload`；它的 Drop panic 与其它用户代码
        // panic 一样是进程级故障，不做恢复。
        let slots = std::mem::take(&mut self.inner.borrow_mut().slots);
        self.active.store(0, Ordering::Relaxed);
        drop(slots);
    }

    pub(crate) fn pending(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker;

    /// session 从 1 开始且永远是正数；0 保留给「无需应答」的消息
    #[test]
    fn session_ids_are_positive() {
        let table = SessionTable::new();
        assert_eq!(table.alloc(), 1);
        assert_eq!(table.alloc(), 2);

        let table = SessionTable::new();
        table.inner.borrow_mut().next = u64::MAX;
        assert_eq!(table.alloc(), 1, "溢出后应回到 1 而不是 0");
    }

    #[test]
    fn session_wrap_skips_ids_that_are_still_live() {
        let table = SessionTable::new();
        assert_eq!(table.alloc(), 1);
        table.inner.borrow_mut().next = u64::MAX;
        assert_eq!(table.alloc(), 2, "回绕不能覆盖仍在等待的 session 1");
        assert_eq!(table.pending(), 2);
    }

    /// 回包到达时唤醒等待者，取走后表项即销毁
    #[test]
    fn reply_wakes_waiter() {
        let table = SessionTable::new();
        let session = table.alloc();
        let waker = noop_waker();
        assert_eq!(table.pending(), 1);

        assert!(table.poll(session, &waker).is_pending());
        assert!(table.complete(session, Ok(Payload::text("pong"))));
        assert_eq!(table.pending(), 1, "回包尚未被任务消费");

        match table.poll(session, &waker) {
            Poll::Ready(Ok(payload)) => assert_eq!(payload.as_str(), Some("pong")),
            other => panic!("应取到回包，实际 {other:?}"),
        }
        // 取走后表项即销毁
        assert_eq!(table.pending(), 0);
    }

    /// 放弃等待直接删除表项：迟到的回包查无此 session，不会留下墓碑
    #[test]
    fn abandoned_session_is_removed_immediately() {
        let table = SessionTable::new();
        let session = table.alloc();
        table.abandon(session);
        assert_eq!(table.pending(), 0, "放弃后不再算活动调用");
        assert_eq!(table.inner.borrow().slots.len(), 0, "表项应立即删除");

        assert!(
            !table.complete(session, Ok(Payload::None)),
            "无人等待应返回 false"
        );
        assert_eq!(table.pending(), 0);
    }

    /// 重复回包不能覆盖第一个尚未取走的结果
    #[test]
    fn duplicate_reply_keeps_the_first_result() {
        let table = SessionTable::new();
        let session = table.alloc();
        let waker = noop_waker();
        assert!(table.poll(session, &waker).is_pending());

        assert!(table.complete(session, Ok(Payload::text("first"))));
        assert!(
            !table.complete(session, Ok(Payload::text("second"))),
            "重复回包应当被丢弃"
        );

        match table.poll(session, &waker) {
            Poll::Ready(Ok(payload)) => assert_eq!(payload.as_str(), Some("first")),
            other => panic!("应取到第一个回包，实际 {other:?}"),
        }
    }

    /// 服务销毁清表后，还在等待的一方要拿到取消错误而不是永久挂起
    #[test]
    fn cleared_table_cancels_waiters() {
        let table = SessionTable::new();
        let session = table.alloc();
        table.clear();
        assert_eq!(table.pending(), 0);
        match table.poll(session, &noop_waker()) {
            Poll::Ready(Err(Error::Canceled)) => {}
            _ => panic!("应得到 Canceled"),
        }
    }

    /// 陌生 session 的回包直接丢弃
    #[test]
    fn unknown_session_reply_is_ignored() {
        let table = SessionTable::new();
        assert!(!table.complete(12345, Ok(Payload::None)));
    }

    struct PanicDropPayload;

    impl Drop for PanicDropPayload {
        fn drop(&mut self) {
            panic!("panic in Payload::drop");
        }
    }

    /// 清表时到达的 Payload 析构 panic 不做恢复，直接向上传播。
    #[test]
    fn clear_propagates_payload_drop_panic() {
        let table = SessionTable::new();
        let session = table.alloc();
        assert!(table.complete(session, Ok(Payload::of(PanicDropPayload))));

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.clear()));
        std::panic::set_hook(previous);

        assert!(result.is_err(), "clear 必须传播 Payload 的析构 panic");
        assert_eq!(table.pending(), 0);
    }

    /// abandon 只负责先释放表借用；正常路径的 Payload 析构 panic 仍向上传播，
    /// 由外层 run_work / cleanup 决定如何善后。
    #[test]
    fn abandon_propagates_payload_drop_panic_after_releasing_borrow() {
        let table = SessionTable::new();
        let session = table.alloc();
        assert!(table.complete(session, Ok(Payload::of(PanicDropPayload))));

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.abandon(session)));
        std::panic::set_hook(previous);

        assert!(result.is_err(), "正常路径的 Payload 析构 panic 应向上传播");
        assert_eq!(table.pending(), 0, "表项应已删除，不留下孤儿 session");
    }
}
