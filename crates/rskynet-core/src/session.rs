//! session 表：把「等应答」的任务挂起来，等回包到了再唤醒。
//!
//! 对照 `lualib/skynet.lua` 里的 `session_id_coroutine`：
//! skynet 用 `session -> coroutine` 的映射，回包到达时 resume 对应协程；
//! 这里换成 `session -> Waker`，回包到达时 wake 对应任务。
//! `sleep` 与 `call` 共用同一张表，因为定时器到期也是以 `RESPONSE` 消息回来的。

use std::collections::HashMap;
use std::task::{Poll, Waker};

use parking_lot::Mutex;

use crate::error::{Error, Result};
use crate::message::Payload;

enum Slot {
    /// 已登记，等待回包。`Waker` 要等任务第一次被 poll 才拿得到。
    Waiting(Option<Waker>),
    /// 回包已到，等任务来取。
    Done(Result<Payload>),
    /// 等待方已经放弃（`Call` 被 drop，比如外层任务被取消）。
    /// 保留标记是为了让迟到的回包知道该直接丢掉，对应 skynet 里把表项置成 `false`。
    Abandoned,
}

struct Inner {
    /// 下一个 session，只发正数，对照 `skynet_context_newsession`。
    next: i32,
    slots: HashMap<i32, Slot>,
}

pub(crate) struct SessionTable {
    inner: Mutex<Inner>,
}

impl SessionTable {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next: 0,
                slots: HashMap::new(),
            }),
        }
    }

    /// 分配一个新的 session 并登记为等待中。
    pub(crate) fn alloc(&self) -> i32 {
        let mut inner = self.inner.lock();
        inner.next = inner.next.checked_add(1).unwrap_or(1);
        let session = inner.next;
        inner.slots.insert(session, Slot::Waiting(None));
        session
    }

    /// 回包到达。返回 false 表示没人在等（迟到或已放弃），消息应当丢弃。
    pub(crate) fn complete(&self, session: i32, result: Result<Payload>) -> bool {
        let mut inner = self.inner.lock();
        match inner.slots.get(&session) {
            None => return false,
            Some(Slot::Abandoned) => {
                inner.slots.remove(&session);
                return false;
            }
            _ => {}
        }
        let previous = inner.slots.insert(session, Slot::Done(result));
        // 先解锁再唤醒：wake 可能同步回调到本服务，避免重入这把锁
        drop(inner);
        if let Some(Slot::Waiting(Some(waker))) = previous {
            waker.wake();
        }
        true
    }

    pub(crate) fn poll(&self, session: i32, waker: &Waker) -> Poll<Result<Payload>> {
        let mut inner = self.inner.lock();
        match inner.slots.get_mut(&session) {
            Some(Slot::Waiting(slot)) => {
                if !slot.as_ref().is_some_and(|old| old.will_wake(waker)) {
                    *slot = Some(waker.clone());
                }
                return Poll::Pending;
            }
            Some(Slot::Done(_)) => {}
            // 表项被服务销毁流程清掉了
            _ => return Poll::Ready(Err(Error::Canceled)),
        }
        match inner.slots.remove(&session) {
            Some(Slot::Done(result)) => Poll::Ready(result),
            _ => unreachable!("刚刚确认过是 Done"),
        }
    }

    /// 等待方放弃等待。若回包已经到了就直接清掉，否则留个墓碑等回包来收。
    pub(crate) fn abandon(&self, session: i32) {
        let mut inner = self.inner.lock();
        if matches!(inner.slots.get(&session), Some(Slot::Waiting(_))) {
            inner.slots.insert(session, Slot::Abandoned);
        } else {
            inner.slots.remove(&session);
        }
    }

    /// 服务销毁时清空。此时所有等待中的任务马上会被一起丢弃，无需逐个唤醒。
    pub(crate) fn clear(&self) {
        self.inner.lock().slots.clear();
    }

    pub(crate) fn pending(&self) -> usize {
        self.inner.lock().slots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker;

    /// session 从 1 开始且永远是正数，溢出后回绕而不是变成负数
    #[test]
    fn session_ids_are_positive() {
        let table = SessionTable::new();
        assert_eq!(table.alloc(), 1);
        assert_eq!(table.alloc(), 2);

        let table = SessionTable::new();
        table.inner.lock().next = i32::MAX;
        assert_eq!(table.alloc(), 1, "溢出后应回到 1 而不是变成负数");
    }

    /// 回包到达时唤醒等待者，取走后表项即销毁
    #[test]
    fn reply_wakes_waiter() {
        let table = SessionTable::new();
        let session = table.alloc();
        let waker = noop_waker();

        assert!(table.poll(session, &waker).is_pending());
        assert!(table.complete(session, Ok(Payload::text("pong"))));

        match table.poll(session, &waker) {
            Poll::Ready(Ok(payload)) => assert_eq!(payload.as_str(), Some("pong")),
            other => panic!("应取到回包，实际 {other:?}"),
        }
        // 取走后表项即销毁
        assert_eq!(table.pending(), 0);
    }

    /// 等待被取消后，迟到的回包要连同墓碑一起丢弃，不能唤醒野 waker
    #[test]
    fn late_reply_after_abandon_is_dropped() {
        let table = SessionTable::new();
        let session = table.alloc();
        table.abandon(session);

        assert!(
            !table.complete(session, Ok(Payload::None)),
            "无人等待应返回 false"
        );
        assert_eq!(table.pending(), 0, "墓碑应随迟到回包一起清掉");
    }

    /// 服务销毁清表后，还在等待的一方要拿到取消错误而不是永久挂起
    #[test]
    fn cleared_table_cancels_waiters() {
        let table = SessionTable::new();
        let session = table.alloc();
        table.clear();
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
}
