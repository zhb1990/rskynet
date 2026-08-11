//! 两级消息队列，对照 `skynet-src/skynet_mq.c`。
//!
//! - [`Mailbox`]：每个服务一个，等价于 C 版的 `struct message_queue`。
//!   C 版手写环形缓冲加倍扩容，这里用 `VecDeque`（本身就是按需扩容的环形缓冲），
//!   语义一致；`in_global` 标志与过载检测原样保留。
//! - [`GlobalQueue`]：全局只有一个，存放「有活干的服务」，等价于 `struct global_queue`。
//!
//! 相比 C 版本，邮箱里多了一条 `ready` 队列存放服务内部被唤醒的 Future 任务，
//! 于是「服务可被调度」的条件从「邮箱非空」推广为「邮箱非空或有就绪任务」。
//! `in_global` 依然是整套调度的核心不变量：
//! **它为 true 时表示该服务要么排在全局队列里，要么正被某个 worker 持有，
//! 因此同一服务绝不会被两个 worker 同时执行。**

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

use crate::message::Message;
use crate::server::ServiceContext;

/// 过载报警的初始阈值，对照 C 版 `MQ_OVERLOAD`。
pub(crate) const OVERLOAD_THRESHOLD: usize = 1024;

/// 一次调度取到的活儿：要么是一条新消息，要么是一个被唤醒的服务内任务。
pub(crate) enum Work {
    Message(Message),
    Task(usize),
}

struct MailboxInner {
    queue: VecDeque<Message>,
    /// 服务内被唤醒、等待 poll 的任务 id（相当于 skynet 里可以 resume 的协程）。
    ready: VecDeque<usize>,
    in_global: bool,
    overload: usize,
    overload_threshold: usize,
}

pub(crate) struct Mailbox {
    inner: Mutex<MailboxInner>,
}

impl Mailbox {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(MailboxInner {
                queue: VecDeque::with_capacity(64),
                ready: VecDeque::new(),
                // 与 C 版一致：创建时就置位，保证服务初始化完成前不会被 worker 领走
                in_global: true,
                overload: 0,
                overload_threshold: OVERLOAD_THRESHOLD,
            }),
        }
    }

    /// 投递消息。返回 true 表示调用方需要把该服务推入全局队列。
    pub(crate) fn push_message(&self, msg: Message) -> bool {
        let mut inner = self.inner.lock();
        inner.queue.push_back(msg);
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 标记某个服务内任务就绪。返回 true 表示调用方需要把该服务推入全局队列。
    pub(crate) fn push_ready(&self, task: usize) -> bool {
        let mut inner = self.inner.lock();
        inner.ready.push_back(task);
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 取一件活干。返回 `None` 表示服务彻底空闲，此时会**原子地**清掉
    /// `in_global`，让后续的投递方负责把服务重新推进全局队列。
    ///
    /// 就绪任务优先于新消息：对应 skynet 里被 resume 的协程会一路跑到下一次
    /// yield，之后才轮到下一条消息。
    pub(crate) fn take_work(&self) -> Option<Work> {
        let mut inner = self.inner.lock();
        if let Some(task) = inner.ready.pop_front() {
            return Some(Work::Task(task));
        }
        match inner.queue.pop_front() {
            Some(msg) => {
                let length = inner.queue.len();
                while length > inner.overload_threshold {
                    inner.overload = length;
                    inner.overload_threshold *= 2;
                }
                Some(Work::Message(msg))
            }
            None => {
                inner.in_global = false;
                inner.overload_threshold = OVERLOAD_THRESHOLD;
                None
            }
        }
    }

    /// 只取就绪任务，不碰 `in_global`。服务初始化阶段用它把 init 推进到第一次挂起。
    pub(crate) fn take_ready(&self) -> Option<usize> {
        self.inner.lock().ready.pop_front()
    }

    /// 强行置位 `in_global`。返回 true 表示调用方需要入队。
    /// 服务被 kill 时用它保证一定会被某个 worker 领走一次以完成销毁。
    pub(crate) fn mark_in_global(&self) -> bool {
        let mut inner = self.inner.lock();
        !std::mem::replace(&mut inner.in_global, true)
    }

    /// 清掉 `in_global`。服务清理完毕后调用，好让清理之后迟到的消息还能
    /// 把这个服务重新推进全局队列、再被清一次。
    pub(crate) fn reset_in_global(&self) {
        self.inner.lock().in_global = false;
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().queue.len()
    }

    /// 取出并清零过载读数，对照 `skynet_mq_overload`。
    pub(crate) fn take_overload(&self) -> usize {
        std::mem::take(&mut self.inner.lock().overload)
    }

    /// 服务销毁时清空邮箱，未处理的消息交回调用方以便给发起者回错误。
    pub(crate) fn drain(&self) -> Vec<Message> {
        let mut inner = self.inner.lock();
        inner.ready.clear();
        inner.queue.drain(..).collect()
    }
}

struct GlobalInner {
    queue: VecDeque<Arc<ServiceContext>>,
    /// 正在条件变量上睡觉的 worker 数量，对照 C 版 `struct monitor` 的 `sleep`。
    sleeping: usize,
    quit: bool,
}

/// 全局队列：worker 线程唯一的取活来源。
pub(crate) struct GlobalQueue {
    inner: Mutex<GlobalInner>,
    idle: Condvar,
}

impl GlobalQueue {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(GlobalInner {
                queue: VecDeque::new(),
                sleeping: 0,
                quit: false,
            }),
            idle: Condvar::new(),
        }
    }

    /// C 版的 `skynet_globalmq_push` 并不唤醒 worker，靠定时器线程每 2.5ms 顺手唤醒，
    /// 代价是消息在所有 worker 都睡着时最坏要等一个 tick。这里改成有人睡就直接唤一个，
    /// 延迟更低而开销可忽略（无人睡时只是多读一个已持有锁的字段）。
    pub(crate) fn push(&self, ctx: Arc<ServiceContext>) {
        let mut inner = self.inner.lock();
        inner.queue.push_back(ctx);
        let need_wakeup = inner.sleeping > 0;
        drop(inner);
        if need_wakeup {
            self.idle.notify_one();
        }
    }

    pub(crate) fn pop(&self) -> Option<Arc<ServiceContext>> {
        self.inner.lock().queue.pop_front()
    }

    /// worker 空闲时睡在这里，等待新活或退出信号。
    pub(crate) fn wait_for_work(&self) {
        let mut inner = self.inner.lock();
        if inner.quit || !inner.queue.is_empty() {
            return;
        }
        inner.sleeping += 1;
        self.idle.wait(&mut inner);
        inner.sleeping -= 1;
    }

    /// 定时器线程的兜底唤醒，对照 C 版 `wakeup(m, m->count - 1)`。
    pub(crate) fn poke(&self) {
        let inner = self.inner.lock();
        let need_wakeup = inner.sleeping > 0 && !inner.queue.is_empty();
        drop(inner);
        if need_wakeup {
            self.idle.notify_one();
        }
    }

    pub(crate) fn set_quit(&self) {
        self.inner.lock().quit = true;
        self.idle.notify_all();
    }

    pub(crate) fn is_quit(&self) -> bool {
        self.inner.lock().quit
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.lock().queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{MsgType, Payload};

    fn message(session: i32) -> Message {
        Message::new(1, session, MsgType::USER, Payload::None)
    }

    /// in_global 标志只有在邮箱确实空了之后才清掉
    #[test]
    fn in_global_cleared_only_when_empty() {
        let mailbox = Mailbox::new();
        // 新建时 in_global 已置位，第一次投递不需要入队
        assert!(!mailbox.push_message(message(1)));
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
        // 队列空 -> 清标志
        assert!(mailbox.take_work().is_none());
        // 清掉之后再投递就需要调用方入队了
        assert!(mailbox.push_message(message(2)));
        assert!(!mailbox.push_message(message(3)));
    }

    /// 就绪任务要排在新消息前面，对应协程被 resume 后跑到下次 yield 才轮到下条消息
    #[test]
    fn ready_tasks_come_before_messages() {
        let mailbox = Mailbox::new();
        mailbox.push_message(message(1));
        mailbox.push_ready(7);
        match mailbox.take_work() {
            Some(Work::Task(id)) => assert_eq!(id, 7),
            other => panic!("应先取到就绪任务，实际是 {:?}", other.is_some()),
        }
        assert!(matches!(mailbox.take_work(), Some(Work::Message(_))));
    }

    /// 报警一次之后阈值翻倍，免得持续过载时把日志刷爆
    #[test]
    fn overload_threshold_doubles() {
        let mailbox = Mailbox::new();
        for i in 0..(OVERLOAD_THRESHOLD * 2 + 2) {
            mailbox.push_message(message(i as i32));
        }
        // 弹出一条后剩余长度超过 1024，触发一次报警，阈值抬到 2048
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), OVERLOAD_THRESHOLD * 2 + 1);
        // 读过即清零，且没有再次越过新阈值
        assert_eq!(mailbox.take_overload(), 0);
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), 0);

        // 队列排空后阈值复位
        while mailbox.take_work().is_some() {}
        for i in 0..(OVERLOAD_THRESHOLD + 2) {
            mailbox.push_message(message(i as i32));
        }
        mailbox.take_work();
        assert_eq!(mailbox.take_overload(), OVERLOAD_THRESHOLD + 1);
    }

    /// 销毁时要能把未处理的消息全部取出来，好给发送方回错误
    #[test]
    fn drain_returns_all_pending_messages() {
        let mailbox = Mailbox::new();
        mailbox.push_message(message(1));
        mailbox.push_message(message(2));
        mailbox.push_ready(3);
        let left = mailbox.drain();
        assert_eq!(left.len(), 2);
        assert!(mailbox.take_ready().is_none());
    }
}
