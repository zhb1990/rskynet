//! 定时器服务，对照 `skynet-src/skynet_start.c` 里的 `thread_timer`。
//!
//! C 版把定时器写成一条专用线程，这里它是一个[独占线程服务][crate::Exclusive]：
//! 时间轮就长在服务身上（只有它那条线程碰得到，所以不必加锁），空闲钩子每 2.5ms
//! 醒一次推进刻度、派发到期事件。
//!
//! 挂表的路径没变——`Ctx::sleep` 仍然是往 [`crate::timer::Timer`] 的无锁队列里
//! 压一个事件，由本服务在下一个 tick 插进轮子。也就是说这个服务并不靠邮箱工作，
//! 它只是「那条线程」的新住所。

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;

use crate::context::{Ctx, Service};
use crate::exclusive::{Exclusive, Idler};
use crate::message::Message;
use crate::task::SvcCell;
use crate::timer::Wheel;

/// 两个 tick 之间的间隔，与 C 版一致：刻度精度是 10ms，2.5ms 醒一次绰绰有余。
const TICK: Duration = Duration::from_micros(2500);

pub struct Timer {
    /// 分层时间轮。只有本服务那条独占线程碰得到它，所以一把锁都不需要。
    wheel: SvcCell<Wheel>,
}

impl Default for Timer {
    fn default() -> Self {
        Self {
            wheel: SvcCell::new(Wheel::new()),
        }
    }
}

impl Service for Timer {
    /// 定时器不靠消息工作：挂表走的是无锁队列，不经过邮箱。名字由
    /// [`crate::start`] 在拉起时注册，与 logger 一样。
    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

impl Exclusive for Timer {
    /// 一个 tick，对照 C 版 `thread_timer` 的循环体。
    ///
    /// `interrupt` 走默认实现（什么都不做）：本服务不收消息，内核那记 `unpark`
    /// 顶多让这一觉早醒一点，重扫一遍邮箱再睡下去而已。
    fn idle(&self, ctx: &Ctx, idler: &Idler) {
        // 内置服务住在内核里，可以直接摸 Node；外部服务只有 Ctx 上那些公开方法
        let node = &ctx.inner.node;
        if idler.is_quit() {
            // 节点已经收工，刻度不必再推了，等着收尾流程把自己摘除
            idler.park();
            return;
        }
        node.fire_timers(&mut self.wheel.borrow_mut());
        if node.total() == 0 {
            // 最后一个服务也走了，对照 C 版那句 break
            node.quit();
            return;
        }
        // 兜底唤醒：投递方漏叫的话，睡着的 worker 最多等一个 tick
        node.sched.poke();
        idler.park_timeout(TICK);
    }
}
