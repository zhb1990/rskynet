//! 定时器服务，对照 `skynet-src/skynet_start.c` 里的 `thread_timer`。
//!
//! C 版把定时器写成一条专用线程，这里它是一个[独占线程服务][rskynet_core::Exclusive]：
//! 时间轮就长在服务身上（只有它那条线程碰得到，所以不必加锁），空闲钩子按配置的
//! 节拍醒来推进刻度、派发到期事件。
//!
//! 挂表的路径不经过它的邮箱——`Ctx::sleep` 是往 [`WheelTimer`] 的无锁队列里压一个
//! 事件，由本服务在下一个 tick 插进轮子。邮箱留给真正的请求，比如查时间戳。

use std::sync::Arc;
use std::time::Duration;

use rskynet_core::{Ctx, Error, Idler, Message, MsgType, Payload, Result, SvcCell, Timer, service};
use serde::Deserialize;

use crate::wheel::Wheel;
use crate::{Request, Timestamp, WheelTimer};

/// 两个 tick 之间的间隔，与 C 版一致：刻度精度是 10ms，2.5ms 醒一次绰绰有余。
const DEFAULT_TICK_MICROS: u64 = 2500;

/// `[timer]` 段。`name` 归内核解析，这里只关心节拍。
#[derive(Debug, Deserialize)]
#[serde(default)]
struct TimerConfig {
    /// 两次醒来之间隔多少微秒。
    tick_micros: u64,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            tick_micros: DEFAULT_TICK_MICROS,
        }
    }
}

pub struct TimerService {
    /// 与内核共用的那一份时钟，注入节点的就是它。
    timer: Arc<WheelTimer>,
    /// 分层时间轮。只有本服务那条独占线程碰得到它，所以一把锁都不需要。
    wheel: SvcCell<Wheel>,
    /// 两次醒来之间的间隔，`init` 时从配置里读。
    tick: SvcCell<Duration>,
}

impl TimerService {
    pub fn new(timer: Arc<WheelTimer>) -> Self {
        Self {
            timer,
            wheel: SvcCell::new(Wheel::new()),
            tick: SvcCell::new(Duration::from_micros(DEFAULT_TICK_MICROS)),
        }
    }

    fn stamp(&self) -> Timestamp {
        Timestamp {
            now: self.timer.now(),
            wall_clock: self.timer.wall_clock(),
            start_seconds: self.timer.start_seconds(),
        }
    }
}

// 这里不能写 `name = "timer"` 做宏自动注册：TimerService::new 需要接收由
// Builder 同时注入内核的同一个 Arc<WheelTimer>，而自动注册工厂只能是无参数函数。
// 注册与注入必须由 BuilderExt::with_wheel_timer 一次完成。
#[rskynet_macros::exclusive(crate = ::rskynet_core)]
impl TimerService {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let config: TimerConfig = ctx.node().section(service::TIMER)?.unwrap_or_default();
        if config.tick_micros == 0 {
            return Err(Error::Config("[timer] tick_micros 必须大于 0".into()));
        }
        self.tick.set(Duration::from_micros(config.tick_micros));
        Ok(())
    }

    /// 定时器也是个正常的服务：挂表不走邮箱，但查时间走。
    async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
        if msg.mtype != MsgType::USER {
            return;
        }
        match msg.take_payload().downcast::<Request>() {
            Ok(request) => match *request {
                Request::Timestamp => {
                    let _ = ctx.reply(&msg, Payload::of(self.stamp()));
                }
            },
            Err(_) => {
                let _ = ctx.reply_error(&msg);
            }
        }
    }

    /// 一个 tick，对照 C 版 `thread_timer` 的循环体。
    ///
    /// `interrupt` 走默认实现（什么都不做）：内核那记 `unpark` 顶多让这一觉早醒
    /// 一点，重扫一遍邮箱再睡下去而已。
    fn idle(&self, ctx: &Ctx, idler: &Idler) {
        let node = ctx.node();
        if idler.is_quit() {
            // 节点已经收工，刻度不必再推了，等着收尾流程把自己摘除
            idler.park();
            return;
        }

        for event in self.timer.update(&mut self.wheel.borrow_mut()) {
            // 到期即回包，与 `call` 的应答走同一条路：source 填 0 表示来自内核
            let _ = node.send(
                0,
                event.handle,
                MsgType::RESPONSE,
                event.session,
                Payload::None,
            );
        }

        // 最后一个服务也走了，对照 C 版那句 break。必须先确认引导已经完成：本服务
        // 排在引导之前起，那会儿服务数本来就是 0，光看这个数会当场误判成「散场了」
        if node.is_booted() && node.service_count() == 0 {
            node.quit();
            return;
        }

        // 兜底唤醒：投递方漏叫的话，睡着的 worker 最多等一个 tick
        node.poke();
        idler.park_timeout(self.tick.get());
    }
}
