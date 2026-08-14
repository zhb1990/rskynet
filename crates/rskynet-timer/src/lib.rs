//! # rskynet-timer
//!
//! rskynet 的时间：分层时间轮，加一条推着它走的独占线程。
//!
//! C 版把定时器写死在内核里（`skynet_timer.c` 加 `thread_timer` 那条专用线程），
//! 这里它整个住在内核之外，分成配合的两半：
//!
//! - [`WheelTimer`]：内核 [`Timer`] 抽象的实现，负责记账与读时钟。谁都能碰它
//!   （`ctx.sleep` 就是往它的无锁队列里压一个事件），所以启动前要把它注入节点。
//! - [`TimerService`]：一个[独占线程的服务][rskynet_core::Exclusive]，按节拍醒来
//!   推刻度、把到期的表派发成消息。它同时也是个正常的服务，会应答
//!   [`Request::Timestamp`] 这样的查询。
//!
//! 分成两半不只是为了拆包。记账的那一半在节点建起来之前就存在，于是引导期间挂的
//! 表一条都不会丢——哪怕那时推刻度的线程还没上线。
//!
//! ## 用法
//!
//! ```no_run
//! # use rskynet_core::{Builder, Config, Registry};
//! use rskynet_timer::BuilderExt;
//!
//! Builder::new(Config::default())
//!     .registry(Registry::new())
//!     // 注册定时器服务，同时把时间来源注入节点
//!     .with_wheel_timer()
//!     .run()
//!     .unwrap();
//! ```
//!
//! 用门面 crate `rskynet` 的话这一步是默认带上的，不必自己写。

mod service;
mod wheel;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crossbeam_queue::SegQueue;
use rskynet_core::service::TIMER;
use rskynet_core::{Builder, Ctx, Error, Payload, Result, Timer};

pub use service::TimerService;

use wheel::{TimerEvent, Wheel};

/// 时间轮版的时间来源，注入给内核的就是它，见 [`Timer`]。
///
/// 跨线程共享的只有这里的两样东西：谁都可以把事件压进 `incoming`，由定时器服务
/// 每 tick 排空后插进轮子；`elapsed` 则是所有人都要读的时钟。时间轮本身不在这里
/// ——它归定时器服务独占，因此一把锁都不需要。
pub struct WheelTimer {
    /// 等着被插进时间轮的事件。
    ///
    /// `sleep` 与 `call` 超时都要挂表，而挂表的是任意 worker 线程；它们够不着
    /// 时间轮，只能排队等定时器服务代插——精度本来就是 10ms，晚一个 tick 没区别。
    incoming: SegQueue<TimerEvent>,
    /// 进程启动时刻，用来把单调时钟换算成 unix 时间。
    started: Instant,
    /// 启动时刻的 unix 时间，单位秒。
    start_seconds: u64,
    /// 启动时刻的 unix 时间零头，单位毫秒。
    start_centis: u64,
    /// 已经推进过的刻度数（毫秒），对照 C 版 `TI->current`。
    ///
    /// 只有定时器服务会写；`ctx.now()` / `time()` 每次调用都要读它，所以是原子量。
    elapsed: AtomicU64,
}

impl Default for WheelTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl WheelTimer {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self {
            incoming: SegQueue::new(),
            started: Instant::now(),
            start_seconds: now.as_secs(),
            start_centis: u64::from(now.subsec_millis() / 10),
            elapsed: AtomicU64::new(0),
        }
    }

    /// 推进到真实时间，返回这期间到期的全部事件。定时器服务每个 tick 调一次。
    ///
    /// `wheel` 由调用方（定时器服务）持有，这里只负责收集事件，派发由调用方做。
    fn update(&self, wheel: &mut Wheel) -> Vec<TimerEvent> {
        let now = self.started.elapsed().as_millis() as u64 / 10;
        let elapsed = self.elapsed.load(Ordering::Relaxed);
        let diff = now.saturating_sub(elapsed);

        let mut out = Vec::new();
        // 新挂的表先插进轮子：哪怕这一 tick 没走满一格，也不能把它们攒着
        while let Some(event) = self.incoming.pop() {
            wheel.add(event);
        }
        if diff == 0 {
            // 也可能有刚挂上就该响的（`ticks` 小到落在本刻度）
            wheel.execute(&mut out);
            return out;
        }
        self.elapsed.store(now, Ordering::Relaxed);
        for _ in 0..diff {
            // 先捞一遍 0 延迟的（极少见），再推进刻度、再捞一遍
            wheel.execute(&mut out);
            wheel.shift();
            wheel.execute(&mut out);
        }
        out
    }
}

impl Timer for WheelTimer {
    /// 只是排进队列，真正插轮子由定时器服务在下一个 tick 做。到期时刻按当前刻度
    /// 算好带上，所以延后插入不会让定时器变长。
    fn timeout(&self, handle: u32, session: i32, ticks: u32) {
        let expire = (self.now() as u32).wrapping_add(ticks);
        self.incoming.push(TimerEvent {
            handle,
            session,
            expire,
        });
    }

    fn now(&self) -> u64 {
        self.elapsed.load(Ordering::Relaxed)
    }

    fn wall_clock(&self) -> u64 {
        self.start_seconds * 100 + self.start_centis + self.now()
    }

    fn start_seconds(&self) -> u64 {
        self.start_seconds
    }
}

/// 向定时器服务发起的请求，走 `MsgType::USER`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// 要一份当前时间戳，回包是 [`Timestamp`]。
    Timestamp,
}

/// 定时器眼里的当下，[`Request::Timestamp`] 的回包。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    /// 节点启动至今的毫秒数，对照 `skynet.now`。
    pub now: u64,
    /// 当前 unix 时间，单位毫秒。
    pub wall_clock: u64,
    /// 节点启动时刻的 unix 时间，单位秒，对照 `skynet.starttime`。
    pub start_seconds: u64,
}

impl Timestamp {
    /// 当前 unix 时间，单位秒，对照 `skynet.time`。
    pub fn unix_time(&self) -> f64 {
        self.wall_clock as f64 / 100.0
    }
}

/// 问定时器服务要一份时间戳。
///
/// 服务内部直接用 `ctx.now()` / `time()` 更省事（那是一次原子读，不经过邮箱）。
/// 这条消息路径是给「手里只有一个地址」的场景准备的：调试命令、将来的跨节点查询，
/// 以及任何想把时间也当成一次普通服务调用来看待的地方。
pub async fn timestamp(ctx: &Ctx) -> Result<Timestamp> {
    let reply = ctx.request(TIMER, Payload::of(Request::Timestamp)).await?;
    reply
        .downcast::<Timestamp>()
        .map(|stamp| *stamp)
        .map_err(|_| Error::service("定时器回的不是时间戳"))
}

/// 把定时器装进 [`Builder`]。
pub trait BuilderExt {
    /// 注册定时器服务，并把同一个 [`WheelTimer`] 注入成节点的时间来源。
    ///
    /// 两件事必须一起做：服务推的和内核读的得是同一个时钟，否则挂上的表永远等不
    /// 到人来插轮子。
    #[must_use]
    fn with_wheel_timer(self) -> Self;
}

impl BuilderExt for Builder {
    fn with_wheel_timer(self) -> Self {
        let timer = Arc::new(WheelTimer::new());
        let owned = timer.clone();
        self.exclusive_service(TIMER, move || TimerService::new(owned.clone()))
            .timer(timer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 时钟只会前进，unix 时间也得是个合理值
    #[test]
    fn clock_never_goes_backwards() {
        let timer = WheelTimer::new();
        let mut wheel = Wheel::new();
        let before = timer.now();
        timer.update(&mut wheel);
        assert!(timer.now() >= before);
        assert!(
            timer.start_seconds() > 1_600_000_000,
            "unix 时间应当是合理值"
        );
        assert!(timer.wall_clock() >= timer.start_seconds() * 100);
    }

    /// 挂上的表要在推到那个刻度时到期，而且回的是挂表时那个 session
    #[test]
    fn an_armed_timer_fires_on_its_tick() {
        let timer = WheelTimer::new();
        let mut wheel = Wheel::new();
        timer.timeout(7, 42, 1);
        // 第一次 update 只负责把队列里的表插进轮子
        timer.update(&mut wheel);

        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let mut fired = Vec::new();
        while fired.is_empty() && Instant::now() < deadline {
            fired = timer.update(&mut wheel);
        }
        assert_eq!(fired.len(), 1, "一毫秒的表应当在 5 秒内到期");
        assert_eq!(fired[0].handle, 7);
        assert_eq!(fired[0].session, 42);
    }
}
