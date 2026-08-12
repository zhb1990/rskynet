//! 分层时间轮，对照 `skynet-src/skynet_timer.c`。
//!
//! 时间精度是厘秒（10ms），和 skynet 完全一致：近期轮 256 格覆盖 2.56 秒，
//! 之后 4 层各 64 格逐级放大，添加与到期都是 O(1)。
//! 到期后不直接回调，而是给服务投一条 `RESPONSE` 消息——这样 `sleep` 和 `call`
//! 就能共用同一套 session 等待机制。
//!
//! 与 C 版的实现差异有两处：一是 C 版用侵入式链表串联同一格里的定时器，这里用
//! `Vec`（事件是定长的小结构，Vec 更省事也更快），逐级迁移的算法完全照搬；二是
//! C 版给时间轮配了一把自旋锁，这里改成时间轮由定时器线程独占持有（[`Wheel`]），
//! 别的线程挂表一律走无锁队列（[`Timer::incoming`]），于是一把锁都不需要。

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crossbeam_queue::SegQueue;

const TIME_NEAR_SHIFT: u32 = 8;
const TIME_NEAR: usize = 1 << TIME_NEAR_SHIFT;
const TIME_LEVEL_SHIFT: u32 = 6;
const TIME_LEVEL: usize = 1 << TIME_LEVEL_SHIFT;
const TIME_NEAR_MASK: u32 = TIME_NEAR as u32 - 1;
const TIME_LEVEL_MASK: u32 = TIME_LEVEL as u32 - 1;

/// 一个到期事件：把 `session` 号的应答发给 `handle`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerEvent {
    pub(crate) handle: u32,
    pub(crate) session: i32,
    expire: u32,
}

/// 分层时间轮本体。
///
/// 由定时器线程独占持有（建在它自己的栈上，见 `start::timer_loop`），所以这里
/// 全是普通的 `&mut` 方法，不需要锁：别的线程想挂表只能走 [`Timer::incoming`]。
pub(crate) struct Wheel {
    /// 当前刻度，单位厘秒。
    time: u32,
    near: Vec<Vec<TimerEvent>>,
    levels: [Vec<Vec<TimerEvent>>; 4],
}

impl Wheel {
    pub(crate) fn new() -> Self {
        Self {
            time: 0,
            near: (0..TIME_NEAR).map(|_| Vec::new()).collect(),
            levels: std::array::from_fn(|_| (0..TIME_LEVEL).map(|_| Vec::new()).collect()),
        }
    }

    /// 按到期时刻把事件放进合适的格子，对照 C 版 `add_node`。
    fn add(&mut self, mut event: TimerEvent) {
        // 挂表的人看到的刻度可能比这里旧几格（挂表无锁、插表由定时器线程统一做，
        // 见 [`Timer::add`]），于是到期时刻可能已经过去了。按「本刻度到期」处理，
        // 否则它会落进近期轮里已经走过的格子，白等整整一圈 2.56 秒
        if event.expire.wrapping_sub(self.time) as i32 <= 0 {
            event.expire = self.time;
        }
        let time = event.expire;
        let current = self.time;
        if (time | TIME_NEAR_MASK) == (current | TIME_NEAR_MASK) {
            // 2.56 秒内到期，直接进近期轮
            let idx = (time & TIME_NEAR_MASK) as usize;
            self.near[idx].push(event);
            return;
        }
        let mut mask = (TIME_NEAR as u32) << TIME_LEVEL_SHIFT;
        let mut level = 0usize;
        while level < 3 {
            if (time | (mask - 1)) == (current | (mask - 1)) {
                break;
            }
            mask <<= TIME_LEVEL_SHIFT;
            level += 1;
        }
        let idx = ((time >> (TIME_NEAR_SHIFT + level as u32 * TIME_LEVEL_SHIFT))
            & TIME_LEVEL_MASK) as usize;
        self.levels[level][idx].push(event);
    }

    /// 把某一层某一格里的事件重新散落到更细的层级，对照 C 版 `move_list`。
    fn move_list(&mut self, level: usize, idx: usize) {
        let events = std::mem::take(&mut self.levels[level][idx]);
        for event in events {
            self.add(event);
        }
    }

    /// 刻度前进一格，必要时做层级迁移，对照 C 版 `timer_shift`。
    fn shift(&mut self) {
        self.time = self.time.wrapping_add(1);
        let current = self.time;
        if current == 0 {
            // 刻度回绕，最高层整格搬下来
            self.move_list(3, 0);
            return;
        }
        let mut mask = TIME_NEAR as u32;
        let mut time = current >> TIME_NEAR_SHIFT;
        let mut level = 0usize;
        while current & (mask - 1) == 0 {
            let idx = (time & TIME_LEVEL_MASK) as usize;
            if idx != 0 {
                self.move_list(level, idx);
                break;
            }
            mask <<= TIME_LEVEL_SHIFT;
            time >>= TIME_LEVEL_SHIFT;
            level += 1;
        }
    }

    /// 摘走当前刻度上所有到期事件，对照 C 版 `timer_execute`。
    fn execute(&mut self, out: &mut Vec<TimerEvent>) {
        let idx = (self.time & TIME_NEAR_MASK) as usize;
        out.append(&mut self.near[idx]);
    }
}

/// 节点级定时器里跨线程共享的那部分。
///
/// 时间轮不在这儿——它归定时器线程独占（见 [`Wheel`]）。这里只剩真正需要共享的
/// 东西：谁都可以把事件压进 [`Timer::incoming`]，由定时器线程每 tick 排空后插进
/// 轮子；`elapsed` 则是所有人都要读的时钟。
pub(crate) struct Timer {
    /// 等着被插进时间轮的事件。
    ///
    /// `sleep` 与 `call` 超时都要挂表，而挂表的是任意 worker 线程；它们够不着
    /// 时间轮，只能排队等定时器线程代插——精度本来就是 10ms，晚一个 tick 没区别。
    incoming: SegQueue<TimerEvent>,
    /// 进程启动时刻，用来把单调时钟换算成 unix 时间。
    started: Instant,
    /// 启动时刻的 unix 时间，单位秒。
    start_seconds: u64,
    /// 启动时刻的 unix 时间零头，单位厘秒。
    start_centis: u64,
    /// 已经推进过的刻度数（厘秒），对照 C 版 `TI->current`。
    ///
    /// 只有定时器线程会写；`ctx.now()` / `ctx.time()` 每次调用都要读它，所以是原子量。
    elapsed: AtomicU64,
}

impl Timer {
    pub(crate) fn new() -> Self {
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

    /// 挂一个 `ticks` 厘秒后到期的定时器。`ticks` 为 0 时调用方应当立即投递应答。
    ///
    /// 只是排进队列，真正插轮子由定时器线程在下一个 tick 做。到期时刻按当前刻度
    /// 算好带上，所以延后插入不会让定时器变长。
    pub(crate) fn add(&self, handle: u32, session: i32, ticks: u32) {
        let expire = (self.now() as u32).wrapping_add(ticks);
        self.incoming.push(TimerEvent {
            handle,
            session,
            expire,
        });
    }

    /// 推进到真实时间，返回这期间到期的全部事件。定时器线程每 2.5ms 调一次。
    ///
    /// `wheel` 由调用方（定时器线程）持有，这里只负责收集事件，派发由调用方做。
    pub(crate) fn update(&self, wheel: &mut Wheel) -> Vec<TimerEvent> {
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

    /// 节点启动至今的厘秒数，对照 `skynet_now`。
    pub(crate) fn now(&self) -> u64 {
        self.elapsed.load(Ordering::Relaxed)
    }

    /// 节点启动时刻的 unix 时间（秒），对照 `skynet_starttime`。
    pub(crate) fn start_seconds(&self) -> u64 {
        self.start_seconds
    }

    /// 当前 unix 时间，单位厘秒。
    pub(crate) fn wall_clock(&self) -> u64 {
        self.start_seconds * 100 + self.start_centis + self.now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 把时间轮拨过 ticks 个刻度，返回这期间到期的定时器
    fn advance(wheel: &mut Wheel, ticks: u32) -> Vec<TimerEvent> {
        let mut out = Vec::new();
        for _ in 0..ticks {
            wheel.execute(&mut out);
            wheel.shift();
            wheel.execute(&mut out);
        }
        out
    }

    /// 挂一个 ticks 个刻度之后到期的定时器
    fn arm(wheel: &mut Wheel, session: i32, ticks: u32) {
        let expire = wheel.time.wrapping_add(ticks);
        wheel.add(TimerEvent {
            handle: 1,
            session,
            expire,
        });
    }

    /// 近期轮里的定时器按刻度先后到期
    #[test]
    fn near_wheel_fires_in_order() {
        let mut wheel = Wheel::new();
        arm(&mut wheel, 1, 1);
        arm(&mut wheel, 2, 3);

        let fired = advance(&mut wheel, 1);
        assert_eq!(fired.iter().map(|e| e.session).collect::<Vec<_>>(), vec![1]);
        assert!(advance(&mut wheel, 1).is_empty());
        let fired = advance(&mut wheel, 1);
        assert_eq!(fired.iter().map(|e| e.session).collect::<Vec<_>>(), vec![2]);
    }

    /// 零延迟的定时器在当前刻度就该被捞走，不能等到下一格
    #[test]
    fn zero_delay_fires_immediately() {
        let mut wheel = Wheel::new();
        arm(&mut wheel, 1, 0);
        let fired = advance(&mut wheel, 1);
        assert_eq!(fired.len(), 1);
    }

    /// 落在高层轮的定时器经过迁移后仍要准点，早一刻度都不行
    #[test]
    fn timer_migrated_from_upper_level_is_punctual() {
        let mut wheel = Wheel::new();
        // 300 > 256，会被放进第 1 层，必须经过 move_list 迁移才会落到近期轮
        arm(&mut wheel, 7, 300);
        assert!(wheel.levels[0].iter().any(|slot| !slot.is_empty()));

        assert!(advance(&mut wheel, 299).is_empty(), "提前到期就说明迁移算错了");
        let fired = advance(&mut wheel, 1);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].session, 7);
    }

    /// 逐层覆盖：每一层的入口时长都要准点
    #[test]
    fn all_levels_are_punctual() {
        // 覆盖每一层的入口：256 * 64 = 16384，再乘 64 是第 2 层
        for ticks in [255u32, 256, 257, 1000, 16_384, 20_000, 1_048_576] {
            let mut wheel = Wheel::new();
            arm(&mut wheel, 1, ticks);
            let early = advance(&mut wheel, ticks - 1);
            assert!(early.is_empty(), "{ticks} 厘秒的定时器提前到期了");
            let fired = advance(&mut wheel, 1);
            assert_eq!(fired.len(), 1, "{ticks} 厘秒的定时器没有按时到期");
        }
    }

    /// 同一格里挂多个定时器时要一起到期，不能漏掉链表后面的
    #[test]
    fn timers_in_same_slot_fire_together() {
        let mut wheel = Wheel::new();
        for session in 1..=5 {
            arm(&mut wheel, session, 10);
        }
        assert!(advance(&mut wheel, 9).is_empty());
        assert_eq!(advance(&mut wheel, 1).len(), 5);
    }

    /// 系统时间被回拨时钟也只能前进，不能倒退
    #[test]
    fn clock_never_goes_backwards() {
        let timer = Timer::new();
        let mut wheel = Wheel::new();
        let before = timer.now();
        timer.update(&mut wheel);
        assert!(timer.now() >= before);
        assert!(timer.start_seconds() > 1_600_000_000, "unix 时间应当是合理值");
    }
}
