//! 分层时间轮，对照 `skynet-src/skynet_timer.c`。
//!
//! 时间精度是毫秒（10ms），和 skynet 完全一致：近期轮 256 格覆盖 2.56 秒，
//! 之后 4 层各 64 格逐级放大，添加与到期都是 O(1)。
//!
//! 与 C 版的实现差异有两处：一是 C 版用侵入式链表串联同一格里的定时器，这里用
//! `Vec`（事件是定长的小结构，Vec 更省事也更快），逐级迁移的算法完全照搬；二是
//! C 版给时间轮配了一把自旋锁，这里改成时间轮由定时器服务独占持有，别的线程挂表
//! 一律走无锁队列（见 [`crate::WheelTimer`]），于是一把锁都不需要。

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
    pub(crate) expire: u32,
}

/// 分层时间轮本体。
///
/// 由定时器服务独占持有（而那个服务独占一条线程），所以这里全是普通的 `&mut`
/// 方法，不需要锁：别的线程想挂表只能走 [`crate::WheelTimer::timeout`]。
pub(crate) struct Wheel {
    /// 当前刻度，单位毫秒。
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
    pub(crate) fn add(&mut self, mut event: TimerEvent) {
        // 挂表的人看到的刻度可能比这里旧几格（挂表无锁、插表由定时器线程统一做），
        // 于是到期时刻可能已经过去了。按「本刻度到期」处理，否则它会落进近期轮里
        // 已经走过的格子，白等整整一圈 2.56 秒
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
        let idx = ((time >> (TIME_NEAR_SHIFT + level as u32 * TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)
            as usize;
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
    pub(crate) fn shift(&mut self) {
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
    pub(crate) fn execute(&mut self, out: &mut Vec<TimerEvent>) {
        let idx = (self.time & TIME_NEAR_MASK) as usize;
        out.append(&mut self.near[idx]);
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

        assert!(
            advance(&mut wheel, 299).is_empty(),
            "提前到期就说明迁移算错了"
        );
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
            assert!(early.is_empty(), "{ticks} 毫秒的定时器提前到期了");
            let fired = advance(&mut wheel, 1);
            assert_eq!(fired.len(), 1, "{ticks} 毫秒的定时器没有按时到期");
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
}
