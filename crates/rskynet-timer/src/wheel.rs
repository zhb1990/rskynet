//! 分层时间轮，对照 `skynet-src/skynet_timer.c`。
//!
//! 时间轮内部每格 10ms：近期轮 256 格覆盖 2.56 秒，之后各层 64 格逐级放大。
//! 这里把层数扩到足以覆盖完整的 `u64` 刻度，并额外维护事件位置索引，使取消仍是
//! O(1)。时间轮由定时器服务独占持有，因此这些结构都不需要锁。

use std::collections::HashMap;

const TIME_NEAR_SHIFT: u32 = 8;
const TIME_NEAR: usize = 1 << TIME_NEAR_SHIFT;
const TIME_LEVEL_SHIFT: u32 = 6;
const TIME_LEVEL: usize = 1 << TIME_LEVEL_SHIFT;
const TIME_NEAR_MASK: u64 = TIME_NEAR as u64 - 1;
const TIME_LEVEL_MASK: u64 = TIME_LEVEL as u64 - 1;
/// 8 个近期位加 10 层、每层 6 位，共覆盖 68 位，足够容纳全部 `u64` 刻度。
const TIME_LEVELS: usize = 10;

/// `(handle, session)` 在节点内唯一标识一次 sleep 定时器。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TimerKey {
    pub(crate) handle: rskynet_core::Handle,
    pub(crate) session: u64,
}

/// 一个到期事件：把 `session` 号的应答发给 `handle`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TimerEvent {
    pub(crate) handle: rskynet_core::Handle,
    pub(crate) session: u64,
    pub(crate) expire: u64,
}

impl TimerEvent {
    pub(crate) fn key(self) -> TimerKey {
        TimerKey {
            handle: self.handle,
            session: self.session,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Location {
    Near {
        slot: usize,
        index: usize,
    },
    Level {
        level: usize,
        slot: usize,
        index: usize,
    },
}

/// 分层时间轮本体。
pub(crate) struct Wheel {
    /// 当前时间轮刻度，每格 10ms。
    time: u64,
    near: Vec<Vec<TimerEvent>>,
    levels: [Vec<Vec<TimerEvent>>; TIME_LEVELS],
    locations: HashMap<TimerKey, Location>,
}

impl Wheel {
    pub(crate) fn new() -> Self {
        Self {
            time: 0,
            near: (0..TIME_NEAR).map(|_| Vec::new()).collect(),
            levels: std::array::from_fn(|_| (0..TIME_LEVEL).map(|_| Vec::new()).collect()),
            locations: HashMap::new(),
        }
    }

    /// 按到期时刻把事件放进合适的格子。
    pub(crate) fn add(&mut self, mut event: TimerEvent) {
        // 入队和实际插轮之间可能跨过若干刻度；已到期事件归到当前格。
        if event.expire <= self.time {
            event.expire = self.time;
        }

        let expire = event.expire;
        let current = self.time;
        if (expire | TIME_NEAR_MASK) == (current | TIME_NEAR_MASK) {
            let slot = (expire & TIME_NEAR_MASK) as usize;
            let index = self.near[slot].len();
            self.near[slot].push(event);
            self.locations
                .insert(event.key(), Location::Near { slot, index });
            return;
        }

        let mut selected = TIME_LEVELS - 1;
        for level in 0..TIME_LEVELS {
            let upper_shift = TIME_NEAR_SHIFT as usize + (level + 1) * TIME_LEVEL_SHIFT as usize;
            if upper_shift >= u64::BITS as usize || expire >> upper_shift == current >> upper_shift
            {
                selected = level;
                break;
            }
        }
        let shift = TIME_NEAR_SHIFT as usize + selected * TIME_LEVEL_SHIFT as usize;
        let slot = ((expire >> shift) & TIME_LEVEL_MASK) as usize;
        let index = self.levels[selected][slot].len();
        self.levels[selected][slot].push(event);
        self.locations.insert(
            event.key(),
            Location::Level {
                level: selected,
                slot,
                index,
            },
        );
    }

    /// 物理摘除一个定时器。已经到期、已经取消或从未存在都静默忽略。
    pub(crate) fn cancel(&mut self, key: TimerKey) {
        let Some(location) = self.locations.remove(&key) else {
            return;
        };
        match location {
            Location::Near { slot, index } => {
                let moved = {
                    let bucket = &mut self.near[slot];
                    bucket.swap_remove(index);
                    bucket.get(index).copied()
                };
                if let Some(event) = moved {
                    self.locations
                        .insert(event.key(), Location::Near { slot, index });
                }
            }
            Location::Level { level, slot, index } => {
                let moved = {
                    let bucket = &mut self.levels[level][slot];
                    bucket.swap_remove(index);
                    bucket.get(index).copied()
                };
                if let Some(event) = moved {
                    self.locations
                        .insert(event.key(), Location::Level { level, slot, index });
                }
            }
        }
    }

    /// 把某一层某一格里的事件重新散落到更细的层级。
    fn move_list(&mut self, level: usize, slot: usize) {
        let events = std::mem::take(&mut self.levels[level][slot]);
        for event in events {
            self.locations.remove(&event.key());
            self.add(event);
        }
    }

    /// 刻度前进一格，必要时做层级迁移。
    pub(crate) fn shift(&mut self) {
        self.time = self.time.wrapping_add(1);
        let current = self.time;
        if current == 0 {
            self.move_list(TIME_LEVELS - 1, 0);
            return;
        }
        if current & TIME_NEAR_MASK != 0 {
            return;
        }

        let mut shifted = current >> TIME_NEAR_SHIFT;
        for level in 0..TIME_LEVELS {
            let slot = (shifted & TIME_LEVEL_MASK) as usize;
            if slot != 0 {
                self.move_list(level, slot);
                break;
            }
            shifted >>= TIME_LEVEL_SHIFT;
        }
    }

    /// 摘走当前刻度上所有到期事件。
    pub(crate) fn execute(&mut self, out: &mut Vec<TimerEvent>) {
        let slot = (self.time & TIME_NEAR_MASK) as usize;
        let events = std::mem::take(&mut self.near[slot]);
        for event in &events {
            self.locations.remove(&event.key());
        }
        out.extend(events);
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn advance(wheel: &mut Wheel, ticks: u64) -> Vec<TimerEvent> {
        let mut out = Vec::new();
        for _ in 0..ticks {
            wheel.execute(&mut out);
            wheel.shift();
            wheel.execute(&mut out);
        }
        out
    }

    fn arm(wheel: &mut Wheel, session: u64, ticks: u64) -> TimerKey {
        let event = TimerEvent {
            handle: 1,
            session,
            expire: wheel.time.saturating_add(ticks),
        };
        let key = event.key();
        wheel.add(event);
        key
    }

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

    #[test]
    fn zero_delay_fires_immediately() {
        let mut wheel = Wheel::new();
        arm(&mut wheel, 1, 0);
        assert_eq!(advance(&mut wheel, 1).len(), 1);
    }

    #[test]
    fn timer_migrated_from_upper_level_is_punctual() {
        let mut wheel = Wheel::new();
        arm(&mut wheel, 7, 300);
        assert!(wheel.levels[0].iter().any(|slot| !slot.is_empty()));
        assert!(advance(&mut wheel, 299).is_empty());
        let fired = advance(&mut wheel, 1);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].session, 7);
    }

    #[test]
    fn all_levels_are_punctual() {
        for ticks in [255u64, 256, 257, 1000, 16_384, 20_000, 1_048_576] {
            let mut wheel = Wheel::new();
            arm(&mut wheel, 1, ticks);
            assert!(advance(&mut wheel, ticks - 1).is_empty());
            assert_eq!(advance(&mut wheel, 1).len(), 1);
        }
    }

    #[test]
    fn timers_in_same_slot_fire_together() {
        let mut wheel = Wheel::new();
        for session in 1..=5 {
            arm(&mut wheel, session, 10);
        }
        assert!(advance(&mut wheel, 9).is_empty());
        assert_eq!(advance(&mut wheel, 1).len(), 5);
    }

    #[test]
    fn cancel_physically_removes_near_and_level_events() {
        let mut wheel = Wheel::new();
        let near = arm(&mut wheel, 1, 10);
        let level = arm(&mut wheel, 2, 300);
        wheel.cancel(near);
        wheel.cancel(level);
        assert!(wheel.locations.is_empty());
        assert!(advance(&mut wheel, 300).is_empty());
    }

    #[test]
    fn swap_remove_keeps_moved_event_location_current() {
        let mut wheel = Wheel::new();
        let first = arm(&mut wheel, 1, 10);
        let second = arm(&mut wheel, 2, 10);
        wheel.cancel(first);
        wheel.cancel(second);
        assert!(wheel.locations.is_empty());
        assert!(advance(&mut wheel, 10).is_empty());
    }

    #[test]
    fn ticks_cross_the_old_u32_boundary_without_wrapping() {
        let mut wheel = Wheel::new();
        wheel.time = u32::MAX as u64 - 2;
        arm(&mut wheel, 1, 5);
        assert!(advance(&mut wheel, 4).is_empty());
        assert_eq!(advance(&mut wheel, 1).len(), 1);
    }

    #[test]
    fn highest_u64_ticks_use_the_top_level_and_can_be_cancelled() {
        let mut wheel = Wheel::new();
        let key = arm(&mut wheel, 1, u64::MAX);
        assert!(
            wheel.levels[TIME_LEVELS - 1]
                .iter()
                .any(|slot| !slot.is_empty())
        );
        wheel.cancel(key);
        assert!(wheel.locations.is_empty());
    }
}
