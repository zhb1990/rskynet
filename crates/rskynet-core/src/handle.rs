//! handle 注册表与本地名字表，对照 `skynet-src/skynet_handle.c`。
//!
//! handle 是纯本地的单调递增 `u64` 序号，不编入节点信息。跨节点寻址由
//! `rskynet-cluster` 的独立地址类型承担。槽位数组按 2 的幂倍增，
//! 用 `handle & (slot_size - 1)` 直接定位，与 C 版一致。
//!
//! # 读路径为什么不加锁
//!
//! 每发一条消息都要按 handle 查一次表（按名字发还要多查一次名字表），C 版为此
//! 上了一把读写锁，于是这把锁成了整条投递路径上最先撞上的争抢点。这里改用
//! [`ArcSwap`] 的快照语义：
//!
//! - 槽位数组本身很少变（只有扩容才换），所以整数组一个 `ArcSwap`；
//! - 每个槽单独一个 `ArcSwapOption`，注册与摘除只动一个槽，不必换整个数组；
//! - 名字表整表 COW，注册名字时克隆一份改完再换上去。
//!
//! 读者拿到的是某一瞬的快照，写者永不阻塞读者。写者之间仍然互斥（`alloc` 那把锁），
//! 但注册、摘除、改名都是冷路径。

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use parking_lot::Mutex;

use crate::server::ServiceContext;

const DEFAULT_SLOT_SIZE: usize = 4;

/// 槽位数组。扩容时整条换掉，平时只动其中某个槽。
type Slots = Box<[ArcSwapOption<ServiceContext>]>;

pub(crate) struct HandleStorage {
    slots: ArcSwap<Slots>,
    /// 本地名字表。C 版用有序数组加二分查找，这里用 `BTreeMap`，语义相同。
    names: ArcSwap<BTreeMap<String, crate::Handle>>,
    /// 下一个尝试分配的序号（0 号保留给内核），同时充当写者之间的互斥锁。
    ///
    /// 摘除与改名也要拿它：否则「扩容时整数组搬家」与「摘掉某个槽」并发时，
    /// 被摘掉的那个会随着搬家又回到新数组里。
    alloc: Mutex<crate::Handle>,
}

impl HandleStorage {
    pub(crate) fn new() -> Self {
        Self {
            slots: ArcSwap::from_pointee(empty_slots(DEFAULT_SLOT_SIZE)),
            names: ArcSwap::from_pointee(BTreeMap::new()),
            alloc: Mutex::new(1),
        }
    }

    /// 测试钩子：把下一次分配的起点设成指定 handle。
    #[cfg(test)]
    pub(crate) fn set_next_for_test(&self, handle: crate::Handle) {
        *self.alloc.lock() = handle;
    }

    /// 分配 handle 并落表。
    ///
    /// 服务上下文构造时就需要知道自己的 handle，而 handle 又要等槽位定下来才有，
    /// 所以这里把构造过程交给调用方的闭包（C 版是先塞 0 再回填 `ctx->handle`）。
    ///
    /// handle 是节点内单调递增的 u64，永不复用；销毁只释放物理 slot。因此旧地址、
    /// 迟到的回包和 stale `ReplyToken` 不可能命中后来复用同一 slot 的新服务。
    pub(crate) fn register_with<F>(&self, make: F) -> Arc<ServiceContext>
    where
        F: FnOnce(crate::Handle) -> Arc<ServiceContext>,
    {
        let mut next = self.alloc.lock();
        let mut make = Some(make);
        loop {
            let slots = self.slots.load();
            let mut handle = *next;
            for _ in 0..slots.len() {
                let hash = (handle as usize) & (slots.len() - 1);
                if slots[hash].load().is_none() {
                    // 先把后继值算出来：handle 空间耗尽是进程级故障，
                    // 不能把上下文插进表里之后才发现没有 next 可用。
                    let next_handle = handle
                        .checked_add(1)
                        .expect("service handle 空间耗尽（u64 单调序列走到头）");
                    let ctx = make.take().expect("闭包只会被调用一次")(handle);
                    slots[hash].store(Some(ctx.clone()));
                    *next = next_handle;
                    return ctx;
                }
                handle = handle
                    .checked_add(1)
                    .expect("service handle 空间耗尽（u64 单调序列走到头）");
            }
            drop(slots);
            self.grow();
        }
    }

    /// 槽位翻倍并重新散列。调用方必须持有 `alloc`。
    fn grow(&self) {
        let old = self.slots.load();
        let new_size = old.len() * 2;
        // 上限约束的是**槽位数组**的长度，不是 handle 身份空间：handle 本身是
        // u64 单调序列，永不复用；这里只是防止数组长度失去控制（每个槽还挂着
        // 一个 ArcSwapOption）。
        assert!(
            new_size <= u32::MAX as usize,
            "服务数量超出槽位数组容量上限"
        );
        let new_slots = empty_slots(new_size);
        for ctx in old.iter().filter_map(|slot| slot.load_full()) {
            let hash = (ctx.handle as usize) & (new_size - 1);
            debug_assert!(new_slots[hash].load().is_none());
            new_slots[hash].store(Some(ctx));
        }
        // 换数组的这一瞬，还捏着旧数组快照的读者看到的仍是搬家前的样子；
        // 里面的 Arc 与新数组指向同一批服务，读到旧的也没错
        self.slots.store(Arc::new(new_slots));
    }

    /// 把服务从表里摘除，同时清掉它注册过的名字。返回被摘除的上下文。
    pub(crate) fn retire(&self, handle: crate::Handle) -> Option<Arc<ServiceContext>> {
        let _writer = self.alloc.lock();
        let slots = self.slots.load();
        let hash = (handle as usize) & (slots.len() - 1);
        let ctx = slots[hash].load_full().filter(|ctx| ctx.handle == handle)?;
        slots[hash].store(None);

        let names = self.names.load();
        if names.values().any(|owner| *owner == handle) {
            let mut next = BTreeMap::clone(&names);
            next.retain(|_, owner| *owner != handle);
            self.names.store(Arc::new(next));
        }
        Some(ctx)
    }

    pub(crate) fn grab(&self, handle: crate::Handle) -> Option<Arc<ServiceContext>> {
        let slots = self.slots.load();
        let hash = (handle as usize) & (slots.len() - 1);
        slots[hash].load_full().filter(|ctx| ctx.handle == handle)
    }

    /// 当前所有活着的 handle，退出流程逐个 kill 时用。
    pub(crate) fn handles(&self) -> Vec<crate::Handle> {
        self.contexts().iter().map(|ctx| ctx.handle).collect()
    }

    /// 当前所有活着的服务上下文，节点收工时逐个敲一遍用。
    pub(crate) fn contexts(&self) -> Vec<Arc<ServiceContext>> {
        self.slots
            .load()
            .iter()
            .filter_map(|slot| slot.load_full())
            .collect()
    }

    /// 给监控接口取一次拥有所有权的快照。名字按 handle 聚合，避免调用方为每个
    /// service 重复扫描整张名字表。
    pub(crate) fn snapshot(
        &self,
    ) -> (
        Vec<Arc<ServiceContext>>,
        BTreeMap<crate::Handle, Vec<String>>,
    ) {
        let contexts = self.contexts();
        let names = self.names.load();
        let mut grouped = BTreeMap::<crate::Handle, Vec<String>>::new();
        for (name, handle) in names.iter() {
            grouped.entry(*handle).or_default().push(name.clone());
        }
        (contexts, grouped)
    }

    pub(crate) fn find_name(&self, name: &str) -> Option<crate::Handle> {
        self.names.load().get(name).copied()
    }

    /// 注册本地名字。与 C 版一致，名字已被占用时返回 false（不覆盖）。
    ///
    /// 只允许给仍活着的 handle 起名：`alloc` 锁同时序列化了 `retire`，因此这里
    /// 检查槽位后，直到名字表更新完成都不会有并发摘除。
    pub(crate) fn register_name(&self, handle: crate::Handle, name: &str) -> bool {
        let _writer = self.alloc.lock();
        let slots = self.slots.load();
        let hash = (handle as usize) & (slots.len() - 1);
        if !slots[hash]
            .load_full()
            .is_some_and(|ctx| ctx.handle == handle)
        {
            return false;
        }
        let names = self.names.load();
        if names.contains_key(name) {
            return false;
        }
        let mut next = BTreeMap::clone(&names);
        next.insert(name.to_string(), handle);
        self.names.store(Arc::new(next));
        true
    }
}

fn empty_slots(size: usize) -> Slots {
    (0..size).map(|_| ArcSwapOption::empty()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::dummy_context;

    /// handle 从 1 开始，身份空间是单调 u64。
    #[test]
    fn allocated_handle_is_monotonic_u64() {
        let storage = HandleStorage::new();
        let ctx = storage.register_with(dummy_context);
        assert_eq!(ctx.handle, 1);
    }

    /// u32 边界不是回绕点：0xffff_ffff 之后继续分配 0x1_0000_0000。
    #[test]
    fn handles_cross_the_u32_boundary_without_wrapping() {
        let storage = HandleStorage::new();
        *storage.alloc.lock() = u32::MAX as u64;
        assert_eq!(storage.register_with(dummy_context).handle, u32::MAX as u64);
        assert_eq!(
            storage.register_with(dummy_context).handle,
            0x1_0000_0000,
            "u64 handle 必须跨过 u32 边界继续前进"
        );
    }

    /// 销毁只回收物理 slot：新服务复用 slot，但必须拿到新的 handle。
    #[test]
    fn a_reused_slot_gets_a_fresh_handle() {
        let storage = HandleStorage::new();
        let old = storage.register_with(dummy_context).handle;
        // 占满剩余 3 个 slot，分配器推进到 5。
        for _ in 0..DEFAULT_SLOT_SIZE - 1 {
            let _ = storage.register_with(dummy_context).handle;
        }
        assert!(storage.retire(old).is_some());

        // 1 号与 5 号都散列到下标 1：物理 slot 复用，身份不复用。
        let reused = storage.register_with(dummy_context);
        assert_eq!(reused.handle, 5, "handle 必须接着单调前进");
        assert_eq!(old & (DEFAULT_SLOT_SIZE as u64 - 1), 1);
        assert_eq!(reused.handle & (DEFAULT_SLOT_SIZE as u64 - 1), 1);
        assert!(storage.grab(old).is_none(), "旧 handle 不得命中新服务");
        assert!(storage.grab(reused.handle).is_some());
    }

    /// 高位 handle 仍能正常落表/寻址，不能在某处被截成 u32。
    #[test]
    fn high_u64_handle_routes_through_the_table() {
        const HIGH: crate::Handle = 0x1_0000_0001;
        let storage = HandleStorage::new();
        *storage.alloc.lock() = HIGH;
        let ctx = storage.register_with(dummy_context);
        assert_eq!(ctx.handle, HIGH);
        assert!(storage.grab(HIGH).is_some());
        assert!(storage.retire(HIGH).is_some());
        assert!(storage.grab(HIGH).is_none());
    }

    /// 槽位不够时按倍数扩容，扩容过程中一个服务都不能丢
    #[test]
    fn slots_grow_without_losing_services() {
        let storage = HandleStorage::new();
        let mut handles = Vec::new();
        for _ in 0..DEFAULT_SLOT_SIZE * 4 + 1 {
            handles.push(storage.register_with(dummy_context).handle);
        }
        assert_eq!(storage.handles().len(), handles.len());
        for handle in &handles {
            assert!(storage.grab(*handle).is_some(), "{handle:#x} 应仍在表中");
        }
    }

    /// 摘除服务时要连带把它注册过的名字一起清掉
    #[test]
    fn retire_also_clears_names() {
        let storage = HandleStorage::new();
        let handle = storage.register_with(dummy_context).handle;
        assert!(storage.register_name(handle, "logger"));
        // 名字不可重复注册
        assert!(!storage.register_name(handle, "logger"));
        assert_eq!(storage.find_name("logger"), Some(handle));

        assert!(storage.retire(handle).is_some());
        assert!(storage.retire(handle).is_none());
        assert!(storage.grab(handle).is_none());
        assert_eq!(storage.find_name("logger"), None);
    }

    /// 已摘除的 handle 不能再注册名字，避免产生永远指向死服务的 ghost name
    #[test]
    fn dead_handle_cannot_register_names() {
        let storage = HandleStorage::new();
        let handle = storage.register_with(dummy_context).handle;
        assert!(storage.retire(handle).is_some());
        assert!(!storage.register_name(handle, "ghost"));
        assert_eq!(storage.find_name("ghost"), None);
    }
}
