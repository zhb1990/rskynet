//! handle 注册表与本地名字表，对照 `skynet-src/skynet_handle.c`。
//!
//! handle 的低 24 位是节点内序号，高 8 位是 harbor（节点）编号，
//! 这样跨节点通信时看一眼地址就知道该不该转发。槽位数组按 2 的幂倍增，
//! 用 `handle & (slot_size - 1)` 直接定位，与 C 版一致。

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::server::ServiceContext;

/// handle 里属于节点内序号的位，对照 C 版 `HANDLE_MASK`。
pub(crate) const HANDLE_MASK: u32 = 0x00ff_ffff;
/// harbor 编号所在的位移，对照 C 版 `HANDLE_REMOTE_SHIFT`。
pub(crate) const HANDLE_REMOTE_SHIFT: u32 = 24;

const DEFAULT_SLOT_SIZE: usize = 4;

struct Inner {
    /// 下一个尝试分配的序号，0 号保留给内核。
    handle_index: u32,
    slot: Vec<Option<Arc<ServiceContext>>>,
    /// 本地名字表。C 版用有序数组加二分查找，这里用 `BTreeMap`，语义相同。
    names: BTreeMap<String, u32>,
}

pub(crate) struct HandleStorage {
    /// 已经左移到高 8 位的 harbor 编号。
    harbor: u32,
    inner: RwLock<Inner>,
}

impl HandleStorage {
    pub(crate) fn new(harbor: u32) -> Self {
        Self {
            harbor: (harbor & 0xff) << HANDLE_REMOTE_SHIFT,
            inner: RwLock::new(Inner {
                handle_index: 1,
                slot: vec![None; DEFAULT_SLOT_SIZE],
                names: BTreeMap::new(),
            }),
        }
    }

    pub(crate) fn harbor(&self) -> u32 {
        self.harbor >> HANDLE_REMOTE_SHIFT
    }

    /// 分配 handle 并落表。
    ///
    /// 服务上下文构造时就需要知道自己的 handle，而 handle 又要等槽位定下来才有，
    /// 所以这里把构造过程交给调用方的闭包（C 版是先塞 0 再回填 `ctx->handle`）。
    pub(crate) fn register_with<F>(&self, make: F) -> Arc<ServiceContext>
    where
        F: FnOnce(u32) -> Arc<ServiceContext>,
    {
        let mut inner = self.inner.write();
        loop {
            let mut handle = inner.handle_index;
            for _ in 0..inner.slot.len() {
                if handle > HANDLE_MASK {
                    // 0 号保留
                    handle = 1;
                }
                let hash = (handle as usize) & (inner.slot.len() - 1);
                if inner.slot[hash].is_none() {
                    let ctx = make(handle | self.harbor);
                    inner.slot[hash] = Some(ctx.clone());
                    inner.handle_index = handle + 1;
                    return ctx;
                }
                handle += 1;
            }
            self.grow(&mut inner);
        }
    }

    /// 槽位翻倍并重新散列。
    fn grow(&self, inner: &mut Inner) {
        let new_size = inner.slot.len() * 2;
        assert!(new_size <= HANDLE_MASK as usize + 1, "服务数量超出 handle 空间");
        let mut new_slot = vec![None; new_size];
        for ctx in inner.slot.drain(..).flatten() {
            let hash = (ctx.handle as usize) & (new_size - 1);
            debug_assert!(new_slot[hash].is_none());
            new_slot[hash] = Some(ctx);
        }
        inner.slot = new_slot;
    }

    /// 把服务从表里摘除，同时清掉它注册过的名字。返回被摘除的上下文。
    pub(crate) fn retire(&self, handle: u32) -> Option<Arc<ServiceContext>> {
        let mut inner = self.inner.write();
        let hash = (handle as usize) & (inner.slot.len() - 1);
        let hit = matches!(&inner.slot[hash], Some(ctx) if ctx.handle == handle);
        if !hit {
            return None;
        }
        let ctx = inner.slot[hash].take();
        inner.names.retain(|_, owner| *owner != handle);
        ctx
    }

    pub(crate) fn grab(&self, handle: u32) -> Option<Arc<ServiceContext>> {
        let inner = self.inner.read();
        let hash = (handle as usize) & (inner.slot.len() - 1);
        match &inner.slot[hash] {
            Some(ctx) if ctx.handle == handle => Some(ctx.clone()),
            _ => None,
        }
    }

    /// 当前所有活着的 handle，退出流程逐个 kill 时用。
    pub(crate) fn handles(&self) -> Vec<u32> {
        let inner = self.inner.read();
        inner.slot.iter().flatten().map(|ctx| ctx.handle).collect()
    }

    pub(crate) fn find_name(&self, name: &str) -> Option<u32> {
        self.inner.read().names.get(name).copied()
    }

    /// 注册本地名字。与 C 版一致，名字已被占用时返回 false（不覆盖）。
    pub(crate) fn register_name(&self, handle: u32, name: &str) -> bool {
        let mut inner = self.inner.write();
        if inner.names.contains_key(name) {
            return false;
        }
        inner.names.insert(name.to_string(), handle);
        true
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::dummy_context;

    /// 分配出的 handle 高 8 位应当是 harbor 编号
    #[test]
    fn allocated_handle_carries_harbor() {
        let storage = HandleStorage::new(7);
        let ctx = storage.register_with(dummy_context);
        assert_eq!(ctx.handle >> HANDLE_REMOTE_SHIFT, 7);
        assert_eq!(ctx.handle & HANDLE_MASK, 1);
        assert_eq!(storage.harbor(), 7);
    }

    /// 槽位不够时按倍数扩容，扩容过程中一个服务都不能丢
    #[test]
    fn slots_grow_without_losing_services() {
        let storage = HandleStorage::new(0);
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
        let storage = HandleStorage::new(0);
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
}
