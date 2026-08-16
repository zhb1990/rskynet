//! BWoS 块式工作窃取队列，移植自 stdexec 的 `exec/detail/bwos_lifo_queue.hpp`，
//! 原始论文是 *BWoS: Formally Verified Block-based Work Stealing for Parallel
//! Processing*（Wang et al. 2023）。
//!
//! # 它凭什么比 Chase-Lev 快
//!
//! 传统窃取队列的 owner 每取一个元素都要和窃贼在同一个原子变量上较劲。BWoS 把
//! 环形缓冲切成若干个**块**，owner 和窃贼在任意时刻工作在**不同的块**上：
//! owner 只碰当前块的 `tail`，窃贼只碰更早那些已经「交出去」的块的 `steal_tail`，
//! 于是常态下两者一次 CAS 都不用打照面。只有跨块的时刻（`grant` / `takeover` /
//! `reclaim`）才需要同步，摊薄到每 `BLOCK_SIZE` 个元素一次。
//!
//! # 计数器编码
//!
//! 全局计数器（`last_block` / `start_block`）直接存**逻辑 block sequence**：
//! 从 0 开始单调递增的 u64，永不回绕。物理块下标是 `sequence & mask`，
//! 块 generation 是 `sequence >> block_shift`；因此块被复用时 generation 必然变化，
//! 旧 thief 拿着的旧 sequence 不可能再读到新世代数据。generation 最终编码进
//! 块内状态的高 56 bit，所以它不能越过 `2^56 - 1`——每前进一步都由
//! [`LifoQueue::next_sequence`] 检查这条上限，越界直接 fail-fast，禁止回绕。
//!
//! 块内状态（`head` / `steal_tail`）仍是单个 64 位原子量：
//!
//! - 高 56 位：块 generation
//! - 低 8 位：块内槽位下标（`BLOCK_SIZE` 必须小于 256）
//!
//! generation 的作用是防 ABA；`Block::steal` 在做任何 `ptr::read` 之前先比对
//! generation，不同则直接停下。
//!
//! # 线程安全
//!
//! - owner 线程：独占调用 [`Owner::push_back`] / [`Owner::pop_back`]
//! - 窃贼线程：任意多个线程并发调用 [`Stealer::steal_front`]
//!
//! owner 侧方法标成 `unsafe`，契约是「同一时刻只有一个线程在调用 owner 侧方法」。
//! 之所以不用 `&mut self` 表达这条契约，是因为调度器要把 `Owner` 放进
//! `Arc<Node>` 里跨线程共享（真正的互斥由 worker 编号保证），
//! 和 [`crate::task::SvcCell`] 是同一种取舍。

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize};

/// 块内槽位占用的低 bit 数。当前最大块长 16，留 8 bit 已经足够，
/// 而且让「generation << SLOT_BITS | slot」继续装进一个 AtomicU64。
const SLOT_BITS: u32 = 8;
/// 块内槽位掩码。
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// 第 0 代的「上一代」哨兵：未使用块的初始 generation 取最大值，
/// 让第 0 代的 `is_writable` 一开始就成立。generation 只占高 56 bit。
const PREV_GEN_0: u64 = (1 << (64 - SLOT_BITS)) - 1;

/// 块内状态里 generation 可表达的最大值（高 56 bit 全 1）。
///
/// 逻辑 block sequence 的 generation 是 `sequence >> block_shift`，它最终要编码进
/// 块内状态的高 56 bit（低 8 bit 留给槽位），所以不能越过这个上限。越过即意味着
/// 编码回绕：旧世代的位置模式会被新世代原样复用，`is_writable` 的「停在上一代」
/// 判断也随之失效——这是块协议不可恢复的破坏，必须在 [`LifoQueue::next_sequence`]
/// 里 fail-fast，而不是等回绕真正发生。
const MAX_GENERATION: u64 = (1 << (64 - SLOT_BITS)) - 1;

/// 测试钩子：置位后，`Block::steal` 里所有目标块（地址匹配
/// [`STEAL_PAUSE_TARGET`]）的 thief 都会在读走 `steal_tail` 之后、CAS 之前停下。
#[cfg(test)]
static STEAL_PAUSE: AtomicBool = AtomicBool::new(false);
/// 测试钩子：受 [`STEAL_PAUSE`] 影响的块地址，0 表示不暂停任何块。
#[cfg(test)]
static STEAL_PAUSE_TARGET: AtomicUsize = AtomicUsize::new(0);
/// 测试钩子：某个 thief 已经停在 [`STEAL_PAUSE`] 的窗口里。
#[cfg(test)]
static PAUSED_THIEF: AtomicBool = AtomicBool::new(false);

/// 每条本地队列的块数，必须是 2 的幂且不小于 2。
pub(crate) const NUM_BLOCKS: usize = 64;

/// 每块的槽位数。
///
/// 块要小：只有 owner **走过**的块才可被窃取，当前正在写的那块对窃贼是隐形的。
/// 块开得越大，队列里能被偷走的比例越低。64 × 16 合计 1024 个槽位，
/// 既够装下一次扇出风暴，又能让攒够 16 个服务就立刻暴露给空闲 worker。
pub(crate) const BLOCK_SIZE: usize = 16;

/// 独占一条 cache line，避免 owner 与窃贼互相打脏对方的缓存。
#[repr(align(64))]
pub(crate) struct CachePad<T>(pub(crate) T);

impl<T> Deref for CachePad<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> DerefMut for CachePad<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

/// 一次压入的结果：调用方靠它知道「有没有新的整块可以被偷了」。
///
/// 调度器用这个信号维护一张「哪条本地队列有货可偷」的位图，于是窃贼挑受害者时
/// 只需读一个字，不必挨个去碰别人的队列（那可是 N 条跨核缓存行）。
/// 因为跨块每 `block_size` 个元素才发生一次，这个信号本身几乎不要钱。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pushed {
    /// 值落在 owner 正在写的那一块里，对窃贼还是隐形的。
    Local,
    /// 跨块了：前一块已整块交给窃贼，现在偷得动。
    Granted,
}

/// 一次窃取的结果。
#[derive(Debug)]
enum Steal<T> {
    Taken(T),
    /// 本块暂时没有可偷的（owner 还在往里写），不必再往后找。
    Empty,
    /// 和别的窃贼撞上了，重试即可。
    Conflict,
    /// 本块已被偷空，可以看下一块。
    Done,
}

/// 环形缓冲的一块。
struct Block<T> {
    /// owner 侧边界：高 56 bit generation，低 8 bit slot。`takeover` / `grant`
    /// 时与 `steal_tail` 互换。
    head: CachePad<AtomicU64>,
    /// owner 的写入位置，纯索引，只有 owner 会改。
    tail: CachePad<AtomicU64>,
    /// 已完成的窃取次数，`reclaim` 靠它确认窃贼都收工了。
    steal_count: CachePad<AtomicU64>,
    /// 窃贼侧边界：高 56 bit generation，低 8 bit slot。低位等于块长即表示本块已偷空。
    steal_tail: CachePad<AtomicU64>,
    ring: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> Block<T> {
    fn new(block_size: usize) -> Self {
        let mut ring = Vec::with_capacity(block_size);
        ring.resize_with(block_size, || UnsafeCell::new(MaybeUninit::uninit()));
        let size = block_size as u64;
        let initial = (PREV_GEN_0 << SLOT_BITS) | size;
        Self {
            head: CachePad(AtomicU64::new(initial)),
            tail: CachePad(AtomicU64::new(size)),
            steal_count: CachePad(AtomicU64::new(size)),
            steal_tail: CachePad(AtomicU64::new(initial)),
            ring: ring.into_boxed_slice(),
        }
    }

    fn block_size(&self) -> u64 {
        self.ring.len() as u64
    }

    fn slot(&self, idx: u64) -> *mut T {
        self.ring[idx as usize].get().cast::<T>()
    }

    /// 写入一个元素。返回 `Err` 表示本块写满了，值原样交还给调用方。
    ///
    /// # Safety
    ///
    /// 只能由 owner 线程调用。
    unsafe fn put(&self, value: T) -> Result<(), T> {
        let back = self.tail.load(Ordering::Relaxed);
        if back < self.block_size() {
            unsafe { ptr::write(self.slot(back), value) };
            // Release：与窃贼 steal() 里对 tail 的 acquire 配对，保证它读到的槽位是写完的
            self.tail.store(back + 1, Ordering::Release);
            Ok(())
        } else {
            Err(value)
        }
    }

    /// 从本块尾部取一个元素（LIFO）。
    ///
    /// # Safety
    ///
    /// 只能由 owner 线程调用。
    unsafe fn get(&self) -> Option<T> {
        let back = self.tail.load(Ordering::Relaxed);
        if back == 0 {
            return None;
        }
        let front_idx = self.head.load(Ordering::Relaxed) & SLOT_MASK;
        if front_idx == back {
            // owner 的可取区间 [head, tail) 空了
            return None;
        }
        let value = unsafe { ptr::read(self.slot(back - 1)) };
        self.tail.store(back - 1, Ordering::Release);
        Some(value)
    }

    /// 从本块头部偷一个元素（FIFO），任意线程可调用。
    fn steal(&self, thief_generation: u64) -> Steal<T> {
        let spos = self.steal_tail.load(Ordering::Relaxed);
        let sidx = spos & SLOT_MASK;
        let generation = spos >> SLOT_BITS;
        // generation 校验必须先于任何 ptr::read：旧 thief 可能落后到物理块已经被
        // 复用给新世代，此时必须直接停下，绝不能把新世代数据当成自己的猎物。
        if generation != thief_generation {
            return Steal::Empty;
        }
        if sidx == self.block_size() {
            // 本块已经偷空，可以往后找。
            return Steal::Done;
        }
        // Acquire：与 owner put() 的 release 配对
        let back = self.tail.load(Ordering::Acquire);
        if sidx == back {
            return Steal::Empty;
        }
        #[cfg(test)]
        {
            // 测试钩子：让 thief 停在「已经确认本块可偷、还没 CAS」的窗口里，
            // 便于把「owner 在 CAS 前把整块换了个世代」的竞态放大成确定性用例。
            if STEAL_PAUSE.load(Ordering::Acquire)
                && self as *const Self as usize == STEAL_PAUSE_TARGET.load(Ordering::Relaxed)
            {
                PAUSED_THIEF.store(true, Ordering::Release);
                while STEAL_PAUSE.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
            }
        }
        // 成功的那一下必须是 Acquire：CAS 成功是 thief 对「这块归我」的最终确认，
        // 与 grant() 里发布新边界的 Release store 配对。owner 在 grant 之前写好的
        // 槽位（put 的写入、尾部的 Release 推进）都先于那道 Release，而两次 grant
        // 之间的 takeover / reclaim / reduce_generation 与其它 thief 的 CAS 都是
        // 同一条 release sequence 上的成员，因此成功 CAS 之后对槽位的读取一定能
        // 看到对应世代的数据。失败时没有读取，Relaxed 即可。
        if self
            .steal_tail
            .compare_exchange(spos, spos + 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Steal::Conflict;
        }
        let value = unsafe { ptr::read(self.slot(sidx)) };
        // Release：保证 reclaim() 看到这次计数时，我们对槽位的读取已经完成
        self.steal_count.fetch_add(1, Ordering::Release);
        Steal::Taken(value)
    }

    /// owner 退回本块时调用：把窃贼的边界收回来当作自己的取值边界。
    fn takeover(&self) {
        let head = self.head.load(Ordering::Relaxed);
        let spos = self.steal_tail.swap(head, Ordering::Relaxed);
        self.head.store(spos, Ordering::Relaxed);
    }

    /// owner 离开本块时调用：把整块交给窃贼。
    fn grant(&self) {
        let block_end = self.steal_tail.load(Ordering::Relaxed);
        let old_head = self.head.swap(block_end, Ordering::Relaxed);
        // Release：保证窃贼看到新边界时，本块里的元素都已经写完
        self.steal_tail.store(old_head, Ordering::Release);
    }

    /// 本块能否安全地被第 `generation` 代复用：必须已被偷空，且停在上一代。
    ///
    /// generation 0 需要和「上一代」哨兵比较，因此这里保留 `wrapping_sub`；
    /// 它只是块协议里的 sentinel 计算，不是全局 sequence 回绕。
    fn is_writable(&self, generation: u64) -> bool {
        let writable = (generation.wrapping_sub(1) << SLOT_BITS) | self.block_size();
        self.steal_tail.load(Ordering::Relaxed) == writable
    }

    /// 复位本块以供第 `generation` 代使用，会自旋等待上一代的窃贼全部收工。
    fn reclaim(&self, generation: u64) {
        let expected = self.head.load(Ordering::Relaxed) & SLOT_MASK;
        while self.steal_count.load(Ordering::Acquire) != expected {
            std::hint::spin_loop();
        }
        let expanded = generation << SLOT_BITS;
        self.head.store(expanded, Ordering::Relaxed);
        self.tail.store(0, Ordering::Relaxed);
        self.steal_tail
            .store(expanded | self.block_size(), Ordering::Relaxed);
        self.steal_count.store(0, Ordering::Relaxed);
    }

    /// owner 往回走时把窃贼边界的 generation 减一，好让后续的 `is_writable`
    /// 判断依旧成立。减法同样属于块协议 sentinel 运算，不是全局回绕。
    fn reduce_generation(&self) {
        let steal_tail = self.steal_tail.load(Ordering::Relaxed);
        self.steal_tail
            .store(steal_tail.wrapping_sub(1 << SLOT_BITS), Ordering::Relaxed);
    }
}

impl<T> Drop for Block<T> {
    fn drop(&mut self) {
        if !std::mem::needs_drop::<T>() {
            return;
        }
        // 此刻已无并发，可以直接读原子量。活元素区间统一是
        // [min(head_idx, steal_idx), tail_idx)，对四种块状态都成立：
        // 活跃块 head_idx = 0；已 grant 的块 head_idx = 块长而 steal_idx 是窃取位置；
        // 被偷空的块与从未使用的块两个下标都等于块长，区间自然为空。
        let head_idx = *self.head.get_mut() & SLOT_MASK;
        let steal_idx = *self.steal_tail.get_mut() & SLOT_MASK;
        let tail_idx = *self.tail.get_mut() & SLOT_MASK;
        for idx in head_idx.min(steal_idx)..tail_idx {
            unsafe { ptr::drop_in_place(self.slot(idx)) };
        }
    }
}

/// 队列本体，由一个 [`Owner`] 和任意多个 [`Stealer`] 共享。
///
/// `last_block` / `start_block` 直接存**逻辑 block sequence**：从 0 开始单调递增，
/// 永不回绕。物理块下标是 `sequence & mask`，块 generation 是
/// `sequence >> block_shift`。sequence 只增不减，因此旧 thief 永远拿不到新
/// generation 对应的身份。
struct LifoQueue<T> {
    /// owner 当前所在块的逻辑 sequence，只有 owner 会改。
    last_block: CachePad<AtomicU64>,
    /// 最老的、还可能有货可偷的块的逻辑 sequence，只有 owner 会改。
    start_block: CachePad<AtomicU64>,
    blocks: Box<[Block<T>]>,
    /// `blocks.len() - 1`，用来从 sequence 里抠出物理块下标。
    mask: u64,
    /// `blocks.len()` 是 2 的幂，sequence >> shift 就是块 generation。
    block_shift: u32,
}

// 安全性：见模块头的线程安全说明。槽位的并发访问由块协议保证不重叠，
// owner 侧方法的互斥由调用方（调度器按 worker 编号）保证。
unsafe impl<T: Send> Send for LifoQueue<T> {}
unsafe impl<T: Send> Sync for LifoQueue<T> {}

impl<T> LifoQueue<T> {
    fn new(num_blocks: usize, block_size: usize) -> Self {
        assert!(
            num_blocks >= 2 && num_blocks.is_power_of_two(),
            "块数必须是不小于 2 的 2 的幂"
        );
        assert!(block_size > 0, "块长必须大于 0");
        assert!(
            block_size < (1 << SLOT_BITS) as usize,
            "块长必须小于 2^SLOT_BITS，装不进块内 generation 编码"
        );
        let mut blocks = Vec::with_capacity(num_blocks);
        blocks.resize_with(num_blocks, || Block::new(block_size));
        blocks[0].reclaim(0);
        Self {
            last_block: CachePad(AtomicU64::new(0)),
            start_block: CachePad(AtomicU64::new(0)),
            blocks: blocks.into_boxed_slice(),
            mask: (num_blocks - 1) as u64,
            block_shift: num_blocks.trailing_zeros(),
        }
    }

    /// 逻辑 sequence 只增不减。u64 耗尽属于进程级故障，直接 fail-fast。
    ///
    /// 先查 u64 溢出，再查 generation 编码上限（见 [`MAX_GENERATION`]）：
    /// 两者都不可恢复，一律 panic。禁止任何形式的回绕——`wrapping_add` 或
    /// `& MASK` 都会让旧世代位模式复活，ABA 防线随之失效。
    fn next_sequence(&self, sequence: u64) -> u64 {
        let next = sequence
            .checked_add(1)
            .expect("BWoS block sequence 耗尽（u64 单调序列走到头）");
        let generation = next >> self.block_shift;
        assert!(
            generation <= MAX_GENERATION,
            "BWoS generation 溢出：块内 64 位编码装不下第 {generation} 代"
        );
        next
    }

    /// 逻辑 sequence 只增不减；owner 往回走不会越过 0。
    fn previous_sequence(&self, sequence: u64) -> u64 {
        sequence
            .checked_sub(1)
            .expect("BWoS block sequence 不能越过 0 回退")
    }

    /// # Safety
    ///
    /// 只能由 owner 线程调用。
    unsafe fn push_back(&self, value: T) -> Result<Pushed, T> {
        let mut owner = self.last_block.load(Ordering::Relaxed);
        let mut value = value;
        let mut granted = false;
        loop {
            let owner_index = (owner & self.mask) as usize;
            match unsafe { self.blocks[owner_index].put(value) } {
                Ok(()) => {
                    return Ok(if granted {
                        Pushed::Granted
                    } else {
                        Pushed::Local
                    });
                }
                Err(back) => value = back,
            }
            if !self.advance_put_index(&mut owner) {
                return Err(value);
            }
            // 迈进下一块的同时把前一块交给了窃贼，见 advance_put_index 里的 grant
            granted = true;
        }
    }

    /// # Safety
    ///
    /// 只能由 owner 线程调用。
    unsafe fn pop_back(&self) -> Option<T> {
        let mut owner = self.last_block.load(Ordering::Relaxed);
        loop {
            let owner_index = (owner & self.mask) as usize;
            if let Some(value) = unsafe { self.blocks[owner_index].get() } {
                return Some(value);
            }
            if !self.advance_get_index(&mut owner, owner_index) {
                return None;
            }
        }
    }

    /// 窃取一个元素。允许「伪失败」：返回 `None` 不代表队列真的空了，
    /// owner 当前正在写的那一块对窃贼始终不可见。
    fn steal_front(&self) -> Option<T> {
        let mut thief = self.start_block.load(Ordering::Relaxed);
        loop {
            let thief_generation = thief >> self.block_shift;
            let block = &self.blocks[(thief & self.mask) as usize];
            loop {
                match block.steal(thief_generation) {
                    Steal::Taken(value) => return Some(value),
                    Steal::Empty => return None,
                    Steal::Conflict => continue,
                    Steal::Done => break,
                }
            }
            if !self.advance_steal_index(&mut thief) {
                return None;
            }
        }
    }

    /// owner 往回退一块。返回 false 表示已经退到 `start_block`，队列空了。
    fn advance_get_index(&self, owner: &mut u64, owner_index: usize) -> bool {
        if self.start_block.load(Ordering::Relaxed) == *owner {
            return false;
        }
        let predecessor = self.previous_sequence(*owner);
        self.blocks[owner_index].reduce_generation();
        self.blocks[(predecessor & self.mask) as usize].takeover();
        self.last_block.store(predecessor, Ordering::Relaxed);
        *owner = predecessor;
        true
    }

    /// owner 往前进一块。返回 false 表示下一块还不能复用，队列满了。
    fn advance_put_index(&self, owner: &mut u64) -> bool {
        let owner_index = *owner & self.mask;
        let next_sequence = self.next_sequence(*owner);
        let next_index = next_sequence & self.mask;
        if next_index == owner_index {
            return false;
        }
        let next_generation = next_sequence >> self.block_shift;
        let next_block = &self.blocks[next_index as usize];
        if !next_block.is_writable(next_generation) {
            // 上一代的窃贼还没走完，不能覆盖
            return false;
        }
        let first = self.start_block.load(Ordering::Relaxed);
        if next_index == (first & self.mask) {
            // 马上要复用 start_block 那一块了，把窃取起点往前推
            self.start_block
                .store(self.next_sequence(first), Ordering::Relaxed);
        }
        self.blocks[owner_index as usize].grant();
        *owner = next_sequence;
        next_block.reclaim(next_generation);
        self.last_block.store(*owner, Ordering::Relaxed);
        true
    }

    fn advance_steal_index(&self, thief: &mut u64) -> bool {
        *thief = self.next_sequence(*thief);
        *thief < self.last_block.load(Ordering::Relaxed)
    }
}

/// 队列的 owner 句柄：可以 push、pop，也能派生窃取句柄。
pub(crate) struct Owner<T> {
    queue: Arc<LifoQueue<T>>,
}

/// 窃取句柄：可以随意克隆并分发给别的线程，只能从队列头部取。
pub(crate) struct Stealer<T> {
    queue: Arc<LifoQueue<T>>,
}

impl<T> Clone for Stealer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

/// 造一条队列，返回配套的 owner 句柄与窃取句柄。
pub(crate) fn queue<T>(num_blocks: usize, block_size: usize) -> (Owner<T>, Stealer<T>) {
    let queue = Arc::new(LifoQueue::new(num_blocks, block_size));
    (
        Owner {
            queue: queue.clone(),
        },
        Stealer { queue },
    )
}

impl<T> Owner<T> {
    /// 压入一个元素。返回 `Err` 表示队列满了，值原样交还，调用方自行找地方安置；
    /// 返回 [`Pushed::Granted`] 表示这一下让出了一整块，窃贼从此偷得动。
    ///
    /// # Safety
    ///
    /// 同一时刻只能有一个线程调用本队列的 owner 侧方法。
    pub(crate) unsafe fn push_back(&self, value: T) -> Result<Pushed, T> {
        unsafe { self.queue.push_back(value) }
    }

    /// 取出最近压入的元素（LIFO）。返回 `None` 时队列确实空了。
    ///
    /// # Safety
    ///
    /// 同一时刻只能有一个线程调用本队列的 owner 侧方法。
    pub(crate) unsafe fn pop_back(&self) -> Option<T> {
        unsafe { self.queue.pop_back() }
    }
}

impl<T> Stealer<T> {
    /// 偷一个最早压入的元素（FIFO）。返回 `None` 只表示「这次没偷到」。
    pub(crate) fn steal_front(&self) -> Option<T> {
        self.queue.steal_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::thread;

    /// 三个测试钩子（[`STEAL_PAUSE`] / [`STEAL_PAUSE_TARGET`] / [`PAUSED_THIEF`]）
    /// 是进程级全局量，用它们的用例必须串行执行，否则两个用例会互相踩坏对方的
    /// 暂停窗口。`unwrap_or_else` 容忍前一个用例 panic 留下的中毒锁。
    static STEAL_PAUSE_LOCK: Mutex<()> = Mutex::new(());

    /// owner 侧是后进先出，这是 BWoS lifo 变体的定义
    #[test]
    fn owner_side_is_lifo() {
        let (owner, _stealer) = queue::<u32>(4, 4);
        for i in 0..3 {
            unsafe { owner.push_back(i) }.expect("不该满");
        }
        let popped: Vec<u32> = std::iter::from_fn(|| unsafe { owner.pop_back() }).collect();
        assert_eq!(popped, vec![2, 1, 0]);
        assert!(unsafe { owner.pop_back() }.is_none());
    }

    /// 元素跨块之后 push/pop 依然首尾相接，不漏不乱
    #[test]
    fn push_pop_spans_blocks() {
        let (owner, _stealer) = queue::<u32>(4, 4);
        // 12 个元素要横跨 3 块，中间会经历两次 grant + reclaim
        let total = 12;
        for i in 0..total {
            unsafe { owner.push_back(i) }.expect("不该满");
        }
        let popped: Vec<u32> = std::iter::from_fn(|| unsafe { owner.pop_back() }).collect();
        let expected: Vec<u32> = (0..total).rev().collect();
        assert_eq!(popped, expected);
    }

    /// 队列满了要把值原样还回来，好让调用方转投别处而不是把活弄丢
    #[test]
    fn full_queue_hands_the_value_back() {
        let (owner, _stealer) = queue::<u32>(4, 4);
        let mut pushed = 0u32;
        let rejected = loop {
            match unsafe { owner.push_back(pushed) } {
                Ok(_) => pushed += 1,
                Err(value) => break value,
            }
            assert!(pushed < 64, "容量应当有限");
        };
        assert_eq!(rejected, pushed, "被拒的值必须原样交还");
        // 弹掉一个之后仍然进不去：owner 还在原地那一块，得等它退回去腾出空间
        assert!(unsafe { owner.pop_back() }.is_some());
    }

    /// 跨块时要报出「有整块可偷了」，调度器靠这个信号维护可偷位图
    #[test]
    fn crossing_a_block_reports_the_grant() {
        let (owner, _stealer) = queue::<u32>(4, 4);
        for i in 0..4 {
            let pushed = unsafe { owner.push_back(i) }.expect("不该满");
            assert_eq!(pushed, Pushed::Local, "同一块里的压入对窃贼还是隐形的");
        }
        // 第 5 个装不进当前块，owner 迈进下一块，前一块就交出去了
        let pushed = unsafe { owner.push_back(4) }.expect("不该满");
        assert_eq!(pushed, Pushed::Granted);
    }

    /// 窃贼从最早的元素开始拿，而且只能拿 owner 已经走过的块
    #[test]
    fn stealer_takes_the_oldest_first() {
        let (owner, stealer) = queue::<u32>(4, 4);
        // 只填半块时对窃贼是隐形的
        for i in 0..2 {
            unsafe { owner.push_back(i) }.expect("不该满");
        }
        assert!(stealer.steal_front().is_none(), "当前块不该可偷");

        // 填够一整块，owner 迈进下一块，前一块就交出去了
        for i in 2..6 {
            unsafe { owner.push_back(i) }.expect("不该满");
        }
        assert_eq!(stealer.steal_front(), Some(0));
        assert_eq!(stealer.steal_front(), Some(1));
        assert_eq!(stealer.steal_front(), Some(2));
        assert_eq!(stealer.steal_front(), Some(3));
        assert!(stealer.steal_front().is_none(), "第二块还在 owner 手里");
    }

    /// 旧 generation 的 thief 即使落后到物理块已经复用，也绝不能读到新世代数据。
    ///
    /// 这是 PR3 最关键的 ABA 防线：generation 校验必须先于任何 `ptr::read`。
    #[test]
    fn a_stale_generation_thief_cannot_touch_a_reused_block() {
        let block = Block::<u64>::new(4);

        // 第 0 代：写满、交给窃贼、偷空。
        block.reclaim(0);
        for value in 0..4 {
            unsafe { block.put(value) }.expect("不该满");
        }
        block.grant();
        for value in 0..4 {
            assert!(matches!(block.steal(0), Steal::Taken(taken) if taken == value));
        }
        assert!(matches!(block.steal(0), Steal::Done));
        assert!(block.is_writable(1), "偷空且停在上一代的块应当可复用");

        // 第 1 代复用同一个物理块，并已经 grant 给窃贼。
        block.reclaim(1);
        for value in 10..14 {
            unsafe { block.put(value) }.expect("不该满");
        }
        block.grant();

        // 旧 thief 拿着第 0 代的 generation 回来：必须 Empty，绝不能偷到 10。
        assert!(
            matches!(block.steal(0), Steal::Empty),
            "旧 generation 不得读取已经复用给新世代的数据"
        );
        // 新 thief 正常读到第 1 代数据。
        assert!(matches!(block.steal(1), Steal::Taken(taken) if taken == 10));
        assert!(matches!(block.steal(1), Steal::Taken(taken) if taken == 11));
        assert!(matches!(block.steal(1), Steal::Taken(taken) if taken == 12));
        assert!(matches!(block.steal(1), Steal::Taken(taken) if taken == 13));
    }

    /// 旧 thief 停在「已读 `steal_tail`、还没 CAS」的窗口里时，owner 把整块收回、
    /// 掏空、重填、再交出去——收尾时 `steal_tail` 恰好回到与旧值**位模式相同**
    /// 的值（同世代内退回再重填，边界值会原样重现）。恢复后的 CAS 因此会命中，
    /// 而它读到的必须是**重填后**的槽位数据：成功 CAS 的 Acquire 语义正是这条
    /// 可见性保证的落点，relaxed CAS 会让这次读取与 owner 的写入失去同步。
    #[test]
    fn a_stale_thief_cas_reads_the_refilled_data() {
        // 本用例与 a_stale_thief_cannot_cross_generation 共用全局暂停钩子，必须串行
        let _serial = STEAL_PAUSE_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let block = Block::<u64>::new(4);

        // 第 0 代第一轮：写满、grant，主线程偷走 0 号，让 steal_tail 停在 slot 1。
        block.reclaim(0);
        for value in 0..4 {
            unsafe { block.put(value) }.expect("不该满");
        }
        block.grant();
        assert!(matches!(block.steal(0), Steal::Taken(0)));

        // PAUSED_THIEF 由上一个用例留下时可能是 true，这里先清掉再等新的暂停
        PAUSED_THIEF.store(false, Ordering::Release);
        STEAL_PAUSE_TARGET.store(&block as *const Block<u64> as usize, Ordering::Release);
        STEAL_PAUSE.store(true, Ordering::Release);

        let stolen = thread::scope(|scope| {
            // 无论用例在哪一步失败，都要先放行窃贼再让 scope 等它结束，
            // 否则它会带着 STEAL_PAUSE 一直转下去，scope 的 join 就等死了。
            struct ReleasePause;
            impl Drop for ReleasePause {
                fn drop(&mut self) {
                    STEAL_PAUSE.store(false, Ordering::Release);
                    STEAL_PAUSE_TARGET.store(0, Ordering::Release);
                }
            }
            let _release = ReleasePause;

            // thread::scope 保证所有线程在 block 析构前收工，这里按地址访问是安全的。
            let raw = &block as *const Block<u64> as usize;
            let handle = scope.spawn(move || unsafe { &*(raw as *const Block<u64>) }.steal(0));

            // 等窃贼读走 steal_tail = (0<<8)|1 并停在 CAS 之前。
            while !PAUSED_THIEF.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // owner 在这段窗口里把整块收回来、掏空、重填、再交出去：
            // 收尾时 steal_tail 又变回 (0<<8)|1，与窃贼手里的旧值完全一致。
            block.takeover();
            for _ in 0..3 {
                unsafe { block.get() }.expect("应能取回三个元素");
            }
            for value in 10..13 {
                unsafe { block.put(value) }.expect("不该满");
            }
            block.grant();

            // 先放行窃贼，再 join。
            drop(_release);
            handle.join().expect("窃贼线程不应 panic")
        });

        assert!(
            matches!(stolen, Steal::Taken(10)),
            "stale CAS 必须读到重填后的数据，实际是 {stolen:?}"
        );
    }

    /// 旧 thief 停在「已读 `steal_tail`、还没 CAS」的窗口里时，owner 把整块收回、
    /// 掏空、推进到**下一代**、重填、再交出去。这一次收尾时 `steal_tail` 的
    /// generation 位已经变了，与旧 thief 手里的旧值不可能相同，它的 CAS 必然落空，
    /// 也就没有机会读到新世代数据——这是「同世代位模式重现」那条用例的跨世代补集：
    /// 同世代复用靠成功 CAS 的 Acquire 保证读到重填后的数据，跨世代复用靠
    /// generation 参与边界值编码让 CAS 直接失败。
    #[test]
    fn a_stale_thief_cannot_cross_generation() {
        // 本用例与 a_stale_thief_cas_reads_the_refilled_data 共用全局暂停钩子，必须串行
        let _serial = STEAL_PAUSE_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let block = Block::<u64>::new(4);

        // 第 0 代：写满、grant，主线程偷走 0 号，让 steal_tail 停在 slot 1。
        block.reclaim(0);
        for value in 0..4 {
            unsafe { block.put(value) }.expect("不该满");
        }
        block.grant();
        assert!(matches!(block.steal(0), Steal::Taken(0)));

        PAUSED_THIEF.store(false, Ordering::Release);
        STEAL_PAUSE_TARGET.store(&block as *const Block<u64> as usize, Ordering::Release);
        STEAL_PAUSE.store(true, Ordering::Release);

        let stolen = thread::scope(|scope| {
            // 无论用例在哪一步失败，都要先放行窃贼再让 scope 等它结束，
            // 否则它会带着 STEAL_PAUSE 一直转下去，scope 的 join 就等死了。
            struct ReleasePause;
            impl Drop for ReleasePause {
                fn drop(&mut self) {
                    STEAL_PAUSE.store(false, Ordering::Release);
                    STEAL_PAUSE_TARGET.store(0, Ordering::Release);
                }
            }
            let _release = ReleasePause;

            // thread::scope 保证所有线程在 block 析构前收工，这里按地址访问是安全的。
            let raw = &block as *const Block<u64> as usize;
            let handle = scope.spawn(move || unsafe { &*(raw as *const Block<u64>) }.steal(0));

            // 等窃贼读走 steal_tail = (0<<8)|1 并停在 CAS 之前。
            while !PAUSED_THIEF.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // owner 在这段窗口里把整块收回来、掏空、推进到第 1 代、重填、再交出去：
            // 收尾时 steal_tail 变成 (1<<8)|0，旧 thief 手里的 (0<<8)|1 永远对不上。
            block.takeover();
            for _ in 0..3 {
                unsafe { block.get() }.expect("应能取回三个元素");
            }
            block.reclaim(1);
            for value in 10..14 {
                unsafe { block.put(value) }.expect("不该满");
            }
            block.grant();

            // 先放行窃贼，再 join。
            drop(_release);
            handle.join().expect("窃贼线程不应 panic")
        });

        assert!(
            matches!(stolen, Steal::Conflict),
            "跨 generation 的 stale CAS 必须落空，绝不能读到新世代数据，实际是 {stolen:?}"
        );
        // 新一代 thief 正常读到第 1 代数据。
        assert!(matches!(block.steal(1), Steal::Taken(10)));
        assert!(matches!(block.steal(1), Steal::Taken(11)));
        assert!(matches!(block.steal(1), Steal::Taken(12)));
        assert!(matches!(block.steal(1), Steal::Taken(13)));
    }

    /// generation 编码上限：越过 `(1 << 56) - 1` 必须 fail-fast，而不是回绕。
    ///
    /// 回绕会让旧世代的位置模式复活——`is_writable` 的「停在上一代」判断随之
    /// 失效，ABA 防线整体崩塌，所以这里守的是编码不变量本身。
    #[test]
    fn next_sequence_fails_fast_on_generation_overflow() {
        // 4 块队列的 block_shift = 2：generation = sequence >> 2，
        // 最大 generation = (1<<56)-1，即 sequence 最大到 (1<<58)-1。
        let (owner, _stealer) = queue::<u64>(4, 4);
        let queue = &owner.queue;
        let max_ok = (1u64 << 58) - 1;
        assert_eq!(
            queue.next_sequence(max_ok - 1),
            max_ok,
            "最大 generation 内仍应前进"
        );

        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.next_sequence(max_ok);
        }));
        std::panic::set_hook(previous);
        assert!(result.is_err(), "越过最大 generation 必须 fail-fast");
    }

    /// owner 与多个窃贼并发跑，元素必须不丢不重
    #[test]
    fn concurrent_steal_loses_nothing() {
        const TOTAL: usize = 20_000;
        const THIEVES: usize = 3;

        let (owner, stealer) = queue::<usize>(NUM_BLOCKS, BLOCK_SIZE);
        let done = Arc::new(AtomicBool::new(false));
        let seen: Arc<Vec<AtomicUsize>> =
            Arc::new((0..TOTAL).map(|_| AtomicUsize::new(0)).collect());

        thread::scope(|scope| {
            for _ in 0..THIEVES {
                let stealer = stealer.clone();
                let done = done.clone();
                let seen = seen.clone();
                scope.spawn(move || {
                    while !done.load(Ordering::Acquire) {
                        while let Some(value) = stealer.steal_front() {
                            seen[value].fetch_add(1, Ordering::Relaxed);
                        }
                        std::hint::spin_loop();
                    }
                });
            }

            // 队列满了就先攒着，模拟调度器溢出到 injector 的那条路
            let mut backlog: VecDeque<usize> = VecDeque::new();
            for value in 0..TOTAL {
                backlog.push_back(value);
                while let Some(item) = backlog.pop_front() {
                    if let Err(back) = unsafe { owner.push_back(item) } {
                        backlog.push_front(back);
                        break;
                    }
                }
                // owner 自己也消费一点，制造 owner 与窃贼在块边界上的交错
                if value % 3 == 0 {
                    if let Some(item) = unsafe { owner.pop_back() } {
                        seen[item].fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            while let Some(item) = unsafe { owner.pop_back() } {
                seen[item].fetch_add(1, Ordering::Relaxed);
            }
            for item in backlog {
                seen[item].fetch_add(1, Ordering::Relaxed);
            }
            done.store(true, Ordering::Release);
        });

        // 窃贼可能在 done 置位前后各偷走一些，这里再兜一遍
        while let Some(item) = stealer.steal_front() {
            seen[item].fetch_add(1, Ordering::Relaxed);
        }
        for (value, count) in seen.iter().enumerate() {
            assert_eq!(count.load(Ordering::Relaxed), 1, "{value} 被取到的次数不对");
        }
    }

    /// 队列析构时要把没取走的元素放掉，既不能泄漏也不能重复释放
    #[test]
    fn drop_releases_remaining_items() {
        let token = Arc::new(());
        {
            let (owner, stealer) = queue::<Arc<()>>(4, 4);
            for _ in 0..10 {
                unsafe { owner.push_back(token.clone()) }.expect("不该满");
            }
            // 制造出三种状态的块：偷空的、偷了一半的、owner 手里的
            assert!(stealer.steal_front().is_some());
            assert!(stealer.steal_front().is_some());
            assert!(unsafe { owner.pop_back() }.is_some());
            // 取走的三个是临时值，出了语句就还了，队列里应当还剩 7 个
            assert_eq!(Arc::strong_count(&token), 1 + 7);
        }
        assert_eq!(Arc::strong_count(&token), 1, "队列析构后不该还有残留引用");
    }
}
