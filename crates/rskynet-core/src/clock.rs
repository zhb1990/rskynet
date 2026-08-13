//! 节点的时间来源。C 版把分层时间轮直接焊在内核里（`skynet-src/skynet_timer.c`），
//! 这里内核只留一个抽象：刻度怎么推、表挂在哪，全由启动前注入的实现说了算，
//! 现成的实现见 `rskynet-timer`。
//!
//! 拆成 trait 不只是让内核变薄。注入的对象在节点建起来之前就存在，于是「挂表」
//! 从第一条指令起就是可用的，哪怕推刻度的那条线程还没上线——引导期间挂的表不会
//! 丢，等定时器服务起来后照常到期。

/// 节点的时间来源，启动前必须注入，见 [`crate::Builder::timer`]。
///
/// 实现方通常分成两半：这个对象负责记账与读时钟（谁都可以碰，所以要 `Sync`），
/// 另有一个独占线程的服务负责推刻度、把到期的表派发成消息。
pub trait Timer: Send + Sync + 'static {
    /// 挂一个 `ticks` 毫秒后到期的定时器，对照 `skynet_timeout`。
    ///
    /// 到期时由实现方给 `handle` 投一条 `RESPONSE`（`session` 与请求配对），
    /// 走 [`crate::NodeRef::send`] 那条路。`ticks` 为 0 的情形内核直接回包，
    /// 不会走到这里。
    fn timeout(&self, handle: u32, session: i32, ticks: u32);

    /// 节点启动至今的毫秒数，对照 `skynet_now`。
    fn now(&self) -> u64;

    /// 当前 unix 时间，单位毫秒。
    fn wall_clock(&self) -> u64;

    /// 节点启动时刻的 unix 时间，单位秒，对照 `skynet_starttime`。
    fn start_seconds(&self) -> u64;
}
