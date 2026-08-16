//! 独占线程服务，对照 ltask 的 exclusive service（C 版 skynet 没有对应物）。
//!
//! skynet 的定时器与 socket 各占一条专用线程，与服务体系并行：它们不是服务，
//! 没有邮箱也没有地址，内核得为它们单开一套代码。这里换个思路——把「独占一条
//! 线程」做成**服务的一种运行方式**，于是日志、定时器、将来的网络层都是普通
//! 服务，只是执行者不同：
//!
//! - 共享服务：邮箱有活时进运行队列，被某个 worker 领走执行。
//! - 独占服务：邮箱有活时叫醒它自己那条线程；没活时由 [`Exclusive::idle`] 决定
//!   怎么睡——阻塞在 park 上（日志）、定时醒来推时间轮（定时器）、或者阻塞在
//!   epoll 上顺便把 IO 事件派发成消息（网络层）。
//!
//! 两者共用同一个邮箱状态机与同一套取活逻辑，`init` / `dispatch` 的写法一字不差。
//! 「同一服务任意时刻只在一个线程上执行」这条不变量在独占模式下只会更强，所以
//! [`crate::SvcCell`] 照旧可用。

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::context::{Ctx, Service};
use crate::server::ServiceContext;

/// 独占一条线程的服务。
///
/// 在 [`crate::Registry::with_exclusive`] 上注册的服务走这条路：每 `launch` 一次
/// 就新起一条线程，那条线程只跑这一个服务。适合两类活：
///
/// - **要阻塞在自己的事件源上**：epoll / kqueue / 第三方库的阻塞 API。放在共享
///   worker 上会把整个 worker 卡住，独占线程卡的只有自己。
/// - **要按节拍醒来**：定时器那样每隔几毫秒推进一次。
///
/// 纯消息驱动的服务不必用它——那是共享 worker 池的主场，`launch` 一万个也只是
/// 一万个邮箱。用了它就是一万条线程。
///
/// # 唤醒的约定
///
/// 有活投进邮箱时，内核会先 `unpark` 这条线程、再调一次 [`Exclusive::interrupt`]。
/// 于是：
///
/// - 阻塞在 [`Idler::park`] / [`Idler::park_timeout`] 上的服务什么都不用写，
///   两个钩子都走默认实现即可。
/// - 阻塞在别处（mio 的 `Poll`、channel、条件变量）的服务必须实现
///   [`Exclusive::interrupt`]，而且**它必须能接住早到的唤醒**：唤醒可能发生在
///   本线程真正阻塞下去之前，那一下不能丢，否则邮箱里的消息要等到下一次事件
///   才被看见。`std` 的 park 令牌与 mio 的 `Waker` 都满足这一点，`Condvar`
///   不满足（得自己配一个标志位）。
pub trait Exclusive: Service {
    /// 邮箱与就绪队列都空了，该睡了。跑在本服务的专属线程上，可以放心阻塞。
    ///
    /// 返回后内核会重扫一遍邮箱，所以「阻塞一会儿就返回」是完全正常的用法：
    /// 定时器就是每 2.5ms 返回一次，顺手把时间轮推一格。
    ///
    /// 这里也可以放心碰服务自己的 [`crate::SvcCell`]、调 `ctx.spawn` ——
    /// 内核已经标记好「本线程正在执行本服务」。但**不要在这里 `block_on` 一个
    /// 需要本服务处理消息才能完成的 future**：那些活得等本函数返回才轮得到。
    fn idle(&self, ctx: &Ctx, idler: &Idler) {
        let _ = ctx;
        idler.park();
    }

    /// 把 [`Exclusive::idle`] 里的阻塞叫醒，会被任意线程调用。
    ///
    /// 默认什么都不做——内核那记 `unpark` 已经够把默认的 `park` 叫醒了。
    /// 阻塞在自己事件源上的服务必须实现它，语义见 trait 文档里「唤醒的约定」。
    fn interrupt(&self) {}
}

/// 独占线程的挂起把手，由内核交给 [`Exclusive::idle`]。
///
/// 之所以不让服务自己拿 `thread::current()` 去 park：线程句柄得在**任何投递
/// 发生之前**就登记好，否则「投递方叫醒」与「本线程登记」会撞车，早到的那记
/// 唤醒就丢了。登记这件事由内核在循环启动前做掉。
pub struct Idler {
    ctx: Arc<ServiceContext>,
}

impl Idler {
    fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    /// 睡到被叫醒。有活投进邮箱、服务被 kill、节点收工都会把它叫起来。
    ///
    /// 早到的唤醒不会丢：`std` 的 park 自带令牌，睡之前来的 `unpark` 会让这次
    /// park 直接返回。
    pub fn park(&self) {
        thread::park();
    }

    /// 同 [`Idler::park`]，但最多睡 `timeout`。按节拍醒来的服务用它。
    pub fn park_timeout(&self, timeout: Duration) {
        thread::park_timeout(timeout);
    }

    /// 节点是否已经收工。收工之后本服务仍然活着，直到被摘除。
    pub fn is_quit(&self) -> bool {
        self.ctx.node.sched.is_quit()
    }

    /// 本服务是否已被摘除。返回 true 时 `idle` 应当尽快返回，好让线程去收尾。
    pub fn is_dead(&self) -> bool {
        self.ctx.is_dead()
    }
}

impl std::fmt::Debug for Idler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Idler(:{:08x})", self.ctx.handle)
    }
}

/// 独占线程的主循环，对照 [`crate::start::worker_loop`]。
///
/// 与 worker 的区别只有两处：手里的服务永远是同一个（不必去运行队列取，也不必
/// 让渡），以及没活干时去调服务自己的 [`Exclusive::idle`] 而不是找活或挂起。
///
/// idle / dispatch / drain / destroy 或最终 Drop 中的 panic 都不做恢复，
/// 由进程级崩溃处理统一记录后 abort。
pub(crate) fn exclusive_loop(ctx: Arc<ServiceContext>, service: Arc<dyn Exclusive>) {
    // 必须在任何一次挂起之前登记，见 [`Idler`] 的说明
    ctx.bind_thread();

    let node = ctx.node.clone();
    let idler = Idler::new(ctx.clone());
    let cx = Ctx::new(ctx.clone());

    loop {
        // 取回执行权。只有本线程会执行本服务，所以这一步不必与谁竞争：投递方
        // 要么已经把状态推成 QUEUED（那也是给本线程看的），要么随后压进来的活
        // 会把状态推成 NOTIFIED，被 take_work 的重扫接住
        ctx.mailbox.mark_running();
        if node.run_service(&ctx, false).is_dead() || ctx.is_dead() {
            break;
        }
        ctx.with_ownership(|| service.idle(&cx, &idler));
    }

    // 收尾：邮箱里积压的活先干完（日志服务靠这一步把最后几行写出去），再走与
    // worker 完全一样的销毁流程。`mark_running` 是因为排空那一步会把状态放回
    // IDLE，而销毁的最后一步要从 RUNNING 放生
    ctx.mailbox.mark_running();
    node.drain_service(&ctx);
    ctx.mailbox.mark_running();
    node.destroy(ctx);
}
