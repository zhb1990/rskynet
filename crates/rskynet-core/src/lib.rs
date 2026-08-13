//! # rskynet-core
//!
//! [skynet](https://github.com/cloudwu/skynet) Actor 内核的 Rust 实现。
//!
//! **本 crate 是内核，不是给使用方直接依赖的门面**——业务代码请用 `rskynet`，
//! 它按 feature 把内核、过程宏与各个服务包拼在一处。这里只放内核本体：
//!
//! 1. **服务即 Actor**：每个服务有独立地址与独立邮箱，彼此只靠消息往来。
//! 2. **两级消息队列调度**：每服务一个邮箱，每 worker 一条运行队列（闲了去偷），
//!    同一服务任意时刻只在一个线程上执行，因此服务内部不需要锁。
//! 3. **session 配对把异步写成同步**：`call` 分配一个 session、挂起当前任务，
//!    回包带着同一个 session 回来时再唤醒它。
//!
//! 第三点在 skynet 里由 Lua 协程承载，这里换成 Rust 的 `Future`——
//! 语义一致，还多了编译期类型检查，也不再需要 Lua。
//!
//! 服务默认跑在共享的 worker 池上，也可以让它[独占一条线程][Exclusive]——
//! 日志、定时器、网络层都是后者，于是内核不必为「专用线程」另开一套东西。
//!
//! ## 内核里没有服务
//!
//! 日志、定时器、引导这三样在 C 版里是内核的一部分，这里一个都不在：它们各是
//! 一个独立 crate（`rskynet-logger` / `rskynet-timer` / `rskynet-bootstrap`），
//! 与网络层走同一条接入路子——[`Registry`] 注册类型，配置里各占一段。内核这边
//! 只留下三个约定俗成的名字（见 [`service`]）与拉起它们的顺序。
//!
//! 时间也一样。分层时间轮不在内核里，内核只认 [`Timer`] 这个抽象，启动前必须
//! 由调用方注入一个实现（[`Builder::timer`]）。`ctx.sleep` 与 `ctx.now` 都从
//! 那里取值。
//!
//! ## 模块划分
//!
//! 模块名刻意与 `skynet-src` 的文件名对齐，方便逐一比对。C 版没有对应物的是
//! 三块：[`Exclusive`] 那套独占线程的服务、[`ext`] 里给内核之外的线程用的接口
//! （服务住在独立 crate 里，总得有条公开的路进得来），以及 [`Timer`] 这个把
//! 时间实现挡在内核之外的抽象。

// 给过程宏一个在 crate 内外都稳定的绝对路径；也避免 rust-analyzer 在单元测试
// 的宏展开中把 `::rskynet_core` 误判成不存在的外部 crate。
extern crate self as rskynet_core;

mod bwos;
mod clock;
mod context;
mod error;
mod exclusive;
mod handle;
mod message;
mod module;
mod monitor;
mod mq;
mod payload;
mod server;
mod session;
mod start;
mod task;

pub mod ext;
pub mod service;

pub use clock::Timer;
pub use context::{Ctx, Service};
pub use error::{Error, Result};
pub use exclusive::{Exclusive, Idler};
pub use ext::{NodeRef, ReplyToken};
pub use message::{Addr, Message, MsgType, Payload};
pub use module::{AutoService, Registry};
pub use payload::{FromPayload, IntoPayload};
pub use start::{Builder, Config, start};
pub use task::SvcCell;

/// 服务方法的返回类型。从本 crate 导出，使用方不必再直接依赖 `futures`。
pub use futures_util::future::BoxFuture;

/// 过程宏展开所需的实现细节，不属于稳定的业务 API。
#[doc(hidden)]
pub mod __private {
    pub use inventory;
}

/// 写日志的便捷宏，等价于 `ctx.log(format!(...))`。
///
/// ```ignore
/// rskynet::log!(ctx, "第 {} 次往返，耗时 {} 厘秒", round, cost);
/// ```
#[macro_export]
macro_rules! log {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.log(::std::format!($($arg)*))
    };
}
