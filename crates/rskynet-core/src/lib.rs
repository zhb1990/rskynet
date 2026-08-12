//! # rskynet-core
//!
//! [skynet](https://github.com/cloudwu/skynet) Actor 内核的 Rust 实现。
//!
//! **本 crate 是内核，不是给使用方直接依赖的门面**——业务代码请用 `rskynet`，
//! 它按 feature 把内核、过程宏与网络层拼在一处。这里只放内核本体：
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
//! ## 模块划分
//!
//! 模块名刻意与 `skynet-src` 的文件名对齐，方便逐一比对；[`ext`] 是 C 版没有的
//! 那一块——网络层住在独立 crate 里，得有一套公开接口才进得来内核。

mod bwos;
mod context;
mod error;
mod handle;
mod message;
mod module;
mod mq;
mod server;
mod session;
mod start;
mod task;
mod timer;

pub mod ext;
pub mod service;

pub use context::{Ctx, Service};
pub use error::{Error, Result};
pub use ext::{NodeRef, Plugin, ReplyToken};
pub use message::{Addr, Message, MsgType, Payload};
pub use module::Registry;
pub use start::{Builder, Config, start};
pub use task::SvcCell;

/// 服务方法的返回类型。从本 crate 导出，使用方不必再直接依赖 `futures`。
pub use futures_util::future::BoxFuture;

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
