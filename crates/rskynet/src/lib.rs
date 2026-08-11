//! # rskynet
//!
//! 用 Rust 复刻 [skynet](https://github.com/cloudwu/skynet) 的 Actor 内核。
//!
//! skynet 的灵魂是三件事，rskynet 逐条对应：
//!
//! 1. **服务即 Actor**：每个服务有独立地址与独立邮箱，彼此只靠消息往来。
//! 2. **两级消息队列调度**：每服务一个邮箱，全局一个「有活干的服务」队列，
//!    N 个 worker 线程从中取活。同一服务任意时刻只在一个线程上执行，
//!    因此服务内部不需要锁。
//! 3. **session 配对把异步写成同步**：`call` 分配一个 session、挂起当前任务，
//!    回包带着同一个 session 回来时再唤醒它。
//!
//! 第三点在 skynet 里由 Lua 协程承载，这里换成 Rust 的 `Future`——
//! 语义一致，还多了编译期类型检查，也不再需要 Lua。
//!
//! ## 上手
//!
//! ```no_run
//! use std::sync::Arc;
//! use rskynet::{BoxFuture, Config, Ctx, Message, Payload, Registry, Service};
//!
//! struct Echo;
//!
//! impl Service for Echo {
//!     fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
//!         Box::pin(async move {
//!             ctx.register_name("echo");
//!             Ok(())
//!         })
//!     }
//!
//!     fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
//!         Box::pin(async move {
//!             let payload = msg.take_payload();
//!             let _ = ctx.reply(&msg, payload);
//!         })
//!     }
//! }
//!
//! let registry = Registry::new().with_builtins().with("echo", || Echo);
//! let config = Config::default().with_bootstrap("bootstrap echo");
//! rskynet::start(config, registry).unwrap();
//! ```

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

pub mod service;

pub use context::{Ctx, Service};
pub use error::{Error, Result};
pub use message::{Addr, Message, MsgType, Payload};
pub use module::Registry;
pub use start::{Config, start};
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
