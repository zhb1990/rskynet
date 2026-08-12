//! # rskynet
//!
//! 用 Rust 复刻 [skynet](https://github.com/cloudwu/skynet) 的 Actor 内核。
//!
//! skynet 的灵魂是三件事，rskynet 逐条对应：
//!
//! 1. **服务即 Actor**：每个服务有独立地址与独立邮箱，彼此只靠消息往来。
//! 2. **两级消息队列调度**：每服务一个邮箱，每 worker 一条运行队列（闲了去偷别人的），
//!    同一服务任意时刻只在一个线程上执行，因此服务内部不需要锁。
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
//! let config = Config::default().with_bootstrap(["echo"]);
//! rskynet::start(config, registry).unwrap();
//! ```
//!
//! ## crate 构成
//!
//! 本 crate 是门面，按 feature 把几个成员拼在一处，使用方只依赖它一个：
//!
//! | crate | 内容 | feature |
//! | --- | --- | --- |
//! | [`rskynet_core`] | Actor 内核：邮箱、调度、session、定时器 | 总是启用 |
//! | `rskynet-macros` | 消去 `Service` 实现样板的过程宏 | `macros`（默认开） |
//! | `rskynet-net` | socket 层，一个[独占线程的服务][Exclusive] | `net` |
//!
//! 网络层做成独立 crate 而不是内核的一个模块，是为了让内核不碰 epoll/kqueue
//! ——它因此是纯跨平台的，也不必为了跑单元测试就拉起一套 IO。它进内核的路子
//! 与别的服务没两样，只是用 [`Registry::with_exclusive`] 注册，于是那条阻塞在
//! epoll 上的线程就是它自己的。真要再起子线程，[`rskynet_core::ext`] 里的
//! [`NodeRef`] 与 [`ReplyToken`] 负责把消息与回话带回内核。

pub use rskynet_core::*;

/// 网络层：socket / gate / agent。
#[cfg(feature = "net")]
pub use rskynet_net as net;
