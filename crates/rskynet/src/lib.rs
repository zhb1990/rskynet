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
//! use rskynet::{Config, ConfigExt, Ctx, Message, Registry};
//!
//! struct Echo;
//!
//! #[rskynet::service]
//! impl Echo {
//!     async fn init(&self, ctx: Ctx) -> rskynet::Result<()> {
//!         ctx.register_name("echo");
//!         Ok(())
//!     }
//!
//!     async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
//!         let payload = msg.take_payload();
//!         let _ = ctx.reply(&msg, payload);
//!     }
//! }
//!
//! let registry = Registry::new().with("echo", || Echo);
//! let config = Config::default().with_bootstrap(["echo"]);
//! rskynet::start(config, registry).unwrap();
//! ```
//!
//! 日志、定时器、引导这三个服务不必自己注册：[`start`] 会按 feature 把它们挂上，
//! 并把时间来源注入节点。想换掉其中某一个，用 [`Builder`] 自己拼，见 [`BuilderExt`]。
//!
//! ## crate 构成
//!
//! 本 crate 是门面，按 feature 把几个成员拼在一处，使用方只依赖它一个：
//!
//! | crate | 内容 | feature |
//! | --- | --- | --- |
//! | [`rskynet_core`] | Actor 内核：邮箱、调度、session | 总是启用 |
//! | `rskynet-macros` | 消去 `Service` 实现样板的过程宏 | `macros`（默认开） |
//! | `rskynet-logger` | 日志服务，一个[独占线程的服务][Exclusive] | `logger`（默认开） |
//! | `rskynet-timer` | 分层时间轮与定时器服务 | `timer`（默认开） |
//! | `rskynet-bootstrap` | 引导服务：按清单拉起业务服务 | `bootstrap`（默认开） |
//! | `rskynet-net` | socket 层，一个[独占线程的服务][Exclusive] | `net` |
//!
//! 内核里一个服务都没有，连时间都不在里面：分层时间轮住在 `rskynet-timer`，内核
//! 只认 [`Timer`] 这个抽象，启动前必须注入一个实现。这么切的好处与网络层独立是
//! 同一个——内核不碰 epoll/kqueue、不碰文件 IO、不碰系统时钟，因此是纯跨平台的，
//! 跑单元测试也不必拉起这些东西。服务进内核的路子彼此没两样：用 [`Registry`]
//! 注册类型，在配置里占一段，要独占线程就用 [`Registry::with_exclusive`]。真要
//! 再起子线程，[`rskynet_core::ext`] 里的 [`NodeRef`] 与 [`ReplyToken`] 负责把
//! 消息与回话带回内核。

pub use rskynet_core::*;

/// 消去 `Service` 实现样板的过程宏，用法见 [`service`] 与 [`exclusive`]。
///
/// `msg` 单独用没有意义（它由前两个宏在展开时摘走），导出它只是为了让写错地方时
/// 报一句人话。
#[cfg(feature = "macros")]
pub use rskynet_macros::{exclusive, msg, service};

/// 网络层：socket / gate / agent。
#[cfg(feature = "net")]
pub use rskynet_net as net;

#[cfg(feature = "bootstrap")]
pub use rskynet_bootstrap as bootstrap;
#[cfg(feature = "logger")]
pub use rskynet_logger as logger;
#[cfg(feature = "timer")]
pub use rskynet_timer as timer;

/// 引导清单的链式写法：`Config::default().with_bootstrap(["echo"])`。
#[cfg(feature = "bootstrap")]
pub use rskynet_bootstrap::{ConfigExt, ServiceSpec};

/// 把内置服务装进 [`Builder`]。
pub trait BuilderExt {
    /// 按 feature 挂上内置服务，并注入时间来源。
    ///
    /// 挂的都是约定名字（`logger` / `timer` / `bootstrap`），所以配置里不写
    /// `name` 也能对上号。要换掉其中某一个，在这之后用自己的实现重新注册同名
    /// 类型即可——后注册的覆盖先注册的。
    #[must_use]
    fn with_builtins(self) -> Self;
}

impl BuilderExt for Builder {
    fn with_builtins(self) -> Self {
        #[allow(unused_mut)]
        let mut builder = self;
        #[cfg(feature = "logger")]
        {
            builder = builder.exclusive_service(
                rskynet_core::service::LOGGER,
                rskynet_logger::Logger::default,
            );
        }
        #[cfg(feature = "bootstrap")]
        {
            builder = builder.service(
                rskynet_core::service::BOOTSTRAP,
                rskynet_bootstrap::Bootstrap::default,
            );
        }
        // 定时器要一次做两件事：注册服务，并把同一个时钟注入节点
        #[cfg(feature = "timer")]
        {
            use rskynet_timer::BuilderExt as _;
            builder = builder.with_wheel_timer();
        }
        // 网络层只注册类型，不拉起：它不是系统服务，什么时候起由 `[bootstrap]`
        // 的清单说了算
        #[cfg(feature = "net")]
        {
            builder = builder.exclusive_service(rskynet_net::NAME, rskynet_net::NetService::new);
        }
        builder
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// 与 [`rskynet_core::start`] 的区别是这里会先挂上内置服务、注入时间来源，
/// 所以使用方只管注册自己的服务类型。要自己拼这些，走 [`Builder`]。
pub fn start(config: Config, registry: Registry) -> Result<()> {
    Builder::new(config)
        .registry(registry)
        .with_builtins()
        .run()
}
