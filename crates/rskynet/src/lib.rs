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
//! 日志、信号、定时器、引导这些服务不必自己注册：[`start`] 会按 feature 把它们挂上，
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
//! | `rskynet-signal` | 进程信号、优雅关停与独立崩溃报告 | `signal`（默认开） |
//! | 标准命令行入口 | 读取 TOML 并启动自动注册服务 | `main`（默认开） |
//! | `rskynet-net` | socket 层，一个[独占线程的服务][Exclusive] | `net` |
//! | `rskynet-tls` | 基于 rustls、复用 net 的双向 TLS 协议服务 | `tls` |
//! | `rskynet-http` | HTTP/1.1 客户端服务与可嵌入服务端 | `http` / `https` |
//! | `rskynet-dashboard` | 节点统计 API 与内嵌 Dashboard | `dashboard` |
//! | `rskynet-cluster` | Protobuf 节点间通信 | `cluster` |
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
pub use rskynet_macros::{debug, exclusive, msg, service, signal};

/// 网络层：socket / gate / agent。
#[cfg(feature = "net")]
pub use rskynet_net as net;

/// TLS 协议服务：底层复用 [`net`]，向业务投递明文事件。
#[cfg(feature = "tls")]
pub use rskynet_tls as tls;

/// HTTP/1.1 客户端服务与可嵌入业务服务的服务端驱动。
#[cfg(feature = "http")]
pub use rskynet_http as http;

/// 节点统计 HTTP API 与内嵌 Dashboard。
#[cfg(feature = "dashboard")]
pub use rskynet_dashboard as dashboard;

/// WebSocket 客户端、服务端升级与消息类型。
#[cfg(feature = "websocket")]
pub use rskynet_http::websocket;

/// 可选的 Protobuf 跨节点通信层。
#[cfg(feature = "cluster")]
pub use rskynet_cluster as cluster;

#[cfg(feature = "bootstrap")]
pub use rskynet_bootstrap as bootstrap;
#[cfg(feature = "logger")]
pub use rskynet_logger as logger;
#[cfg(feature = "signal")]
pub use rskynet_signal as signal;
#[cfg(feature = "timer")]
pub use rskynet_timer as timer;

/// 独立崩溃报告进程，也是 fail-fast 进程契约的安装点。
///
/// 标准 [`main::run`] 会在读取参数与配置之前自动安装；自定义入口同样必须最先
/// 调用 [`crash::install`]，并把返回的 guard 留到进程结束：
///
/// ```no_run
/// fn main() -> rskynet::Result<()> {
///     let _crash = rskynet::crash::install()?;
///     // 之后才能初始化 / 启动 rskynet
///     Ok(())
/// }
/// ```
///
/// rskynet 内核不恢复 panic。workspace profile 使用 `panic = "abort"`，而普通
/// `abort` 仍会执行 panic hook：panic 与 native crash 都由这里统一记录并生成 dump。
#[cfg(feature = "signal")]
pub use rskynet_signal::crash;

/// 标准命令行入口：读取 TOML，并启动所有配置的自动注册服务。
#[cfg(feature = "main")]
pub mod main {
    use std::ffi::OsString;
    use std::process::ExitCode;

    use super::{Config, Error, Registry, Result};

    /// 使用进程参数启动节点。要求且只接受一个 TOML 配置路径。
    ///
    /// 启用 `signal` feature 时，在读取参数与配置之前先安装崩溃处理器，
    /// 因此启动阶段、运行阶段与关停阶段的 panic 都已在崩溃报告覆盖范围内。
    pub fn run() -> ExitCode {
        #[cfg(feature = "signal")]
        let _crash = match crate::crash::install() {
            Ok(guard) => guard,
            Err(err) => {
                eprintln!("rskynet: {err}");
                return ExitCode::FAILURE;
            }
        };
        match run_from(std::env::args_os()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("rskynet: {err}");
                ExitCode::FAILURE
            }
        }
    }

    /// 可测试、可嵌入的参数入口。第一项按程序名处理。
    pub fn run_from<I>(args: I) -> Result<()>
    where
        I: IntoIterator<Item = OsString>,
    {
        let mut args = args.into_iter();
        let program = args.next().unwrap_or_else(|| OsString::from("rskynet"));
        let Some(path) = args.next() else {
            return Err(usage(&program));
        };
        if args.next().is_some() {
            return Err(usage(&program));
        }

        let config = Config::from_toml_file(path)?;
        let registry = Registry::from_auto()?;
        super::start(config, registry)
    }

    fn usage(program: &OsString) -> Error {
        Error::Config(format!(
            "用法：{} <config.toml>",
            std::path::Path::new(program).display()
        ))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn missing_config_path_reports_usage() {
            let err = run_from([OsString::from("node")]).expect_err("缺参数应失败");
            assert!(err.to_string().contains("用法：node <config.toml>"));
        }

        #[test]
        fn extra_arguments_report_usage() {
            let err = run_from([
                OsString::from("node"),
                OsString::from("one.toml"),
                OsString::from("two.toml"),
            ])
            .expect_err("多余参数应失败");
            assert!(err.to_string().contains("用法：node <config.toml>"));
        }
    }
}

/// 引导清单的链式写法：`Config::default().with_bootstrap(["echo"])`。
#[cfg(feature = "bootstrap")]
pub use rskynet_bootstrap::{ConfigExt, ServiceSpec};

/// 把内置服务装进 [`Builder`]。
pub trait BuilderExt {
    /// 按 feature 挂上内置服务，并注入时间来源。
    ///
    /// 挂的都是约定名字（`logger` / `signal` / `timer` / `bootstrap`），所以配置里不写
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
        #[cfg(feature = "signal")]
        {
            builder = builder.exclusive_service(
                rskynet_core::service::SIGNAL,
                rskynet_signal::SignalService::default,
            );
        }
        // 定时器要一次做两件事：注册服务，并把同一个时钟注入节点
        #[cfg(feature = "timer")]
        {
            use rskynet_timer::BuilderExt as _;
            builder = builder.with_wheel_timer();
        }
        // 可选内置服务这里只注册类型；`start` 会根据配置段和依赖关系把实际启动项
        // 交给 Builder，放在 timer 与 bootstrap 之间依次等待完整初始化。
        #[cfg(feature = "net")]
        {
            builder = builder.exclusive_service(rskynet_net::NAME, rskynet_net::NetService::new);
        }
        #[cfg(feature = "tls")]
        {
            builder = builder.service(rskynet_tls::NAME, rskynet_tls::TlsService::new);
        }
        #[cfg(feature = "http")]
        {
            builder = builder.service(rskynet_http::NAME, rskynet_http::HttpClientService::new);
        }
        #[cfg(feature = "dashboard")]
        {
            builder = builder.service(
                rskynet_dashboard::NAME,
                rskynet_dashboard::DashboardService::new,
            );
        }
        builder
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// 与 [`rskynet_core::start`] 的区别是这里会先挂上内置服务、注入时间来源，
/// 所以使用方只管注册自己的服务类型。要自己拼这些，走 [`Builder`]。
pub fn start(config: Config, registry: Registry) -> Result<()> {
    let mut registry = registry;
    let startup = prepare_startup(&config, &mut registry)?;
    let mut builder = Builder::new(config).registry(registry).with_builtins();
    for kind in startup {
        builder = builder.startup_service(kind, "");
    }
    builder.run()
}

fn prepare_startup(config: &Config, registry: &mut Registry) -> Result<Vec<&'static str>> {
    const NET: &str = "net";
    const TLS: &str = "tls";
    const HTTP: &str = "http-client";
    const CLUSTER: &str = "cluster";
    const DASHBOARD: &str = "dashboard";

    let has_net = config.has_section(NET);
    let has_tls = config.has_section(TLS);
    let has_http = config.has_section(HTTP);
    let has_cluster = config.has_section(CLUSTER);
    let has_dashboard = config.has_section(DASHBOARD);
    #[cfg(not(feature = "cluster"))]
    let _ = registry;

    #[cfg(not(feature = "net"))]
    if has_net {
        return Err(Error::Config("[net] 需要启用 `net` feature".into()));
    }
    #[cfg(not(feature = "tls"))]
    if has_tls {
        return Err(Error::Config(
            "[tls] 需要启用 `tls` 或 `https` feature".into(),
        ));
    }
    #[cfg(not(feature = "http"))]
    if has_http {
        return Err(Error::Config(
            "[http-client] 需要启用 `http` 或 `https` feature".into(),
        ));
    }
    #[cfg(not(feature = "cluster"))]
    if has_cluster {
        return Err(Error::Config("[cluster] 需要启用 `cluster` feature".into()));
    }
    #[cfg(not(feature = "dashboard"))]
    if has_dashboard {
        return Err(Error::Config(
            "[dashboard] 需要启用 `dashboard` feature".into(),
        ));
    }

    #[cfg(feature = "bootstrap")]
    if let Some(bootstrap) =
        config.section::<rskynet_bootstrap::BootstrapConfig>(rskynet_core::service::BOOTSTRAP)?
    {
        if let Some(service) = bootstrap.services.iter().find(|service| {
            matches!(
                service.name.as_str(),
                NET | TLS | HTTP | CLUSTER | DASHBOARD
            )
        }) {
            return Err(Error::Config(format!(
                "[bootstrap].services 不应手工启动 `{}`；请改用对应配置段",
                service.name
            )));
        }
    }

    #[cfg(feature = "cluster")]
    if has_cluster && !registry.contains(rskynet_cluster::NAME) {
        use rskynet_cluster::{ClusterService, HandlerRegistry};
        let handlers =
            HandlerRegistry::from_auto().map_err(|error| Error::Config(error.to_string()))?;
        registry.register(rskynet_cluster::NAME, move || {
            ClusterService::new(handlers.clone())
        });
    }

    let need_http = has_http;
    let need_cluster = has_cluster;
    let need_dashboard = has_dashboard;
    let need_tls = has_tls || (need_http && cfg!(feature = "https"));
    let need_net = has_net || need_tls || need_http || need_cluster || need_dashboard;
    let mut startup = Vec::new();
    if need_net {
        startup.push(NET);
    }
    if need_tls {
        startup.push(TLS);
    }
    if need_http {
        startup.push(HTTP);
    }
    if need_cluster {
        startup.push(CLUSTER);
    }
    if need_dashboard {
        startup.push(DASHBOARD);
    }
    Ok(startup)
}

#[cfg(test)]
mod startup_tests {
    use super::*;

    fn with_empty_section(mut config: Config, name: &str) -> Config {
        config.section_mut(name);
        config
    }

    #[cfg(feature = "bootstrap")]
    #[test]
    fn bootstrap_must_not_repeat_automatic_services() {
        let config = Config::default().with_bootstrap(["net"]);
        let err = prepare_startup(&config, &mut Registry::new()).unwrap_err();
        assert!(err.to_string().contains("不应手工启动 `net`"));
    }

    #[cfg(not(feature = "net"))]
    #[test]
    fn configured_but_uncompiled_service_is_an_error() {
        let config = with_empty_section(Config::default(), "net");
        let err = prepare_startup(&config, &mut Registry::new()).unwrap_err();
        assert!(err.to_string().contains("`net` feature"));
    }

    #[cfg(feature = "net")]
    #[test]
    fn empty_net_section_enables_net() {
        let config = with_empty_section(Config::default(), "net");
        assert_eq!(
            prepare_startup(&config, &mut Registry::new()).unwrap(),
            ["net"]
        );
    }

    #[cfg(feature = "tls")]
    #[test]
    fn tls_adds_default_net_dependency() {
        let config = with_empty_section(Config::default(), "tls");
        assert_eq!(
            prepare_startup(&config, &mut Registry::new()).unwrap(),
            ["net", "tls"]
        );
    }

    #[cfg(all(feature = "http", not(feature = "https")))]
    #[test]
    fn plain_http_client_adds_only_net() {
        let config = with_empty_section(Config::default(), "http-client");
        assert_eq!(
            prepare_startup(&config, &mut Registry::new()).unwrap(),
            ["net", "http-client"]
        );
    }

    #[cfg(feature = "https")]
    #[test]
    fn https_client_adds_net_and_tls_in_order() {
        let config = with_empty_section(Config::default(), "http-client");
        assert_eq!(
            prepare_startup(&config, &mut Registry::new()).unwrap(),
            ["net", "tls", "http-client"]
        );
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_adds_net_and_builds_handler_registry() {
        let config = with_empty_section(Config::default(), "cluster");
        let mut registry = Registry::new();
        assert_eq!(
            prepare_startup(&config, &mut registry).unwrap(),
            ["net", "cluster"]
        );
        assert!(registry.contains("cluster"));
    }

    #[cfg(feature = "dashboard")]
    #[test]
    fn dashboard_adds_net_before_dashboard() {
        let config = with_empty_section(Config::default(), "dashboard");
        assert_eq!(
            prepare_startup(&config, &mut Registry::new()).unwrap(),
            ["net", "dashboard"]
        );
    }

    #[cfg(not(feature = "dashboard"))]
    #[test]
    fn configured_dashboard_requires_the_feature() {
        let config = with_empty_section(Config::default(), "dashboard");
        let error = prepare_startup(&config, &mut Registry::new()).unwrap_err();
        assert!(error.to_string().contains("`dashboard` feature"));
    }

    #[cfg(feature = "bootstrap")]
    #[test]
    fn bootstrap_must_not_repeat_dashboard() {
        let config = Config::default().with_bootstrap(["dashboard"]);
        let error = prepare_startup(&config, &mut Registry::new()).unwrap_err();
        assert!(error.to_string().contains("不应手工启动 `dashboard`"));
    }
}
