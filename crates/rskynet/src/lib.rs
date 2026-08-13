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
pub use rskynet_macros::{exclusive, msg, service, signal};

/// 网络层：socket / gate / agent。
#[cfg(feature = "net")]
pub use rskynet_net as net;

/// TLS 协议服务：底层复用 [`net`]，向业务投递明文事件。
#[cfg(feature = "tls")]
pub use rskynet_tls as tls;

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

/// 独立崩溃报告进程。标准 [`main::run`] 会自动安装；自定义入口应在启动节点前
/// 调用 [`crash::install`] 并把返回的 guard 留到进程结束。
#[cfg(feature = "signal")]
pub use rskynet_signal::crash;

/// 标准命令行入口：读取 TOML，并启动所有配置的自动注册服务。
#[cfg(feature = "main")]
pub mod main {
    use std::ffi::OsString;
    use std::process::ExitCode;

    use super::{Config, Error, Registry, Result};

    /// 使用进程参数启动节点。要求且只接受一个 TOML 配置路径。
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
        // 网络层只注册类型，不拉起：它不是系统服务，什么时候起由 `[bootstrap]`
        // 的清单说了算
        #[cfg(feature = "net")]
        {
            builder = builder.exclusive_service(rskynet_net::NAME, rskynet_net::NetService::new);
        }
        #[cfg(feature = "tls")]
        {
            builder = builder.service(rskynet_tls::NAME, rskynet_tls::TlsService::new);
        }
        builder
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// 与 [`rskynet_core::start`] 的区别是这里会先挂上内置服务、注入时间来源，
/// 所以使用方只管注册自己的服务类型。要自己拼这些，走 [`Builder`]。
pub fn start(config: Config, registry: Registry) -> Result<()> {
    #[cfg(feature = "cluster")]
    let (config, registry) = {
        let mut config = config;
        let mut registry = registry;
        prepare_cluster(&mut config, &mut registry)?;
        (config, registry)
    };
    Builder::new(config)
        .registry(registry)
        .with_builtins()
        .run()
}

#[cfg(feature = "cluster")]
fn prepare_cluster(config: &mut Config, registry: &mut Registry) -> Result<()> {
    use rskynet_cluster::{ClusterConfig, ClusterService, HandlerRegistry};

    if config
        .section::<ClusterConfig>(rskynet_cluster::NAME)?
        .is_none()
    {
        return Ok(());
    }

    let bootstrap = config.section_mut(rskynet_core::service::BOOTSTRAP);
    let bootstrap_kind = bootstrap
        .get("name")
        .and_then(toml::Value::as_str)
        .unwrap_or(rskynet_core::service::BOOTSTRAP);
    if bootstrap_kind != rskynet_core::service::BOOTSTRAP {
        return Err(Error::Config(
            "[cluster] 自动启动要求使用默认 bootstrap；定制启动请使用 Builder".into(),
        ));
    }

    let services = match bootstrap.remove("services") {
        Some(value) => value.try_into::<Vec<ServiceSpec>>()?,
        None => Vec::new(),
    };
    let mut net = None;
    let mut cluster = None;
    let mut business = Vec::new();
    for service in services {
        match service.name.as_str() {
            rskynet_net::NAME if net.is_none() => net = Some(service),
            rskynet_cluster::NAME if cluster.is_none() => cluster = Some(service),
            rskynet_net::NAME | rskynet_cluster::NAME => {}
            _ => business.push(service),
        }
    }
    let mut services = vec![
        net.unwrap_or_else(|| ServiceSpec::new(rskynet_net::NAME)),
        cluster.unwrap_or_else(|| ServiceSpec::new(rskynet_cluster::NAME)),
    ];
    services.extend(business);
    bootstrap.insert(
        "services".into(),
        toml::Value::try_from(services).expect("启动项一定能编成 TOML"),
    );

    if !registry.contains(rskynet_cluster::NAME) {
        let handlers =
            HandlerRegistry::from_auto().map_err(|error| Error::Config(error.to_string()))?;
        registry.register(rskynet_cluster::NAME, move || {
            ClusterService::new(handlers.clone())
        });
    }
    Ok(())
}

#[cfg(all(test, feature = "cluster"))]
mod cluster_start_tests {
    use super::*;
    use rskynet_cluster::{HandlerRegistry, RegistryExt as _};
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct BootstrapSection {
        services: Vec<ServiceSpec>,
    }

    fn clustered(services: impl IntoIterator<Item = impl Into<ServiceSpec>>) -> Config {
        let mut config = Config::default().with_bootstrap(services);
        config
            .section_mut(rskynet_cluster::NAME)
            .insert("node_id".into(), 1.into());
        config
    }

    #[test]
    fn cluster_section_prepends_and_deduplicates_infrastructure() {
        let mut config = clustered([
            ("cluster", "first"),
            ("net", "net-args"),
            ("cluster", "ignored"),
            ("business", "business-args"),
            ("net", "ignored"),
        ]);
        let mut registry = Registry::new();
        prepare_cluster(&mut config, &mut registry).unwrap();
        let section: BootstrapSection = config.section("bootstrap").unwrap().unwrap();
        let actual: Vec<(&str, &str)> = section
            .services
            .iter()
            .map(|service| (service.name.as_str(), service.args.as_str()))
            .collect();
        assert_eq!(
            actual,
            [
                ("net", "net-args"),
                ("cluster", "first"),
                ("business", "business-args")
            ]
        );
        assert!(registry.contains(rskynet_cluster::NAME));
    }

    #[test]
    fn missing_cluster_section_changes_nothing() {
        let mut config = Config::default().with_bootstrap(["business"]);
        let mut registry = Registry::new();
        prepare_cluster(&mut config, &mut registry).unwrap();
        let section: BootstrapSection = config.section("bootstrap").unwrap().unwrap();
        assert_eq!(section.services.len(), 1);
        assert_eq!(section.services[0].name, "business");
        assert!(!registry.contains(rskynet_cluster::NAME));
    }

    #[test]
    fn custom_bootstrap_is_rejected() {
        let mut config = clustered(std::iter::empty::<&str>());
        config
            .section_mut("bootstrap")
            .insert("name".into(), "custom".into());
        let error = prepare_cluster(&mut config, &mut Registry::new()).unwrap_err();
        assert!(error.to_string().contains("默认 bootstrap"));
    }

    #[test]
    fn explicit_cluster_registration_is_preserved() {
        let mut config = clustered(std::iter::empty::<&str>());
        let mut registry = Registry::new().with_cluster(HandlerRegistry::new());
        prepare_cluster(&mut config, &mut registry).unwrap();
        assert!(registry.contains(rskynet_cluster::NAME));
    }
}
