//! 节点启动与线程模型，对照 `skynet-src/skynet_start.c`。
//!
//! 线程构成与 C 版一样多，只是归属变了（少了 monitor 线程，见 README 的取舍说明）：
//!
//! - N 个 worker 线程：从运行队列取服务、跑消息与任务
//! - 每个独占服务一条线程：定时器与日志各一条，网络层也是一条。
//!   C 版那是内核里的专用线程，这里它们是普通服务，见 [`crate::Exclusive`]
//!
//! worker 之间没有分工：C 版那张按线程编号分档的权重表这里没有照搬，让渡改成
//! 由争用情况决定，见 [`crate::server::Node::run_service`] 与 README。
//!
//! # 系统服务
//!
//! 日志、定时器、引导这三个服务的实现都不在内核里（各是一个独立 crate），内核
//! 只做两件事：按配置里那一段的 `name` 把它们拉起来，以及定下先后顺序。顺序是
//! 日志 → 定时器 → 引导：日志最先，好让后面每一步出的岔子都有人记；定时器排在
//! 引导之前，于是引导期间刻度就在走，那时挂的表、打的日志时间戳都是准的。

use std::any::Any;
use std::sync::Arc;
use std::thread;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::clock::Timer;
use crate::error::{Error, Result};
use crate::module::Registry;
use crate::server::Node;
use crate::service;

/// 系统服务在配置里的样子。内核只认一个 `name`（服务类型名），段里其余的键
/// 一概不看——那是认领这一段的服务自己的事，它在 `init` 里用
/// [`crate::NodeRef::section`] 取。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct SystemService {
    name: Option<String>,
}

/// 节点配置，对照 skynet 的 `config` 文件。
///
/// 内核认的只有这三个标量，剩下的全是段：`[logger]`、`[timer]`、`[bootstrap]`
/// 归三个内置服务，`[net]` 之类归各自的服务。内核对系统服务的段只读一个 `name`，
/// 段里其余内容原样留着，由服务自己解析。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// worker 线程数。
    pub thread: usize,
    /// 本节点编号，会占据 handle 的高 8 位。
    pub harbor: u32,
    /// 是否统计各服务的消息处理耗时。
    pub profile: bool,
    /// 所有配置段原样留一份，认领它的服务在自己的 `init` 里用
    /// [`Config::section`] 各取所需。
    #[serde(flatten)]
    sections: toml::Table,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            thread: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            harbor: 0,
            profile: true,
            sections: toml::Table::new(),
        }
    }
}

impl Config {
    pub fn from_toml_str(text: &str) -> Result<Self> {
        let config: Config = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_toml_file(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| Error::ConfigIo {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_toml_str(&text)
    }

    #[must_use]
    pub fn with_thread(mut self, thread: usize) -> Self {
        self.thread = thread;
        self
    }

    /// 取某一段配置，例如 `config.section::<NetConfig>("net")`。
    ///
    /// 段不存在时返回 `Ok(None)`，由认领它的服务自己决定是走默认值还是报错。
    /// 段里多出来的键一律忽略，所以服务不必把 `name` 这类内核字段也声明一遍。
    pub fn section<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        match self.sections.get(name) {
            None => Ok(None),
            Some(value) => Ok(Some(T::deserialize(value.clone())?)),
        }
    }

    /// 塞一个配置段，给「不从 TOML 来」的场景（测试、代码里搭配置）用。
    #[must_use]
    pub fn with_section(mut self, name: impl Into<String>, section: toml::Table) -> Self {
        self.sections
            .insert(name.into(), toml::Value::Table(section));
        self
    }

    /// 取某一段的可写引用，段不存在就先建一张空表。
    ///
    /// 服务包用它给自己那一段加链式配置方法，例如 `rskynet-bootstrap` 的
    /// `with_bootstrap`。段里原本是个标量（写配置的人拼错了）时按空表处理，
    /// 反正 [`Config::validate`] 会在启动前把它拦下来。
    pub fn section_mut(&mut self, name: &str) -> &mut toml::Table {
        let entry = self
            .sections
            .entry(name.to_string())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if !entry.is_table() {
            *entry = toml::Value::Table(toml::Table::new());
        }
        entry.as_table_mut().expect("上一句已经保证它是表")
    }

    /// 某个系统服务该用哪个类型名：段里写了就用段里的，没写就用内置的默认名。
    ///
    /// 返回空串表示「不拉起这个服务」，写配置的人可以用 `name = ""` 显式关掉。
    fn system_kind(&self, section: &str, fallback: &str) -> Result<String> {
        let spec: Option<SystemService> = self.section(section)?;
        Ok(spec
            .and_then(|spec| spec.name)
            .unwrap_or_else(|| fallback.to_string()))
    }

    fn validate(&self) -> Result<()> {
        if self.thread == 0 {
            return Err(Error::Config("worker 线程数必须大于 0".into()));
        }
        if self.harbor > 0xff {
            return Err(Error::Config("harbor 编号必须在 0..=255 之间".into()));
        }
        // `flatten` 把不认识的键统统收进 sections，于是「拼错内核字段就报错」这条
        // 防线得自己补上：配置段一律是表，落在顶层的散键只可能是拼错
        for (key, value) in &self.sections {
            if !value.is_table() {
                return Err(Error::Config(format!(
                    "不认识的配置项 `{key}`；扩展的配置要写成 `[{key}]` 这样的段"
                )));
            }
        }
        Ok(())
    }
}

/// 节点构建器，[`start`] 的链式写法。
///
/// 时间来源必须注入，内核里没有现成的实现：
///
/// ```no_run
/// # use std::sync::Arc;
/// # use rskynet_core::{Builder, Config, Registry, Timer};
/// # struct MyTimer;
/// # impl Timer for MyTimer {
/// #     fn timeout(&self, _handle: u32, _session: i32, _ticks: u32) {}
/// #     fn now(&self) -> u64 { 0 }
/// #     fn wall_clock(&self) -> u64 { 0 }
/// #     fn start_seconds(&self) -> u64 { 0 }
/// # }
/// Builder::new(Config::default())
///     .registry(Registry::new())
///     .timer(Arc::new(MyTimer))
///     .run()
///     .unwrap();
/// ```
pub struct Builder {
    config: Config,
    registry: Registry,
    timer: Option<Arc<dyn Timer>>,
}

impl Builder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            registry: Registry::new(),
            timer: None,
        }
    }

    /// 服务类型表，覆盖式设置。
    #[must_use]
    pub fn registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// 注入时间来源，不注入就起不来，见 [`crate::Timer`]。
    #[must_use]
    pub fn timer(mut self, timer: Arc<dyn Timer>) -> Self {
        self.timer = Some(timer);
        self
    }

    /// 往注册表里补一个服务类型，[`Registry::register`] 的链式转发。
    #[must_use]
    pub fn service<S, F>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: crate::Service,
    {
        self.registry.register(kind, factory);
        self
    }

    /// 往注册表里补一个独占线程的服务类型，[`Registry::register_exclusive`] 的
    /// 链式转发。服务包用它把「注册类型」和「注入对象」写成一句话。
    #[must_use]
    pub fn exclusive_service<S, F>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: crate::Exclusive,
    {
        self.registry.register_exclusive(kind, factory);
        self
    }

    /// 启动节点并阻塞到所有服务退出。
    pub fn run(self) -> Result<()> {
        self.config.validate()?;
        let timer = self.timer.ok_or(Error::MissingTimer)?;
        run(self.config, self.registry, timer)
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// 流程与 C 版 `skynet_start` 一致：先起 logger 并占用 `.logger` 这个名字，
/// 再起定时器与引导服务，最后拉起 worker 线程池。任何一个服务调用 `Ctx::abort`，
/// 或者最后一个服务退出，都会让本函数返回。
pub fn start(config: Config, registry: Registry, timer: Arc<dyn Timer>) -> Result<()> {
    Builder::new(config).registry(registry).timer(timer).run()
}

fn run(config: Config, registry: Registry, timer: Arc<dyn Timer>) -> Result<()> {
    let node = Node::new(&config, registry, timer);

    // 引导失败也要走一遍收尾：logger 可能已经起来了，它邮箱里的日志得刷出去，
    // 那条独占线程也得收工。对照 C 版的 skynet_context_dispatchall
    if let Err(err) = boot(&node, &config) {
        // 万一独占线程也 panic 了，让位给信息量更大的那个启动错误
        let _ = shutdown(&node);
        return Err(err);
    }
    // 从这一刻起，「服务数为 0」才真的意味着节点该收工了。定时器盯着这个信号，
    // 见 [`crate::NodeRef::is_booted`]
    node.mark_booted();

    // 各 worker 共享同一个不可变引用，&Node 是 Copy 的，每个闭包各拿一份
    let shared: &Node = &node;
    let panic = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(config.thread);
        for id in 0..config.thread {
            workers.push(
                thread::Builder::new()
                    .name(format!("rskynet-worker-{id}"))
                    .spawn_scoped(scope, move || worker_loop(shared, id))
                    .expect("worker 线程创建失败"),
            );
        }
        // 显式 join 是为了把 worker 里的 panic 接住（隐式 join 会当场抛出去）。
        // 重抛留到收尾之后：独占线程还等着「自己被摘除」这个通知，先抛就没人给了
        let mut panic = None;
        for worker in workers {
            if let Err(payload) = worker.join() {
                panic = panic.or(Some(payload));
            }
        }
        panic
    });

    // worker 收工了，在主线程上把尾收完
    let panic = panic.or(shutdown(&node));
    if let Some(payload) = panic {
        std::panic::resume_unwind(payload);
    }
    Ok(())
}

/// 按配置拉起三个系统服务，对照 C 版 `skynet_start` 里 worker 之前那一段。
fn boot(node: &Arc<Node>, config: &Config) -> Result<()> {
    // 日志最先：从这里开始，后面每一步的动静都有人记
    let kind = config.system_kind(service::LOGGER, service::LOGGER)?;
    if !kind.trim().is_empty() {
        let logger = node.new_service(&kind, "")?;
        node.set_logger(logger);
        node.handles.register_name(logger, service::LOGGER);
        // logger 不计入「服务数归零就退出」的判断，并且留到最后才送走，
        // 这样关停过程中产生的日志仍然写得出来
        node.reserve(logger);
    }

    // 定时器排在引导之前，于是引导期间刻度就在走：那会儿挂的表立刻开始计时，
    // 日志时间戳也不再是一片 0。代价是定时器服务得自己判断「节点是否已经
    // 有过服务」，否则它一上来就会看到服务数为 0 而宣布收工
    let kind = config.system_kind(service::TIMER, service::TIMER)?;
    if !kind.trim().is_empty() {
        let timer = node.new_service(&kind, "")?;
        node.handles.register_name(timer, service::TIMER);
        node.reserve(timer);
    }

    // 引导服务自己去读 `[bootstrap]` 段里的清单，内核不掺和
    let kind = config.system_kind(service::BOOTSTRAP, service::BOOTSTRAP)?;
    if !kind.trim().is_empty() {
        node.new_service(&kind, "")?;
    }
    Ok(())
}

/// 节点收尾：把欠着的活干完，再按「普通服务 → 保留服务」的顺序送走所有人。
///
/// 普通服务的销毁由主线程接手（worker 都收工了），独占服务各自在自己那条线程上
/// 收尾——摘除就是通知，[`Node::retire`] 会把它们叫醒。保留服务（logger 与定时器）
/// 留到最后，而 logger 又是保留服务里最后走的那个，这样关停过程中每一条日志都
/// 还有人写。
///
/// 返回独占线程里没接住的 panic，由调用方重抛。
fn shutdown(node: &Arc<Node>) -> Option<Box<dyn Any + Send + 'static>> {
    let mut panic = None;
    drain(node);
    node.retire_all();
    drain(node);
    panic = panic.or(node.join_retired_exclusives());

    // 保留服务分两批走：先送走定时器之类，等它们的线程收完工，logger 才最后走
    node.retire_reserved(true);
    drain(node);
    panic = panic.or(node.join_retired_exclusives());
    node.retire_reserved(false);
    drain(node);
    panic.or(node.join_exclusives())
}

/// worker 主循环，对照 C 版 `thread_worker`。
fn worker_loop(node: &Node, id: usize) {
    // 绑定本线程的运行队列：从此本线程的投递与取活优先走它，取空了才去偷别人的
    let _worker = node.sched.register_worker(id);
    let mut hold = None;
    while !node.sched.is_quit() {
        hold = node.dispatch(hold.take());
        if hold.is_none() {
            // 找活：先自旋几轮，还是空手就登记空闲位并挂起，等人定向叫醒
            hold = node.sched.find_work_or_park();
        }
    }
    // 收工时手里可能还捏着一个服务：它的 in_global 仍是置位的，却已经不在运行
    // 队列里了。必须放回去，否则收尾流程找不到它，它邮箱里的消息（比如最后几条
    // 日志）就永远没人处理。
    if let Some(ctx) = hold {
        node.sched.push(ctx);
    }
    // 本地队列里剩下的服务同理。收尾在主线程上做，而主线程取不到别人的本地队列，
    // 所以必须由 owner 自己倒进 injector。
    node.sched.flush_local();
}

/// 在当前线程上把运行队列里剩下的活干完，用于启动失败或退出时的清理。
///
/// 主线程没有本地队列，只取 injector——此时 worker 都已收工并把本地队列倒进去了。
/// 独占服务从不进运行队列，所以这里碰不到它们，也就不会破坏「一个服务只由一条
/// 线程执行」那条不变量。
fn drain(node: &Node) {
    let mut hold = None;
    loop {
        hold = node.dispatch(hold);
        if hold.is_none() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 配置能从 TOML 解析，未写的字段走默认值
    #[test]
    fn config_parses_from_toml() {
        let config = Config::from_toml_str(
            r#"
            thread = 4
            harbor = 1
            profile = false

            [logger]
            path = "run/rskynet.log"
            "#,
        )
        .expect("配置应解析成功");
        assert_eq!(config.thread, 4);
        assert_eq!(config.harbor, 1);
        assert!(!config.profile);
        // 段里没写 name，系统服务就走内置的默认类型名
        assert_eq!(
            config
                .system_kind(service::LOGGER, service::LOGGER)
                .unwrap(),
            service::LOGGER
        );
        // 段整个缺席也是一样
        assert_eq!(
            config.system_kind(service::TIMER, service::TIMER).unwrap(),
            service::TIMER
        );
    }

    /// 系统服务的类型名可以换掉，空串表示不拉起
    #[test]
    fn system_service_kind_is_configurable() {
        let config = Config::from_toml_str(
            r#"
            [logger]
            name = "my-logger"

            [timer]
            name = ""
            "#,
        )
        .expect("配置应解析成功");
        assert_eq!(
            config
                .system_kind(service::LOGGER, service::LOGGER)
                .unwrap(),
            "my-logger"
        );
        assert!(
            config
                .system_kind(service::TIMER, service::TIMER)
                .unwrap()
                .is_empty()
        );
    }

    /// 非法取值和拼错的键都要在启动前拦下
    #[test]
    fn invalid_config_is_rejected() {
        assert!(Config::from_toml_str("thread = 0").is_err());
        assert!(Config::from_toml_str("harbor = 256").is_err());
        assert!(Config::from_toml_str("不认识的键 = 1").is_err());
    }

    /// 没注入时间来源就不许启动
    #[test]
    fn missing_timer_is_rejected() {
        let err = Builder::new(Config::default())
            .registry(Registry::new())
            .run()
            .expect_err("没注入 timer 应当启动失败");
        assert!(matches!(err, Error::MissingTimer));
    }

    /// 配置错误比缺少 timer 先报，免得写配置的人被后一个错误带偏
    #[test]
    fn config_errors_come_first() {
        let err = Builder::new(Config::default().with_thread(0))
            .run()
            .expect_err("线程数为 0 应当启动失败");
        assert!(matches!(err, Error::Config(_)));
    }

    /// 扩展的配置段原样留着，由认领它的服务自己解析；内核不认识也不该报错
    #[test]
    fn unclaimed_sections_are_kept_for_services() {
        #[derive(Debug, Deserialize)]
        struct NetConfig {
            listen: String,
            backlog: u32,
        }

        let config = Config::from_toml_str(
            r#"
            thread = 2

            [net]
            listen = "0.0.0.0:8888"
            backlog = 128
            "#,
        )
        .expect("不认领的段不该被内核拒掉");
        let net: NetConfig = config.section("net").unwrap().expect("应取到 net 段");
        assert_eq!(net.listen, "0.0.0.0:8888");
        assert_eq!(net.backlog, 128);

        // 没有的段返回 None，段里的字段类型不对则报错
        assert!(config.section::<NetConfig>("cluster").unwrap().is_none());
        let empty = Config::default().with_section("net", toml::Table::new());
        assert!(empty.section::<NetConfig>("net").is_err());
    }

    /// section_mut 给服务包拼自己那一段用：段不存在就先建一张空表
    #[test]
    fn section_mut_creates_the_section() {
        let mut config = Config::default();
        config
            .section_mut("bootstrap")
            .insert("name".into(), "my-boot".into());
        assert_eq!(
            config
                .system_kind(service::BOOTSTRAP, service::BOOTSTRAP)
                .unwrap(),
            "my-boot"
        );
    }

    /// 仓库里附带的示例配置必须始终可用
    #[test]
    fn shipped_example_config_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/dev.toml");
        let config = Config::from_toml_file(path).expect("示例配置应可用");
        assert!(config.thread >= 1);
    }
}
