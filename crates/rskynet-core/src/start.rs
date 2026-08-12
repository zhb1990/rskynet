//! 节点启动与线程模型，对照 `skynet-src/skynet_start.c`。
//!
//! 线程构成与 C 版一样多，只是归属变了（少了 monitor 线程，见 README 的取舍说明）：
//!
//! - N 个 worker 线程：从运行队列取服务、跑消息与任务
//! - 每个独占服务一条线程：定时器与日志各一条，将来网络层也是一条。
//!   C 版那是内核里的专用线程，这里它们是普通服务，见 [`crate::Exclusive`]
//!
//! worker 的 `weight` 沿用 C 版那张表：前 4 个线程一次只处理一条消息（响应快），
//! 后面的线程一次处理 `队列长度 >> weight` 条（吞吐高）。

use std::any::Any;
use std::sync::Arc;
use std::thread;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::module::Registry;
use crate::server::Node;

/// 各 worker 线程的消息批处理权重，直接照搬 C 版 `skynet_start.c` 里的常量表。
const WORKER_WEIGHTS: [i32; 32] = [
    -1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
    3, 3,
];

/// 节点配置，对照 skynet 的 `config` 文件。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// worker 线程数。
    pub thread: usize,
    /// 本节点编号，会占据 handle 的高 8 位。
    pub harbor: u32,
    /// 引导服务，格式是「类型名 参数」，例如 `"bootstrap pong; ping 100"`。
    pub bootstrap: String,
    /// 日志服务的类型名。
    pub logservice: String,
    /// 传给日志服务的参数：日志文件路径，留空表示只写标准输出。
    pub logger: String,
    /// 是否统计各服务的消息处理耗时。
    pub profile: bool,
    /// 扩展的配置段：`[net]` 这类内核不认领的表原样留在这里，
    /// 由认领它的服务在自己的 `init` 里用 [`Config::section`] 各取所需。
    #[serde(flatten)]
    extra: toml::Table,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            thread: thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            harbor: 0,
            bootstrap: format!("{} ", crate::service::BOOTSTRAP),
            logservice: crate::service::LOGGER.to_string(),
            logger: String::new(),
            profile: true,
            extra: toml::Table::new(),
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

    /// 覆盖引导服务，链式配置用。
    #[must_use]
    pub fn with_bootstrap(mut self, bootstrap: impl Into<String>) -> Self {
        self.bootstrap = bootstrap.into();
        self
    }

    #[must_use]
    pub fn with_thread(mut self, thread: usize) -> Self {
        self.thread = thread;
        self
    }

    /// 取某一段配置，例如 `config.section::<NetConfig>("net")`。
    ///
    /// 段不存在时返回 `Ok(None)`，由认领它的服务自己决定是走默认值还是报错。
    pub fn section<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>> {
        match self.extra.get(name) {
            None => Ok(None),
            Some(value) => Ok(Some(T::deserialize(value.clone())?)),
        }
    }

    /// 塞一个配置段，给「不从 TOML 来」的场景（测试、代码里搭配置）用。
    #[must_use]
    pub fn with_section(mut self, name: impl Into<String>, section: toml::Table) -> Self {
        self.extra.insert(name.into(), toml::Value::Table(section));
        self
    }

    fn validate(&self) -> Result<()> {
        if self.thread == 0 {
            return Err(Error::Config("worker 线程数必须大于 0".into()));
        }
        if self.harbor > 0xff {
            return Err(Error::Config("harbor 编号必须在 0..=255 之间".into()));
        }
        if self.bootstrap.trim().is_empty() {
            return Err(Error::Config("必须指定引导服务".into()));
        }
        // `flatten` 把不认识的键统统收进 extra，于是「拼错内核字段就报错」这条防线
        // 得自己补上：扩展的配置一律写成表，落在顶层的散键只可能是拼错
        for (key, value) in &self.extra {
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
/// ```no_run
/// # use rskynet_core::{Builder, Config, Registry};
/// Builder::new(Config::default())
///     .registry(Registry::new().with_builtins())
///     .run()
///     .unwrap();
/// ```
pub struct Builder {
    config: Config,
    registry: Registry,
}

impl Builder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            registry: Registry::new(),
        }
    }

    /// 服务类型表，覆盖式设置。
    #[must_use]
    pub fn registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// 启动节点并阻塞到所有服务退出。
    pub fn run(self) -> Result<()> {
        run(self.config, self.registry)
    }
}

/// 把「类型名 参数」拆成两截，对照 C 版 `bootstrap()` 里的 `sscanf`。
pub(crate) fn split_cmdline(cmdline: &str) -> (&str, &str) {
    let cmdline = cmdline.trim();
    match cmdline.split_once(char::is_whitespace) {
        Some((kind, args)) => (kind, args.trim_start()),
        None => (cmdline, ""),
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// 流程与 C 版 `skynet_start` 一致：先起 logger 并占用 `.logger` 这个名字，
/// 再起引导服务与定时器，最后拉起 worker 线程池。任何一个服务调用 `Ctx::abort`，
/// 或者最后一个服务退出，都会让本函数返回。
pub fn start(config: Config, registry: Registry) -> Result<()> {
    Builder::new(config).registry(registry).run()
}

fn run(config: Config, registry: Registry) -> Result<()> {
    config.validate()?;
    let node = Node::new(&config, registry);

    // 引导失败也要走一遍收尾：logger 可能已经起来了，它邮箱里的日志得刷出去，
    // 那条独占线程也得收工。对照 C 版的 skynet_context_dispatchall
    if let Err(err) = boot(&node, &config) {
        // 万一独占线程也 panic 了，让位给信息量更大的那个启动错误
        let _ = shutdown(&node);
        return Err(err);
    }

    // 各 worker 共享同一个不可变引用，&Node 是 Copy 的，每个闭包各拿一份
    let shared: &Node = &node;
    let panic = thread::scope(|scope| {
        let mut workers = Vec::with_capacity(config.thread);
        for id in 0..config.thread {
            let weight = WORKER_WEIGHTS.get(id).copied().unwrap_or(0);
            workers.push(
                thread::Builder::new()
                    .name(format!("rskynet-worker-{id}"))
                    .spawn_scoped(scope, move || worker_loop(shared, id, weight))
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

/// 起 logger、引导服务与定时器，对照 C 版 `skynet_start` 里 worker 之前那一段。
fn boot(node: &Arc<Node>, config: &Config) -> Result<()> {
    let logger = node.new_service(&config.logservice, &config.logger)?;
    node.set_logger(logger);
    node.handles.register_name(logger, "logger");
    // logger 不计入「服务数归零就退出」的判断，并且留到最后才送走，
    // 这样关停过程中产生的日志仍然写得出来
    node.reserve(logger);

    let (kind, args) = split_cmdline(&config.bootstrap);
    node.new_service(kind, args)?;

    // 定时器必须排在引导服务之后：它一看到「活着的服务数归零」就宣布节点收工，
    // 而引导服务出场之前那个数字本来就是 0
    let timer = node.new_service(crate::service::TIMER, "")?;
    node.handles.register_name(timer, "timer");
    node.reserve(timer);
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
fn worker_loop(node: &Node, id: usize, weight: i32) {
    // 绑定本线程的运行队列：从此本线程的投递与取活优先走它，取空了才去偷别人的
    let _worker = node.sched.register_worker(id);
    let mut hold = None;
    while !node.sched.is_quit() {
        hold = node.dispatch(hold.take(), weight);
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
        hold = node.dispatch(hold, -1);
        if hold.is_none() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 引导命令按首个空白拆成服务名与参数
    #[test]
    fn cmdline_splits_at_first_space() {
        assert_eq!(
            split_cmdline("bootstrap pong; ping 100"),
            ("bootstrap", "pong; ping 100")
        );
        assert_eq!(split_cmdline("  logger  "), ("logger", ""));
        assert_eq!(split_cmdline("logger\tfile.log"), ("logger", "file.log"));
    }

    /// 配置能从 TOML 解析，未写的字段走默认值
    #[test]
    fn config_parses_from_toml() {
        let config = Config::from_toml_str(
            r#"
            thread = 4
            harbor = 1
            bootstrap = "bootstrap ping"
            logger = "run/rskynet.log"
            profile = false
            "#,
        )
        .expect("配置应解析成功");
        assert_eq!(config.thread, 4);
        assert_eq!(config.harbor, 1);
        assert_eq!(config.bootstrap, "bootstrap ping");
        assert_eq!(config.logger, "run/rskynet.log");
        assert!(!config.profile);
        // 没写的字段走默认值
        assert_eq!(config.logservice, crate::service::LOGGER);
    }

    /// 非法取值和拼错的键都要在启动前拦下
    #[test]
    fn invalid_config_is_rejected() {
        assert!(Config::from_toml_str("thread = 0").is_err());
        assert!(Config::from_toml_str("harbor = 256").is_err());
        assert!(Config::from_toml_str(r#"bootstrap = "  ""#).is_err());
        assert!(Config::from_toml_str("不认识的键 = 1").is_err());
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

    /// 仓库里附带的示例配置必须始终可用
    #[test]
    fn shipped_example_config_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/dev.toml");
        let config = Config::from_toml_file(path).expect("示例配置应可用");
        assert!(config.thread >= 1);
    }
}
