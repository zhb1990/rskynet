//! 节点启动与线程模型，对照 `skynet-src/skynet_start.c`。
//!
//! 线程构成与 C 版一致（少了 socket 线程与 monitor 线程，见 README 的取舍说明）：
//!
//! - 1 个定时器线程：每 2.5ms 推进时间轮，派发到期消息，并兜底唤醒睡着的 worker
//! - N 个 worker 线程：从全局队列取服务、跑消息与任务
//!
//! worker 的 `weight` 沿用 C 版那张表：前 4 个线程一次只处理一条消息（响应快），
//! 后面的线程一次处理 `队列长度 >> weight` 条（吞吐高）。

use std::thread;
use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::module::Registry;
use crate::server::Node;
use crate::timer::Wheel;

/// 各 worker 线程的消息批处理权重，直接照搬 C 版 `skynet_start.c` 里的常量表。
const WORKER_WEIGHTS: [i32; 32] = [
    -1, -1, -1, -1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3,
    3, 3,
];

/// 节点配置，对照 skynet 的 `config` 文件。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
        Ok(())
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
/// 再起引导服务，最后拉起线程池。任何一个服务调用 `Ctx::abort`，或者最后一个
/// 服务退出，都会让本函数返回。
pub fn start(config: Config, registry: Registry) -> Result<()> {
    config.validate()?;
    let node = Node::new(&config, registry);

    let logger = node.new_service(&config.logservice, &config.logger)?;
    node.set_logger(logger);
    node.handles.register_name(logger, "logger");
    // logger 不计入「服务数归零就退出」的判断，并且留到最后才送走，
    // 这样关停过程中产生的日志仍然写得出来
    node.reserve(logger);

    let (kind, args) = split_cmdline(&config.bootstrap);
    if let Err(err) = node.new_service(kind, args) {
        // 引导失败也要把日志刷出来，对照 C 版的 skynet_context_dispatchall
        drain(&node);
        node.retire_all();
        node.retire_reserved();
        drain(&node);
        return Err(err);
    }

    // 各线程共享同一个不可变引用，&Node 是 Copy 的，每个闭包各拿一份
    let shared: &Node = &node;
    thread::scope(|scope| {
        thread::Builder::new()
            .name("rskynet-timer".into())
            .spawn_scoped(scope, move || timer_loop(shared))
            .expect("定时器线程创建失败");

        for id in 0..config.thread {
            let weight = WORKER_WEIGHTS.get(id).copied().unwrap_or(0);
            thread::Builder::new()
                .name(format!("rskynet-worker-{id}"))
                .spawn_scoped(scope, move || worker_loop(shared, id, weight))
                .expect("worker 线程创建失败");
        }
    });

    // 线程都收工了，在主线程上把收尾工作做完：
    // 先把 logger 邮箱里积压的日志刷出来，再补一遍清理，最后送走 logger
    drain(&node);
    node.retire_all();
    drain(&node);
    node.retire_reserved();
    drain(&node);
    Ok(())
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

/// 定时器主循环，对照 C 版 `thread_timer`。
///
/// 时间轮就建在这个栈帧上，全节点只有本线程碰得到它，因此不必加锁。
fn timer_loop(node: &Node) {
    let mut wheel = Wheel::new();
    while !node.sched.is_quit() {
        node.fire_timers(&mut wheel);
        if node.total() == 0 {
            break;
        }
        node.sched.poke();
        thread::sleep(Duration::from_micros(2500));
    }
    node.sched.set_quit();
}

/// 在当前线程上把运行队列里剩下的活干完，用于启动失败或退出时刷日志。
///
/// 主线程没有本地队列，只取 injector——此时 worker 都已收工并把本地队列倒进去了。
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
        assert_eq!(split_cmdline("bootstrap pong; ping 100"), ("bootstrap", "pong; ping 100"));
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

    /// 仓库里附带的示例配置必须始终可用
    #[test]
    fn shipped_example_config_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/dev.toml");
        let config = Config::from_toml_file(path).expect("示例配置应可用");
        assert!(config.thread >= 1);
    }
}
