//! 节点启动与线程模型，对照 `skynet-src/skynet_start.c`。
//!
//! 线程构成与 C 版一样多，只是专用 IO 线程的归属变了：
//!
//! - N 个 worker 线程：从运行队列取服务、跑消息与任务
//! - 1 个 monitor 线程：每 5 秒检查一次 worker 是否卡在同一次 Future poll
//! - 每个独占服务一条线程：定时器与日志各一条，网络层也是一条。
//!   C 版那是内核里的专用线程，这里它们是普通服务，见 [`crate::Exclusive`]
//!
//! worker 之间没有分工：C 版那张按线程编号分档的权重表这里没有照搬，让渡改成
//! 由争用情况决定，见 [`crate::server::Node::run_service`] 与 README。
//!
//! # 系统服务
//!
//! 日志、信号、定时器、引导这些服务的实现都不在内核里（各是一个独立 crate），内核
//! 只做两件事：按配置里那一段的 `name` 把它们拉起来，以及定下先后顺序。顺序是
//! 日志 → 信号 → 定时器 → 可选基础设施 → 引导：每一步都等完整 `init`；日志最先，
//! 好让后面每一步出的岔子都有人记，定时器排在业务引导之前，于是那时挂的表、打的
//! 日志时间戳都是准的。

use std::any::Any;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::clock::Timer;
use crate::error::{Error, Result};
use crate::module::Registry;
use crate::monitor::Monitor;
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
/// 内核认的只有这两个标量，剩下的全是段：`[logger]`、`[signal]`、`[timer]`、`[bootstrap]`
/// 归四个内置服务，`[net]` 之类归各自的服务。内核对系统服务的段只读一个 `name`，
/// 段里其余内容原样留着，由服务自己解析。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// worker 线程数。
    pub thread: usize,
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

    /// 配置中是否显式存在某个段。空段同样返回 true。
    pub fn has_section(&self, name: &str) -> bool {
        self.sections.contains_key(name)
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
/// #     fn timeout(&self, _handle: u32, _session: u64, _delay_ms: u32) {}
/// #     fn now(&self) -> u64 { 0 }
/// #     fn wall_clock(&self) -> u64 { 0 }
/// #     fn start_time(&self) -> u64 { 0 }
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
    startup_services: Vec<(String, String)>,
}

impl Builder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            registry: Registry::new(),
            timer: None,
            startup_services: Vec::new(),
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

    /// 在 timer 完整初始化之后、bootstrap 之前启动并等待一个基础设施服务。
    #[must_use]
    pub fn startup_service(mut self, kind: impl Into<String>, args: impl Into<String>) -> Self {
        self.startup_services.push((kind.into(), args.into()));
        self
    }

    /// 启动节点并阻塞到所有服务退出。
    pub fn run(self) -> Result<()> {
        self.config.validate()?;
        let timer = self.timer.ok_or(Error::MissingTimer)?;
        run(self.config, self.registry, timer, self.startup_services)
    }
}

/// 启动节点并阻塞到所有服务退出。
///
/// worker 与 monitor 先进入运行状态，随后主线程依次等待 logger、signal、timer、
/// 可选基础设施与默认 bootstrap 完整初始化。任何一方调用 `NodeRef::abort`，或者
/// 启动完成后最后一个服务退出，都会让本函数返回。
pub fn start(config: Config, registry: Registry, timer: Arc<dyn Timer>) -> Result<()> {
    Builder::new(config).registry(registry).timer(timer).run()
}

fn run(
    config: Config,
    registry: Registry,
    timer: Arc<dyn Timer>,
    startup_services: Vec<(String, String)>,
) -> Result<()> {
    let node = Node::new(&config, registry, timer);

    let monitors: Vec<_> = (0..config.thread)
        .map(|_| Arc::new(Monitor::new()))
        .collect();

    // 各 worker 共享同一个不可变引用，&Node 是 Copy 的，每个闭包各拿一份
    let shared: &Node = &node;
    let mut boot_panic: Option<Box<dyn Any + Send + 'static>> = None;
    let (boot_result, panic) = thread::scope(|scope| {
        let (stop_monitor, stopped) = mpsc::channel();
        let observed = &monitors;
        let monitor_thread = thread::Builder::new()
            .name("rskynet-monitor".to_string())
            .spawn_scoped(scope, move || monitor_loop(shared, observed, stopped))
            .expect("monitor 线程创建失败");

        let mut workers = Vec::with_capacity(config.thread);
        for (id, monitor) in monitors.iter().cloned().enumerate() {
            workers.push(
                thread::Builder::new()
                    .name(format!("rskynet-worker-{id}"))
                    .spawn_scoped(scope, move || worker_loop(shared, id, monitor))
                    .expect("worker 线程创建失败"),
            );
        }
        // worker 已经在跑，下面同步等待 init 时，服务仍能处理 RPC、timer 与 IO 回包。
        //
        // boot 跑在主线程上，panic 不能直接穿过 scope 闭包：那样 Rust 会先隐式
        // join 所有 scoped worker，而此刻还没人叫 quit，join 会永久卡住。
        // 先接住 panic、叫醒所有 worker，等收尾完成后再重抛。
        let boot_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            boot(&node, &config, &startup_services)
        })) {
            Ok(result) => result,
            Err(payload) => {
                boot_panic = Some(payload);
                node.quit();
                Err(Error::Service("启动过程发生 panic".into()))
            }
        };
        if boot_result.is_ok() {
            node.mark_booted();
        } else {
            node.quit();
        }
        // 显式 join 是为了把 worker 里的 panic 接住（隐式 join 会当场抛出去）。
        // 重抛留到收尾之后：独占线程还等着「自己被摘除」这个通知，先抛就没人给了
        let mut panic = None;
        for worker in workers {
            if let Err(payload) = worker.join() {
                panic = panic.or(Some(payload));
            }
        }
        // recv_timeout 会被这条消息立刻打断，正常关停不必等完 5 秒检查周期。
        let _ = stop_monitor.send(());
        if let Err(payload) = monitor_thread.join() {
            panic = panic.or(Some(payload));
        }
        (boot_result, panic)
    });

    // worker 收工了，在主线程上把尾收完
    let shutdown_panic = shutdown(&node);
    let panic = boot_panic.take().or(panic).or(shutdown_panic);
    if let Some(payload) = panic {
        std::panic::resume_unwind(payload);
    }
    boot_result
}

/// 给系统服务绑定约定名字。名字已被别的 handle 占用时启动失败；
/// 若服务自己在 init 里已经注册过同一个名字，则视为成功。
fn bind_system_name(node: &Arc<Node>, handle: u32, name: &str) -> Result<()> {
    if node.handles.register_name(handle, name) || node.handles.find_name(name) == Some(handle) {
        return Ok(());
    }
    Err(Error::Service(format!(
        "系统服务 `{name}` 无法注册到 :{handle:08x}"
    )))
}

/// 按配置依次拉起基础服务、可选启动项与 bootstrap。
fn boot(node: &Arc<Node>, config: &Config, startup_services: &[(String, String)]) -> Result<()> {
    // 日志最先：从这里开始，后面每一步的动静都有人记
    let kind = config.system_kind(service::LOGGER, service::LOGGER)?;
    if !kind.trim().is_empty() {
        let logger = node.new_service(&kind, "")?;
        logger.init.wait()?;
        node.set_logger(logger.handle);
        bind_system_name(node, logger.handle, service::LOGGER)?;
        // logger 不计入「服务数归零就退出」的判断，并且留到最后才送走，
        // 这样关停过程中产生的日志仍然写得出来
        node.reserve(logger.handle);
    }

    // 信号服务也必须留到普通服务全部退出之后；否则关停途中再来的信号会落回
    // 操作系统默认动作，直接截断日志与邮箱的排空。
    let kind = config.system_kind(service::SIGNAL, service::SIGNAL)?;
    if !kind.trim().is_empty() {
        let signal = node.new_service(&kind, "")?;
        signal.init.wait()?;
        bind_system_name(node, signal.handle, service::SIGNAL)?;
        node.reserve(signal.handle);
    }

    // 定时器排在引导之前，于是引导期间刻度就在走：那会儿挂的表立刻开始计时，
    // 日志时间戳也不再是一片 0。代价是定时器服务得自己判断「节点是否已经
    // 有过服务」，否则它一上来就会看到服务数为 0 而宣布收工
    let kind = config.system_kind(service::TIMER, service::TIMER)?;
    if !kind.trim().is_empty() {
        let timer = node.new_service(&kind, "")?;
        timer.init.wait()?;
        bind_system_name(node, timer.handle, service::TIMER)?;
        node.reserve(timer.handle);
    }

    for (kind, args) in startup_services {
        node.new_service(kind, args)?.init.wait()?;
    }

    // 引导服务自己去读 `[bootstrap]` 段里的清单，内核不掺和
    let kind = config.system_kind(service::BOOTSTRAP, service::BOOTSTRAP)?;
    if !kind.trim().is_empty() {
        let bootstrap = node.new_service(&kind, "")?;
        if kind == service::BOOTSTRAP {
            bootstrap.init.wait()?;
        }
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
fn worker_loop(node: &Node, id: usize, monitor: Arc<Monitor>) {
    let _monitor = crate::monitor::Binding::install(monitor);
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

/// 对照 C 版 `thread_monitor`：每 5 秒扫描一遍，但每秒都给关停信号一次立即打断的机会。
fn monitor_loop(node: &Node, monitors: &[Arc<Monitor>], stop: mpsc::Receiver<()>) {
    loop {
        for monitor in monitors {
            if let Some((source, destination, version)) = monitor.check() {
                node.report_endless(source, destination, version);
            }
        }
        for _ in 0..5 {
            match stop.recv_timeout(Duration::from_secs(1)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
        }
    }
}

/// 在当前线程上把运行队列里剩下的活干完，用于启动失败或退出时的清理。
///
/// 主线程没有本地队列，只取 injector——此时 worker 都已收工并把本地队列倒进去了。
/// 独占服务从不进运行队列，所以这里碰不到它们，也就不会破坏「一个服务只由一条
/// 线程执行」那条不变量。
fn drain(node: &Node) {
    let mut hold = None;
    loop {
        // 收工之后 `run_service` 每 64 件活就会因为 quit 让渡一次；对一个
        // 自唤醒任务来说，dispatch 会永远返回同一个 service。记录本轮前后的
        // 可观测进度，确认「没有别人在排队、且同一 service 没有处理任何消息、
        // 任务数也没变化」时，就主动摘除它，避免收尾流程被一个自唤醒任务挂死。
        let before = hold
            .as_ref()
            .map(|ctx: &Arc<crate::server::ServiceContext>| {
                (
                    ctx.handle,
                    ctx.message_count(),
                    ctx.mailbox.len(),
                    ctx.task_count(),
                )
            });
        hold = node.dispatch(hold);
        if hold.is_none() {
            return;
        }
        let after = hold
            .as_ref()
            .map(|ctx: &Arc<crate::server::ServiceContext>| {
                (
                    ctx.handle,
                    ctx.message_count(),
                    ctx.mailbox.len(),
                    ctx.task_count(),
                )
            });
        let stalled = node.sched.is_quit()
            && node.sched.len() == 0
            && match (before, after) {
                (Some(before), Some(after)) => before == after,
                _ => false,
            };
        if stalled {
            let ctx = hold.take().expect("上面已经确认仍持有 service");
            node.retire(ctx.handle);
            hold = node.dispatch(Some(ctx));
            if hold.is_none() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BoxFuture;
    use crate::context::{Ctx, Service};
    use crate::message::Message;

    /// init 里直接 panic 的服务：旧实现会让 `Builder::run` 永久卡在
    /// `InitTicket::wait()`，现在应当被隔离成 `Error::Init`。
    #[derive(Default)]
    struct PanicInit;

    impl Service for PanicInit {
        fn init(self: Arc<Self>, _ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { panic!("boom in init") })
        }

        fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    /// 保留服务里挂一个自唤醒任务，用于验证 shutdown 的 drain 不会无限循环。
    #[derive(Default)]
    struct SelfWakeLogger;

    impl Service for SelfWakeLogger {
        fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                ctx.spawn(async move {
                    std::future::poll_fn(|cx| {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    })
                    .await
                });
                Ok(())
            })
        }

        fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    /// bootstrap 在 init 里 abort 全部业务服务，让节点快速进入 shutdown。
    #[derive(Default)]
    struct AbortBootstrap;

    impl Service for AbortBootstrap {
        fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                ctx.abort();
                Ok(())
            })
        }

        fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    #[test]
    fn init_panic_is_contained_and_reported() {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = {
            let mut registry = Registry::new();
            registry.register("panic-init", PanicInit::default);
            let config = Config::from_toml_str(
                r#"
                thread = 1

                [logger]
                name = "panic-init"

                [signal]
                name = ""

                [timer]
                name = ""

                [bootstrap]
                name = ""
                "#,
            )
            .expect("配置应解析成功");
            start(config, registry, Arc::new(crate::server::tests::StubTimer))
        };
        std::panic::set_hook(previous);

        match result {
            Err(Error::Init { kind, reason }) => {
                assert_eq!(kind, "panic-init");
                assert!(reason.contains("panic"), "原因应包含 panic：{reason}");
            }
            other => panic!("init panic 应当变成 Init 错误，实际是 {other:?}"),
        }
    }

    #[test]
    fn shutdown_drain_terminates_self_waking_reserved_service() {
        let mut registry = Registry::new();
        registry.register("self-wake-logger", SelfWakeLogger::default);
        registry.register("bootstrap", AbortBootstrap::default);
        let config = Config::from_toml_str(
            r#"
            thread = 1

            [logger]
            name = "self-wake-logger"

            [signal]
            name = ""

            [timer]
            name = ""
            "#,
        )
        .expect("配置应解析成功");

        let result = start(config, registry, Arc::new(crate::server::tests::StubTimer));
        match result {
            Err(Error::Init { kind, .. }) if kind == "bootstrap" => {}
            other => panic!("abort 的 bootstrap 应当以 Init 错误结束，实际是 {other:?}"),
        }
    }

    #[test]
    fn monitor_stop_interrupts_the_wait_immediately() {
        let node = crate::server::tests::test_node();
        let monitors = vec![Arc::new(Monitor::new())];
        let started = std::time::Instant::now();
        thread::scope(|scope| {
            let (stop, stopped) = mpsc::channel();
            let handle = scope.spawn(|| monitor_loop(&node, &monitors, stopped));
            stop.send(()).unwrap();
            handle.join().unwrap();
        });
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "关停不应等待 5 秒检查周期"
        );
    }

    /// 配置能从 TOML 解析，未写的字段走默认值
    #[test]
    fn config_parses_from_toml() {
        let config = Config::from_toml_str(
            r#"
            thread = 4
            profile = false

            [logger]
            path = "run/rskynet.log"
            "#,
        )
        .expect("配置应解析成功");
        assert_eq!(config.thread, 4);
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
        assert!(Config::from_toml_str("harbor = 1").is_err());
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
