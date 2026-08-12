//! 内核扩展点的端到端验证。
//!
//! 网络层住在别的 crate 里，它进内核只有 [`rskynet_core::ext`] 那一条路。这里
//! 用一个最小插件把那条路整个走一遍：插件在 `init` 里登记扩展对象、在自己的
//! 线程里等活干、被 `shutdown` 钩子叫醒收工；服务侧则通过扩展槽找到它，用
//! `call_external` 把「派活给外部线程并等结果」写成一句 `await`。
//!
//! 形态刻意照着网络层来：`Worker` 对应 `SocketServer`（命令队列 + 叫醒手段），
//! `Command` 自带一张回执单，`Poke` 走的是 socket 事件那条「外部线程直接投邮箱」
//! 的路径。

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rskynet_core::{
    Addr, BoxFuture, Builder, Config, Ctx, Error, Message, MsgType, NodeRef, Payload, Plugin,
    Registry, ReplyToken, Result, Service, SvcCell,
};
use serde::Deserialize;

/// 插件线程主动敲服务邮箱时用的协议号，对应网络层的 `MsgType::SOCKET`。
const POKE: MsgType = MsgType(60);

/// 用例观察到的现象，节点退出后统一断言。
type Journal = Arc<Mutex<Vec<String>>>;

fn note(journal: &Journal, entry: impl Into<String>) {
    journal.lock().unwrap().push(entry.into());
}

// ------------------------------------------------------------ 插件侧

/// 交给外部线程去办的一件活。
enum Command {
    /// 把字符串倒过来，办完拿回执单回话。
    Reverse(String, ReplyToken),
    /// 直接往某个服务的邮箱投一条消息，不需要回话。
    Poke(u32),
    /// 收工。
    Quit,
}

/// 插件登记进扩展槽的那个对象，服务侧靠它派活。
///
/// 对应网络层的 `SocketServer`：一条命令队列，加一个把外部线程从阻塞里叫醒的手段
/// （这里是队列自带的唤醒，网络层会是 mio 的 `Waker`）。
struct Worker {
    /// `mpsc::Sender` 不是 `Sync`，而扩展槽里的东西要被多个服务同时使用，
    /// 所以套一层锁。真的网络层会换成无锁队列。
    commands: Mutex<Sender<Command>>,
}

impl Worker {
    fn submit(&self, command: Command) {
        let _ = self.commands.lock().unwrap().send(command);
    }
}

/// 插件自己那段配置，从 `[worker]` 段来。
#[derive(Debug, Deserialize)]
struct WorkerConfig {
    greeting: String,
}

/// 三个钩子各自的到访记录，节点退出后统一断言。
#[derive(Default)]
struct Trace {
    inited: AtomicBool,
    ran: AtomicBool,
    shutdown: AtomicBool,
    /// `run` 那条线程是被 `shutdown` 叫醒才退出的，而不是自己等超时等出来的。
    woken_by_shutdown: AtomicBool,
    greeting: Mutex<String>,
}

struct WorkerPlugin {
    /// 只有 `run` 会取走它。`Plugin` 的方法都收 `&self`，所以放在锁里。
    inbox: Mutex<Option<Receiver<Command>>>,
    handle: Arc<Worker>,
    trace: Arc<Trace>,
}

impl WorkerPlugin {
    fn new() -> (Self, Arc<Trace>) {
        let (sender, receiver) = channel();
        let trace = Arc::new(Trace::default());
        let plugin = Self {
            inbox: Mutex::new(Some(receiver)),
            handle: Arc::new(Worker {
                commands: Mutex::new(sender),
            }),
            trace: trace.clone(),
        };
        (plugin, trace)
    }
}

impl Plugin for WorkerPlugin {
    fn name(&self) -> &str {
        "worker"
    }

    fn init(&self, node: &NodeRef, config: &Config) -> Result<Option<Arc<dyn Any + Send + Sync>>> {
        // init 必须排在所有服务之前，否则服务在自己的 init 里就取不到扩展对象
        assert!(
            node.resolve(&Addr::from(".logger")).is_err(),
            "init 应当早于 logger 服务"
        );
        let section: WorkerConfig = config
            .section("worker")?
            .expect("插件应当读到自己的 [worker] 段");
        *self.trace.greeting.lock().unwrap() = section.greeting;
        self.trace.inited.store(true, SeqCst);
        Ok(Some(self.handle.clone()))
    }

    fn run(&self, node: NodeRef) {
        self.trace.ran.store(true, SeqCst);
        let inbox = self.inbox.lock().unwrap().take().expect("run 只会被调一次");
        loop {
            // 真插件是阻塞在 epoll 上的，这里阻塞在队列上。超时给得很宽：健康的
            // 节点一定是被 shutdown 钩子叫醒的，走到超时就说明钩子的时序错了
            match inbox.recv_timeout(Duration::from_secs(5)) {
                Ok(Command::Reverse(text, token)) => {
                    token.reply(Payload::text(text.chars().rev().collect::<String>()));
                }
                Ok(Command::Poke(dest)) => {
                    node.send(0, dest, POKE, 0, Payload::None)
                        .expect("敲门的对象应当还活着");
                }
                Ok(Command::Quit) => {
                    assert!(node.is_quit(), "收到 Quit 时节点应当已经收工");
                    self.trace.woken_by_shutdown.store(true, SeqCst);
                    return;
                }
                Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn shutdown(&self, node: &NodeRef) {
        assert!(node.is_quit(), "shutdown 时节点应当已经收工");
        self.trace.shutdown.store(true, SeqCst);
        self.handle.submit(Command::Quit);
    }
}

// ------------------------------------------------------------ 服务侧

/// 用例逻辑都在它的 init 里，办完就关停节点。
struct Probe {
    journal: Journal,
    poked: SvcCell<bool>,
}

impl Service for Probe {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            // 扩展槽：服务能取到插件登记的那个对象，没登记过的类型取不到
            let worker = ctx
                .node()
                .extension::<Worker>()
                .expect("扩展槽里应当有 Worker");
            assert!(ctx.node().extension::<String>().is_none());

            // 外部回包：活是别的线程干的，这里照样写成一句 await
            let reply = ctx
                .call_external(|token| worker.submit(Command::Reverse("hello".into(), token)))
                .await?;
            note(&self.journal, reply.as_str().unwrap_or("<非字节负载>"));

            // 回执单被丢弃（外部线程半路撂挑子）时要拿到错误，而不是永久挂着
            let dropped = ctx.call_external(drop).await;
            note(
                &self.journal,
                format!("撂挑子={}", matches!(dropped, Err(Error::CallFailed(_)))),
            );

            // NodeRef::send：外部线程直接往邮箱投消息，走的是 socket 事件那条路
            worker.submit(Command::Poke(ctx.handle()));
            while !self.poked.get() {
                ctx.sleep(1).await;
            }
            note(&self.journal, "收到敲门");

            ctx.abort();
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if msg.mtype == POKE {
                self.poked.set(true);
            }
        })
    }
}

/// 插件的三个钩子、扩展槽、外部回包与分段配置要凑成一套能用的东西
#[test]
fn a_plugin_gets_its_hooks_slot_and_replies() {
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let (plugin, trace) = WorkerPlugin::new();

    let shared = journal.clone();
    let registry = Registry::new()
        .with_builtins()
        .with("probe", move || Probe {
            journal: shared.clone(),
            poked: SvcCell::new(false),
        });

    let config = Config::from_toml_str(
        r#"
        thread = 2
        bootstrap = "probe"

        [worker]
        greeting = "你好"
        "#,
    )
    .expect("配置应解析成功");

    Builder::new(config)
        .registry(registry)
        .plugin(plugin)
        .run()
        .expect("节点应当正常启动并退出");

    assert!(trace.inited.load(SeqCst), "init 钩子应当被调用");
    assert!(trace.ran.load(SeqCst), "run 钩子应当被调用");
    assert!(trace.shutdown.load(SeqCst), "shutdown 钩子应当被调用");
    assert!(
        trace.woken_by_shutdown.load(SeqCst),
        "run 那条线程应当是被 shutdown 钩子叫醒的：\
         若 scope 的隐式 join 排在钩子之前，这里会先撞上超时"
    );
    assert_eq!(
        trace.greeting.lock().unwrap().as_str(),
        "你好",
        "插件应当读到自己的配置段"
    );

    let seen = journal.lock().unwrap();
    assert_eq!(*seen, vec!["olleh", "撂挑子=true", "收到敲门"]);
}
