//! 独占线程服务的端到端验证。
//!
//! 网络层就长这样，所以这里拿一个最小的「轮询器」把那条路整个走一遍：它阻塞
//! 在自己的事件源上（用 `mpsc` 冒充 mio 的 `Poll`），靠 `interrupt` 被叫醒，
//! 把外部事件转成消息投给业务服务，收工时线程自己退掉。
//!
//! 几个断言是钉时序的：
//!
//! - 邮箱里的消息必须靠 `interrupt` 才叫得动这条线程。`idle` 里那个超时给到 5 秒，
//!   一旦走到超时就说明唤醒的约定破了——`timed_out` 会把这件事记下来。
//! - `start` 返回时所有独占线程都必须已经收工，所以 `dropped` 必须与创建的实例数
//!   对得上。真要是退不掉，这个用例会直接卡住。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rskynet::{
    BoxFuture, Builder, BuilderExt, Config, ConfigExt, Ctx, Error, Exclusive, Idler, Message,
    MsgType, Payload, Registry, Result, Service, SvcCell,
};
use serde::Deserialize;

/// 轮询器把「IO 事件」投给业务服务时用的协议号，对应网络层的 `MsgType::SOCKET`。
const POKE: MsgType = MsgType(60);
/// 业务服务请轮询器代发一次 [`POKE`]，对应「服务给 socket 层下条命令」。
const POKE_REQUEST: MsgType = MsgType(61);

/// 用例观察到的现象，节点退出后统一断言。
type Journal = Arc<Mutex<Vec<String>>>;

fn note(journal: &Journal, entry: impl Into<String>) {
    journal.lock().unwrap().push(entry.into());
}

// ------------------------------------------------------------ 独占线程那侧

/// 轮询器线程等到的一件事。
enum Event {
    /// 内核说「邮箱里有活」，回去重扫一遍即可。
    Wake,
    /// 该给某个服务投一条 IO 事件了。
    Poke(u32),
}

/// 轮询器自己那段配置，从 `[poller]` 段来。
#[derive(Debug, Deserialize)]
struct PollerConfig {
    greeting: String,
}

/// 几个实例共享的记账，节点退出后统一断言。
#[derive(Default)]
struct Trace {
    inited: AtomicUsize,
    /// 有实例的 `idle` 真的阻塞过——也就是这条线程确实在自己的事件源上等着。
    idled: AtomicBool,
    /// `idle` 是被超时叫醒的，说明 `interrupt` 那条路没通。
    timed_out: AtomicBool,
    /// 收工的实例数。它由 `Drop` 记账，所以能证明那条线程真的退掉了。
    dropped: AtomicUsize,
    greeting: Mutex<String>,
}

/// 一个独占线程的服务：阻塞在自己的队列上，与网络层的 socket 线程同形。
struct Poller {
    /// `interrupt` 与 `dispatch` 都往这里投事件，`idle` 在另一头等着。
    events: Sender<Event>,
    /// 只有 `idle` 会用它。`Exclusive` 的方法都收 `&self`，所以搁在锁里。
    inbox: Mutex<Option<Receiver<Event>>>,
    trace: Arc<Trace>,
    greeting: SvcCell<String>,
}

impl Poller {
    fn new(trace: Arc<Trace>) -> Self {
        let (events, inbox) = channel();
        Self {
            events,
            inbox: Mutex::new(Some(inbox)),
            trace,
            greeting: SvcCell::new(String::new()),
        }
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, SeqCst);
    }
}

impl Service for Poller {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            // 成段的配置从节点上取：`init` 只收得到那个字符串参数
            let section: PollerConfig = ctx
                .node()
                .section("poller")?
                .expect("应当读到自己的 [poller] 段");
            *self.greeting.borrow_mut() = section.greeting.clone();
            *self.trace.greeting.lock().unwrap() = section.greeting;
            self.trace.inited.fetch_add(1, SeqCst);
            // 运行期另起的那个实例不抢名字
            if args.trim().is_empty() {
                ctx.register_name("poller");
            }
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let payload = msg.take_payload();
            if msg.mtype == POKE_REQUEST {
                // 把活交给自己那条线程的事件源，等它下一轮醒来再办
                let dest = *payload.downcast::<u32>().expect("敲门对象应当是个 handle");
                let _ = self.events.send(Event::Poke(dest));
                return;
            }
            let text = payload.as_str().unwrap_or_default();
            let reversed: String = text.chars().rev().collect();
            let _ = ctx.reply(&msg, Payload::text(reversed));
        })
    }
}

impl Exclusive for Poller {
    fn idle(&self, ctx: &Ctx, idler: &Idler) {
        let inbox = self.inbox.lock().unwrap();
        let inbox = inbox.as_ref().expect("接收端只由 idle 使用");
        self.trace.idled.store(true, SeqCst);
        // 真的网络层是阻塞在 epoll 上的，这里阻塞在队列上。超时给得很宽：健康的
        // 节点一定是被 interrupt 叫醒的，走到超时就说明唤醒的约定破了
        match inbox.recv_timeout(Duration::from_secs(5)) {
            // 邮箱里有活，回去重扫一遍
            Ok(Event::Wake) => {}
            Ok(Event::Poke(dest)) => {
                // 外部事件转成消息，与定时器回包同一条路径
                let _ = ctx.send(dest, POKE, Payload::None);
            }
            Err(RecvTimeoutError::Timeout) => {
                self.trace.timed_out.store(true, SeqCst);
            }
            Err(RecvTimeoutError::Disconnected) => {
                assert!(idler.is_dead(), "队列断开只该发生在服务收工时");
            }
        }
    }

    fn interrupt(&self) {
        // 早到的唤醒不会丢：队列自己把它存着，下一次 recv 立刻返回
        let _ = self.events.send(Event::Wake);
    }
}

// ------------------------------------------------------------ 业务服务那侧

/// 用例逻辑都在它的 init 里，办完就关停节点。
struct Probe {
    journal: Journal,
    trace: Arc<Trace>,
    poked: SvcCell<bool>,
}

impl Service for Probe {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            // 邮箱：独占线程正阻塞在自己的事件源上，这一次 call 全靠 interrupt
            // 把它叫回来。真要是没叫醒，下面这句会等满 5 秒的超时
            let reply = ctx.request(".poller", Payload::text("hello")).await?;
            note(&self.journal, reply.as_str().unwrap_or("<非字节负载>"));

            // 回执单被丢弃（外部线程半路撂挑子）时要拿到错误，而不是永久挂着
            let dropped = ctx.call_external(drop).await;
            note(
                &self.journal,
                format!("撂挑子={}", matches!(dropped, Err(Error::CallFailed(_)))),
            );

            // 独占线程把外部事件投成消息：请它敲自己一下，然后等那一下到
            ctx.send(".poller", POKE_REQUEST, Payload::of(ctx.handle()))?;
            while !self.poked.get() {
                ctx.sleep(1).await;
            }
            note(&self.journal, "收到敲门");

            // 运行期起一个独占服务再杀掉：它那条线程必须自己收工
            let before = self.trace.dropped.load(SeqCst);
            let handle = ctx.launch("poller", "anonymous").await?;
            assert!(ctx.kill(handle));
            let mut rounds = 0;
            while self.trace.dropped.load(SeqCst) == before && rounds < 500 {
                ctx.sleep(1).await;
                rounds += 1;
            }
            note(
                &self.journal,
                format!(
                    "杀掉后线程收工={}",
                    self.trace.dropped.load(SeqCst) > before
                ),
            );

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

/// 独占服务该有的样子：自定义阻塞、被 interrupt 叫醒、外部事件转消息、收工退线程
#[test]
fn an_exclusive_service_owns_its_thread() {
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let trace = Arc::new(Trace::default());

    let poller_trace = trace.clone();
    let probe_trace = trace.clone();
    let shared = journal.clone();
    let registry = Registry::new()
        .with_exclusive("poller", move || Poller::new(poller_trace.clone()))
        .with("probe", move || Probe {
            journal: shared.clone(),
            trace: probe_trace.clone(),
            poked: SvcCell::new(false),
        });

    let config = Config::from_toml_str(
        r#"
        thread = 2

        [poller]
        greeting = "你好"

        [bootstrap]
        services = [{ name = "poller" }, { name = "probe" }]
        "#,
    )
    .expect("配置应解析成功");

    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    let seen = journal.lock().unwrap();
    assert_eq!(
        *seen,
        vec!["olleh", "撂挑子=true", "收到敲门", "杀掉后线程收工=true"]
    );

    assert_eq!(trace.inited.load(SeqCst), 2, "两个实例的 init 都该跑过");
    assert!(trace.idled.load(SeqCst), "idle 钩子应当被调用");
    assert!(
        !trace.timed_out.load(SeqCst),
        "每一次唤醒都该来自 interrupt：走到超时说明投递方漏叫了"
    );
    assert_eq!(
        trace.dropped.load(SeqCst),
        2,
        "start 返回时所有独占线程都该已经收工"
    );
    assert_eq!(
        trace.greeting.lock().unwrap().as_str(),
        "你好",
        "应当读到自己的配置段"
    );
}

// ------------------------------------------------------------ 关停不丢消息

/// 一条日志的目的地，慢得刚好能让消息在邮箱里堆起来。
///
/// 日志服务就是这个形状：关停时它邮箱里往往还压着一批，销毁流程必须先把它们
/// 写完再动手清理，否则最后几行就没了。
struct Sink {
    written: Arc<Mutex<Vec<String>>>,
}

impl Service for Sink {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            ctx.register_name("sink");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let text = msg.take_payload().as_str().unwrap_or_default().to_string();
            // 第一条故意写得很慢，好让后面那些在邮箱里排上队。这条线程是自己的，
            // 阻塞它不影响任何 worker
            if text == "0" {
                std::thread::sleep(Duration::from_millis(150));
            }
            self.written.lock().unwrap().push(text);
        })
    }
}

impl Exclusive for Sink {}

/// 灌一批消息进去，趁对方还在写第一条就关停节点。
struct Flooder {
    total: usize,
}

impl Service for Flooder {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            for index in 0..self.total {
                ctx.post(".sink", Payload::text(index.to_string()))?;
            }
            ctx.abort();
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// 独占服务被摘除时，邮箱里积压的消息要先处理完再销毁
#[test]
fn a_backlog_survives_the_shutdown() {
    const TOTAL: usize = 100;

    let written: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let shared = written.clone();
    let registry = Registry::new()
        .with_exclusive("sink", move || Sink {
            written: shared.clone(),
        })
        .with("flooder", || Flooder { total: TOTAL });

    let config = Config::default().with_bootstrap(["sink", "flooder"]);
    // 这条用 Builder 走一遍：与 rskynet::start 等价，顺便验证 with_builtins 的写法
    Builder::new(config)
        .registry(registry)
        .with_builtins()
        .run()
        .expect("节点应当正常启动并退出");

    let written = written.lock().unwrap();
    assert_eq!(written.len(), TOTAL, "关停时积压的消息一条都不能丢");
    let expected: Vec<String> = (0..TOTAL).map(|index| index.to_string()).collect();
    assert_eq!(*written, expected, "而且要保序");
}
