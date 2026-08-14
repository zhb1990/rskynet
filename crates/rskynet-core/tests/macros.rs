//! 过程宏在**内核这一侧**的用例。
//!
//! 宏生成的代码默认引门面 crate（`::rskynet`），本仓内部那几个 crate 只依赖内核，
//! 依赖门面会成环，于是有 `crate = ::rskynet_core` 这条覆盖路径。这个文件就是它
//! 的用例：整份代码里一个 `rskynet` 都没有，全靠 `rskynet_core`。
//!
//! 宏本身的行为（路由、自动回包、兜底）在门面那侧还有一份更全的用例，见
//! `crates/rskynet/tests/macros.rs`。

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::Duration;

use rskynet_core::{
    Config, Ctx, Error, FromPayload, Idler, Message, MsgType, Payload, Registry, Result, Timer,
    boxed_payload,
};

/// 请求与应答都走对象负载，各写一句声明。
struct Add(i64, i64);
#[derive(Debug)]
struct Sum(i64);
struct Ping(u32);
#[derive(Debug)]
struct Pong(u32);
boxed_payload!(Add, Sum, Ping, Pong);

/// 用例观察到的现象，节点退出后统一断言。
type Journal = Arc<Mutex<Vec<String>>>;

/// 没有定时器服务的节点也得有个时间来源。本用例不 `sleep`，所以它什么都不用干。
struct StoppedClock;

impl Timer for StoppedClock {
    fn timeout(&self, _handle: u32, _session: i32, _delay_ms: u32) {}
    fn now(&self) -> u64 {
        0
    }
    fn wall_clock(&self) -> u64 {
        0
    }
    fn start_time(&self) -> u64 {
        0
    }
}

// ------------------------------------------------------------ 共享服务

struct Adder {
    journal: Journal,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Adder {
    /// 尾部参数不关心就不写：这里少了 `args: String`，宏会补齐。
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name(self.label());
        Ok(())
    }

    /// 有返回值且对方在等回话 → 宏自动 `ctx.reply`。
    #[msg(MsgType::USER)]
    async fn on_add(&self, _ctx: Ctx, add: Add) -> Sum {
        Sum(add.0 + add.1)
    }

    /// 字节负载走 `String`，一个处理函数认两个协议号。
    #[msg(MsgType::TEXT, MsgType::SYSTEM)]
    async fn on_text(&self, _ctx: Ctx, text: String) {
        self.note(format!("文本={text}"));
    }

    /// 兜底。参数写 `Message` 就是整条消息都归它，回包也得自己发。
    #[msg(default)]
    async fn on_other(&self, ctx: Ctx, msg: Message) {
        self.note(format!("兜底={:?}", msg.mtype));
        let _ = ctx.reply(&msg, Payload::text("兜底接住了"));
    }

    /// 没被认领的方法留在原地，一个字都不动。
    fn label(&self) -> &'static str {
        "adder"
    }

    fn note(&self, entry: String) {
        self.journal.lock().unwrap().push(entry);
    }
}

// ------------------------------------------------------------ 独占线程的服务

/// 几个实例共享的记账。
#[derive(Default)]
struct Trace {
    idled: AtomicBool,
    /// `idle` 是被超时叫醒的，说明 `interrupt` 那条路没通。
    timed_out: AtomicBool,
}

struct Ticker {
    /// `interrupt` 往这里投，`idle` 在另一头等着。
    wakeups: Sender<()>,
    inbox: Mutex<Option<Receiver<()>>>,
    trace: Arc<Trace>,
}

#[rskynet_macros::exclusive(crate = ::rskynet_core)]
impl Ticker {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("ticker");
        Ok(())
    }

    /// 独占服务照样能用路由糖。
    #[msg(MsgType::USER)]
    async fn on_ping(&self, _ctx: Ctx, ping: Ping) -> Pong {
        Pong(ping.0 + 1)
    }

    /// 同步钩子原样搬过去：这条线程是自己的，阻塞它不碍着谁。
    fn idle(&self, _ctx: &Ctx, idler: &Idler) {
        let inbox = self.inbox.lock().unwrap();
        let inbox = inbox.as_ref().expect("接收端只由 idle 使用");
        self.trace.idled.store(true, SeqCst);
        // 超时给得很宽：健康的节点一定是被 interrupt 叫醒的
        match inbox.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => self.trace.timed_out.store(true, SeqCst),
            Err(RecvTimeoutError::Disconnected) => assert!(idler.is_dead(), "断开只该发生在收工时"),
        }
    }

    /// 会被任意线程调用，所以只碰能跨线程的东西。早到的唤醒由队列自己存着。
    fn interrupt(&self) {
        let _ = self.wakeups.send(());
    }
}

// ------------------------------------------------------------ 用例本体

/// 用例逻辑都在 init 里，办完就关停节点。
struct Probe {
    journal: Journal,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Probe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.launch("adder", "").await?;
        ctx.launch("ticker", "").await?;

        // 对象负载按声明的类型取，返回值自动装回去
        let reply = ctx
            .call(".adder", MsgType::USER, Payload::of(Add(2, 40)))
            .await?;
        let sum = Sum::from_payload(reply)?;
        self.note(format!("和={}", sum.0));

        // 字节负载，不等回话
        ctx.send(".adder", MsgType::TEXT, Payload::text("你好"))?;

        // 不认识的协议号落到兜底那一支
        let reply = ctx.call(".adder", MsgType(77), Payload::None).await?;
        self.note(reply.as_str().unwrap_or("<非字节负载>").to_string());

        // 负载类型不符：对方在等回话，就该拿到错误应答而不是永久挂着
        let wrong = ctx
            .call(".adder", MsgType::USER, Payload::text("并不是 Add"))
            .await;
        self.note(format!(
            "类型不符={}",
            matches!(wrong, Err(Error::CallFailed(_)))
        ));

        // 独占服务正阻塞在自己的 idle 里，这一次 call 全靠 interrupt 把它叫回来
        let reply = ctx
            .call(".ticker", MsgType::USER, Payload::of(Ping(7)))
            .await?;
        self.note(format!("ping={}", Pong::from_payload(reply)?.0));

        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            task_ctx.yield_now().await;
            task_ctx.abort();
        });
        Ok(())
    }

    fn note(&self, entry: String) {
        self.journal.lock().unwrap().push(entry);
    }
}

/// `crate = ::rskynet_core` 覆盖路径下，三个宏生成的东西都能跑起来
#[test]
fn macros_work_against_the_kernel_crate() {
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let trace = Arc::new(Trace::default());

    let adder_journal = journal.clone();
    let probe_journal = journal.clone();
    let ticker_trace = trace.clone();
    let registry = Registry::new()
        .with("adder", move || Adder {
            journal: adder_journal.clone(),
        })
        .with_exclusive("ticker", move || {
            let (wakeups, inbox) = channel();
            Ticker {
                wakeups,
                inbox: Mutex::new(Some(inbox)),
                trace: ticker_trace.clone(),
            }
        })
        // 内核默认拉起的那个引导服务就叫 bootstrap，本用例拿它当入口
        .with("bootstrap", move || Probe {
            journal: probe_journal.clone(),
        });

    // 日志与定时器都不起：本用例不 sleep，也不必往文件里写什么
    let config = Config::from_toml_str(
        r#"
        thread = 2

        [logger]
        name = ""

        [signal]
        name = ""

        [timer]
        name = ""
        "#,
    )
    .expect("配置应解析成功");

    rskynet_core::start(config, registry, Arc::new(StoppedClock)).expect("节点应当正常启动并退出");

    let seen = journal.lock().unwrap();
    assert_eq!(
        *seen,
        vec![
            "和=42",
            "文本=你好",
            "兜底=MsgType(77)",
            "兜底接住了",
            "类型不符=true",
            "ping=8",
        ]
    );

    assert!(trace.idled.load(SeqCst), "idle 钩子应当被调用");
    assert!(
        !trace.timed_out.load(SeqCst),
        "每一次唤醒都该来自 interrupt：走到超时说明 interrupt 没生成对"
    );
}
