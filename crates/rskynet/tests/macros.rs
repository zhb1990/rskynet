//! 过程宏的端到端验证：`#[service]` / `#[exclusive]` / `#[msg]` 生成的服务，
//! 在真的节点上跑起来是不是那么回事。
//!
//! 这里走的是默认路径（生成的代码引 `::rskynet`）。`crate = ` 覆盖那条路径的用例
//! 在内核那侧，见 `crates/rskynet-core/tests/macros.rs`。
//!
//! 钉住的几件事：
//!
//! - 按协议号分发，一个处理函数可以认多个协议号。
//! - 负载按声明的类型取；取不出来时对方拿到的是错误应答，不是永久挂起。
//! - 有返回值就自动回包，返回 `()` 就不回。
//! - 没有 `#[msg]` 命中时走 `default`；连 `default` 都没有的服务也不会让对方挂着。
//! - 没被认领的方法原样留在 impl 块里，照样能直接调。

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rskynet::{
    Config, ConfigExt, Ctx, Error, FromPayload, Idler, Message, MsgType, Payload, Registry, Result,
    boxed_payload,
};

/// 业务自己的协议号，看宏认不认「不是内置常量」的表达式。
const PING: MsgType = MsgType(42);

struct Add(i64, i64);
#[derive(Debug)]
struct Sum(i64);
boxed_payload!(Add, Sum);

type Journal = Arc<Mutex<Vec<String>>>;

// ------------------------------------------------------------ 共享服务

struct Calc {
    journal: Journal,
    calls: AtomicUsize,
}

#[rskynet::service]
impl Calc {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("calc");
        Ok(())
    }

    /// 对象负载 + 自动回包。
    #[msg(MsgType::USER)]
    async fn on_add(&self, _ctx: Ctx, add: Add) -> Sum {
        self.calls.fetch_add(1, SeqCst);
        Sum(add.0 + add.1)
    }

    /// 字节负载，认两个协议号，没有返回值所以不回包。
    #[msg(MsgType::TEXT, PING)]
    async fn on_text(&self, _ctx: Ctx, text: String) {
        self.note(format!("文本={text}"));
    }

    /// 收整条消息的处理函数：回包得自己发。
    #[msg(default)]
    async fn on_other(&self, ctx: Ctx, msg: Message) {
        self.note(format!("兜底={}", msg.mtype.0));
        let _ = ctx.reply(&msg, Payload::text("兜底"));
    }

    /// 没被认领的方法留在原地。
    fn note(&self, entry: String) {
        self.journal.lock().unwrap().push(entry);
    }
}

// ------------------------------------------------------------ 只有 init 的服务

/// `dispatch` 与 `#[msg]` 一个都没写。宏会补一个「没人认领就回错误」的 dispatch，
/// 于是「只有 init 的服务」不必再写一句空 `Box::pin`。
struct Mute;

#[rskynet::service]
impl Mute {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("mute");
        Ok(())
    }
}

// ------------------------------------------------------------ 独占线程的服务

/// `idle` / `interrupt` 都不写：`Exclusive` 各有默认实现（挂起 / 什么都不做），
/// 于是纯消息驱动的独占服务只写 `init` 与处理函数就行，日志服务就是这个形状。
struct Sink {
    journal: Journal,
}

#[rskynet::exclusive]
impl Sink {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("sink");
        Ok(())
    }

    #[msg(MsgType::TEXT)]
    async fn on_text(&self, _ctx: Ctx, text: String) {
        self.journal.lock().unwrap().push(format!("落库={text}"));
    }
}

/// 自带阻塞的独占服务：`idle` / `interrupt` 由宏原样搬进 `impl Exclusive`。
struct Ticker {
    wakeups: Sender<()>,
    inbox: Mutex<Option<Receiver<()>>>,
    timed_out: Arc<AtomicUsize>,
}

#[rskynet::exclusive]
impl Ticker {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("ticker");
        Ok(())
    }

    /// 参数一个都不要也行，宏会按钩子的签名补齐。
    #[msg(PING)]
    async fn on_ping(&self) -> String {
        "pong".to_string()
    }

    fn idle(&self, _ctx: &Ctx, idler: &Idler) {
        let inbox = self.inbox.lock().unwrap();
        let inbox = inbox.as_ref().expect("接收端只由 idle 使用");
        match inbox.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => {
                self.timed_out.fetch_add(1, SeqCst);
            }
            Err(RecvTimeoutError::Disconnected) => assert!(idler.is_dead(), "断开只该发生在收工时"),
        }
    }

    fn interrupt(&self) {
        let _ = self.wakeups.send(());
    }
}

// ------------------------------------------------------------ 用例本体

struct Probe {
    journal: Journal,
}

#[rskynet::service]
impl Probe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        // 对象负载按声明的类型取，返回值自动装回去
        let reply = ctx
            .call(".calc", MsgType::USER, Payload::of(Add(2, 40)))
            .await?;
        self.note(format!("和={}", Sum::from_payload(reply)?.0));

        // 一个处理函数认两个协议号
        ctx.send(".calc", MsgType::TEXT, Payload::text("甲"))?;
        ctx.send(".calc", PING, Payload::text("乙"))?;

        // 兜底那一支：整条消息交给它，回包也由它自己发
        let reply = ctx.call(".calc", MsgType::SYSTEM, Payload::None).await?;
        self.note(reply.as_str().unwrap_or("<非字节负载>").to_string());

        // 负载类型不符：对方在等回话，就该拿到错误应答而不是永久挂着
        let wrong = ctx
            .call(".calc", MsgType::USER, Payload::text("并不是 Add"))
            .await;
        self.note(format!(
            "类型不符={}",
            matches!(wrong, Err(Error::CallFailed(_)))
        ));

        // 一个 #[msg] 都没有的服务：宏补的那个 dispatch 也得把话说清楚
        let mute = ctx.call(".mute", MsgType::USER, Payload::None).await;
        self.note(format!(
            "无人认领={}",
            matches!(mute, Err(Error::CallFailed(_)))
        ));

        // 独占服务之一：`idle` / `interrupt` 都没写，靠 Exclusive 的默认实现挂起等消息。
        // 它没有回包，所以等它落库了再往下走，好让下面那句断言的顺序是定的
        ctx.send(".sink", MsgType::TEXT, Payload::text("丙"))?;
        while self.journal.lock().unwrap().len() < 8 {
            ctx.sleep(1).await;
        }

        // 独占服务之二：自带阻塞，这一次 call 全靠 interrupt 把它叫回来
        let reply = ctx.call(".ticker", PING, Payload::None).await?;
        self.note(reply.as_str().unwrap_or("<非字节负载>").to_string());

        ctx.abort();
        Ok(())
    }

    fn note(&self, entry: String) {
        self.journal.lock().unwrap().push(entry);
    }
}

/// 三个宏生成的服务在真节点上跑一遍：分发、取负载、自动回包、兜底、独占线程
#[test]
fn generated_services_behave() {
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let timed_out = Arc::new(AtomicUsize::new(0));

    let calc_journal = journal.clone();
    let sink_journal = journal.clone();
    let probe_journal = journal.clone();
    let ticker_timeouts = timed_out.clone();
    let registry = Registry::new()
        .with("calc", move || Calc {
            journal: calc_journal.clone(),
            calls: AtomicUsize::new(0),
        })
        .with("mute", || Mute)
        .with_exclusive("sink", move || Sink {
            journal: sink_journal.clone(),
        })
        .with_exclusive("ticker", move || {
            let (wakeups, inbox) = channel();
            Ticker {
                wakeups,
                inbox: Mutex::new(Some(inbox)),
                timed_out: ticker_timeouts.clone(),
            }
        })
        .with("probe", move || Probe {
            journal: probe_journal.clone(),
        });

    let config = Config::default().with_bootstrap(["calc", "mute", "sink", "ticker", "probe"]);
    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    let seen = journal.lock().unwrap();
    assert_eq!(
        *seen,
        vec![
            "和=42",
            "文本=甲",
            "文本=乙",
            "兜底=4",
            "兜底",
            "类型不符=true",
            "无人认领=true",
            "落库=丙",
            "pong",
        ]
    );
    assert_eq!(
        timed_out.load(SeqCst),
        0,
        "每一次唤醒都该来自 interrupt：走到超时说明 interrupt 没生成对"
    );
}

/// 没被认领的方法原样留着，不必起节点就能直接调
#[test]
fn plain_methods_are_left_alone() {
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let calc = Calc {
        journal: journal.clone(),
        calls: AtomicUsize::new(0),
    };
    calc.note("直接调".to_string());
    assert_eq!(*journal.lock().unwrap(), vec!["直接调"]);
}
