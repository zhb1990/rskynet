//! 内核的端到端测试：每个用例都真的起一个节点跑一遍。
//!
//! 套路是固定的：注册一个「驱动」服务，它在 init 里执行用例逻辑、把观察到的
//! 现象写进共享记录，最后 `abort` 关停节点；`start` 返回后再断言记录内容。

use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rskynet::{BoxFuture, Config, Ctx, Error, Message, MsgType, Payload, Registry, Service, SvcCell};

/// 用例观察到的现象，跨线程收集，节点退出后统一断言。
type Journal = Arc<Mutex<Vec<String>>>;
type Scenario = Arc<dyn Fn(Ctx, Journal) -> BoxFuture<'static, ()> + Send + Sync>;

// ------------------------------------------------------------ 测试用的几个服务

/// 原样回声，并在负载后面补个后缀，方便断言。
#[derive(Default)]
struct Echo;

impl Service for Echo {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async move {
            ctx.register_name("echo");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let text = msg.take_payload().as_str().unwrap_or_default().to_string();
            let _ = ctx.reply(&msg, Payload::text(format!("{text}!")));
        })
    }
}

/// 先睡一会儿再应答，睡多久由请求负载给定（单位厘秒）。
#[derive(Default)]
struct SlowEcho;

impl Service for SlowEcho {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async move {
            ctx.register_name("slow");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let centis: u32 = msg
                .take_payload()
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            ctx.sleep(centis).await;
            let _ = ctx.reply(&msg, Payload::text(format!("睡了{centis}")));
        })
    }
}

/// 收到请求后不应答，直接退出自己：用来验证请求方不会被永久挂起。
#[derive(Default)]
struct Quitter;

impl Service for Quitter {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async move {
            ctx.register_name("quitter");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            ctx.exit();
        })
    }
}

/// 数一数自己收到过多少条不需要应答的消息，被 call 时报数。
#[derive(Default)]
struct Counter {
    received: SvcCell<u64>,
}

impl Service for Counter {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async move {
            ctx.register_name("counter");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            if msg.needs_reply() {
                let total = *self.received.borrow();
                let _ = ctx.reply(&msg, Payload::text(total.to_string()));
            } else {
                *self.received.borrow_mut() += 1;
            }
        })
    }
}

/// 初始化就失败的服务。
#[derive(Default)]
struct Stillborn;

impl Service for Stillborn {
    fn init(self: Arc<Self>, _ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async { Err(Error::service("我起不来")) })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

/// 告诉中转站它在环上的下家是谁。
const SETUP: MsgType = MsgType(50);
/// 一个正在环上转圈的令牌，负载是还剩几跳。
const TOKEN: MsgType = MsgType(51);

/// 调度压测的共享记账。
#[derive(Default)]
struct RelayShared {
    started: Mutex<Option<Instant>>,
    /// 最后一个令牌停下时由它自己记下总耗时，免得读数被驱动方的轮询精度污染。
    elapsed: Mutex<Option<std::time::Duration>>,
    finished: AtomicU64,
    hops: AtomicU64,
}

/// 环上的一个中转站：收到令牌就传给下家，直到令牌跑完预定的跳数。
struct Relay {
    next: SvcCell<u32>,
    shared: Arc<RelayShared>,
}

impl Service for Relay {
    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let payload = msg.take_payload();
            if msg.mtype == SETUP {
                self.next.set(*payload.downcast::<u32>().unwrap());
                return;
            }
            let left = *payload.downcast::<u64>().unwrap();
            self.shared.hops.fetch_add(1, SeqCst);
            if left > 1 {
                let _ = ctx.send(self.next.get(), TOKEN, Payload::of(left - 1));
            } else if self.shared.finished.fetch_add(1, SeqCst) == 0 {
                // 第一个停下的令牌来记时：此刻其余令牌也都到期了，误差在一跳之内
                let started = self.shared.started.lock().unwrap().unwrap();
                *self.shared.elapsed.lock().unwrap() = Some(started.elapsed());
            }
        })
    }
}

/// 承载用例逻辑的驱动服务。
struct Driver {
    scenario: Scenario,
    journal: Journal,
}

impl Service for Driver {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
        Box::pin(async move {
            (self.scenario)(ctx.clone(), self.journal.clone()).await;
            ctx.abort();
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

// ------------------------------------------------------------ 测试脚手架

/// 起一个节点跑完用例，返回用例记录下来的内容。
fn run_node<F>(bootstrap: &str, scenario: F) -> Vec<String>
where
    F: Fn(Ctx, Journal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let scenario: Scenario = Arc::new(scenario);
    let shared = journal.clone();

    let registry = Registry::new()
        .with_builtins()
        .with("echo", Echo::default)
        .with("slow", SlowEcho::default)
        .with("quitter", Quitter::default)
        .with("counter", Counter::default)
        .with("stillborn", Stillborn::default)
        .with("driver", move || Driver {
            scenario: scenario.clone(),
            journal: shared.clone(),
        });

    let config = Config::default().with_thread(2).with_bootstrap(bootstrap);
    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    let entries = journal.lock().unwrap();
    entries.clone()
}

fn note(journal: &Journal, entry: impl Into<String>) {
    journal.lock().unwrap().push(entry.into());
}

// ------------------------------------------------------------ 用例

/// call 的基本功：发出去的请求能拿回对端的应答
#[test]
fn call_returns_the_reply() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            let reply = ctx.request(".echo", Payload::text("你好")).await.unwrap();
            note(&journal, reply.as_str().unwrap());
        })
    });
    assert_eq!(seen, vec!["你好!"]);
}

/// 按名字寻址和按 handle 寻址应当等价，名字不存在时立刻失败而不是挂住
#[test]
fn name_and_handle_addressing_are_equivalent() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            let handle = ctx.query_name("echo").expect("echo 应已注册名字");
            note(&journal, format!("查到 {}", handle != 0));

            let reply = ctx.request(handle, Payload::text("按句柄")).await.unwrap();
            note(&journal, reply.as_str().unwrap());

            let reply = ctx.request(".echo", Payload::text("按名字")).await.unwrap();
            note(&journal, reply.as_str().unwrap());

            let err = ctx.request(".查无此人", Payload::None).await.unwrap_err();
            note(&journal, format!("{}", matches!(err, Error::NameNotFound(_))));
        })
    });
    assert_eq!(seen, vec!["查到 true", "按句柄!", "按名字!", "true"]);
}

/// sleep 由定时器线程唤醒，既不能早退也不能睡过头
#[test]
fn sleep_is_woken_by_the_timer() {
    let seen = run_node("bootstrap driver", |ctx, journal| {
        Box::pin(async move {
            let started = Instant::now();
            ctx.sleep(15).await;
            let millis = started.elapsed().as_millis();
            note(&journal, format!("{}", (140..3000).contains(&millis)));
            // 时间轮的刻度也应当推进了
            note(&journal, format!("{}", ctx.now() >= 15));
        })
    });
    assert_eq!(seen, vec!["true", "true"], "sleep(15) 应睡满 150ms 上下");
}

/// 主任务被 call 挂起时，服务照常处理其它任务——挂起的是任务不是服务
#[test]
fn a_pending_call_does_not_block_the_service() {
    // 主任务去 call 一个睡 20 厘秒才回话的服务，同时 fork 一个任务去 call 快服务。
    // 若服务是被整个卡住的，快的那个不可能先回来。
    let seen = run_node("bootstrap echo; slow; driver", |ctx, journal| {
        Box::pin(async move {
            let fast_ctx = ctx.clone();
            let fast_journal = journal.clone();
            ctx.spawn(async move {
                let reply = fast_ctx.request(".echo", Payload::text("快")).await.unwrap();
                note(&fast_journal, format!("先到：{}", reply.as_str().unwrap()));
            });

            let reply = ctx.request(".slow", Payload::text("20")).await.unwrap();
            note(&journal, format!("后到：{}", reply.as_str().unwrap()));
        })
    });
    assert_eq!(seen, vec!["先到：快!", "后到：睡了20"]);
}

/// 多个请求在对端是并发处理的，不是排队
#[test]
fn requests_are_served_concurrently() {
    // 三个请求分别要睡 20、10、15 厘秒。若对端是串行处理的，总耗时会是 45 厘秒；
    // 并发处理则接近最慢的 20 厘秒。
    let seen = run_node("bootstrap slow; driver", |ctx, journal| {
        Box::pin(async move {
            let done: Arc<SvcCell<Vec<String>>> = Arc::new(SvcCell::default());
            let started = Instant::now();

            for centis in ["20", "10", "15"] {
                let task_ctx = ctx.clone();
                let done = done.clone();
                ctx.spawn(async move {
                    let reply = task_ctx.request(".slow", Payload::text(centis)).await.unwrap();
                    done.borrow_mut().push(reply.as_str().unwrap().to_string());
                });
            }

            while done.borrow().len() < 3 {
                ctx.sleep(1).await;
            }
            let millis = started.elapsed().as_millis();

            note(&journal, format!("{:?}", done.borrow()));
            note(&journal, format!("{}", millis < 400));
        })
    });
    assert_eq!(
        seen,
        vec![r#"["睡了10", "睡了15", "睡了20"]"#, "true"],
        "应按睡眠时长先后完成，且总耗时接近最慢的那个"
    );
}

/// 对端处理到一半退出时，请求方要收到错误而不是永久挂死
#[test]
fn caller_gets_an_error_when_callee_exits_midway() {
    let seen = run_node("bootstrap quitter; driver", |ctx, journal| {
        Box::pin(async move {
            let err = ctx.request(".quitter", Payload::None).await.unwrap_err();
            note(&journal, format!("{}", matches!(err, Error::CallFailed(_))));
        })
    });
    assert_eq!(seen, vec!["true"]);
}

/// kill 之后地址与名字同时失效，再发过去立刻失败
#[test]
fn requests_to_a_dead_service_fail_immediately() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            let handle = ctx.query_name("echo").unwrap();
            assert!(ctx.kill(handle));
            let err = ctx.request(handle, Payload::None).await.unwrap_err();
            note(&journal, format!("{}", matches!(err, Error::NoService(_))));
            note(&journal, format!("{}", ctx.query_name("echo").is_none()));
        })
    });
    assert_eq!(seen, vec!["true", "true"], "kill 之后地址与名字都应失效");
}

/// send 不等应答，且同一对收发方之间的消息严格保序
#[test]
fn send_is_fire_and_forget_but_ordered() {
    let seen = run_node("bootstrap counter; driver", |ctx, journal| {
        Box::pin(async move {
            for i in 0..100u32 {
                ctx.post(".counter", Payload::text(i.to_string())).unwrap();
            }
            // 后发的这个 call 排在 100 条 send 之后，对端处理到它时应该已经数完
            let reply = ctx.request(".counter", Payload::None).await.unwrap();
            note(&journal, reply.as_str().unwrap());
        })
    });
    assert_eq!(seen, vec!["100"], "同一发送方到同一接收方的消息必须保序");
}

/// 运行期能动态拉起服务，类型不存在或 init 失败时给出对应的错误
#[test]
fn services_can_be_launched_at_runtime() {
    let seen = run_node("bootstrap driver", |ctx, journal| {
        Box::pin(async move {
            let handle = ctx.launch("echo", "").await.unwrap();
            note(&journal, format!("{}", handle != 0));

            let reply = ctx.request(handle, Payload::text("新起的")).await.unwrap();
            note(&journal, reply.as_str().unwrap());

            let err = ctx.launch("查无此类", "").await.unwrap_err();
            note(&journal, format!("{}", matches!(err, Error::UnknownService(_))));

            let err = ctx.launch("stillborn", "").await.unwrap_err();
            note(&journal, format!("{}", matches!(err, Error::Init { .. })));
        })
    });
    assert_eq!(seen, vec!["true", "新起的!", "true", "true"]);
}

/// init 失败的服务不能留在节点里占着服务计数
#[test]
fn failed_init_leaves_no_trace() {
    let seen = run_node("bootstrap driver", |ctx, journal| {
        Box::pin(async move {
            // 先等引导服务退场，服务计数稳定下来再取基准
            ctx.sleep(3).await;
            let before = ctx.service_count();

            note(&journal, format!("{}", ctx.launch("stillborn", "").await.is_err()));
            ctx.sleep(3).await;
            note(&journal, format!("{}", ctx.service_count() == before));
        })
    });
    assert_eq!(seen, vec!["true", "true"]);
}

/// 名字是先到先得的，注册过就不能再被顶掉
#[test]
fn names_cannot_be_registered_twice() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            note(&journal, format!("{}", ctx.register_name("独一份")));
            note(&journal, format!("{}", ctx.register_name("独一份")));
            note(&journal, format!("{}", ctx.register_name("echo")));
        })
    });
    assert_eq!(seen, vec!["true", "false", "false"]);
}

/// 服务内起大量并发任务时，每一个都要被调度到，一个都不能饿死
#[test]
fn many_local_tasks_all_get_scheduled() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            let done = Arc::new(SvcCell::new(0u32));
            for i in 0..200u32 {
                let task_ctx = ctx.clone();
                let done = done.clone();
                ctx.spawn(async move {
                    let _ = task_ctx.request(".echo", Payload::text(i.to_string())).await;
                    *done.borrow_mut() += 1;
                });
            }
            note(&journal, format!("在飞任务 {}", ctx.task_count() >= 200));
            while *done.borrow() < 200 {
                ctx.sleep(1).await;
            }
            note(&journal, format!("全部完成 {}", *done.borrow()));
        })
    });
    assert_eq!(seen, vec!["在飞任务 true", "全部完成 200"]);
}

/// 对照 skynet.stat 的那几个统计口径都要有值
#[test]
fn runtime_stats_are_available() {
    let seen = run_node("bootstrap echo; driver", |ctx, journal| {
        Box::pin(async move {
            for _ in 0..10 {
                ctx.request(".echo", Payload::text("x")).await.unwrap();
            }
            // 10 条应答消息
            note(&journal, format!("{}", ctx.message_count() >= 10));
            note(&journal, format!("{}", ctx.pending_calls() == 0));
            note(&journal, format!("{}", ctx.cpu_cost().as_nanos() > 0));
            note(&journal, format!("{}", ctx.handle() >> 24 == ctx.harbor()));
        })
    });
    assert_eq!(seen, vec!["true", "true", "true", "true"]);
}

/// 压测：单服务连收一百万条消息。
///
/// 默认跳过，跑法：`cargo test --release -- --ignored --nocapture`
#[test]
#[ignore = "压测，请用 cargo test --release -- --ignored 运行"]
fn throughput_one_million_messages() {
    const TOTAL: u64 = 1_000_000;

    let seen = run_node("bootstrap counter; driver", |ctx, journal| {
        Box::pin(async move {
            let started = Instant::now();
            for _ in 0..TOTAL {
                ctx.post(".counter", Payload::None).unwrap();
            }
            let sent = started.elapsed();

            // 这条 call 排在一百万条消息之后，回来时说明对端已经全部处理完
            let reply = ctx.request(".counter", Payload::None).await.unwrap();
            let total = started.elapsed();

            note(&journal, reply.as_str().unwrap());
            println!(
                "发送 {TOTAL} 条耗时 {sent:?}，全部处理完耗时 {total:?}，约 {:.2} 万条/秒",
                TOTAL as f64 / total.as_secs_f64() / 10_000.0
            );
        })
    });
    assert_eq!(seen, vec![TOTAL.to_string()], "一条都不能丢");
}

/// 压测：一堆服务同时可运行时的调度吞吐。
///
/// 上一条压测只有一个收件人，压的是它那把邮箱锁；这条把 64 个服务串成一个环、
/// 同时放 64 个令牌进去转圈，每一跳都要把一个服务重新推进运行队列再取出来，
/// 压的是调度器本身。
///
/// 默认跳过，跑法：`cargo test --release -- --ignored --nocapture`
#[test]
#[ignore = "压测，请用 cargo test --release -- --ignored 运行"]
fn scheduling_throughput_across_many_services() {
    const SERVICES: usize = 64;
    const TOKENS: usize = 64;
    const HOPS_PER_TOKEN: u64 = 10_000;
    /// 固定线程数：令牌数远多于线程数，worker 一直有活干，读数才反映队列本身的
    /// 开销而不是线程反复挂起唤醒的开销。
    const THREADS: usize = 4;

    let shared = Arc::new(RelayShared::default());
    let relay_state = shared.clone();
    let scenario_state = shared.clone();
    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let scenario_journal = journal.clone();

    let registry = Registry::new()
        .with_builtins()
        .with("relay", move || Relay {
            next: SvcCell::new(0),
            shared: relay_state.clone(),
        })
        .with("driver", move || Driver {
            journal: scenario_journal.clone(),
            scenario: {
                let shared = scenario_state.clone();
                Arc::new(move |ctx: Ctx, journal: Journal| {
                    let shared = shared.clone();
                    Box::pin(async move {
                        let mut ring = Vec::with_capacity(SERVICES);
                        for _ in 0..SERVICES {
                            ring.push(ctx.launch("relay", "").await.unwrap());
                        }
                        // 先把环接好。这些 SETUP 都排在令牌之前进各自的邮箱，
                        // 所以任何一个中转站收到令牌时一定已经知道下家是谁
                        for (index, handle) in ring.iter().enumerate() {
                            let next = ring[(index + 1) % SERVICES];
                            ctx.send(*handle, SETUP, Payload::of(next)).unwrap();
                        }

                        *shared.started.lock().unwrap() = Some(Instant::now());
                        for token in 0..TOKENS {
                            let entry = ring[token * SERVICES / TOKENS];
                            ctx.send(entry, TOKEN, Payload::of(HOPS_PER_TOKEN)).unwrap();
                        }
                        // 收尾时刻由最后一个令牌自己记下，所以这里的轮询精度不影响读数
                        while shared.finished.load(SeqCst) < TOKENS as u64 {
                            ctx.sleep(1).await;
                        }

                        let elapsed = shared.elapsed.lock().unwrap().unwrap();
                        let hops = TOKENS as u64 * HOPS_PER_TOKEN;
                        note(&journal, hops.to_string());
                        println!(
                            "{THREADS} 线程 / {SERVICES} 个服务 × {TOKENS} 个令牌共接力 {hops} 跳，耗时 {elapsed:?}，约 {:.2} 万次调度/秒",
                            hops as f64 / elapsed.as_secs_f64() / 10_000.0
                        );
                    })
                })
            },
        });

    rskynet::start(
        Config::default()
            .with_thread(THREADS)
            .with_bootstrap("bootstrap driver"),
        registry,
    )
    .unwrap();

    assert_eq!(
        shared.hops.load(SeqCst),
        TOKENS as u64 * HOPS_PER_TOKEN,
        "一跳都不能丢"
    );
}

/// 引导服务不存在时，启动阶段就要报错
#[test]
fn missing_bootstrap_service_fails_startup() {
    let config = Config::default().with_bootstrap("根本没这个服务");
    let err = rskynet::start(config, Registry::new().with_builtins()).unwrap_err();
    assert!(matches!(err, Error::UnknownService(_)));
}

/// 协议号可以自定义，服务能按 mtype 区别对待
#[test]
fn custom_message_types_work() {
    const MY_PROTO: MsgType = MsgType(42);

    /// 只认 MY_PROTO，其它协议一律回错误。
    #[derive(Default)]
    struct Picky;

    impl Service for Picky {
        fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, rskynet::Result<()>> {
            Box::pin(async move {
                ctx.register_name("picky");
                Ok(())
            })
        }

        fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()> {
            Box::pin(async move {
                if msg.mtype == MY_PROTO {
                    let _ = ctx.reply(&msg, Payload::text("对味"));
                } else {
                    let _ = ctx.reply_error(&msg);
                }
            })
        }
    }

    let journal: Journal = Arc::new(Mutex::new(Vec::new()));
    let shared = journal.clone();
    let registry = Registry::new()
        .with_builtins()
        .with("picky", Picky::default)
        .with("driver", move || Driver {
            journal: shared.clone(),
            scenario: Arc::new(move |ctx: Ctx, journal: Journal| {
                Box::pin(async move {
                    let good = ctx.call(".picky", MY_PROTO, Payload::None).await;
                    note(&journal, format!("{}", good.is_ok()));
                    let bad = ctx.call(".picky", MsgType::USER, Payload::None).await;
                    note(&journal, format!("{}", matches!(bad, Err(Error::CallFailed(_)))));
                })
            }),
        });

    rskynet::start(
        Config::default().with_bootstrap("bootstrap picky; driver"),
        registry,
    )
    .unwrap();

    let seen = journal.lock().unwrap();
    assert_eq!(*seen, vec!["true", "true"]);
}
