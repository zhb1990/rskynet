//! 内置服务与内核的接缝：谁在什么时候被拉起、配置缺席时走什么默认、
//! 时间从哪儿来。
//!
//! 这些用例都真的起一个节点跑一遍，因为要验的正是启动顺序本身。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rskynet::{
    BoxFuture, Builder, BuilderExt, Config, ConfigExt, Ctx, Error, Registry, Result, Timer,
};

struct OrderedService {
    label: &'static str,
    order: Arc<Mutex<Vec<String>>>,
    exit: bool,
}

#[rskynet::service]
impl OrderedService {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        self.order
            .lock()
            .unwrap()
            .push(format!("{}:start", self.label));
        std::thread::yield_now();
        self.order
            .lock()
            .unwrap()
            .push(format!("{}:end", self.label));
        if self.exit {
            ctx.exit();
        }
        Ok(())
    }
}

struct SharedThreadProbe {
    thread: Arc<Mutex<Option<String>>>,
}

#[rskynet::service]
impl SharedThreadProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        *self.thread.lock().unwrap() = std::thread::current().name().map(str::to_string);
        ctx.exit();
        Ok(())
    }
}

struct ExclusiveThreadProbe {
    thread: Arc<Mutex<Option<String>>>,
}

#[rskynet::exclusive]
impl ExclusiveThreadProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        *self.thread.lock().unwrap() = std::thread::current().name().map(str::to_string);
        ctx.exit();
        Ok(())
    }
}

#[derive(Default)]
struct PendingBootstrap;

#[rskynet::service]
impl PendingBootstrap {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            task_ctx.sleep(1).await;
            task_ctx.abort();
        });
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// 用例里那个「跑一段逻辑然后关停节点」的驱动服务。
struct Driver<F> {
    scenario: Mutex<Option<F>>,
}

impl<F> Driver<F>
where
    F: FnOnce(Ctx) -> BoxFuture<'static, ()> + Send + 'static,
{
    fn new(scenario: F) -> Self {
        Self {
            scenario: Mutex::new(Some(scenario)),
        }
    }
}

#[rskynet::service]
impl<F> Driver<F>
where
    F: FnOnce(Ctx) -> BoxFuture<'static, ()> + Send + Sync + 'static,
{
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let scenario = self.scenario.lock().unwrap().take();
        if let Some(scenario) = scenario {
            let task_ctx = ctx.clone();
            ctx.spawn(async move {
                scenario(task_ctx.clone()).await;
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(20));
                    task_ctx.abort();
                });
            });
        } else {
            let task_ctx = ctx.clone();
            ctx.spawn(async move {
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(20));
                    task_ctx.abort();
                });
            });
        }
        Ok(())
    }
}

/// 配置段全都不写时，内置服务照样按约定名字被拉起来
#[test]
fn builtins_start_without_any_configuration() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let shared = seen.clone();
    let registry = Registry::new().with("driver", move || {
        let shared = shared.clone();
        Driver::new(move |ctx: Ctx| {
            Box::pin(async move {
                let mut names = Vec::new();
                for name in ["logger", "signal", "timer"] {
                    if ctx.query_name(name).is_some() {
                        names.push(name.to_string());
                    }
                }
                *shared.lock().unwrap() = names;
            })
        })
    });

    // 配置里只写了引导清单，其余系统服务段整个缺席
    let config = Config::default().with_thread(2).with_bootstrap(["driver"]);
    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            "logger".to_string(),
            "signal".to_string(),
            "timer".to_string()
        ],
        "系统服务都该按约定名字注册好"
    );
}

#[test]
fn startup_services_are_complete_ordering_barriers() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    for (kind, label, exit) in [
        ("optional", "optional", true),
        ("business-a", "business-a", true),
        ("business-b", "business-b", true),
    ] {
        let order = order.clone();
        registry.register(kind, move || OrderedService {
            label,
            order: order.clone(),
            exit,
        });
    }
    let config = Config::default().with_bootstrap(["business-a", "business-b"]);
    Builder::new(config)
        .registry(registry)
        .with_builtins()
        .startup_service("optional", "")
        .run()
        .unwrap();

    let expected: Vec<String> = [
        "optional:start",
        "optional:end",
        "business-a:start",
        "business-a:end",
        "business-b:start",
        "business-b:end",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(*order.lock().unwrap(), expected);
}

#[test]
fn init_runs_on_the_assigned_executor() {
    let shared = Arc::new(Mutex::new(None));
    let exclusive = Arc::new(Mutex::new(None));
    let shared_factory = shared.clone();
    let exclusive_factory = exclusive.clone();
    let registry = Registry::new()
        .with("shared-probe", move || SharedThreadProbe {
            thread: shared_factory.clone(),
        })
        .with_exclusive("exclusive-probe", move || ExclusiveThreadProbe {
            thread: exclusive_factory.clone(),
        });
    let config = Config::default().with_bootstrap(["shared-probe", "exclusive-probe"]);
    rskynet::start(config, registry).unwrap();

    assert!(
        shared
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|name| name.starts_with("rskynet-worker-"))
    );
    assert!(
        exclusive
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|name| name.starts_with("rskynet-exclusive-probe-"))
    );
}

#[test]
fn a_custom_bootstrap_is_scheduled_without_a_sync_init_wait() {
    let config = Config::default().with_bootstrap_service("pending-bootstrap");
    let registry = Registry::new().with("pending-bootstrap", PendingBootstrap::default);
    rskynet::start(config, registry).expect("自定义 bootstrap 的未完成 init 不应让启动阶段报错");
}

/// 定时器排在引导之前：引导期间挂的表能醒，刻度也已经在走
#[test]
fn the_clock_runs_during_bootstrap() {
    let woke = Arc::new(AtomicBool::new(false));
    let ticked = Arc::new(AtomicBool::new(false));
    let woke_probe = woke.clone();
    let ticked_probe = ticked.clone();

    let registry = Registry::new().with("driver", move || {
        let woke = woke_probe.clone();
        let ticked = ticked_probe.clone();
        Driver::new(move |ctx: Ctx| {
            Box::pin(async move {
                // 这一觉发生在引导期间：驱动服务是被 bootstrap 拉起来的
                ctx.sleep(2).await;
                woke.store(true, SeqCst);
                ticked.store(ctx.now() > 0, SeqCst);
            })
        })
    });

    let config = Config::default().with_thread(2).with_bootstrap(["driver"]);
    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    assert!(woke.load(SeqCst), "引导期间挂的表必须能醒过来");
    assert!(
        ticked.load(SeqCst),
        "引导期间刻度就该在走，now() 不能恒为 0"
    );
}

/// 引导清单为空时节点要正常退出，而不是等在那儿
#[test]
fn an_empty_bootstrap_list_still_exits() {
    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    let worker = std::thread::spawn(move || {
        rskynet::start(Config::default().with_thread(2), Registry::new())
            .expect("节点应当正常启动并退出");
        flag.store(true, SeqCst);
    });

    // 没有业务服务，引导服务办完事就退场，节点跟着收工
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !done.load(SeqCst) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(done.load(SeqCst), "空清单的节点应当自己退出");
    worker.join().unwrap();
}

/// 定时器也是个正常服务：能应答时间戳查询，而且读数与 node.now() 对得上
#[test]
fn the_timer_service_answers_timestamp_requests() {
    let stamp = Arc::new(Mutex::new(None));
    let shared = stamp.clone();
    let registry = Registry::new().with("driver", move || {
        let shared = shared.clone();
        Driver::new(move |ctx: Ctx| {
            Box::pin(async move {
                // 先睡一下，好让刻度真的走过几格
                ctx.sleep(3).await;
                let stamp = rskynet::timer::timestamp(&ctx)
                    .await
                    .expect("定时器应当回一份时间戳");
                *shared.lock().unwrap() = Some((stamp, ctx.now(), ctx.start_time()));
            })
        })
    });

    let config = Config::default().with_thread(2).with_bootstrap(["driver"]);
    rskynet::start(config, registry).expect("节点应当正常启动并退出");

    let seen = stamp.lock().unwrap();
    let (stamp, now, start_time) = seen.expect("应当拿到时间戳");
    assert!(stamp.now > 0, "睡过之后刻度应当已经推进");
    assert!(stamp.now >= now, "消息晚于本地读数发出，读到的时间只会更大");
    assert_eq!(stamp.start_seconds, start_time);
    assert!(stamp.unix_time() > 1_600_000_000.0, "unix 时间应当是合理值");
}

/// tick 配成 0 是没法工作的，启动时就要报错
#[test]
fn a_zero_tick_is_rejected() {
    let config = Config::from_toml_str(
        r#"
        thread = 2

        [timer]
        tick_micros = 0
        "#,
    )
    .expect("配置本身是合法 TOML");
    let err = rskynet::start(config, Registry::new()).expect_err("tick 为 0 应当启动失败");
    assert!(matches!(err, Error::Init { .. }));
}

/// 换掉时间来源：内核只认注入进来的那个实现
#[test]
fn a_custom_timer_can_replace_the_wheel() {
    /// 走得飞快的假时钟：每读一次就跳一格，挂表一律当场到期。
    struct FakeTimer {
        elapsed: AtomicU64,
    }

    impl Timer for FakeTimer {
        fn timeout(&self, _handle: u32, _session: i32, _ticks: u32) {}

        fn now(&self) -> u64 {
            self.elapsed.fetch_add(1, SeqCst)
        }

        fn wall_clock(&self) -> u64 {
            self.now()
        }

        fn start_seconds(&self) -> u64 {
            42
        }
    }

    let seen = Arc::new(AtomicU64::new(0));
    let shared = seen.clone();
    let registry = Registry::new().with("driver", move || {
        let shared = shared.clone();
        Driver::new(move |ctx: Ctx| {
            Box::pin(async move {
                shared.store(ctx.start_time(), SeqCst);
            })
        })
    });

    // 假时钟自己不会派发到期消息，所以不必起定时器服务
    let config = Config::from_toml_str(
        r#"
        thread = 2

        [timer]
        name = ""
        "#,
    )
    .expect("配置应解析成功")
    .with_bootstrap(["driver"]);

    Builder::new(config)
        .registry(registry)
        .with_builtins()
        // with_builtins 已经注入过时间轮，这一句把它换掉：后来者说了算
        .timer(Arc::new(FakeTimer {
            elapsed: AtomicU64::new(0),
        }))
        .run()
        .expect("节点应当正常启动并退出");

    assert_eq!(seen.load(SeqCst), 42, "读到的应当是假时钟给的启动时刻");
}
