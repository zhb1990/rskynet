//! 网络层的端到端验证：真的开端口、真的连上去、真的收发字节。
//!
//! 走的都是公开路径——业务服务用 `net::listen` / `net::connect` 这些函数下命令，
//! 用 [`MsgType::SOCKET`] 收事件，与使用方写出来的代码没有两样。
//!
//! 几个断言是钉住设计的：
//!
//! - **accept 出来的连接默认不收数据**，属主调 `start` 才开始。那个空档是留给
//!   「把连接转交给别的服务」的，与 C 版的 `SOCKET_TYPE_PACCEPT` 一致。
//! - **`pause` 之后一个字节都不该上来**，`start` 之后先前压着的还得原样上来。
//! - **写缓冲堆高了要报警，但一个字节都不能丢也不能乱序**。
//! - **节点收工时网络层那条线程自己退掉**，否则 `rskynet::start` 就不会返回，
//!   用例会直接卡住。
//!
//! 用例里的服务只往「账本」上记，不 `assert`：服务跑在 worker 线程上，在那儿
//! panic 只会让节点带着一堆活着的服务僵住，看不到是哪一步出的岔子。所以每一步的
//! 结论都记一行，节点退出之后再统一断言。
//!
//! 网络层在 dev-dependencies 里，所以这些用例不加 feature 也跑。

use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr, TcpListener, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rskynet::{Builder, BuilderExt, Config, ConfigExt, Ctx, MsgType, Registry, Result, SvcCell};
use rskynet_net::{self as net, RegistryExt, SocketEvent};

/// 慢机器上也够用的上限：`ctx.sleep` 的单位是毫秒，1000 次就是 10 秒。
const PATIENCE: u32 = 1000;

/// 服务之间传监听地址用的黑板。
///
/// 端口写的是 0，得等系统挑完才知道是哪个。黑板用于把实际地址从回声服务传给
/// 客户端，避免测试把动态端口写死。
type Blackboard = Arc<Mutex<Option<SocketAddr>>>;

/// 用例观察到的现象，节点退出后统一断言。
#[derive(Default)]
struct Tape {
    notes: Mutex<Vec<String>>,
    received: Mutex<Vec<u8>>,
}

impl Tape {
    fn note(&self, entry: impl Into<String>) {
        self.notes.lock().unwrap().push(entry.into());
    }

    /// 记一条判断题的答案，好让断言指得出是哪一步。
    fn check(&self, what: &str, ok: bool) {
        self.note(format!("{what}={ok}"));
    }

    fn len(&self) -> usize {
        self.received.lock().unwrap().len()
    }

    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut self.received.lock().unwrap())
    }

    fn notes(&self) -> Vec<String> {
        self.notes.lock().unwrap().clone()
    }
}

/// 等某件事发生，超时返回 `false`。
///
/// 事件从别的服务经邮箱过来，所以只能轮询——服务内部不加锁的代价就是这个。
async fn until(ctx: &Ctx, mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..PATIENCE {
        if done() {
            return true;
        }
        ctx.sleep(1).await;
    }
    false
}

/// 等回声服务把它的监听地址写上黑板。
async fn server_addr(ctx: &Ctx, board: &Blackboard) -> Option<SocketAddr> {
    let mut found = None;
    until(ctx, || {
        found = *board.lock().unwrap();
        found.is_some()
    })
    .await;
    found
}

// ------------------------------------------------------------ 回声服务

/// 收到什么就发回什么，顺手把「新连接要先 start」那条路走一遍。
struct Echo {
    board: Blackboard,
    /// accept 过几条连接。
    accepted: SvcCell<usize>,
}

#[rskynet::service]
impl Echo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        assert!(ctx.register_name("echo-net"));
        let id = net::listen(&ctx, "127.0.0.1:0").await?;
        // 端口写的是 0，系统挑了哪个得问
        let info = net::info(&ctx, id).await?;
        *self.board.lock().unwrap() = info.local;
        // 到这一步才开始 accept，与 C 版的 PLISTEN → LISTEN 一致
        net::start(&ctx, id).await?;
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Accept { id, .. } => {
                self.accepted.set(self.accepted.get() + 1);
                // 属主在这里定下来：从此这条连接的数据投给我
                let _ = net::start(&ctx, id).await;
                let _ = net::set_nodelay(&ctx, id, true);
            }
            SocketEvent::Data { id, data } => {
                let _ = net::send(&ctx, id, data);
            }
            _ => {}
        }
    }
}

/// 起一个「网络层 + 回声服务 + 待测服务」的节点，跑完返回账本。
fn run_against_echo(
    kind: &'static str,
    tape: &Arc<Tape>,
    make: impl Fn(Blackboard, Arc<Tape>) -> Registry,
    config: Config,
) -> Vec<String> {
    let board: Blackboard = Arc::new(Mutex::new(None));
    let registry = make(board.clone(), tape.clone())
        .with_net()
        .with("echo", move || Echo {
            board: board.clone(),
            accepted: SvcCell::new(0),
        });
    let config = config.with_bootstrap(["echo", kind]);
    run_node(config, registry);
    tape.notes()
}

fn run_node(config: Config, registry: Registry) {
    Builder::new(config)
        .registry(registry)
        .with_builtins()
        .startup_service(net::NAME, "")
        .run()
        .expect("节点应当正常启动并退出");
}

// ------------------------------------------------------------ TCP 全程

#[derive(Clone)]
struct Client {
    board: Blackboard,
    tape: Arc<Tape>,
}

#[rskynet::service]
impl Client {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        assert!(ctx.register_name("client-net"));
        let this = self.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            if let Err(err) = this.run(&task_ctx).await {
                this.tape.note(format!("出错={err}"));
            }
            task_ctx.abort();
        });
        Ok(())
    }

    /// 没被宏认领的方法，原样留在 impl 块里。
    async fn run(&self, ctx: &Ctx) -> Result<()> {
        let Some(server) = server_addr(ctx, &self.board).await else {
            self.tape.note("等不到监听地址");
            return Ok(());
        };
        // 连上了才返回，返回之后数据就投给我了，不必再 start
        let id = net::connect(ctx, server.to_string()).await?;

        // 一来一回
        net::send(ctx, id, b"hello".to_vec())?;
        let echoed = until(ctx, || self.tape.len() >= 5).await;
        self.tape.check("回声", echoed);
        self.tape.note(format!(
            "收到={}",
            String::from_utf8_lossy(&self.tape.take())
        ));

        // 掐掉读：这期间发过去的东西压在内核缓冲里，一个字节都不该上来
        net::pause(ctx, id).await?;
        self.tape
            .check("暂停统计为不读", !net::info(ctx, id).await?.reading);
        net::send(ctx, id, b"paused".to_vec())?;
        for _ in 0..20 {
            ctx.sleep(1).await;
        }
        self.tape.check("暂停期间静默", self.tape.len() == 0);

        // 恢复读：先前压着的还得原样上来
        net::start(ctx, id).await?;
        self.tape
            .check("恢复统计为读", net::info(ctx, id).await?.reading);
        let resumed = until(ctx, || self.tape.len() >= 6).await;
        self.tape.check("恢复后收到", resumed);
        self.tape.note(format!(
            "收到={}",
            String::from_utf8_lossy(&self.tape.take())
        ));

        // 连接的现状问得出来
        let info = net::info(ctx, id).await?;
        self.tape.note(format!("现状={} {}", info.kind, info.state));
        self.tape.check("对端地址", info.peer == Some(server));
        self.tape.check("读了字节", info.read_bytes >= 11);

        let stats = net::netstat(ctx).await?;
        let current = stats
            .iter()
            .find(|info| info.id == id)
            .expect("连接应在统计中");
        self.tape
            .check("统计归属 handle", current.owner == ctx.handle());
        self.tape.check(
            "统计归属类型",
            current.owner_kind.as_deref() == Some("client"),
        );
        self.tape.check(
            "统计归属名字",
            current.owner_names.iter().any(|name| name == "client-net"),
        );
        self.tape.check(
            "统计收发时间",
            current.last_read_at_ms.is_some() && current.last_write_at_ms.is_some(),
        );
        self.tape
            .check("统计无待写", !current.writing && current.write_pending == 0);
        let listener = stats
            .iter()
            .find(|info| info.kind == "listener")
            .expect("监听口应在统计中");
        self.tape.check(
            "监听统计",
            listener.accept_count == 1 && listener.last_read_at_ms.is_some(),
        );
        self.tape.check(
            "监听归属",
            listener.owner_kind.as_deref() == Some("echo")
                && listener.owner_names.iter().any(|name| name == "echo-net"),
        );

        // 自己关的连接也会收到一条 Close：「socket 没了」只有这一个信号
        net::close(ctx, id).await?;
        let closed = until(ctx, || {
            self.tape.notes().iter().any(|entry| entry == "关了")
        })
        .await;
        self.tape.check("收到关闭事件", closed);
        self.tape.check(
            "释放后不显示",
            !net::netstat(ctx).await?.iter().any(|info| info.id == id),
        );

        // 已经关掉的 id 再用就该报错，而不是打到别人身上
        self.tape
            .check("旧 id 失效", net::info(ctx, id).await.is_err());
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, _ctx: Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Data { data, .. } => {
                self.tape.received.lock().unwrap().extend_from_slice(&data);
            }
            SocketEvent::Close { .. } => self.tape.note("关了"),
            SocketEvent::Error { reason, .. } => self.tape.note(format!("出错={reason}")),
            _ => {}
        }
    }
}

/// TCP 全程：listen → start → accept → start → 收发 → pause / resume → close
#[test]
fn a_tcp_connection_goes_the_whole_way() {
    let tape = Arc::new(Tape::default());
    let notes = run_against_echo(
        "client",
        &tape,
        |board, tape| {
            Registry::new().with("client", move || Client {
                board: board.clone(),
                tape: tape.clone(),
            })
        },
        Config::default(),
    );

    assert_eq!(
        notes,
        vec![
            "回声=true",
            "收到=hello",
            "暂停统计为不读=true",
            "暂停期间静默=true",
            "恢复统计为读=true",
            "恢复后收到=true",
            "收到=paused",
            "现状=stream connected",
            "对端地址=true",
            "读了字节=true",
            "统计归属 handle=true",
            "统计归属类型=true",
            "统计归属名字=true",
            "统计收发时间=true",
            "统计无待写=true",
            "监听统计=true",
            "监听归属=true",
            "关了",
            "收到关闭事件=true",
            "释放后不显示=true",
            "旧 id 失效=true",
        ]
    );
}

// ------------------------------------------------------------ 背压与告警

/// 灌多少。比 `warn_size` 大得多，好让写缓冲一定堆到报警线以上。
const FLOOD: usize = 2 * 1024 * 1024;

/// 第 `index` 个字节该是什么。用它验证回来的数据既没丢也没乱序。
fn pattern(index: usize) -> u8 {
    (index % 251) as u8
}

#[derive(Clone)]
struct Flooder {
    board: Blackboard,
    tape: Arc<Tape>,
}

#[rskynet::service]
impl Flooder {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let this = self.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            if let Err(err) = this.run(&task_ctx).await {
                this.tape.note(format!("出错={err}"));
            }
            task_ctx.abort();
        });
        Ok(())
    }

    async fn run(&self, ctx: &Ctx) -> Result<()> {
        let Some(server) = server_addr(ctx, &self.board).await else {
            self.tape.note("等不到监听地址");
            return Ok(());
        };
        let id = net::connect(ctx, server.to_string()).await?;

        // 一次压 2 MiB 进去：内核缓冲吃不下这么多，剩下的都进写缓冲，于是必定越过
        // warn_size（配置里调到了 4 KiB）
        net::send(ctx, id, (0..FLOOD).map(pattern).collect())?;

        let all_back = until(ctx, || self.tape.len() >= FLOOD).await;
        self.tape.check("原样回来", all_back);
        self.tape.note(format!("回来={}", self.tape.len()));

        let received = self.tape.take();
        let wrong = received
            .iter()
            .enumerate()
            .position(|(index, byte)| *byte != pattern(index));
        self.tape.note(match wrong {
            None => "顺序无误".to_string(),
            Some(at) => format!("第 {at} 个字节就对不上了"),
        });
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, _ctx: Ctx, event: SocketEvent) {
        match event {
            SocketEvent::Data { data, .. } => {
                self.tape.received.lock().unwrap().extend_from_slice(&data);
            }
            SocketEvent::Warning { kilobytes, .. } => {
                // 只记第一条：越过阈值之后每翻倍还会再来一条，条数不该被钉死
                if !self.tape.notes().iter().any(|e| e.starts_with("告警")) {
                    self.tape.note(format!("告警 {kilobytes} KiB"));
                }
            }
            SocketEvent::Error { reason, .. } => self.tape.note(format!("出错={reason}")),
            _ => {}
        }
    }
}

/// 写缓冲堆高了要报警，但数据既不丢也不乱序
#[test]
fn a_flood_warns_but_keeps_every_byte() {
    let tape = Arc::new(Tape::default());
    // 告警线压到 4 KiB，好让这一坨一定越线
    let config = Config::from_toml_str(
        r#"
        thread = 4

        [net]
        warn_size = 4096
        "#,
    )
    .expect("配置应解析成功");

    let notes = run_against_echo(
        "flooder",
        &tape,
        |board, tape| {
            Registry::new().with("flooder", move || Flooder {
                board: board.clone(),
                tape: tape.clone(),
            })
        },
        config,
    );

    assert!(
        notes.iter().any(|entry| entry.starts_with("告警 ")),
        "写缓冲堆到 2 MiB 该报警，账本是 {notes:?}"
    );
    assert!(
        notes.contains(&"原样回来=true".to_string()) && notes.contains(&"顺序无误".to_string()),
        "背压之下一个字节都不该丢，账本是 {notes:?}"
    );
}

// ------------------------------------------------------------ UDP

type DatagramLog = Arc<Mutex<Vec<(SocketAddr, Vec<u8>)>>>;

#[derive(Clone)]
struct UdpProbe {
    tape: Arc<Tape>,
    got: DatagramLog,
}

#[rskynet::service]
impl UdpProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        assert!(ctx.register_name("udp-net"));
        let this = self.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            if let Err(err) = this.run(&task_ctx).await {
                this.tape.note(format!("出错={err}"));
            }
            task_ctx.abort();
        });
        Ok(())
    }

    async fn run(&self, ctx: &Ctx) -> Result<()> {
        // UDP 没有握手，开出来就能收，不必像 TCP 那样再 start 一次
        let left = net::udp(ctx, Some("127.0.0.1:0")).await?;
        let right = net::udp(ctx, Some("127.0.0.1:0")).await?;
        let left_addr = net::info(ctx, left).await?.local.expect("应当绑上了");
        let right_addr = net::info(ctx, right).await?.local.expect("应当绑上了");

        // 指定地址发
        net::udp_send(ctx, left, Some(right_addr), b"ping".to_vec())?;
        // 记下默认对端之后就不必再写地址了
        net::udp_connect(ctx, right, left_addr.to_string()).await?;
        net::udp_send(ctx, right, None, b"pong".to_vec())?;

        let both = until(ctx, || self.got.lock().unwrap().len() >= 2).await;
        self.tape.check("两个包都到了", both);

        let (ping_from, pong_from) = {
            let got = self.got.lock().unwrap();
            let from = |what: &[u8]| {
                got.iter()
                    .find(|(_, data)| data == what)
                    .map(|(from, _)| *from)
            };
            (from(b"ping"), from(b"pong"))
        };
        self.tape
            .check("ping 的发件人", ping_from == Some(left_addr));
        self.tape
            .check("pong 走默认对端", pong_from == Some(right_addr));

        let stats = net::netstat(ctx).await?;
        let left_stat = stats
            .iter()
            .find(|info| info.id == left)
            .expect("左端口应在统计中");
        let right_stat = stats
            .iter()
            .find(|info| info.id == right)
            .expect("右端口应在统计中");
        self.tape.check(
            "UDP 收发统计",
            left_stat.read_bytes == 4
                && left_stat.write_bytes == 4
                && right_stat.read_bytes == 4
                && right_stat.write_bytes == 4
                && left_stat.last_read_at_ms.is_some()
                && left_stat.last_write_at_ms.is_some()
                && right_stat.last_read_at_ms.is_some()
                && right_stat.last_write_at_ms.is_some(),
        );
        self.tape.check(
            "UDP 归属",
            [left_stat, right_stat].iter().all(|info| {
                info.owner == ctx.handle()
                    && info.owner_kind.as_deref() == Some("udp-probe")
                    && info.owner_names.iter().any(|name| name == "udp-net")
            }),
        );
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, _ctx: Ctx, event: SocketEvent) {
        if let SocketEvent::Udp { from, data, .. } = event {
            self.got.lock().unwrap().push((from, data));
        }
    }
}

/// UDP 互发：地址如实带上来，默认对端也认
#[test]
fn udp_packets_carry_their_sender() {
    let tape = Arc::new(Tape::default());
    let got = Arc::new(Mutex::new(Vec::new()));
    let probe_tape = tape.clone();
    let probe_got = got.clone();
    let registry = Registry::new()
        .with_net()
        .with("udp-probe", move || UdpProbe {
            tape: probe_tape.clone(),
            got: probe_got.clone(),
        });

    let config = Config::default().with_bootstrap(["udp-probe"]);
    run_node(config, registry);

    assert_eq!(
        tape.notes(),
        vec![
            "两个包都到了=true",
            "ping 的发件人=true",
            "pong 走默认对端=true",
            "UDP 收发统计=true",
            "UDP 归属=true",
        ]
    );
}

// ------------------------------------------------------------ 空统计

struct EmptyStatProbe {
    tape: Arc<Tape>,
}

#[rskynet::service]
impl EmptyStatProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let tape = self.tape.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            tape.check(
                "空网络统计",
                net::netstat(&task_ctx)
                    .await
                    .is_ok_and(|stats| stats.is_empty()),
            );
            task_ctx.abort();
        });
        Ok(())
    }
}

#[test]
fn netstat_is_empty_without_sockets() {
    let tape = Arc::new(Tape::default());
    let probe_tape = tape.clone();
    let registry = Registry::new()
        .with_net()
        .with("empty-stat-probe", move || EmptyStatProbe {
            tape: probe_tape.clone(),
        });
    run_node(
        Config::default().with_bootstrap(["empty-stat-probe"]),
        registry,
    );
    assert_eq!(tape.notes(), vec!["空网络统计=true"]);
}

// ------------------------------------------------------------ DNS 多地址回退

#[derive(Clone)]
struct HostProbe {
    host: String,
    tape: Arc<Tape>,
}

#[rskynet::service]
impl HostProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let this = self.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            this.tape.check(
                "回退地址连通",
                net::connect(&task_ctx, this.host.clone()).await.is_ok(),
            );
            task_ctx.abort();
        });
        Ok(())
    }
}

/// `localhost` 同时有 IPv6 / IPv4 时，只在解析结果靠后的地址监听，验证第一个地址
/// 异步连接失败后会继续尝试。只有单一地址的环境没有可构造的回退场景，直接跳过。
#[test]
fn connect_tries_later_resolved_addresses() {
    let probe: Vec<SocketAddr> = "localhost:0"
        .to_socket_addrs()
        .expect("localhost 应能解析")
        .collect();
    let Some(first_ip) = probe.first().map(SocketAddr::ip) else {
        return;
    };
    let Some(fallback_ip) = probe
        .iter()
        .map(SocketAddr::ip)
        .find(|ip| *ip != first_ip && matches!(ip, IpAddr::V4(_) | IpAddr::V6(_)))
    else {
        return;
    };

    let listener =
        TcpListener::bind(SocketAddr::new(fallback_ip, 0)).expect("应能在回退地址上监听");
    listener
        .set_nonblocking(true)
        .expect("应能把测试 listener 设成非阻塞");
    let port = listener.local_addr().expect("应有本地地址").port();
    let host = format!("localhost:{port}");
    let resolved: Vec<SocketAddr> = host
        .to_socket_addrs()
        .expect("带端口的 localhost 应能解析")
        .collect();
    if resolved.first().map(SocketAddr::ip) == Some(fallback_ip)
        || !resolved.iter().any(|addr| addr.ip() == fallback_ip)
    {
        return;
    }

    let accepted = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match listener.accept() {
                Ok(_) => return true,
                Err(err) if err.kind() == ErrorKind::WouldBlock && Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(err) if err.kind() == ErrorKind::WouldBlock => return false,
                Err(err) => panic!("accept 失败：{err}"),
            }
        }
    });

    let tape = Arc::new(Tape::default());
    let probe_tape = tape.clone();
    let registry = Registry::new()
        .with_net()
        .with("host-probe", move || HostProbe {
            host: host.clone(),
            tape: probe_tape.clone(),
        });
    let config = Config::default().with_bootstrap(["host-probe"]);
    run_node(config, registry);

    assert_eq!(tape.notes(), vec!["回退地址连通=true"]);
    assert!(accepted.join().expect("监听线程不应 panic"));
}
