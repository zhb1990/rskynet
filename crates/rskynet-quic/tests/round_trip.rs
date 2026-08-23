use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, FromPayload, MsgType, Payload, Registry, Result};
use rskynet_net::RegistryExt as _;
use rskynet_quic::{
    ClientTlsConfig, PrivateKeyInput, QuicClientOptions, QuicEvent, QuicServerOptions,
    QuicTransportOptions, RegistryExt as _, ServerTlsConfig, ServerVerification,
};
use rskynet_timer::BuilderExt as _;

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    client_connection: Mutex<Option<rskynet_quic::QuicConnectionId>>,
    unauthorized_shutdown_sent: Mutex<bool>,
    received: Mutex<Vec<u8>>,
    send_finished: Mutex<bool>,
    receive_finished: Mutex<bool>,
}

const PAYLOAD_LEN: usize = 32 * 1024;

fn constrained_transport() -> QuicTransportOptions {
    QuicTransportOptions {
        stream_receive_window: 1024,
        receive_window: 1024,
        ..Default::default()
    }
}

struct Echo {
    board: Arc<Board>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Echo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = rskynet_quic::listen(
            &ctx,
            QuicServerOptions::new("127.0.0.1:0", self.tls.clone())
                .with_transport(constrained_transport()),
        )
        .await?;
        *self.board.address.lock().unwrap() = Some(rskynet_quic::local_addr(&ctx, listener).await?);
        Ok(())
    }

    #[msg(MsgType::QUIC)]
    async fn on_quic(&self, ctx: Ctx, event: QuicEvent) {
        match event {
            QuicEvent::Connected {
                id,
                listener: Some(_),
                ..
            } => {
                let _ = rskynet_quic::start(&ctx, id).await;
            }
            QuicEvent::StreamData { id, stream, data } => {
                let _ = rskynet_quic::send_wait(&ctx, id, stream, data).await;
            }
            QuicEvent::ReceiveFinished { id, stream } => {
                let _ = rskynet_quic::finish(&ctx, id, stream).await;
            }
            _ => {}
        }
    }
}

struct Client {
    board: Arc<Board>,
    tls: ClientTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Client {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let tls = self.tls.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: Result<()> = async {
                let address = loop {
                    if let Some(address) = *board.address.lock().unwrap() {
                        break address;
                    }
                    task_ctx.sleep_ms(10).await;
                };
                let id = rskynet_quic::connect(
                    &task_ctx,
                    QuicClientOptions::new(address.to_string(), "localhost", tls)
                        .with_transport(constrained_transport()),
                )
                .await?;
                *board.client_connection.lock().unwrap() = Some(id);
                loop {
                    if *board.unauthorized_shutdown_sent.lock().unwrap() {
                        break;
                    }
                    task_ctx.sleep_ms(10).await;
                }
                task_ctx.sleep_ms(20).await;
                let stream = rskynet_quic::open_bi(&task_ctx, id).await?;
                rskynet_quic::send(&task_ctx, id, stream, vec![b'q'; PAYLOAD_LEN])?;
                rskynet_quic::finish(&task_ctx, id, stream).await?;
                for _ in 0..500 {
                    if board.received.lock().unwrap().len() == PAYLOAD_LEN
                        && *board.send_finished.lock().unwrap()
                        && *board.receive_finished.lock().unwrap()
                    {
                        break;
                    }
                    task_ctx.sleep_ms(10).await;
                }
                let _ = rskynet_quic::close(&task_ctx, id, 0, Vec::new()).await;
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "QUIC 客户端流程失败：{result:?}");
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::QUIC)]
    async fn on_quic(&self, _ctx: Ctx, event: QuicEvent) {
        match event {
            QuicEvent::StreamData { data, .. } => {
                self.board.received.lock().unwrap().extend(data);
            }
            QuicEvent::SendFinished { .. } => {
                *self.board.send_finished.lock().unwrap() = true;
            }
            QuicEvent::ReceiveFinished { .. } => {
                *self.board.receive_finished.lock().unwrap() = true;
            }
            _ => {}
        }
    }
}

struct UnauthorizedShutdown {
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl UnauthorizedShutdown {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let task = ctx.clone();
        ctx.spawn(async move {
            let id = loop {
                if let Some(id) = *board.client_connection.lock().unwrap() {
                    break id;
                }
                task.sleep_ms(10).await;
            };
            rskynet_quic::shutdown(&task, id).expect("攻击服务应能投递 shutdown 命令");
            *board.unauthorized_shutdown_sent.lock().unwrap() = true;
        });
        Ok(())
    }
}

#[test]
fn client_and_server_exchange_stream_data() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let certificate = rskynet_quic::CertificateDer::from(cert.der().to_vec());
    let server = ServerTlsConfig::single_cert(
        vec![certificate.clone()],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();
    let client = ClientTlsConfig::new(
        ServerVerification::CustomRoots {
            roots: vec![certificate],
        },
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();

    let board = Arc::new(Board::default());
    let echo_board = board.clone();
    let client_board = board.clone();
    let attacker_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_quic()
        .with("echo", move || Echo {
            board: echo_board.clone(),
            tls: server.clone(),
        })
        .with("client", move || Client {
            board: client_board.clone(),
            tls: client.clone(),
        })
        .with("unauthorized-shutdown", move || UnauthorizedShutdown {
            board: attacker_board.clone(),
        });
    let mut config = Config::default().with_bootstrap(["echo", "client", "unauthorized-shutdown"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_quic::NAME, "")
        .run()
        .expect("QUIC 测试节点应正常退出");

    assert_eq!(*board.received.lock().unwrap(), vec![b'q'; PAYLOAD_LEN]);
    assert!(*board.send_finished.lock().unwrap());
    assert!(*board.receive_finished.lock().unwrap());
}

struct InvalidCommandProbe;

#[rskynet_macros::service(crate = ::rskynet_core)]
impl InvalidCommandProbe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let task = ctx.clone();
        ctx.spawn(async move {
            let reply = task
                .call(
                    rskynet_quic::NAME,
                    MsgType::USER,
                    Payload::of(rskynet_quic::Command::Close {
                        id: rskynet_quic::QuicConnectionId(1),
                        error_code: 1_u64 << 62,
                        reason: Vec::new(),
                    }),
                )
                .await
                .expect("QUIC 服务应回复非法错误码，而不是 panic");
            let answer = rskynet_quic::Answer::from_payload(reply).expect("应返回 QUIC Answer");
            assert!(
                matches!(answer, rskynet_quic::Answer::Failed(reason) if reason.contains("error code"))
            );
            task.abort();
        });
        Ok(())
    }
}

#[test]
fn raw_commands_with_invalid_error_codes_do_not_abort() {
    let registry = Registry::new()
        .with_net()
        .with_quic()
        .with("invalid-command-probe", || InvalidCommandProbe);
    let mut config = Config::default().with_bootstrap(["invalid-command-probe"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_quic::NAME, "")
        .run()
        .expect("非法 QUIC 命令不应终止节点");
}

#[derive(Default)]
struct LifecycleBoard {
    address: Mutex<Option<SocketAddr>>,
    terminal_event: Mutex<bool>,
}

struct UnstartedServer {
    board: Arc<LifecycleBoard>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl UnstartedServer {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = rskynet_quic::listen(
            &ctx,
            QuicServerOptions::new("127.0.0.1:0", self.tls.clone()),
        )
        .await?;
        *self.board.address.lock().unwrap() = Some(rskynet_quic::local_addr(&ctx, listener).await?);
        Ok(())
    }

    #[msg(MsgType::QUIC)]
    async fn on_quic(&self, ctx: Ctx, event: QuicEvent) {
        if matches!(event, QuicEvent::Close { .. } | QuicEvent::Error { .. }) {
            *self.board.terminal_event.lock().unwrap() = true;
            ctx.abort();
        }
    }
}

struct ClosingClient {
    board: Arc<LifecycleBoard>,
    tls: ClientTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl ClosingClient {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let tls = self.tls.clone();
        let task = ctx.clone();
        ctx.spawn(async move {
            let address = loop {
                if let Some(address) = *board.address.lock().unwrap() {
                    break address;
                }
                task.sleep_ms(10).await;
            };
            let id = rskynet_quic::connect(
                &task,
                QuicClientOptions::new(address.to_string(), "localhost", tls),
            )
            .await
            .expect("客户端应完成握手");
            rskynet_quic::close(&task, id, 0, b"done".to_vec())
                .await
                .expect("客户端应发起关闭");
            for _ in 0..500 {
                if *board.terminal_event.lock().unwrap() {
                    return;
                }
                task.sleep_ms(10).await;
            }
            panic!("未 start 的服务端连接也必须收到终止事件");
        });
        Ok(())
    }
}

#[test]
fn a_peer_drop_before_start_still_emits_a_terminal_event() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let certificate = rskynet_quic::CertificateDer::from(cert.der().to_vec());
    let server = ServerTlsConfig::single_cert(
        vec![certificate.clone()],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();
    let client = ClientTlsConfig::new(
        ServerVerification::CustomRoots {
            roots: vec![certificate],
        },
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();
    let board = Arc::new(LifecycleBoard::default());
    let server_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_quic()
        .with("unstarted-server", move || UnstartedServer {
            board: server_board.clone(),
            tls: server.clone(),
        })
        .with("closing-client", move || ClosingClient {
            board: client_board.clone(),
            tls: client.clone(),
        });
    let mut config = Config::default().with_bootstrap(["unstarted-server", "closing-client"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_quic::NAME, "")
        .run()
        .expect("终止事件回归测试节点应正常退出");
    assert!(*board.terminal_event.lock().unwrap());
}

#[derive(Default)]
struct FailedHandshakeBoard {
    address: Mutex<Option<SocketAddr>>,
    client_events: Mutex<usize>,
}

struct HandshakeServer {
    board: Arc<FailedHandshakeBoard>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl HandshakeServer {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = rskynet_quic::listen(
            &ctx,
            QuicServerOptions::new("127.0.0.1:0", self.tls.clone()),
        )
        .await?;
        *self.board.address.lock().unwrap() = Some(rskynet_quic::local_addr(&ctx, listener).await?);
        Ok(())
    }
}

struct RejectedClient {
    board: Arc<FailedHandshakeBoard>,
    tls: ClientTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl RejectedClient {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let tls = self.tls.clone();
        let task = ctx.clone();
        ctx.spawn(async move {
            let address = loop {
                if let Some(address) = *board.address.lock().unwrap() {
                    break address;
                }
                task.sleep_ms(10).await;
            };
            let result = rskynet_quic::connect(
                &task,
                QuicClientOptions::new(address.to_string(), "localhost", tls),
            )
            .await;
            assert!(result.is_err(), "错误的根证书必须使握手失败");
            task.sleep_ms(100).await;
            assert_eq!(
                *board.client_events.lock().unwrap(),
                0,
                "未向 API 发布的连接不应产生生命周期事件"
            );
            task.abort();
        });
        Ok(())
    }

    #[msg(MsgType::QUIC)]
    async fn on_quic(&self, _ctx: Ctx, _event: QuicEvent) {
        *self.board.client_events.lock().unwrap() += 1;
    }
}

#[test]
fn a_failed_handshake_does_not_publish_an_internal_connection_id() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let server_certificate = rskynet_quic::CertificateDer::from(cert.der().to_vec());
    let server = ServerTlsConfig::single_cert(
        vec![server_certificate],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();
    let unrelated = generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let client = ClientTlsConfig::new(
        ServerVerification::CustomRoots {
            roots: vec![rskynet_quic::CertificateDer::from(
                unrelated.cert.der().to_vec(),
            )],
        },
        vec![b"rskynet-test".to_vec()],
    )
    .unwrap();
    let board = Arc::new(FailedHandshakeBoard::default());
    let server_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_quic()
        .with("handshake-server", move || HandshakeServer {
            board: server_board.clone(),
            tls: server.clone(),
        })
        .with("rejected-client", move || RejectedClient {
            board: client_board.clone(),
            tls: client.clone(),
        });
    let mut config = Config::default().with_bootstrap(["handshake-server", "rejected-client"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_quic::NAME, "")
        .run()
        .expect("握手失败回归测试节点应正常退出");
    assert_eq!(*board.client_events.lock().unwrap(), 0);
}
