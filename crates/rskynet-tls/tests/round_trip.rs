use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_net::RegistryExt as _;
use rskynet_timer::BuilderExt as _;
use rskynet_tls::{
    CertificateDer, ClientOptions, ClientTlsConfig, PrivateKeyInput, RegistryExt as _, ServerName,
    ServerOptions, ServerTlsConfig, ServerVerification, TlsEvent,
};

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    received: Mutex<Vec<u8>>,
    paused_was_silent: Mutex<bool>,
}

struct Echo {
    board: Arc<Board>,
    config: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Echo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener =
            rskynet_tls::listen(&ctx, ServerOptions::new("127.0.0.1:0", self.config.clone()))
                .await?;
        rskynet_tls::start(&ctx, listener).await?;
        let local = rskynet_tls::info(&ctx, listener)
            .await?
            .local
            .expect("监听口应有地址");
        *self.board.address.lock().unwrap() = Some(local);
        Ok(())
    }

    #[msg(MsgType::TLS)]
    async fn on_tls(&self, ctx: Ctx, event: TlsEvent) {
        if let TlsEvent::Data { id, data } = event {
            let _ = rskynet_tls::send(&ctx, id, data);
        }
    }
}

struct Client {
    board: Arc<Board>,
    config: ClientTlsConfig,
}

fn large_payload() -> Vec<u8> {
    (0..512 * 1024 + 123).map(|index| index as u8).collect()
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Client {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let config = self.config.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: Result<()> = async {
                let address = loop {
                    if let Some(address) = *board.address.lock().unwrap() {
                        break address;
                    }
                    task_ctx.sleep_ms(10).await;
                };
                let id = rskynet_tls::connect(
                    &task_ctx,
                    ClientOptions::new(
                        address.to_string(),
                        ServerName::try_from("localhost").unwrap().to_owned(),
                        config,
                    ),
                )
                .await?;
                rskynet_tls::pause(&task_ctx, id).await?;
                let payload = large_payload();
                rskynet_tls::send_wait(&task_ctx, id, payload.clone()).await?;
                task_ctx.sleep_ms(50).await;
                *board.paused_was_silent.lock().unwrap() =
                    board.received.lock().unwrap().is_empty();
                rskynet_tls::start(&task_ctx, id).await?;
                for _ in 0..500 {
                    if board.received.lock().unwrap().as_slice() == payload {
                        break;
                    }
                    task_ctx.sleep_ms(10).await;
                }
                let _ = rskynet_tls::close(&task_ctx, id).await;
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "TLS 客户端流程失败: {result:?}");
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::TLS)]
    async fn on_tls(&self, _ctx: Ctx, event: TlsEvent) {
        if let TlsEvent::Data { data, .. } = event {
            self.board.received.lock().unwrap().extend(data);
        }
    }
}

#[test]
fn client_and_server_exchange_plaintext_over_net() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let certificate = CertificateDer::from(cert.der().to_vec());
    let server = ServerTlsConfig::single_cert(
        vec![certificate.clone()],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let client = ClientTlsConfig::new(
        ServerVerification::CustomRoots {
            roots: vec![certificate],
        },
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();

    let board = Arc::new(Board::default());
    let echo_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_tls()
        .with("echo", move || Echo {
            board: echo_board.clone(),
            config: server.clone(),
        })
        .with("client", move || Client {
            board: client_board.clone(),
            config: client.clone(),
        });
    let mut config = Config::default().with_bootstrap(["echo", "client"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    config
        .section_mut("tls")
        .insert("buffer_limit".into(), 1024.into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_tls::NAME, "")
        .run()
        .expect("TLS 测试节点应正常退出");

    assert_eq!(*board.received.lock().unwrap(), large_payload());
    assert!(
        *board.paused_was_silent.lock().unwrap(),
        "TLS pause 期间不应投递解密后的明文"
    );
}
