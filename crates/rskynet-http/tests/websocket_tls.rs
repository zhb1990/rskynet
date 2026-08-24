#![cfg(all(feature = "websocket", feature = "tls"))]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_http::HttpServer;
use rskynet_http::websocket::{
    ClientRequestBuilder, Message, WebSocketClient, WebSocketClientConfig, WebSocketUpgradeOptions,
};
use rskynet_net::RegistryExt as _;
use rskynet_timer::BuilderExt as _;
use rskynet_tls::{
    CertificateDer, ClientTlsConfig, PrivateKeyInput, RegistryExt as _, ServerOptions,
    ServerTlsConfig, ServerVerification, TlsEvent,
};

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    echoed: Mutex<Option<String>>,
}

struct Server {
    http: HttpServer,
    board: Arc<Board>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Server {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .http
            .bind_https(&ctx, ServerOptions::new("127.0.0.1:0", self.tls.clone()))
            .await
            .map_err(|error| rskynet_core::Error::Service(error.to_string()))?;
        let address = self
            .http
            .local_addr(&ctx, listener)
            .await
            .map_err(|error| rskynet_core::Error::Service(error.to_string()))?;
        *self.board.address.lock().unwrap() = Some(address);
        Ok(())
    }

    #[msg(MsgType::TLS)]
    async fn on_tls(&self, ctx: Ctx, event: TlsEvent) {
        if !self.http.handles_tls(&event) {
            assert!(
                matches!(event, TlsEvent::Close { .. } | TlsEvent::Error { .. }),
                "WSS 服务端未识别活动事件：{event:?}"
            );
            return;
        }
        let Ok(requests) = self.http.on_tls(&ctx, event).await else {
            return;
        };
        for request in requests {
            let task_ctx = ctx.clone();
            ctx.spawn(async move {
                let Ok(mut socket) = request
                    .upgrade_websocket(
                        &task_ctx,
                        WebSocketUpgradeOptions::default().with_protocol("secure-echo"),
                    )
                    .await
                else {
                    return;
                };
                while let Ok(Some(message)) = socket.recv(&task_ctx).await {
                    if message.is_text() || message.is_binary() {
                        let _ = socket.send(&task_ctx, message).await;
                    }
                }
            });
        }
    }
}

struct Client {
    websockets: WebSocketClient,
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Client {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let client = self.websockets.clone();
        let board = self.board.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: rskynet_http::Result<()> = async {
                let address = loop {
                    if let Some(address) = *board.address.lock().unwrap() {
                        break address;
                    }
                    task_ctx.sleep(10).await;
                };
                let request = ClientRequestBuilder::new(
                    format!("wss://localhost:{}/socket", address.port())
                        .parse()
                        .unwrap(),
                )
                .with_sub_protocol("secure-echo");
                let (mut socket, response) = client.connect(&task_ctx, request).await?;
                assert_eq!(response.status(), 101);
                assert_eq!(socket.protocol(), Some("secure-echo"));
                socket
                    .send(&task_ctx, Message::text("secure websocket"))
                    .await?;
                let echoed = socket.recv(&task_ctx).await?.expect("echo response");
                *board.echoed.lock().unwrap() = Some(echoed.to_text()?.to_owned());
                socket.close(&task_ctx, None).await?;
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "WSS flow failed: {result:?}");
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::TLS)]
    async fn on_tls(&self, ctx: Ctx, event: TlsEvent) {
        if self.websockets.handles_tls(event.id()) {
            let _ = self.websockets.on_tls(&ctx, event).await;
        }
    }
}

#[test]
fn local_client_and_embedded_server_exchange_messages_over_wss_without_http_client_service() {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let certificate = CertificateDer::from(cert.der().to_vec());
    let server_tls = ServerTlsConfig::single_cert(
        vec![certificate.clone()],
        PrivateKeyInput::PlainPem(signing_key.serialize_pem().into()),
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let client_tls = ClientTlsConfig::new(
        ServerVerification::CustomRoots {
            roots: vec![certificate],
        },
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    let board = Arc::new(Board::default());
    let server_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_tls()
        .with("server", move || Server {
            http: HttpServer::default(),
            board: server_board.clone(),
            tls: server_tls.clone(),
        })
        .with("client", move || Client {
            websockets: WebSocketClient::with_tls_config(
                WebSocketClientConfig::default(),
                client_tls.clone(),
            ),
            board: client_board.clone(),
        });
    let mut config = Config::default().with_bootstrap(["server", "client"]);
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
        .startup_service(rskynet_tls::NAME, "")
        .run()
        .unwrap();
    assert_eq!(
        board.echoed.lock().unwrap().as_deref(),
        Some("secure websocket")
    );
}
