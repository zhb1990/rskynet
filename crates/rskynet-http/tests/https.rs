#![cfg(feature = "tls")]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_http::http::{Request, Response};
use rskynet_http::{BodySpec, HttpClientService, HttpServer, ServerRequest};
use rskynet_net::RegistryExt as _;
use rskynet_timer::BuilderExt as _;
use rskynet_tls::{
    CertificateDer, ClientTlsConfig, PrivateKeyInput, RegistryExt as _, ServerOptions,
    ServerTlsConfig, ServerVerification, TlsEvent,
};

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    result: Mutex<Vec<u8>>,
}

struct Api {
    http: HttpServer,
    board: Arc<Board>,
    tls: ServerTlsConfig,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Api {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .http
            .bind_https(&ctx, ServerOptions::new("127.0.0.1:0", self.tls.clone()))
            .await
            .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
        *self.board.address.lock().unwrap() = Some(
            self.http
                .local_addr(&ctx, listener)
                .await
                .map_err(|e| rskynet_core::Error::Service(e.to_string()))?,
        );
        Ok(())
    }

    #[msg(MsgType::TLS)]
    async fn on_tls(&self, ctx: Ctx, event: TlsEvent) {
        if !self.http.handles_tls(&event) {
            assert!(
                matches!(event, TlsEvent::Close { .. } | TlsEvent::Error { .. }),
                "HTTPS 服务端未识别活动事件：{event:?}"
            );
            return;
        }
        let Ok(requests) = self.http.on_tls(&ctx, event).await else {
            return;
        };
        for request in requests {
            let _ = serve(&ctx, request).await;
        }
    }
}

async fn serve(ctx: &Ctx, request: ServerRequest) -> rskynet_http::Result<()> {
    let bytes = request.request.into_body().collect(ctx, 1024).await?;
    let response = Response::builder()
        .body(BodySpec::Fixed(bytes.len() as u64))
        .unwrap();
    let mut body = request.responder.respond(ctx, response).await?;
    body.write_chunk(ctx, bytes).await?;
    body.finish(ctx).await
}

struct Client {
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Client {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: Result<()> = async {
                let address = loop {
                    if let Some(value) = *board.address.lock().unwrap() {
                        break value;
                    }
                    task_ctx.sleep_ms(10).await;
                };
                let request = Request::post(format!("https://localhost:{}/", address.port()))
                    .body(b"secure".to_vec())
                    .unwrap();
                let response = rskynet_http::client::request(&task_ctx, request)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                *board.result.lock().unwrap() = response
                    .into_body()
                    .collect(&task_ctx, 1024)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "HTTPS 客户端流程失败: {result:?}");
            task_ctx.abort();
        });
        Ok(())
    }
}

#[test]
fn streams_over_tls() {
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
    let api_board = board.clone();
    let client_board = board.clone();
    let tls_factory = client_tls.clone();
    let registry = Registry::new()
        .with_net()
        .with_tls()
        .with(rskynet_http::NAME, move || {
            HttpClientService::with_tls_config(tls_factory.clone())
        })
        .with("api", move || Api {
            http: HttpServer::default(),
            board: api_board.clone(),
            tls: server_tls.clone(),
        })
        .with("client", move || Client {
            board: client_board.clone(),
        });
    let mut config = Config::default().with_bootstrap(["api", "client"]);
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
        .startup_service(rskynet_http::NAME, "")
        .run()
        .unwrap();
    assert_eq!(*board.result.lock().unwrap(), b"secure");
}
