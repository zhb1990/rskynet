#![cfg(feature = "websocket")]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_http::HttpServer;
use rskynet_http::websocket::{Message, WebSocketClient, WebSocketUpgradeOptions};
use rskynet_net::{RegistryExt as _, SocketEvent};
use rskynet_timer::BuilderExt as _;

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    echoed: Mutex<Option<String>>,
}

struct Server {
    http: HttpServer,
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Server {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .http
            .bind_http(&ctx, "127.0.0.1:0")
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

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if !self.http.handles_socket(&event) {
            assert!(event.is_gone(), "WebSocket 服务端未识别活动事件：{event:?}");
            return;
        }
        let Ok(requests) = self.http.on_socket(&ctx, event).await else {
            return;
        };
        for request in requests {
            assert_eq!(
                request.request.headers().get("Authorization").unwrap(),
                "Bearer integration-test"
            );
            let task_ctx = ctx.clone();
            ctx.spawn(async move {
                let Ok(mut socket) = request
                    .upgrade_websocket(
                        &task_ctx,
                        WebSocketUpgradeOptions::default().with_protocol("echo"),
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
        let websocket_client = self.websockets.clone();
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
                let request = rskynet_http::websocket::ClientRequestBuilder::new(
                    format!("ws://{address}/socket").parse().unwrap(),
                )
                .with_header("Authorization", "Bearer integration-test")
                .with_sub_protocol("echo");
                let (mut socket, response) = websocket_client.connect(&task_ctx, request).await?;
                assert_eq!(response.status(), 101);
                assert_eq!(socket.protocol(), Some("echo"));
                socket
                    .sender()
                    .send(&task_ctx, Message::text("hello websocket"))
                    .await?;
                let echoed = socket.recv(&task_ctx).await?.expect("echo response");
                *board.echoed.lock().unwrap() = Some(echoed.to_text()?.to_owned());
                socket.close(&task_ctx, None).await?;
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "WebSocket flow failed: {result:?}");
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if self.websockets.handles_socket(event.id()) {
            let _ = self.websockets.on_socket(&ctx, event).await;
        }
    }
}

#[test]
fn embedded_server_and_local_client_exchange_messages_without_http_client_service() {
    let board = Arc::new(Board::default());
    let server_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with("server", move || Server {
            http: HttpServer::default(),
            board: server_board.clone(),
        })
        .with("client", move || Client {
            websockets: WebSocketClient::default(),
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
        .run()
        .unwrap();
    assert_eq!(
        board.echoed.lock().unwrap().as_deref(),
        Some("hello websocket")
    );
}
