use rskynet::http::HttpServer;
use rskynet::net::SocketEvent;
use rskynet::websocket::{ClientRequestBuilder, Message, WebSocketClient, WebSocketUpgradeOptions};
use rskynet::{Ctx, MsgType, Result};

#[derive(Default)]
struct WebSocketExample {
    server: HttpServer,
    client: WebSocketClient,
}

#[rskynet::service(name = "websocket-example")]
impl WebSocketExample {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .server
            .bind_http(&ctx, "127.0.0.1:0")
            .await
            .map_err(|error| rskynet::Error::service(error.to_string()))?;
        let address = self
            .server
            .local_addr(&ctx, listener)
            .await
            .map_err(|error| rskynet::Error::service(error.to_string()))?;
        rskynet::log!(ctx, "WebSocket 服务监听 ws://{address}/ws");

        let client = self.client.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: rskynet::http::Result<()> = async {
                let request = ClientRequestBuilder::new(
                    format!("ws://{address}/ws")
                        .parse()
                        .expect("示例 WebSocket URI 应有效"),
                )
                .with_sub_protocol("echo");
                let (mut socket, response) = client.connect(&task_ctx, request).await?;
                if response.status() != 101 || socket.protocol() != Some("echo") {
                    return Err(rskynet::http::HttpError::Protocol(format!(
                        "WebSocket 握手结果不符合预期：status={}, protocol={:?}",
                        response.status(),
                        socket.protocol()
                    )));
                }

                socket
                    .send(&task_ctx, Message::text("hello websocket"))
                    .await?;
                let echoed =
                    socket
                        .recv(&task_ctx)
                        .await?
                        .ok_or(rskynet::http::HttpError::Protocol(
                            "WebSocket 在返回回显前关闭".into(),
                        ))?;
                if echoed.to_text()? != "hello websocket" {
                    return Err(rskynet::http::HttpError::Protocol(
                        "WebSocket 回显内容不一致".into(),
                    ));
                }
                rskynet::log!(task_ctx, "WebSocket 回显成功：{}", echoed.to_text()?);
                socket.close(&task_ctx, None).await?;
                Ok(())
            }
            .await;

            if let Err(error) = result {
                rskynet::log!(task_ctx, "WebSocket 示例失败：{error}");
            }
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if self.client.handles_socket(event.id()) {
            if let Err(error) = self.client.on_socket(&ctx, event).await {
                rskynet::log!(ctx, "WebSocket 客户端错误：{error}");
                ctx.abort();
            }
            return;
        }

        if !self.server.handles_socket(&event) {
            if !event.is_gone() {
                rskynet::log!(ctx, "忽略未知 socket 事件：{event:?}");
            }
            return;
        }

        let requests = match self.server.on_socket(&ctx, event).await {
            Ok(requests) => requests,
            Err(error) => {
                rskynet::log!(ctx, "WebSocket 服务端错误：{error}");
                ctx.abort();
                return;
            }
        };
        for request in requests {
            if request.request.uri().path() != "/ws" {
                continue;
            }
            let task_ctx = ctx.clone();
            ctx.spawn(async move {
                let result: rskynet::http::Result<()> = async {
                    let mut socket = request
                        .upgrade_websocket(
                            &task_ctx,
                            WebSocketUpgradeOptions::default().with_protocol("echo"),
                        )
                        .await?;
                    while let Some(message) = socket.recv(&task_ctx).await? {
                        if message.is_text() || message.is_binary() {
                            socket.send(&task_ctx, message).await?;
                        }
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    rskynet::log!(task_ctx, "WebSocket 请求处理失败：{error}");
                }
            });
        }
    }
}
