use rskynet::http::http::{Request, Response};
use rskynet::http::{BodySpec, HttpError, HttpServer, ServerRequest};
use rskynet::net::SocketEvent;
use rskynet::{Ctx, MsgType, Result};

#[derive(Default)]
struct HttpExample {
    server: HttpServer,
}

#[rskynet::service(name = "http-example")]
impl HttpExample {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .server
            .bind_http(&ctx, "127.0.0.1:0")
            .await
            .map_err(http_error)?;
        let address = self
            .server
            .local_addr(&ctx, listener)
            .await
            .map_err(http_error)?;
        rskynet::log!(ctx, "HTTP 服务监听 http://{address}/echo");

        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let result: rskynet::http::Result<()> = async {
                let sent = b"hello http".to_vec();
                let request = Request::post(format!("http://{address}/echo"))
                    .body(sent.clone())
                    .expect("示例请求 URI 应有效");
                let response = rskynet::http::client::request(&task_ctx, request).await?;
                if response.status() != 200 {
                    return Err(HttpError::Protocol(format!(
                        "期待状态码 200，实际为 {}",
                        response.status()
                    )));
                }
                let received = response.into_body().collect(&task_ctx, 1024).await?;
                if received != sent {
                    return Err(HttpError::Protocol("HTTP 回显内容不一致".into()));
                }
                rskynet::log!(
                    task_ctx,
                    "HTTP 回显成功：{}",
                    String::from_utf8_lossy(&received)
                );
                Ok(())
            }
            .await;

            if let Err(error) = result {
                rskynet::log!(task_ctx, "HTTP 示例失败：{error}");
            }
            task_ctx.abort();
        });
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if !self.server.handles_socket(&event) {
            if !event.is_gone() {
                rskynet::log!(ctx, "忽略不属于 HTTP 服务端的 socket 事件：{event:?}");
            }
            return;
        }
        let requests = match self.server.on_socket(&ctx, event).await {
            Ok(requests) => requests,
            Err(error) => {
                rskynet::log!(ctx, "HTTP 服务端错误：{error}");
                ctx.abort();
                return;
            }
        };
        for request in requests {
            if let Err(error) = echo(&ctx, request).await {
                rskynet::log!(ctx, "HTTP 请求处理失败：{error}");
                ctx.abort();
            }
        }
    }
}

async fn echo(ctx: &Ctx, request: ServerRequest) -> rskynet::http::Result<()> {
    if request.request.method() != "POST" || request.request.uri().path() != "/echo" {
        let response = Response::builder()
            .status(404)
            .body(BodySpec::Empty)
            .expect("固定响应应有效");
        return request
            .responder
            .respond(ctx, response)
            .await?
            .finish(ctx)
            .await;
    }

    let body = request.request.into_body().collect(ctx, 1024).await?;
    let response = Response::builder()
        .status(200)
        .body(BodySpec::Fixed(body.len() as u64))
        .expect("固定响应应有效");
    let mut output = request.responder.respond(ctx, response).await?;
    output.write_chunk(ctx, body).await?;
    output.finish(ctx).await
}

fn http_error(error: HttpError) -> rskynet::Error {
    rskynet::Error::service(error.to_string())
}
