use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_http::http::{Request, Response};
use rskynet_http::{BodySpec, HttpServer, RegistryExt as _, ServerRequest};
use rskynet_net::{RegistryExt as _, SocketEvent};
use rskynet_timer::BuilderExt as _;

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    responses: Mutex<Vec<Vec<u8>>>,
    accepted: Mutex<usize>,
}

struct Api {
    http: HttpServer,
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Api {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let listener = self
            .http
            .bind_http(&ctx, "127.0.0.1:0")
            .await
            .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
        let address = self
            .http
            .local_addr(&ctx, listener)
            .await
            .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
        *self.board.address.lock().unwrap() = Some(address);
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if matches!(event, SocketEvent::Accept { .. }) {
            *self.board.accepted.lock().unwrap() += 1;
        }
        let Ok(requests) = self.http.on_socket(&ctx, event).await else {
            return;
        };
        for request in requests {
            let _ = serve(&ctx, request).await;
        }
    }
}

async fn serve(ctx: &Ctx, request: ServerRequest) -> rskynet_http::Result<()> {
    let path = request.request.uri().path().to_owned();
    if path == "/reject" {
        let response = Response::builder()
            .status(417)
            .body(BodySpec::Empty)
            .unwrap();
        request
            .responder
            .respond(ctx, response)
            .await?
            .finish(ctx)
            .await?;
        return Ok(());
    }
    let large = path == "/large";
    let body = request.request.into_body().collect(ctx, 1024).await?;
    let bytes = if large {
        vec![b'x'; 512]
    } else if body.is_empty() {
        b"empty".to_vec()
    } else {
        body
    };
    let response = Response::builder()
        .status(200)
        .body(BodySpec::Fixed(bytes.len() as u64))
        .unwrap();
    let mut output = request.responder.respond(ctx, response).await?;
    output.write_chunk(ctx, bytes).await?;
    output.finish(ctx).await
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
                let ctx = task_ctx.clone();
                let address = loop {
                    if let Some(value) = *board.address.lock().unwrap() {
                        break value;
                    }
                    ctx.sleep_ms(10).await;
                };
                for body in [b"first".to_vec(), b"second".to_vec()] {
                    let request = Request::post(format!("http://{address}/echo"))
                        .body(BodySpec::Fixed(body.len() as u64))
                        .unwrap();
                    let mut exchange = rskynet_http::client::start(&ctx, request)
                        .await
                        .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                    exchange
                        .write_chunk(&ctx, body)
                        .await
                        .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                    exchange
                        .finish_request(&ctx)
                        .await
                        .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                    let response = exchange
                        .response(&ctx)
                        .await
                        .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                    let bytes = response
                        .into_body()
                        .collect(&ctx, 1024)
                        .await
                        .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                    board.responses.lock().unwrap().push(bytes);
                }
                let request = Request::get(format!("http://{address}/large"))
                    .body(Vec::new())
                    .unwrap();
                let response = rskynet_http::client::request(&ctx, request)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                let large = response
                    .into_body()
                    .collect(&ctx, 1024)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                board.responses.lock().unwrap().push(large);

                let continued = b"continued".to_vec();
                let request = Request::post(format!("http://{address}/continue"))
                    .header("expect", "100-continue")
                    .body(BodySpec::Fixed(continued.len() as u64))
                    .unwrap();
                let mut exchange = rskynet_http::client::start(&ctx, request)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                exchange
                    .write_chunk(&ctx, continued)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                exchange
                    .finish_request(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                let accepted = exchange
                    .response(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?
                    .into_body()
                    .collect(&ctx, 1024)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                board.responses.lock().unwrap().push(accepted);

                let request = Request::post(format!("http://{address}/reject"))
                    .header("expect", "100-continue")
                    .body(BodySpec::Fixed(8))
                    .unwrap();
                let mut exchange = rskynet_http::client::start(&ctx, request)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                let rejected = exchange.write_chunk(&ctx, b"rejected".to_vec()).await;
                if !matches!(rejected, Err(rskynet_http::HttpError::RequestBodyRejected)) {
                    return Err(rskynet_core::Error::Service(format!(
                        "预期请求体被最终响应拒绝，实际为 {rejected:?}"
                    )));
                }
                let response = exchange
                    .response(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                if response.status().as_u16() != 417 {
                    return Err(rskynet_core::Error::Service(format!(
                        "预期 417，实际为 {}",
                        response.status()
                    )));
                }
                response
                    .into_body()
                    .discard(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                board.responses.lock().unwrap().push(b"rejected".to_vec());
                Ok(())
            }
            .await;
            assert!(result.is_ok(), "HTTP 客户端流程失败: {result:?}");
            task_ctx.abort();
        });
        Ok(())
    }
}

#[test]
fn streams_requests_responses_and_reuses_plain_connection() {
    let board = Arc::new(Board::default());
    let api_board = board.clone();
    let client_board = board.clone();
    let registry = Registry::new()
        .with_net()
        .with_http_client()
        .with("api", move || Api {
            http: HttpServer::default(),
            board: api_board.clone(),
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
    config
        .section_mut("http-client")
        .insert("input_high_water".into(), 128.into());
    config
        .section_mut("http-client")
        .insert("input_low_water".into(), 64.into());
    config
        .section_mut("http-client")
        .insert("max_chunk_size".into(), 64.into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .startup_service(rskynet_http::NAME, "")
        .run()
        .unwrap();
    assert_eq!(
        *board.responses.lock().unwrap(),
        vec![
            b"first".to_vec(),
            b"second".to_vec(),
            vec![b'x'; 512],
            b"continued".to_vec(),
            b"rejected".to_vec(),
        ]
    );
    assert_eq!(*board.accepted.lock().unwrap(), 1, "两次请求应复用同一连接");
}
