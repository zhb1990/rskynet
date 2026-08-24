use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::{Either, select};
use rskynet_bootstrap::ConfigExt as _;
use rskynet_core::{Builder, Config, Ctx, MsgType, Registry, Result};
use rskynet_http::http::{Request, Response};
use rskynet_http::{BodySpec, HttpServer, HttpServerConfig, RegistryExt as _, ServerRequest};
use rskynet_net::{RegistryExt as _, SocketEvent};
use rskynet_timer::BuilderExt as _;

#[derive(Default)]
struct Board {
    address: Mutex<Option<SocketAddr>>,
    responses: Mutex<Vec<Vec<u8>>>,
    accepted: Mutex<usize>,
    served: Mutex<Vec<String>>,
    pipeline_ok: Mutex<bool>,
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
        if !self.http.handles_socket(&event) {
            assert!(event.is_gone(), "HTTP 服务端未识别活动事件：{event:?}");
            return;
        }
        if matches!(event, SocketEvent::Accept { .. }) {
            *self.board.accepted.lock().unwrap() += 1;
        }
        let Ok(requests) = self.http.on_socket(&ctx, event).await else {
            return;
        };
        for request in requests {
            self.board
                .served
                .lock()
                .unwrap()
                .push(request.request.uri().path().to_owned());
            let _ = serve(&ctx, request).await;
        }
    }
}

struct PipelineClient {
    board: Arc<Board>,
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl PipelineClient {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let board = self.board.clone();
        let node = ctx.node();
        std::thread::spawn(move || {
            let outcome = (|| -> std::io::Result<bool> {
                let address = loop {
                    if let Some(address) = *board.address.lock().unwrap() {
                        break address;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                };
                let mut stream = TcpStream::connect(address)?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut request = format!(
                    "POST /echo HTTP/1.1\r\nHost: {address}\r\nContent-Length: 1024\r\n\r\n"
                )
                .into_bytes();
                request.extend(std::iter::repeat_n(b'a', 1024));
                request.extend_from_slice(
                    format!("GET /second HTTP/1.1\r\nHost: {address}\r\n\r\n").as_bytes(),
                );
                stream.write_all(&request)?;

                let mut received = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(size) => {
                            received.extend_from_slice(&buffer[..size]);
                            if received
                                .windows(b"HTTP/1.1 200".len())
                                .filter(|window| *window == b"HTTP/1.1 200")
                                .count()
                                == 2
                            {
                                return Ok(true);
                            }
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                            ) =>
                        {
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(false)
            })();
            *board.pipeline_ok.lock().unwrap() = outcome.unwrap_or(false);
            node.abort();
        });
        Ok(())
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
                    ctx.sleep(10).await;
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

                // 占住唯一连接，再取消一个已经进入连接池等待队列的 start。释放首个
                // exchange 后，第三个请求必须能继续，不能被孤儿 start 吞掉容量。
                let held = Request::get(format!("http://{address}/large"))
                    .body(BodySpec::Empty)
                    .unwrap();
                let mut held = rskynet_http::client::start(&ctx, held)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                let queued = Request::get(format!("http://{address}/echo"))
                    .body(BodySpec::Empty)
                    .unwrap();
                match select(
                    Box::pin(rskynet_http::client::start(&ctx, queued)),
                    Box::pin(ctx.sleep(20)),
                )
                .await
                {
                    Either::Right(((), _)) => {}
                    Either::Left((_result, _)) => {
                        return Err(rskynet_core::Error::Service(
                            "连接池已满时 start 不应提前完成".into(),
                        ));
                    }
                }
                held.response(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?
                    .into_body()
                    .discard(&ctx)
                    .await
                    .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                let final_response = rskynet_http::client::request(
                    &ctx,
                    Request::get(format!("http://{address}/echo"))
                        .body(Vec::new())
                        .unwrap(),
                )
                .await
                .map_err(|e| rskynet_core::Error::Service(e.to_string()))?
                .into_body()
                .collect(&ctx, 1024)
                .await
                .map_err(|e| rskynet_core::Error::Service(e.to_string()))?;
                board.responses.lock().unwrap().push(final_response);
                Ok(())
            }
            .await;
            task_ctx.abort();
            assert!(result.is_ok(), "HTTP 客户端流程失败: {result:?}");
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
    config
        .section_mut("http-client")
        .insert("max_header_size".into(), 256.into());
    config
        .section_mut("http-client")
        .insert("max_connections".into(), 1.into());
    config
        .section_mut("http-client")
        .insert("max_connections_per_origin".into(), 1.into());
    config
        .section_mut("http-client")
        .insert("max_idle_connections".into(), 1.into());
    config
        .section_mut("http-client")
        .insert("max_idle_connections_per_origin".into(), 1.into());
    config
        .section_mut("net")
        .insert("min_read_buffer".into(), 4096.into());
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
            b"empty".to_vec(),
        ]
    );
    assert_eq!(
        *board.accepted.lock().unwrap(),
        2,
        "常规请求应复用连接；拒绝未发送的 Expect body 后允许重建一次"
    );
}

#[test]
fn coalesced_body_and_pipelined_request_are_processed_without_more_input() {
    let board = Arc::new(Board::default());
    let api_board = board.clone();
    let client_board = board.clone();
    let http_config = HttpServerConfig {
        max_header_size: 256,
        ..HttpServerConfig::default()
    };
    let registry = Registry::new()
        .with_net()
        .with("api", move || Api {
            http: HttpServer::new(http_config.clone()),
            board: api_board.clone(),
        })
        .with("pipeline-client", move || PipelineClient {
            board: client_board.clone(),
        });
    let mut config = Config::default().with_bootstrap(["api", "pipeline-client"]);
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());
    config
        .section_mut("net")
        .insert("min_read_buffer".into(), 4096.into());
    Builder::new(config)
        .registry(registry)
        .with_wheel_timer()
        .service("bootstrap", || rskynet_bootstrap::Bootstrap)
        .startup_service(rskynet_net::NAME, "")
        .run()
        .unwrap();

    assert!(*board.pipeline_ok.lock().unwrap());
    assert_eq!(
        *board.served.lock().unwrap(),
        vec!["/echo".to_string(), "/second".to_string()]
    );
}
