//! rskynet 节点统计 HTTP API 与内嵌 Dashboard。
//!
//! 服务只在被显式启动时读取 `[dashboard]`，监听地址没有默认值：
//!
//! ```toml
//! [dashboard]
//! address = "127.0.0.1:8080"
//! ```

use std::net::SocketAddr;

use rskynet_core::{Ctx, Error, MsgType, NodeStats, Registry, Result, ServiceStats, SvcCell};
use rskynet_http::http::{Method, Response, StatusCode};
use rskynet_http::{BodySpec, HttpError, HttpServer, ServerRequest};
use rskynet_net::SocketEvent;
use serde::{Deserialize, Serialize};

/// Dashboard 服务的约定类型名和配置段名。
pub const NAME: &str = "dashboard";

const INDEX_HTML: &str = include_str!("../assets/index.html");
const CLOCK_ICON: &[u8] = include_bytes!("../assets/icons/clock.svg");
const REFRESH_ICON: &[u8] = include_bytes!("../assets/icons/refresh.svg");
const SERVER_ICON: &[u8] = include_bytes!("../assets/icons/server.svg");
const CACHE_CONTROL: &str = "no-store, no-cache, must-revalidate";

/// `[dashboard]` 配置。监听地址必须显式提供。
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    pub address: String,
}

impl DashboardConfig {
    pub fn validate(&self) -> Result<SocketAddr> {
        if self.address.trim().is_empty() {
            return Err(Error::Config("[dashboard] address 不能为空".into()));
        }
        self.address.parse().map_err(|error| {
            Error::Config(format!(
                "[dashboard] address `{}` 不是有效的 SocketAddr：{error}",
                self.address
            ))
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct ClusterIdentity {
    node_id: u32,
}

/// `/api/v1/stats` 返回的完整节点快照。
#[derive(Debug, Clone, Serialize)]
pub struct DashboardSnapshot {
    pub node: NodeStats,
    pub cluster_id: Option<u32>,
    pub start_time_unix_ms: u64,
    pub server_time_unix_ms: u64,
    pub services: Vec<ServiceStats>,
}

impl DashboardSnapshot {
    fn capture(ctx: &Ctx, cluster_id: Option<u32>) -> Self {
        Self {
            node: ctx.node().stats(),
            cluster_id,
            start_time_unix_ms: ctx.node().start_time(),
            server_time_unix_ms: ctx.node().time(),
            services: ctx.node().services(),
        }
    }
}

/// 节点内运行的 Dashboard HTTP 服务。
pub struct DashboardService {
    http: HttpServer,
    cluster_id: SvcCell<Option<u32>>,
}

impl DashboardService {
    pub fn new() -> Self {
        Self {
            http: HttpServer::default(),
            cluster_id: SvcCell::new(None),
        }
    }
}

impl Default for DashboardService {
    fn default() -> Self {
        Self::new()
    }
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl DashboardService {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let config: DashboardConfig = ctx
            .node()
            .section(NAME)?
            .ok_or_else(|| Error::Config("启动 dashboard 需要 [dashboard] 配置段".into()))?;
        let address = config.validate()?;
        let cluster_id = ctx
            .node()
            .section::<ClusterIdentity>("cluster")?
            .map(|config| config.node_id);
        self.cluster_id.replace(cluster_id);

        if !ctx.register_name(NAME) {
            return Err(Error::service("名字 `.dashboard` 已经被占用"));
        }
        let listener = self
            .http
            .bind_http(&ctx, address.to_string())
            .await
            .map_err(http_error)?;
        let local = self
            .http
            .local_addr(&ctx, listener)
            .await
            .map_err(http_error)?;
        rskynet_core::log!(ctx, "Dashboard 监听 http://{local}/");
        Ok(())
    }

    #[msg(MsgType::SOCKET)]
    async fn on_socket(&self, ctx: Ctx, event: SocketEvent) {
        if !self.http.handles_socket(&event) {
            if !event.is_gone() {
                rskynet_core::log!(ctx, "Dashboard 忽略未知 socket 事件：{event:?}");
            }
            return;
        }
        match self.http.on_socket(&ctx, event).await {
            Ok(requests) => {
                let cluster_id = *self.cluster_id.borrow();
                for request in requests {
                    if let Err(error) = serve(&ctx, request, cluster_id).await {
                        rskynet_core::log!(ctx, "Dashboard 请求失败：{error}");
                    }
                }
            }
            Err(error) => rskynet_core::log!(ctx, "Dashboard HTTP 错误：{error}"),
        }
    }
}

async fn serve(
    ctx: &Ctx,
    request: ServerRequest,
    cluster_id: Option<u32>,
) -> rskynet_http::Result<()> {
    let method = request.request.method().clone();
    let path = request.request.uri().path().to_owned();
    let ServerRequest {
        request, responder, ..
    } = request;
    request.into_body().discard(ctx).await?;

    if method != Method::GET {
        return send(
            ctx,
            responder,
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain; charset=utf-8",
            b"method not allowed".to_vec(),
            Some(("allow", "GET")),
        )
        .await;
    }

    match path.as_str() {
        "/" | "/index.html" => {
            send(
                ctx,
                responder,
                StatusCode::OK,
                "text/html; charset=utf-8",
                INDEX_HTML.as_bytes().to_vec(),
                None,
            )
            .await
        }
        "/api/v1/stats" => {
            let bytes = serde_json::to_vec(&DashboardSnapshot::capture(ctx, cluster_id))
                .map_err(|error| HttpError::Protocol(error.to_string()))?;
            send(
                ctx,
                responder,
                StatusCode::OK,
                "application/json; charset=utf-8",
                bytes,
                None,
            )
            .await
        }
        "/assets/clock.svg" => {
            send(
                ctx,
                responder,
                StatusCode::OK,
                "image/svg+xml",
                CLOCK_ICON.to_vec(),
                None,
            )
            .await
        }
        "/assets/refresh.svg" => {
            send(
                ctx,
                responder,
                StatusCode::OK,
                "image/svg+xml",
                REFRESH_ICON.to_vec(),
                None,
            )
            .await
        }
        "/assets/server.svg" => {
            send(
                ctx,
                responder,
                StatusCode::OK,
                "image/svg+xml",
                SERVER_ICON.to_vec(),
                None,
            )
            .await
        }
        _ => {
            send(
                ctx,
                responder,
                StatusCode::NOT_FOUND,
                "text/plain; charset=utf-8",
                b"not found".to_vec(),
                None,
            )
            .await
        }
    }
}

async fn send(
    ctx: &Ctx,
    responder: rskynet_http::ServerResponder,
    status: StatusCode,
    content_type: &'static str,
    bytes: Vec<u8>,
    extra_header: Option<(&'static str, &'static str)>,
) -> rskynet_http::Result<()> {
    let mut builder = Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("cache-control", CACHE_CONTROL)
        .header("x-content-type-options", "nosniff");
    if let Some((name, value)) = extra_header {
        builder = builder.header(name, value);
    }
    let response = builder
        .body(BodySpec::Fixed(bytes.len() as u64))
        .expect("固定 Dashboard 响应头应有效");
    let mut output = responder.respond(ctx, response).await?;
    if !bytes.is_empty() {
        output.write_chunk(ctx, bytes).await?;
    }
    output.finish(ctx).await
}

fn http_error(error: HttpError) -> Error {
    Error::service(error.to_string())
}

/// 把 Dashboard 服务挂进注册表。
pub trait RegistryExt {
    #[must_use]
    fn with_dashboard(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_dashboard(self) -> Self {
        self.with(NAME, DashboardService::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_address_is_required_and_validated() {
        assert!(serde_json::from_str::<DashboardConfig>("{}").is_err());
        assert!(
            DashboardConfig {
                address: String::new()
            }
            .validate()
            .is_err()
        );
        assert!(
            DashboardConfig {
                address: "localhost:8080".into()
            }
            .validate()
            .is_err()
        );
        assert_eq!(
            DashboardConfig {
                address: "127.0.0.1:8080".into()
            }
            .validate()
            .unwrap(),
            "127.0.0.1:8080".parse().unwrap()
        );
    }

    #[test]
    fn snapshot_json_preserves_cluster_identity() {
        let snapshot = DashboardSnapshot {
            node: NodeStats {
                service_count: 2,
                business_service_count: 1,
                runnable_services: 1,
                uptime_ms: 42,
            },
            cluster_id: Some(u32::MAX),
            start_time_unix_ms: 1_000,
            server_time_unix_ms: 2_000,
            services: Vec::new(),
        };
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["cluster_id"], u32::MAX);
        assert_eq!(value["node"]["uptime_ms"], 42);
    }

    #[test]
    fn embedded_page_contains_the_api_and_cluster_condition() {
        assert!(INDEX_HTML.contains("/api/v1/stats"));
        assert!(INDEX_HTML.contains("clusterBadge"));
        assert!(INDEX_HTML.contains("setInterval(refresh, 5000)"));
        assert!(INDEX_HTML.contains("近 1 分钟增量"));
        assert!(INDEX_HTML.contains("单个服务邮箱积压趋势"));
        assert!(!INDEX_HTML.contains("较上次采样"));
    }
}
