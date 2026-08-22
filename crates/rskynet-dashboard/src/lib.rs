//! rskynet 节点统计 HTTP API 与内嵌 Dashboard。
//!
//! 服务只在被显式启动时读取 `[dashboard]`，监听地址没有默认值：
//!
//! ```toml
//! [dashboard]
//! address = "127.0.0.1:8080"
//! ```

use std::net::SocketAddr;

use futures_util::future::{Either, select};
use futures_util::pin_mut;
use rskynet_core::{
    Ctx, Error, MsgType, NodeStats, Registry, Result, ServiceLifecycle, ServiceStats, SvcCell,
};
use rskynet_http::http::{Method, Response, StatusCode};
use rskynet_http::{BodySpec, HttpError, HttpServer, ServerRequest};
use rskynet_net::{SocketEvent, SocketInfo};
use serde::{Deserialize, Serialize};

/// Dashboard 服务的约定类型名和配置段名。
pub const NAME: &str = "dashboard";

const INDEX_HTML: &str = include_str!("../assets/index.html");
const CLOCK_ICON: &[u8] = include_bytes!("../assets/icons/clock.svg");
const REFRESH_ICON: &[u8] = include_bytes!("../assets/icons/refresh.svg");
const SERVER_ICON: &[u8] = include_bytes!("../assets/icons/server.svg");
const CACHE_CONTROL: &str = "no-store, no-cache, must-revalidate";
const DEBUG_BODY_LIMIT: usize = 256 * 1024;
const DEFAULT_DEBUG_CALL_TIMEOUT_MS: u32 = 10_000;

/// `[dashboard]` 配置。监听地址必须显式提供。
#[derive(Debug, Clone, Deserialize)]
pub struct DashboardConfig {
    pub address: String,
    #[serde(default)]
    pub debug_console: bool,
    #[serde(default = "default_debug_call_timeout_ms")]
    pub debug_call_timeout_ms: u32,
}

impl DashboardConfig {
    pub fn validate(&self) -> Result<SocketAddr> {
        if self.address.trim().is_empty() {
            return Err(Error::Config("[dashboard] address 不能为空".into()));
        }
        let address: SocketAddr = self.address.parse().map_err(|error| {
            Error::Config(format!(
                "[dashboard] address `{}` 不是有效的 SocketAddr：{error}",
                self.address
            ))
        })?;
        if self.debug_call_timeout_ms == 0 {
            return Err(Error::Config(
                "[dashboard] debug_call_timeout_ms 必须大于 0".into(),
            ));
        }
        if self.debug_console && !address.ip().is_loopback() {
            return Err(Error::Config(
                "[dashboard] debug_console 只能在 loopback 监听地址上启用".into(),
            ));
        }
        Ok(address)
    }
}

fn default_debug_call_timeout_ms() -> u32 {
    DEFAULT_DEBUG_CALL_TIMEOUT_MS
}

#[derive(Debug, Clone, Copy)]
struct DebugConfig {
    enabled: bool,
    call_timeout_ms: u32,
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
    pub sockets: Vec<SocketInfo>,
    pub debug_console_enabled: bool,
}

impl DashboardSnapshot {
    async fn capture(
        ctx: &Ctx,
        cluster_id: Option<u32>,
        debug_console_enabled: bool,
    ) -> Result<Self> {
        Ok(Self {
            node: ctx.node().stats(),
            cluster_id,
            start_time_unix_ms: ctx.node().start_time(),
            server_time_unix_ms: ctx.node().time(),
            services: ctx.node().services(),
            sockets: rskynet_net::netstat(ctx).await?,
            debug_console_enabled,
        })
    }
}

/// 节点内运行的 Dashboard HTTP 服务。
pub struct DashboardService {
    http: HttpServer,
    cluster_id: SvcCell<Option<u32>>,
    debug: SvcCell<DebugConfig>,
}

impl DashboardService {
    pub fn new() -> Self {
        Self {
            http: HttpServer::default(),
            cluster_id: SvcCell::new(None),
            debug: SvcCell::new(DebugConfig {
                enabled: false,
                call_timeout_ms: DEFAULT_DEBUG_CALL_TIMEOUT_MS,
            }),
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
        self.debug.replace(DebugConfig {
            enabled: config.debug_console,
            call_timeout_ms: config.debug_call_timeout_ms,
        });
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
                let debug = *self.debug.borrow();
                for request in requests {
                    if let Err(error) = serve(&ctx, request, cluster_id, debug).await {
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
    debug: DebugConfig,
) -> rskynet_http::Result<()> {
    let method = request.request.method().clone();
    let path = request.request.uri().path().to_owned();
    let content_length = request
        .request
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    let content_type = request
        .request
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let ServerRequest {
        request, responder, ..
    } = request;

    match path.as_str() {
        "/" | "/index.html" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
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
        "/api/v1/stats" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
            let snapshot = DashboardSnapshot::capture(ctx, cluster_id, debug.enabled)
                .await
                .map_err(|error| HttpError::Protocol(error.to_string()))?;
            let bytes = serde_json::to_vec(&snapshot)
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
        "/api/v1/debug/services" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
            if !debug.enabled {
                return send_api_error(
                    ctx,
                    responder,
                    ApiError::not_found("debug_disabled", "调试控制台未启用"),
                )
                .await;
            }
            send_json(ctx, responder, StatusCode::OK, &debug_services(ctx), None).await
        }
        "/api/v1/debug/invoke" if method == Method::POST => {
            if !debug.enabled {
                request.into_body().discard(ctx).await?;
                return send_api_error(
                    ctx,
                    responder,
                    ApiError::not_found("debug_disabled", "调试控制台未启用"),
                )
                .await;
            }
            if content_length.is_some_and(|length| length > DEBUG_BODY_LIMIT) {
                request.into_body().discard(ctx).await?;
                return send_api_error(
                    ctx,
                    responder,
                    ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "body_too_large",
                        "请求体不能超过 256 KiB",
                    ),
                )
                .await;
            }
            if !content_type
                .split(';')
                .next()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
            {
                request.into_body().discard(ctx).await?;
                return send_api_error(
                    ctx,
                    responder,
                    ApiError::new(
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        "content_type",
                        "Content-Type 必须是 application/json",
                    ),
                )
                .await;
            }
            let bytes = match request.into_body().collect(ctx, DEBUG_BODY_LIMIT).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    return send_api_error(
                        ctx,
                        responder,
                        ApiError::new(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "body_too_large",
                            "请求体不能超过 256 KiB",
                        ),
                    )
                    .await;
                }
            };
            let invocation = match serde_json::from_slice::<DebugInvocation>(&bytes) {
                Ok(invocation) => invocation,
                Err(error) => {
                    return send_api_error(
                        ctx,
                        responder,
                        ApiError::bad_request("invalid_json", format!("请求 JSON 无效：{error}")),
                    )
                    .await;
                }
            };
            match invoke(ctx, invocation, debug.call_timeout_ms).await {
                Ok(result) => send_json(ctx, responder, StatusCode::OK, &result, None).await,
                Err(error) => send_api_error(ctx, responder, error).await,
            }
        }
        "/assets/clock.svg" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
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
        "/assets/refresh.svg" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
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
        "/assets/server.svg" if method == Method::GET => {
            request.into_body().discard(ctx).await?;
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
        "/api/v1/debug/invoke" => {
            request.into_body().discard(ctx).await?;
            send_api_error(ctx, responder, ApiError::method_not_allowed("POST")).await
        }
        "/"
        | "/index.html"
        | "/api/v1/stats"
        | "/api/v1/debug/services"
        | "/assets/clock.svg"
        | "/assets/refresh.svg"
        | "/assets/server.svg" => {
            request.into_body().discard(ctx).await?;
            send_api_error(ctx, responder, ApiError::method_not_allowed("GET")).await
        }
        _ => {
            request.into_body().discard(ctx).await?;
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

#[derive(Debug, Serialize)]
struct DebugServices {
    services: Vec<DebugService>,
}

#[derive(Debug, Serialize)]
struct DebugService {
    handle: String,
    kind: String,
    names: Vec<String>,
    messages: Vec<DebugMessageInfo>,
}

#[derive(Debug, Serialize)]
struct DebugMessageInfo {
    id: String,
    name: &'static str,
    mtype: u8,
    request_type: &'static str,
    response_type: Option<&'static str>,
    request_example: Option<&'static str>,
    call_supported: bool,
}

fn debug_services(ctx: &Ctx) -> DebugServices {
    let services = ctx
        .node()
        .services()
        .into_iter()
        .filter(|service| service.lifecycle == ServiceLifecycle::Running)
        .filter_map(|service| {
            let messages = ctx.node().debug_messages(service.handle).ok()?;
            let messages: Vec<_> = messages
                .into_iter()
                .filter(|message| !message.mtype().is_reply())
                .map(|message| DebugMessageInfo {
                    id: format!("{}:{}", message.name(), message.mtype().raw()),
                    name: message.name(),
                    mtype: message.mtype().raw(),
                    request_type: message.request_type(),
                    response_type: message.response_type(),
                    request_example: message.request_example(),
                    call_supported: message.supports_call(),
                })
                .collect();
            (!messages.is_empty()).then(|| DebugService {
                handle: format_handle(service.handle),
                kind: service.kind,
                names: service.names,
                messages,
            })
        })
        .collect();
    DebugServices { services }
}

fn format_handle(handle: rskynet_core::Handle) -> String {
    format!(":{handle:08x}")
}

#[derive(Debug, Deserialize)]
struct DebugInvocation {
    target: String,
    message: String,
    mtype: u8,
    mode: DebugInvocationMode,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DebugInvocationMode {
    Call,
    Send,
}

#[derive(Debug, Serialize)]
struct DebugInvocationResult {
    ok: bool,
    mode: &'static str,
    accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    duration_ms: u64,
}

async fn invoke(
    ctx: &Ctx,
    invocation: DebugInvocation,
    call_timeout_ms: u32,
) -> std::result::Result<DebugInvocationResult, ApiError> {
    let handle = parse_handle(&invocation.target)?;
    let descriptor = ctx
        .node()
        .debug_messages(handle)
        .map_err(|_| ApiError::not_found("service_not_found", "目标 service 不存在或已经退出"))?
        .into_iter()
        .find(|message| {
            message.name() == invocation.message && message.mtype().raw() == invocation.mtype
        })
        .ok_or_else(|| ApiError::not_found("message_not_found", "该 service 未开放这条调试消息"))?;
    if descriptor.mtype().is_reply() {
        return Err(ApiError::bad_request(
            "reply_type_forbidden",
            "调试控制台不能发送 RESPONSE 或 ERROR",
        ));
    }
    let payload = descriptor.decode(invocation.payload).map_err(|error| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_payload",
            error.to_string(),
        )
    })?;
    let started = ctx.now();
    match invocation.mode {
        DebugInvocationMode::Send => {
            ctx.send(handle, descriptor.mtype(), payload)
                .map_err(core_api_error)?;
            Ok(DebugInvocationResult {
                ok: true,
                mode: "send",
                accepted: true,
                result: None,
                duration_ms: ctx.now().saturating_sub(started),
            })
        }
        DebugInvocationMode::Call => {
            if !descriptor.supports_call() {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "call_not_supported",
                    "该消息处理器没有返回值，只支持 send",
                ));
            }
            let call = ctx.call(handle, descriptor.mtype(), payload);
            let timeout = ctx.sleep(call_timeout_ms);
            pin_mut!(call, timeout);
            let reply = match select(call, timeout).await {
                Either::Left((reply, _)) => reply.map_err(core_api_error)?,
                Either::Right(((), _)) => {
                    return Err(ApiError::new(
                        StatusCode::GATEWAY_TIMEOUT,
                        "call_timeout",
                        format!("call 在 {call_timeout_ms} ms 内没有收到应答"),
                    ));
                }
            };
            let result = descriptor.encode(reply).map_err(|error| {
                ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "invalid_response",
                    format!("service 返回值无法按声明类型转换：{error}"),
                )
            })?;
            Ok(DebugInvocationResult {
                ok: true,
                mode: "call",
                accepted: true,
                result: Some(result),
                duration_ms: ctx.now().saturating_sub(started),
            })
        }
    }
}

fn parse_handle(value: &str) -> std::result::Result<rskynet_core::Handle, ApiError> {
    let hex = value.strip_prefix(':').ok_or_else(|| {
        ApiError::bad_request("invalid_target", "目标 handle 必须使用 :十六进制 格式")
    })?;
    if hex.is_empty() || hex.len() > 16 {
        return Err(ApiError::bad_request(
            "invalid_target",
            "目标 handle 不是有效的 u64 十六进制地址",
        ));
    }
    rskynet_core::Handle::from_str_radix(hex, 16).map_err(|_| {
        ApiError::bad_request("invalid_target", "目标 handle 不是有效的 u64 十六进制地址")
    })
}

fn core_api_error(error: Error) -> ApiError {
    match error {
        Error::NoService(_) | Error::NameNotFound(_) => {
            ApiError::not_found("service_not_found", error.to_string())
        }
        Error::CallFailed(_) => {
            ApiError::new(StatusCode::BAD_GATEWAY, "call_failed", error.to_string())
        }
        _ => ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "service_error",
            error.to_string(),
        ),
    }
}

#[derive(Debug, Serialize)]
struct ApiErrorBody<'a> {
    ok: bool,
    error: ApiErrorDetail<'a>,
}

#[derive(Debug, Serialize)]
struct ApiErrorDetail<'a> {
    code: &'a str,
    message: &'a str,
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    allow: Option<&'static str>,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            allow: None,
        }
    }

    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    fn method_not_allowed(allow: &'static str) -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: "method_not_allowed",
            message: format!("该接口只允许 {allow}"),
            allow: Some(allow),
        }
    }
}

async fn send_api_error(
    ctx: &Ctx,
    responder: rskynet_http::ServerResponder,
    error: ApiError,
) -> rskynet_http::Result<()> {
    let body = ApiErrorBody {
        ok: false,
        error: ApiErrorDetail {
            code: error.code,
            message: &error.message,
        },
    };
    send_json(
        ctx,
        responder,
        error.status,
        &body,
        error.allow.map(|allow| ("allow", allow)),
    )
    .await
}

async fn send_json<T: Serialize>(
    ctx: &Ctx,
    responder: rskynet_http::ServerResponder,
    status: StatusCode,
    value: &T,
    extra_header: Option<(&'static str, &'static str)>,
) -> rskynet_http::Result<()> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| HttpError::Protocol(error.to_string()))?;
    send(
        ctx,
        responder,
        status,
        "application/json; charset=utf-8",
        bytes,
        extra_header,
    )
    .await
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
    for chunk in bytes.chunks(32 * 1024) {
        output.write_chunk(ctx, chunk.to_vec()).await?;
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
                address: String::new(),
                debug_console: false,
                debug_call_timeout_ms: DEFAULT_DEBUG_CALL_TIMEOUT_MS,
            }
            .validate()
            .is_err()
        );
        assert!(
            DashboardConfig {
                address: "localhost:8080".into(),
                debug_console: false,
                debug_call_timeout_ms: DEFAULT_DEBUG_CALL_TIMEOUT_MS,
            }
            .validate()
            .is_err()
        );
        assert_eq!(
            DashboardConfig {
                address: "127.0.0.1:8080".into(),
                debug_console: false,
                debug_call_timeout_ms: DEFAULT_DEBUG_CALL_TIMEOUT_MS,
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
            sockets: vec![SocketInfo {
                id: rskynet_net::SocketId(7),
                owner: 0x100,
                owner_kind: Some(NAME.into()),
                owner_names: vec![NAME.into()],
                kind: "listener",
                state: "listen",
                paused: false,
                local: Some("127.0.0.1:8080".parse().unwrap()),
                peer: None,
                write_pending: 0,
                accept_count: 3,
                read_bytes: 0,
                write_bytes: 0,
                last_read_at_ms: Some(42),
                last_write_at_ms: None,
                reading: true,
                writing: false,
            }],
            debug_console_enabled: true,
        };
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["cluster_id"], u32::MAX);
        assert_eq!(value["node"]["uptime_ms"], 42);
        assert_eq!(value["sockets"][0]["id"], 7);
        assert_eq!(value["sockets"][0]["kind"], "listener");
        assert_eq!(value["sockets"][0]["local"], "127.0.0.1:8080");
        assert_eq!(value["debug_console_enabled"], true);
    }

    #[test]
    fn debug_console_requires_loopback_and_positive_timeout() {
        assert!(
            DashboardConfig {
                address: "0.0.0.0:8080".into(),
                debug_console: true,
                debug_call_timeout_ms: DEFAULT_DEBUG_CALL_TIMEOUT_MS,
            }
            .validate()
            .is_err()
        );
        assert!(
            DashboardConfig {
                address: "[::1]:8080".into(),
                debug_console: true,
                debug_call_timeout_ms: 0,
            }
            .validate()
            .is_err()
        );
        assert!(
            DashboardConfig {
                address: "[::1]:8080".into(),
                debug_console: true,
                debug_call_timeout_ms: 1,
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn embedded_page_contains_the_api_and_cluster_condition() {
        assert!(INDEX_HTML.contains("/api/v1/stats"));
        assert!(INDEX_HTML.contains("/api/v1/debug/services"));
        assert!(INDEX_HTML.contains("/api/v1/debug/invoke"));
        assert!(INDEX_HTML.contains("loadPayloadExample"));
        assert!(INDEX_HTML.contains("debugHistoryKey"));
        assert!(INDEX_HTML.contains("clusterBadge"));
        assert!(INDEX_HTML.contains("setInterval(refresh, 5000)"));
        assert!(INDEX_HTML.contains("近 1 分钟增量"));
        assert!(INDEX_HTML.contains("单个服务邮箱积压趋势"));
        assert!(INDEX_HTML.contains("networkTab"));
        assert!(INDEX_HTML.contains("networkKindFilter"));
        assert!(INDEX_HTML.contains("networkStateFilter"));
        assert!(INDEX_HTML.contains("data-socket-sort=\"write_pending\""));
        assert!(INDEX_HTML.contains("formatActivity(socket.last_read_at_ms)"));
        assert!(!INDEX_HTML.contains("较上次采样"));
    }

    #[test]
    fn handle_display_has_a_minimum_width_without_truncating_u64() {
        assert_eq!(format_handle(7), ":00000007");
        assert_eq!(format_handle(0x1_0000_0000), ":100000000");
    }
}
