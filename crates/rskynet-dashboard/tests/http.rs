use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rskynet_bootstrap::{ConfigExt as _, RegistryExt as _};
use rskynet_core::{Builder, Config, Ctx, MsgType, NodeRef, Registry, Result};
use rskynet_dashboard::RegistryExt as _;
use rskynet_net::RegistryExt as _;
use rskynet_timer::BuilderExt as _;

struct Probe(mpsc::Sender<NodeRef>);

const NOTICE: MsgType = MsgType(42);
const SLOW: MsgType = MsgType(43);

#[derive(serde::Deserialize, rskynet_macros::MessageSchema)]
#[schema(crate = ::rskynet_core)]
struct DoubleRequest {
    /// 要翻倍的原始数值。
    value: u32,
}

#[derive(Debug, serde::Serialize, rskynet_macros::MessageSchema)]
#[schema(crate = ::rskynet_core)]
struct DoubleResponse {
    value: u32,
}

rskynet_core::boxed_payload!(DoubleRequest, DoubleResponse);

struct DebugTarget(mpsc::Sender<String>);

#[rskynet_macros::service(crate = ::rskynet_core)]
impl DebugTarget {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        assert!(ctx.register_name("debug-target"));
        Ok(())
    }

    #[debug(name = "double")]
    #[msg(MsgType::USER)]
    async fn double(&self, _ctx: Ctx, request: DoubleRequest) -> DoubleResponse {
        DoubleResponse {
            value: request.value * 2,
        }
    }

    #[debug]
    #[msg(NOTICE)]
    async fn notice(&self, _ctx: Ctx, message: String) {
        self.0.send(message).unwrap();
    }

    #[debug]
    #[msg(SLOW)]
    async fn slow(&self, ctx: Ctx, message: String) -> String {
        ctx.sleep(100).await;
        message
    }
}

#[rskynet_macros::service(crate = ::rskynet_core)]
impl Probe {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        self.0
            .send(ctx.node())
            .map_err(|error| rskynet_core::Error::service(error.to_string()))
    }
}

#[test]
fn dashboard_serves_embedded_ui_and_live_stats() {
    let address = available_address();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (notice_tx, notice_rx) = mpsc::channel();
    let mut config = Config::default().with_bootstrap(["debug-target", "probe"]);
    config
        .section_mut("dashboard")
        .insert("address".into(), address.to_string().into());
    config
        .section_mut("dashboard")
        .insert("debug_console".into(), true.into());
    config
        .section_mut("dashboard")
        .insert("debug_call_timeout_ms".into(), 10.into());
    config
        .section_mut("cluster")
        .insert("node_id".into(), i64::from(u32::MAX).into());
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());

    let runtime = thread::spawn(move || {
        let registry = Registry::new()
            .with_bootstrap()
            .with_net()
            .with_dashboard()
            .with("debug-target", move || DebugTarget(notice_tx.clone()))
            .with("probe", move || Probe(ready_tx.clone()));
        Builder::new(config)
            .registry(registry)
            .with_wheel_timer()
            .startup_service(rskynet_net::NAME, "")
            .startup_service(rskynet_dashboard::NAME, "")
            .run()
    });

    let node = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("测试节点应完成启动");

    let html = request(
        address,
        "GET / HTTP/1.1\r\nHost: dashboard\r\nConnection: close\r\n\r\n",
    );
    assert!(html.starts_with("HTTP/1.1 200"));
    assert!(html.contains("content-type: text/html"));
    assert!(html.contains("rskynet dashboard"));

    let stats = request(
        address,
        "GET /api/v1/stats HTTP/1.1\r\nHost: dashboard\r\nConnection: close\r\n\r\n",
    );
    assert!(stats.starts_with("HTTP/1.1 200"));
    assert!(stats.contains("cache-control: no-store"));
    let body = stats.split_once("\r\n\r\n").unwrap().1;
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(json["cluster_id"], u32::MAX);
    assert_eq!(json["debug_console_enabled"], true);
    let start_time = json["start_time_unix_ms"].as_u64().unwrap();
    let server_time = json["server_time_unix_ms"].as_u64().unwrap();
    let node_uptime = json["node"]["uptime_ms"].as_u64().unwrap();
    assert!(start_time > 1_600_000_000_000, "启动时间应为 Unix 毫秒");
    assert!(server_time >= start_time);
    assert!(server_time - start_time >= node_uptime);
    assert!(json["services"].as_array().unwrap().iter().any(|service| {
        service["kind"] == rskynet_dashboard::NAME
            && service["start_time_unix_ms"].as_u64().unwrap() >= start_time
            && service["uptime_ms"].as_u64().is_some()
    }));
    let sockets = json["sockets"].as_array().unwrap();
    assert!(sockets.iter().any(|socket| {
        socket["kind"] == "listener"
            && socket["state"] == "listen"
            && socket["owner_kind"] == rskynet_dashboard::NAME
            && socket["local"] == address.to_string()
            && socket["id"].as_u64().is_some()
    }));

    let services = request(
        address,
        "GET /api/v1/messages HTTP/1.1\r\nHost: dashboard\r\nConnection: close\r\n\r\n",
    );
    assert!(services.starts_with("HTTP/1.1 200"));
    let body = services.split_once("\r\n\r\n").unwrap().1;
    let debug: serde_json::Value = serde_json::from_str(body).unwrap();
    let target = debug["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|service| service["kind"] == "debug-target")
        .unwrap();
    let target_handle = target["handle"].as_str().unwrap();
    assert!(
        target_handle.len() >= 9,
        "handle 应至少为冒号加 8 位十六进制"
    );
    assert_eq!(target["messages"].as_array().unwrap().len(), 3);
    assert_eq!(target["messages"][0]["name"], "double");
    assert_eq!(target["messages"][0]["call_supported"], true);
    assert_eq!(target["messages"][0]["request_schema"]["type"], "object");
    assert_eq!(
        target["messages"][0]["request_schema"]["properties"]["value"]["type"],
        "integer"
    );
    assert_eq!(
        target["messages"][0]["response_schema"]["properties"]["value"]["type"],
        "integer"
    );
    assert_eq!(target["messages"][1]["mtype"], NOTICE.raw());

    let call_body = serde_json::json!({
        "target": target_handle,
        "message": "double",
        "mtype": MsgType::USER.raw(),
        "mode": "call",
        "payload": { "value": 21 }
    })
    .to_string();
    let call = post_json(address, "/api/v1/debug/invoke", &call_body);
    assert!(call.starts_with("HTTP/1.1 200"), "{call}");
    let result: serde_json::Value =
        serde_json::from_str(call.split_once("\r\n\r\n").unwrap().1).unwrap();
    assert_eq!(result["result"]["value"], 42);

    let send_body = serde_json::json!({
        "target": target_handle,
        "message": "notice",
        "mtype": NOTICE.raw(),
        "mode": "send",
        "payload": "hello dashboard"
    })
    .to_string();
    let sent = post_json(address, "/api/v1/debug/invoke", &send_body);
    assert!(sent.starts_with("HTTP/1.1 200"), "{sent}");
    assert_eq!(
        notice_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "hello dashboard"
    );

    let unsupported_call = post_json(
        address,
        "/api/v1/debug/invoke",
        &serde_json::json!({
            "target": target_handle,
            "message": "notice",
            "mtype": NOTICE.raw(),
            "mode": "call",
            "payload": "hello"
        })
        .to_string(),
    );
    assert!(unsupported_call.starts_with("HTTP/1.1 409"));
    assert!(unsupported_call.contains("call_not_supported"));

    let invalid_payload = post_json(
        address,
        "/api/v1/debug/invoke",
        &serde_json::json!({
            "target": target_handle,
            "message": "double",
            "mtype": MsgType::USER.raw(),
            "mode": "send",
            "payload": { "wrong": true }
        })
        .to_string(),
    );
    assert!(invalid_payload.starts_with("HTTP/1.1 422"));
    assert!(invalid_payload.contains("invalid_payload"));

    let timed_out = post_json(
        address,
        "/api/v1/debug/invoke",
        &serde_json::json!({
            "target": target_handle,
            "message": "slow",
            "mtype": SLOW.raw(),
            "mode": "call",
            "payload": "wait"
        })
        .to_string(),
    );
    assert!(timed_out.starts_with("HTTP/1.1 504"), "{timed_out}");
    assert!(timed_out.contains("call_timeout"));

    let oversized = post_json(address, "/api/v1/debug/invoke", &" ".repeat(262_145));
    assert!(oversized.starts_with("HTTP/1.1 413"), "{oversized}");
    assert!(oversized.contains("body_too_large"));
    assert!(sockets.iter().any(|socket| {
        socket["kind"] == "stream"
            && socket["owner_kind"] == rskynet_dashboard::NAME
            && socket["peer"].as_str().is_some()
    }));

    let method = request(
        address,
        "POST /api/v1/stats HTTP/1.1\r\nHost: dashboard\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(method.starts_with("HTTP/1.1 405"));
    assert!(method.contains("allow: GET"));

    node.abort();
    runtime
        .join()
        .expect("测试节点线程不应 panic")
        .expect("测试节点应正常退出");
}

#[test]
fn dashboard_debug_api_is_disabled_by_default() {
    let address = available_address();
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut config = Config::default().with_bootstrap(["probe"]);
    config
        .section_mut("dashboard")
        .insert("address".into(), address.to_string().into());
    config
        .section_mut("logger")
        .insert("name".into(), "".into());
    config
        .section_mut("signal")
        .insert("name".into(), "".into());

    let runtime = thread::spawn(move || {
        let registry = Registry::new()
            .with_bootstrap()
            .with_net()
            .with_dashboard()
            .with("probe", move || Probe(ready_tx.clone()));
        Builder::new(config)
            .registry(registry)
            .with_wheel_timer()
            .startup_service(rskynet_net::NAME, "")
            .startup_service(rskynet_dashboard::NAME, "")
            .run()
    });
    let node = ready_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("测试节点应完成启动");

    let stats = request(
        address,
        "GET /api/v1/stats HTTP/1.1\r\nHost: dashboard\r\nConnection: close\r\n\r\n",
    );
    let body = stats.split_once("\r\n\r\n").unwrap().1;
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(json["debug_console_enabled"], false);

    let messages = request(
        address,
        "GET /api/v1/messages HTTP/1.1\r\nHost: dashboard\r\nConnection: close\r\n\r\n",
    );
    assert!(messages.starts_with("HTTP/1.1 200"));

    let invoke = post_json(address, "/api/v1/debug/invoke", "{}");
    assert!(invoke.starts_with("HTTP/1.1 404"));
    assert!(invoke.contains("debug_disabled"));

    node.abort();
    runtime
        .join()
        .expect("测试节点线程不应 panic")
        .expect("测试节点应正常退出");
}

fn available_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn request(address: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

fn post_json(address: SocketAddr, path: &str, body: &str) -> String {
    request(
        address,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: dashboard\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
}
