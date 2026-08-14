use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rskynet_bootstrap::{ConfigExt as _, RegistryExt as _};
use rskynet_core::{Builder, Config, Ctx, NodeRef, Registry, Result};
use rskynet_dashboard::RegistryExt as _;
use rskynet_net::RegistryExt as _;
use rskynet_timer::BuilderExt as _;

struct Probe(mpsc::Sender<NodeRef>);

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
    let mut config = Config::default().with_bootstrap(["probe"]);
    config
        .section_mut("dashboard")
        .insert("address".into(), address.to_string().into());
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
