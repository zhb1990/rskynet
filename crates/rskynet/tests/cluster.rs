use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};

use prost::Message as ProstMessage;
use rskynet::cluster::{
    self, ClusterAddr, ClusterConfig, HandlerRegistry, NodeId, RegistryExt as _, Seed,
};
use rskynet::{Config, ConfigExt, Ctx, Registry, Result};

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 100)]
struct Ping {
    #[prost(uint32, tag = "1")]
    value: u32,
}
#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 101)]
struct Pong {
    #[prost(uint32, tag = "1")]
    value: u32,
}
#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 102)]
struct WrongResponse {}

#[rskynet::cluster::handler("echo")]
async fn echo(
    _remote: rskynet::cluster::RemoteContext,
    ping: Ping,
) -> std::result::Result<Pong, String> {
    Ok(Pong {
        value: ping.value + 1,
    })
}

#[rskynet::cluster::handler(42)]
async fn handle_42(
    _remote: rskynet::cluster::RemoteContext,
    ping: Ping,
) -> std::result::Result<Pong, String> {
    Ok(Pong {
        value: ping.value + 21,
    })
}

struct Stopper {
    done: Arc<AtomicBool>,
}

#[rskynet::service]
impl Stopper {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let done = self.done.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            while !done.load(SeqCst) {
                task_ctx.sleep_ms(10).await;
            }
            task_ctx.abort();
        });
        Ok(())
    }
}

struct Driver {
    done: Arc<AtomicBool>,
}

struct ManualDriver {
    done: Arc<AtomicBool>,
}

#[rskynet::service]
impl ManualDriver {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let done = self.done.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let target = ClusterAddr::new(NodeId::new(12).unwrap(), "echo");
            let deadline = task_ctx.now() + 500;
            loop {
                if matches!(
                    cluster::request::<Ping, Pong>(&task_ctx, target.clone(), Ping { value: 1 })
                        .await,
                    Ok(Pong { value: 101 })
                ) {
                    break;
                }
                assert!(task_ctx.now() < deadline, "显式 cluster handler 未生效");
                task_ctx.sleep_ms(25).await;
            }
            done.store(true, SeqCst);
            task_ctx.abort();
        });
        Ok(())
    }
}

#[rskynet::service]
impl Driver {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let done = self.done.clone();
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let node2 = ClusterAddr::new(NodeId::new(2).unwrap(), "echo");
            let node3 = ClusterAddr::new(NodeId::new(3).unwrap(), 42u64);
            let deadline = task_ctx.now() + 500;
            loop {
                let first =
                    cluster::request::<Ping, Pong>(&task_ctx, node2.clone(), Ping { value: 20 })
                        .await;
                let second =
                    cluster::request::<Ping, Pong>(&task_ctx, node3.clone(), Ping { value: 21 })
                        .await;
                if matches!(first, Ok(Pong { value: 21 }))
                    && matches!(second, Ok(Pong { value: 42 }))
                {
                    break;
                }
                assert!(
                    task_ctx.now() < deadline,
                    "cluster 节点未在 5 秒内建连并完成请求"
                );
                task_ctx.sleep_ms(25).await;
            }
            assert!(
                cluster::request::<Ping, WrongResponse>(&task_ctx, node2, Ping { value: 1 })
                    .await
                    .is_err()
            );
            assert!(
                cluster::request::<Ping, Pong>(
                    &task_ctx,
                    ClusterAddr::new(NodeId::new(99).unwrap(), "echo"),
                    Ping { value: 1 }
                )
                .await
                .is_err()
            );
            done.store(true, SeqCst);
            task_ctx.abort();
        });
        Ok(())
    }
}

fn free_addr() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

fn config(node_id: u32, listen: String, seeds: Vec<Seed>, services: &[&str]) -> Config {
    let cluster = ClusterConfig {
        node_id,
        listen,
        advertise: None,
        seeds,
        request_timeout_ms: 250,
        max_frame_len: 1024 * 1024,
    };
    let table = toml::Value::try_from(cluster)
        .unwrap()
        .as_table()
        .unwrap()
        .clone();
    Config::default()
        .with_thread(2)
        .with_section(cluster::NAME, table)
        .with_bootstrap(services.iter().copied())
}

#[test]
fn three_nodes_route_by_name_and_local_handle() {
    let address2 = free_addr();
    let address3 = free_addr();
    let done = Arc::new(AtomicBool::new(false));

    let done2 = done.clone();
    let config2 = config(2, address2.clone(), vec![], &["stopper"]);
    let thread2 = std::thread::spawn(move || {
        let registry = Registry::new().with("stopper", move || Stopper {
            done: done2.clone(),
        });
        rskynet::start(config2, registry).unwrap();
    });

    let done3 = done.clone();
    let config3 = config(3, address3.clone(), vec![], &["stopper"]);
    let thread3 = std::thread::spawn(move || {
        let registry = Registry::new().with("stopper", move || Stopper {
            done: done3.clone(),
        });
        rskynet::start(config3, registry).unwrap();
    });

    let seeds = vec![
        Seed {
            node_id: 2,
            address: address2,
        },
        Seed {
            node_id: 3,
            address: address3,
        },
    ];
    let config1 = config(1, free_addr(), seeds, &["driver"]);
    let done1 = done.clone();
    let registry = Registry::new().with("driver", move || Driver {
        done: done1.clone(),
    });
    rskynet::start(config1, registry).unwrap();

    thread2.join().unwrap();
    thread3.join().unwrap();
}

#[test]
fn explicit_cluster_registry_overrides_auto_handlers() {
    let address = free_addr();
    let done = Arc::new(AtomicBool::new(false));
    let server_done = done.clone();
    let server_config = config(12, address.clone(), vec![], &["stopper"]);
    let server = std::thread::spawn(move || {
        let mut handlers = HandlerRegistry::new();
        handlers
            .register::<Ping, Pong, _, _>("echo", |_, ping| async move {
                Ok(Pong {
                    value: ping.value + 100,
                })
            })
            .unwrap();
        let registry = Registry::new()
            .with_cluster(handlers)
            .with("stopper", move || Stopper {
                done: server_done.clone(),
            });
        rskynet::start(server_config, registry).unwrap();
    });

    let client_config = config(
        11,
        free_addr(),
        vec![Seed {
            node_id: 12,
            address,
        }],
        &["manual-driver"],
    );
    let client_done = done.clone();
    let registry = Registry::new().with("manual-driver", move || ManualDriver {
        done: client_done.clone(),
    });
    rskynet::start(client_config, registry).unwrap();
    server.join().unwrap();
}
