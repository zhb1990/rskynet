//! 两个 rskynet 进程之间的 ping / pong。
//!
//! 先起 pong：
//! `cargo run -p rskynet-examples -- config/examples/cluster_pong.toml`
//!
//! 再起 ping：
//! `cargo run -p rskynet-examples -- config/examples/cluster_ping.toml`

use std::time::Instant;

use prost::Message as ProstMessage;
use rskynet::cluster::{self, ClusterAddr, NodeId, RemoteContext};
use rskynet::{Ctx, Error, MsgType, Payload, Result, SvcCell};

const PONG_SERVICE: &str = "cluster-pong";
const PONG_CONTROL: &str = "cluster-pong-control";

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 1_001)]
pub struct PingRequest {
    #[prost(uint64, tag = "1")]
    pub round: u64,
}

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 1_002)]
pub struct PongResponse {
    #[prost(uint64, tag = "1")]
    pub round: u64,
    #[prost(uint32, tag = "2")]
    pub node_id: u32,
}

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 1_003)]
pub struct ShutdownRequest {}

#[derive(Clone, PartialEq, ProstMessage, rskynet::cluster::ClusterMessage)]
#[cluster(type_id = 1_004)]
pub struct ShutdownResponse {}

enum PongCommand {
    Ping {
        source_node: NodeId,
        request: PingRequest,
    },
    Shutdown {
        source_node: NodeId,
    },
}

enum PongReply {
    Pong(PongResponse),
    Shutdown,
}

rskynet::boxed_payload!(PongCommand, PongReply);

#[rskynet::cluster::handler("cluster-pong")]
async fn pong(
    remote: RemoteContext,
    ping: PingRequest,
) -> std::result::Result<PongResponse, String> {
    remote.log(format!("转交本地服务处理第 {} 轮 ping", ping.round));
    let reply = remote
        .request(
            PONG_SERVICE,
            Payload::of(PongCommand::Ping {
                source_node: remote.source_node,
                request: ping,
            }),
        )
        .await
        .map_err(|error| error.to_string())?
        .downcast::<PongReply>()
        .map_err(|_| "本地 cluster-pong 返回了错误的负载类型".to_owned())?;
    match *reply {
        PongReply::Pong(response) => Ok(response),
        PongReply::Shutdown => Err("本地 cluster-pong 返回了错误的响应".into()),
    }
}

#[rskynet::cluster::handler("cluster-pong-control")]
async fn shutdown(
    remote: RemoteContext,
    _request: ShutdownRequest,
) -> std::result::Result<ShutdownResponse, String> {
    remote.log("转交本地服务处理关闭请求");
    let reply = remote
        .request(
            PONG_SERVICE,
            Payload::of(PongCommand::Shutdown {
                source_node: remote.source_node,
            }),
        )
        .await
        .map_err(|error| error.to_string())?
        .downcast::<PongReply>()
        .map_err(|_| "本地 cluster-pong 返回了错误的负载类型".to_owned())?;
    match *reply {
        PongReply::Shutdown => Ok(ShutdownResponse {}),
        PongReply::Pong(_) => Err("本地 cluster-pong 返回了错误的响应".into()),
    }
}

#[derive(Default)]
struct ClusterPong {
    served: SvcCell<u64>,
}

#[rskynet::service(name = "cluster-pong")]
impl ClusterPong {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        if !ctx.register_name(PONG_SERVICE) {
            return Err(Error::service("本地名字 cluster-pong 已被占用"));
        }
        ctx.log("本地 cluster-pong 服务就绪");
        Ok(())
    }

    #[msg(MsgType::USER)]
    async fn command(&self, ctx: Ctx, command: PongCommand) -> PongReply {
        match command {
            PongCommand::Ping {
                source_node,
                request,
            } => {
                *self.served.borrow_mut() += 1;
                rskynet::log!(
                    ctx,
                    "处理 node {} 的第 {} 轮 ping，累计 {} 次",
                    source_node.get(),
                    request.round,
                    self.served.borrow()
                );
                PongReply::Pong(PongResponse {
                    round: request.round,
                    node_id: 2,
                })
            }
            PongCommand::Shutdown { source_node } => {
                rskynet::log!(ctx, "收到 node {} 的关闭请求", source_node.get());
                let shutdown_ctx = ctx.clone();
                ctx.spawn(async move {
                    // 等本地应答经 cluster 编码并进入 socket 写缓冲后再关闭节点。
                    shutdown_ctx.sleep_ms(100).await;
                    shutdown_ctx.abort();
                });
                PongReply::Shutdown
            }
        }
    }
}

#[derive(Default)]
struct ClusterPing;

#[rskynet::service(name = "cluster-ping")]
impl ClusterPing {
    async fn init(&self, ctx: Ctx, args: String) -> Result<()> {
        let mut args = args.split_whitespace();
        let pong_node = args.next().and_then(|v| v.parse().ok()).unwrap_or(2);
        let rounds = args.next().and_then(|v| v.parse().ok()).unwrap_or(10u64);
        let pong_node = NodeId::new(pong_node).map_err(|err| Error::service(err.to_string()))?;
        let target = ClusterAddr::new(pong_node, PONG_SERVICE);

        let started = Instant::now();
        for round in 1..=rounds {
            let deadline = ctx.now() + 500;
            let pong = loop {
                match cluster::request::<PingRequest, PongResponse>(
                    &ctx,
                    target.clone(),
                    PingRequest { round },
                )
                .await
                {
                    Ok(pong) => break pong,
                    Err(err) if ctx.now() < deadline => {
                        rskynet::log!(ctx, "等待 pong 节点就绪：{err}");
                        ctx.sleep_ms(100).await;
                    }
                    Err(err) => return Err(Error::service(format!("跨节点 ping 失败：{err}"))),
                }
            };
            if pong.round != round || pong.node_id != pong_node.get() {
                return Err(Error::service("pong 应答与 ping 不匹配"));
            }
            rskynet::log!(
                ctx,
                "ping #{round} -> node {} -> pong #{}, 成功",
                pong.node_id,
                pong.round
            );
        }
        rskynet::log!(
            ctx,
            "{rounds} 次跨节点 ping/pong 耗时 {:?}",
            started.elapsed()
        );
        cluster::request::<ShutdownRequest, ShutdownResponse>(
            &ctx,
            ClusterAddr::new(pong_node, PONG_CONTROL),
            ShutdownRequest {},
        )
        .await
        .map_err(|err| Error::service(format!("关闭 pong 节点失败：{err}")))?;
        ctx.abort();
        Ok(())
    }
}
