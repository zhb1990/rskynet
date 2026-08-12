//! ping-pong 示例：展示 rskynet 的三件核心能力。
//!
//! 1. `call` 把「发请求 - 等回包」写成一句 `await`，读起来像同步代码
//! 2. `spawn` 在服务内起并发任务，一个任务挂起时服务照常干活
//! 3. `sleep` 走定时器回包，与 `call` 共用同一套 session 机制
//!
//! 跑法：`cargo run --example ping_pong`

use std::sync::Arc;
use std::time::Instant;

use rskynet::{
    BoxFuture, Config, ConfigExt, Ctx, Message, MsgType, Payload, Registry, Result, Service,
    SvcCell,
};

/// ping 与 pong 之间的请求。同进程传递，直接塞对象，不需要序列化。
enum Ask {
    /// 打一个回合，把球号原样打回来。
    Ball(u64),
    /// 先睡 `centis` 厘秒再应答，用来观察并发。
    Delayed { centis: u32, tag: String },
}

// ---------------------------------------------------------------- pong

#[derive(Default)]
struct Pong {
    served: SvcCell<u64>,
}

impl Service for Pong {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            if !ctx.register_name("pong") {
                return Err(rskynet::Error::service("名字 .pong 已被占用"));
            }
            ctx.log("pong 就绪");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            *self.served.borrow_mut() += 1;
            let Ok(ask) = msg.take_payload().downcast::<Ask>() else {
                let _ = ctx.reply_error(&msg);
                return;
            };
            match *ask {
                Ask::Ball(round) => {
                    let _ = ctx.reply(&msg, Payload::of(Ask::Ball(round)));
                }
                Ask::Delayed { centis, tag } => {
                    // 这里挂起的只是「处理这条消息」的任务，pong 仍在正常收其它消息
                    ctx.sleep(centis).await;
                    let _ = ctx.reply(&msg, Payload::text(tag));
                }
            }
        })
    }
}

// ---------------------------------------------------------------- ping

#[derive(Default)]
struct Ping {
    /// 心跳任务跑了多少轮。服务内是单线程的，所以 SvcCell 足够，不需要锁。
    beats: SvcCell<u64>,
}

impl Service for Ping {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let rounds: u64 = args.trim().parse().unwrap_or(100);

            // 心跳任务：与下面的主流程并发跑，证明服务不会被 await 卡住。
            // 节点关停时它会随服务一起被丢弃，所以这里可以放心写死循环。
            let beat_ctx = ctx.clone();
            let beats = self.clone();
            ctx.spawn(async move {
                loop {
                    beat_ctx.sleep(20).await;
                    *beats.beats.borrow_mut() += 1;
                }
            });

            self.round_trips(&ctx, rounds).await?;
            self.concurrent_asks(&ctx).await?;

            rskynet::log!(ctx, "心跳共跳了 {} 次", self.beats.borrow());
            rskynet::log!(
                ctx,
                "ping 处理过 {} 条消息，收工关停节点",
                ctx.message_count()
            );
            ctx.abort();
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

impl Ping {
    /// 连续往返，测一发消息一个来回的成本。
    async fn round_trips(&self, ctx: &Ctx, rounds: u64) -> Result<()> {
        let started = Instant::now();
        for round in 1..=rounds {
            let reply = ctx.request(".pong", Payload::of(Ask::Ball(round))).await?;
            let ball = reply
                .downcast::<Ask>()
                .map_err(|_| rskynet::Error::service("pong 回了个看不懂的东西"))?;
            match *ball {
                Ask::Ball(back) if back == round => {}
                _ => return Err(rskynet::Error::service("球号对不上")),
            }
        }
        let cost = started.elapsed();
        rskynet::log!(
            ctx,
            "{rounds} 个来回耗时 {:?}，平均单程 {:?}",
            cost,
            cost / (rounds as u32 * 2)
        );
        Ok(())
    }

    /// 同时发三个「先睡再回」的请求，验证服务内真的是并发而不是排队。
    async fn concurrent_asks(&self, ctx: &Ctx) -> Result<()> {
        let delays = [30u32, 10, 20];
        let done: Arc<SvcCell<Vec<String>>> = Arc::new(SvcCell::default());

        for centis in delays {
            let task_ctx = ctx.clone();
            let done = done.clone();
            ctx.spawn(async move {
                let ask = Ask::Delayed {
                    centis,
                    tag: format!("睡{centis}厘秒"),
                };
                match task_ctx.request(".pong", Payload::of(ask)).await {
                    Ok(reply) => done
                        .borrow_mut()
                        .push(reply.as_str().unwrap_or("?").to_string()),
                    Err(err) => rskynet::log!(task_ctx, "并发请求失败：{err}"),
                }
            });
        }

        let started = Instant::now();
        // 等三个任务都完成：自己也只是一个任务，睡着的时候别人照常跑
        while done.borrow().len() < delays.len() {
            ctx.sleep(1).await;
        }
        let cost = started.elapsed();

        rskynet::log!(
            ctx,
            "三个请求（30/10/20 厘秒）的完成顺序是 {:?}，总耗时 {:?}",
            done.borrow(),
            cost
        );
        assert!(
            cost.as_millis() < 500,
            "并发执行的总耗时应接近最慢的那个请求，而不是三者之和"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------- 启动

fn main() -> Result<()> {
    // 日志、定时器、引导这三个内置服务由 rskynet::start 按 feature 挂上
    let registry = Registry::new()
        .with("ping", Ping::default)
        .with("pong", Pong::default);

    // 也可以用 Config::from_toml_file("config/dev.toml")
    let config = Config::default()
        .with_thread(4)
        .with_bootstrap([("pong", ""), ("ping", "1000")]);

    println!(
        "=== rskynet ping-pong 示例，MsgType::USER = {:?} ===",
        MsgType::USER
    );
    rskynet::start(config, registry)
}
