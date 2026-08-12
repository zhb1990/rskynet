use std::sync::Arc;
use std::time::Instant;

use rskynet::{Ctx, Message, Payload, Result, SvcCell};

enum Ask {
    Ball(u64),
    Delayed { centis: u32, tag: String },
    Shutdown,
}

#[derive(Default)]
struct Pong {
    served: SvcCell<u64>,
}

#[rskynet::service(name = "pong")]
impl Pong {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        if !ctx.register_name("pong") {
            return Err(rskynet::Error::service("名字 .pong 已被占用"));
        }
        ctx.log("pong 就绪");
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
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
                ctx.sleep(centis).await;
                let _ = ctx.reply(&msg, Payload::text(tag));
            }
            Ask::Shutdown => {
                ctx.log("pong 收到关闭通知，退出");
                ctx.exit();
            }
        }
    }
}

#[derive(Default)]
struct Ping {
    beats: SvcCell<u64>,
}

#[rskynet::service(name = "ping")]
impl Ping {
    async fn init(&self, ctx: Ctx, args: String) -> Result<()> {
        let rounds: u64 = args.trim().parse().unwrap_or(100);
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
            "ping 处理过 {} 条消息，通知 pong 后退出",
            ctx.message_count()
        );
        ctx.post(".pong", Payload::of(Ask::Shutdown))?;
        ctx.exit();
        Ok(())
    }

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
        while done.borrow().len() < delays.len() {
            ctx.sleep(1).await;
        }
        let cost = started.elapsed();
        rskynet::log!(
            ctx,
            "三个请求的完成顺序是 {:?}，总耗时 {:?}",
            done.borrow(),
            cost
        );
        assert!(cost.as_millis() < 500, "三个请求应并发执行");
        Ok(())
    }
}
