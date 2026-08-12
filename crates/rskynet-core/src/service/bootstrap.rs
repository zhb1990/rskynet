//! 引导服务，对照 `service/bootstrap.lua`。
//!
//! 参数是一串用 `;` 分隔的「类型名 参数」，按顺序拉起，干完就退场。
//! 例如 `"pong; ping 100"` 会先起 `pong`（它在 init 里注册好 `.pong` 这个名字），
//! 再起 `ping`（于是 ping 一定能查到 pong）。

use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::context::{Ctx, Service};
use crate::error::Result;
use crate::message::Message;
use crate::start::split_cmdline;

#[derive(Default)]
pub struct Bootstrap;

impl Service for Bootstrap {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            for entry in args.split(';') {
                let (kind, params) = split_cmdline(entry);
                if kind.is_empty() {
                    continue;
                }
                let handle = ctx.launch(kind, params).await?;
                ctx.log(format!("bootstrap 拉起 {kind} -> :{handle:08x}"));
            }
            // 引导完成即退场，把舞台交给业务服务
            ctx.exit();
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, _ctx: Ctx, _msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
