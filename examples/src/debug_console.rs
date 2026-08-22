//! Dashboard 强类型消息调试控制台示例。

use rskynet::{Ctx, MsgType, Result};

const NOTICE: MsgType = MsgType(42);

#[derive(Default)]
struct DebugDemo;

#[rskynet::service(name = "debug-demo")]
impl DebugDemo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("debug-demo");
        Ok(())
    }

    #[debug(name = "echo", example = r#""hello""#)]
    #[msg(MsgType::USER)]
    async fn echo(&self, _ctx: Ctx, text: String) -> String {
        format!("echo: {text}")
    }

    #[debug(name = "notice", example = r#""service warmed up""#)]
    #[msg(NOTICE)]
    async fn notice(&self, ctx: Ctx, text: String) {
        rskynet::log!(ctx, "debug notice: {text}");
    }
}
