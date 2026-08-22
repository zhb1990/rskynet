//! Dashboard 强类型消息调试控制台示例。

use rskynet::{Ctx, MsgType, Result};

#[derive(serde::Deserialize, rskynet::MessageSchema)]
struct EchoRequest {
    /// 要回显的正文。
    text: String,
    /// 可选的重复次数。
    repeat: Option<u8>,
}

#[derive(serde::Serialize, rskynet::MessageSchema)]
struct EchoResponse {
    /// 服务生成的完整回显文本。
    echoed: String,
}

enum DebugRequest {
    Echo(EchoRequest),
    Notice(String),
}

rskynet::boxed_payload!(DebugRequest, EchoResponse);

#[derive(Default)]
struct DebugDemo;

#[rskynet::service(name = "debug-demo")]
impl DebugDemo {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        ctx.register_name("debug-demo");
        Ok(())
    }

    #[debug(name = "echo")]
    #[msg(MsgType::USER, variant = DebugRequest::Echo)]
    async fn echo(&self, _ctx: Ctx, request: EchoRequest) -> EchoResponse {
        EchoResponse {
            echoed: std::iter::repeat_n(request.text, usize::from(request.repeat.unwrap_or(1)))
                .collect::<Vec<_>>()
                .join(" "),
        }
    }

    #[debug(name = "notice")]
    #[msg(MsgType::USER, variant = DebugRequest::Notice)]
    async fn notice(&self, ctx: Ctx, text: String) {
        rskynet::log!(ctx, "debug notice: {text}");
    }
}
