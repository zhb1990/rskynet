//! 引导服务，对照 `service/bootstrap.lua`。
//!
//! 参数是一串 JSON 的启动项，按顺序拉起，干完就退场：
//!
//! ```json
//! [{"name": "pong", "args": ""}, {"name": "ping", "args": "100"}]
//! ```
//!
//! 它先起 `pong`（它在 init 里注册好 `.pong` 这个名字），再起 `ping`（于是 ping
//! 一定能查到 pong）。写配置的人不必手搓这串 JSON：`[bootstrap]` 段里的
//! `services` 就是它，内核启动时编码，这里解回来。
//!
//! 之所以绕一趟 JSON，是因为服务的 `init` 只收得到一个字符串。清单是一张表，
//! 用文本承载就得挑个编码；换成 C 版那种 `;` 加空格的分隔法，参数里一旦出现
//! 分隔符就没法转义了。

use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::context::{Ctx, Service};
use crate::error::Result;
use crate::message::Message;
use crate::start::ServiceSpec;

#[derive(Default)]
pub struct Bootstrap;

impl Service for Bootstrap {
    fn init(self: Arc<Self>, ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            for spec in parse(&args)? {
                let handle = ctx.launch(&spec.name, &spec.args).await?;
                ctx.log(format!("bootstrap 拉起 {} -> :{handle:08x}", spec.name));
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

/// 解析启动项清单。空参数按空清单处理，手工 `launch` 时不必写成 `[]`。
fn parse(args: &str) -> Result<Vec<ServiceSpec>> {
    if args.trim().is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_str(args)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 清单从 JSON 解出来，没写的 args 是空串
    #[test]
    fn specs_parse_from_json() {
        let specs = parse(r#"[{"name":"pong"},{"name":"ping","args":"100"}]"#).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "pong");
        assert_eq!(specs[0].args, "");
        assert_eq!(specs[1].name, "ping");
        assert_eq!(specs[1].args, "100");
    }

    /// 参数原样进服务，不再有分隔符之说
    #[test]
    fn args_survive_the_round_trip() {
        let specs = vec![ServiceSpec::new("gate").with_args("0.0.0.0:8888; backlog 128")];
        let text = serde_json::to_string(&specs).unwrap();
        assert_eq!(parse(&text).unwrap()[0].args, "0.0.0.0:8888; backlog 128");
    }

    /// 没有参数等于没有要拉起的服务；参数不是 JSON 则报错，节点起不来
    #[test]
    fn empty_is_no_services_and_garbage_is_rejected() {
        assert!(parse("   ").unwrap().is_empty());
        assert!(parse("pong; ping 100").is_err());
    }
}
