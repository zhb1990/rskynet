//! Service 内组合式插件的完整示例。
//!
//! 为方便直接运行，四个插件写在同一个模块里；真实项目可以把每个插件移到独立
//! crate，只要最终二进制依赖那些 crate，注册与装配方式完全相同。

use std::sync::Arc;

use mlua::{Function, Lua, RegistryKey};
use rskynet::plugin::{
    CommandEnvelope, CommandId, PluginCtx, PluginHost, PluginMount, ServicePlugin,
    register_service_plugin,
};
use rskynet::{BoxFuture, Ctx, Message, Payload, Result, SvcCell};
use serde::{Deserialize, Serialize};

const ADD: CommandId = CommandId(1);
const STOP: CommandId = CommandId(2);
const LUA_ASYNC: CommandId = CommandId(3);

// -----------------------------------------------------------------------------
// counter 插件：发布一个可被依赖插件直接调用的 Capability

trait CounterApi: Send + Sync {
    fn add(self: Arc<Self>, ctx: PluginCtx, amount: u64) -> BoxFuture<'static, Result<u64>>;
}

#[derive(Clone)]
struct CounterCapability(Arc<dyn CounterApi>);

impl CounterCapability {
    async fn add(&self, ctx: PluginCtx, amount: u64) -> Result<u64> {
        self.0.clone().add(ctx, amount).await
    }
}

#[derive(Default)]
struct CounterPlugin {
    total: SvcCell<u64>,
}

impl CounterApi for CounterPlugin {
    fn add(self: Arc<Self>, ctx: PluginCtx, amount: u64) -> BoxFuture<'static, Result<u64>> {
        Box::pin(async move {
            // Capability 可以是异步函数。SvcCell 借用必须放在 await 之后，不能跨 await。
            ctx.service().yield_now().await;
            let mut total = self.total.borrow_mut();
            *total += amount;
            Ok(*total)
        })
    }
}

impl ServicePlugin for CounterPlugin {
    fn mount(self: Arc<Self>, mount: &mut PluginMount<'_>) -> Result<()> {
        let api: Arc<dyn CounterApi> = self;
        mount.provide(CounterCapability(api))
    }
}

register_service_plugin! {
    namespace: "plugin-demo",
    name: "counter",
    plugin: CounterPlugin,
    factory: CounterPlugin::default,
    dependencies: [],
    commands: [],
}

// -----------------------------------------------------------------------------
// command 插件：调用 counter Capability，并广播“计数已变化”事件

#[derive(Deserialize, rskynet::MessageSchema, Serialize)]
struct AddRequest(u64);
rskynet::boxed_payload!(AddRequest);
struct CounterChanged {
    amount: u64,
    total: u64,
}

#[derive(Default)]
struct CommandPlugin;

#[rskynet::service_plugin(
    namespace = "plugin-demo",
    name = "command",
    factory = CommandPlugin::default,
    dependencies = ["counter"],
    debug,
)]
impl CommandPlugin {
    #[msg(ADD)]
    async fn add(&self, ctx: PluginCtx, request: AddRequest) -> String {
        let counter = ctx
            .require::<CounterCapability>()
            .expect("command 已声明 counter 依赖");
        let total = counter
            .add(ctx.clone(), request.0)
            .await
            .expect("counter capability");
        // emit 只负责入队，不等待订阅者执行完成。
        ctx.emit(CounterChanged {
            amount: request.0,
            total,
        })
        .expect("广播 CounterChanged");
        total.to_string()
    }

    #[msg(STOP)]
    async fn stop(&self, ctx: PluginCtx) -> String {
        let task_ctx = ctx.service().clone();
        ctx.service().spawn(async move {
            // 让 service_plugin 生成的包装层先自动回包，再退出宿主。
            task_ctx.yield_now().await;
            task_ctx.exit();
        });
        "stopping".to_owned()
    }
}

// -----------------------------------------------------------------------------
// lua 插件：Rust 异步调用 Lua，Lua 再调用 Rust 注册的异步 sleep

#[derive(Deserialize)]
struct LuaPluginConfig {
    script: String,
}

#[derive(Deserialize, rskynet::MessageSchema, Serialize)]
struct LuaRequest {
    name: String,
    millis: u64,
}
rskynet::boxed_payload!(LuaRequest);

struct LuaPlugin {
    lua: Lua,
    handler: SvcCell<Option<RegistryKey>>,
}

impl Default for LuaPlugin {
    fn default() -> Self {
        Self {
            lua: Lua::new(),
            handler: SvcCell::new(None),
        }
    }
}

impl ServicePlugin for LuaPlugin {
    fn init(
        self: Arc<Self>,
        ctx: PluginCtx,
        config: toml::Value,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let config: LuaPluginConfig = config
                .try_into()
                .map_err(|err| rskynet::Error::service(format!("解析 lua 插件配置失败：{err}")))?;
            let source = std::fs::read_to_string(&config.script).map_err(|err| {
                rskynet::Error::service(format!("读取 Lua 脚本 `{}` 失败：{err}", config.script,))
            })?;

            let sleep_ctx = ctx.clone();
            let sleep = self
                .lua
                .create_async_function(move |_lua, millis: u64| {
                    let sleep_ctx = sleep_ctx.clone();
                    async move {
                        sleep_ctx.service().sleep(millis).await;
                        Ok(())
                    }
                })
                .map_err(|err| {
                    rskynet::Error::service(format!("注册 Lua 异步 sleep 失败：{err}"))
                })?;
            self.lua
                .globals()
                .set("rskynet_sleep", sleep)
                .map_err(|err| {
                    rskynet::Error::service(format!("导出 Lua 异步 sleep 失败：{err}"))
                })?;

            let handler: Function = self
                .lua
                .load(&source)
                .set_name(&config.script)
                .eval()
                .map_err(|err| {
                    rskynet::Error::service(format!(
                        "加载 Lua 脚本 `{}` 失败：{err}",
                        config.script,
                    ))
                })?;
            let handler = self
                .lua
                .create_registry_value(handler)
                .map_err(|err| rskynet::Error::service(format!("保存 Lua handler 失败：{err}")))?;
            *self.handler.borrow_mut() = Some(handler);
            Ok(())
        })
    }

    fn handle(
        self: Arc<Self>,
        ctx: PluginCtx,
        command: CommandId,
        mut msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            if command != LUA_ASYNC {
                return Err(rskynet::Error::service(format!(
                    "lua 插件收到未知命令 {}",
                    command.0,
                )));
            }
            let request = <LuaRequest as rskynet::FromPayload>::from_payload(msg.take_payload())?;
            // 先从 SvcCell 取出拥有所有权的 Function，不能让借用跨过 await。
            let handler: Function = {
                let handler = self.handler.borrow();
                let handler = handler
                    .as_ref()
                    .ok_or_else(|| rskynet::Error::service("Lua handler 尚未初始化"))?;
                self.lua.registry_value(handler).map_err(|err| {
                    rskynet::Error::service(format!("取得 Lua handler 失败：{err}"))
                })?
            };
            // call_async 驱动 Lua coroutine；rskynet_sleep 挂起时不会阻塞 worker。
            let response = handler
                .call_async::<String>((request.name, request.millis))
                .await
                .map_err(|err| rskynet::Error::service(format!("Lua 异步调用失败：{err}")))?;
            ctx.service().reply(&msg, Payload::text(response))?;
            Ok(())
        })
    }
}

register_service_plugin! {
    namespace: "plugin-demo",
    name: "lua",
    plugin: LuaPlugin,
    factory: LuaPlugin::default,
    dependencies: [],
    commands: [LUA_ASYNC],
}

// -----------------------------------------------------------------------------
// audit 插件：订阅广播事件；它与发布者之间没有直接依赖

#[derive(Default)]
struct AuditPlugin;

impl ServicePlugin for AuditPlugin {
    fn mount(self: Arc<Self>, mount: &mut PluginMount<'_>) -> Result<()> {
        mount.subscribe(|ctx, event: Arc<CounterChanged>| async move {
            rskynet::log!(
                ctx.service(),
                "audit 收到 CounterChanged：+{}，累计 {}",
                event.amount,
                event.total,
            );
            Ok(())
        })
    }
}

register_service_plugin! {
    namespace: "plugin-demo",
    name: "audit",
    plugin: AuditPlugin,
    factory: AuditPlugin::default,
    dependencies: [],
    commands: [],
}

// -----------------------------------------------------------------------------
// 宿主 service：仍然只有一个地址和邮箱，dispatch 全部交给 PluginHost

struct DemoHost {
    plugins: PluginHost,
}

impl Default for DemoHost {
    fn default() -> Self {
        Self {
            plugins: PluginHost::from_auto("plugin-demo").expect("插件注册表应当合法"),
        }
    }
}

#[rskynet::service(name = "plugin-demo-host")]
impl DemoHost {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        self.plugins
            .init_from_section(ctx.clone(), "plugin-demo")
            .await?;
        if !ctx.register_name("plugin-demo") {
            return Err(rskynet::Error::service("名字 .plugin-demo 已被占用"));
        }
        ctx.log("plugin-demo 宿主已就绪");
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, msg: Message) {
        self.plugins.dispatch(ctx, msg).await;
    }
}

// -----------------------------------------------------------------------------
// 调用方：它是普通 service，不需要知道命令由宿主内哪个插件处理

#[derive(Default)]
struct DemoClient;

#[rskynet::service(name = "plugin-demo-client")]
impl DemoClient {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            for amount in [3, 5] {
                let reply = task_ctx
                    .request(
                        ".plugin-demo",
                        Payload::of(CommandEnvelope::new(ADD, Payload::of(AddRequest(amount)))),
                    )
                    .await
                    .expect("ADD 应成功");
                rskynet::log!(
                    task_ctx,
                    "ADD {amount} 返回累计值 {}",
                    reply.as_str().unwrap_or("?"),
                );
            }

            let reply = task_ctx
                .request(
                    ".plugin-demo",
                    Payload::of(CommandEnvelope::new(
                        LUA_ASYNC,
                        Payload::of(LuaRequest {
                            name: "rskynet".to_owned(),
                            millis: 20,
                        }),
                    )),
                )
                .await
                .expect("Lua 异步调用应成功");
            let reply = reply.as_str().expect("Lua 应返回字符串");
            assert_eq!(reply, "hello rskynet after 20 ms");
            rskynet::log!(task_ctx, "Lua 异步调用返回：{reply}");

            task_ctx
                .request(
                    ".plugin-demo",
                    Payload::of(CommandEnvelope::new(STOP, Payload::None)),
                )
                .await
                .expect("STOP 应成功");
            task_ctx.exit();
        });
        Ok(())
    }
}
