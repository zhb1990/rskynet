//! 插件宿主端到端：链接期注册、Capability 直调、邮箱广播与独立实例装配。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

use rskynet::plugin::{
    CommandEnvelope, CommandId, PluginCtx, PluginHost, PluginHostConfig, PluginMount, PluginSpec,
    ServicePlugin, register_service_plugin,
};
use rskynet::{BoxFuture, Config, ConfigExt, Ctx, Message, Payload, Registry, Result, SvcCell};
use serde::{Deserialize, Serialize};

const CALL_COUNTER: CommandId = CommandId(1);
const EMIT_EVENT: CommandId = CommandId(2);
const UNDECLARED_CAPABILITY: CommandId = CommandId(3);
const VARIANT_COMMAND: CommandId = CommandId(4);

static DIRECT_RESULT: AtomicU64 = AtomicU64::new(0);
static EVENT_RESULT: AtomicU64 = AtomicU64::new(0);
static HIDDEN_CALLS: AtomicU64 = AtomicU64::new(0);
static HIDDEN_CALL_RESULT: AtomicU64 = AtomicU64::new(0);

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
    value: SvcCell<u64>,
}

impl CounterApi for CounterPlugin {
    fn add(self: Arc<Self>, ctx: PluginCtx, amount: u64) -> BoxFuture<'static, Result<u64>> {
        Box::pin(async move {
            ctx.service().yield_now().await;
            let mut value = self.value.borrow_mut();
            *value += amount;
            Ok(*value)
        })
    }
}

impl ServicePlugin for CounterPlugin {
    fn mount(self: Arc<Self>, mount: &mut PluginMount<'_>) -> Result<()> {
        let api: Arc<dyn CounterApi> = self;
        mount.provide(CounterCapability(api))
    }

    fn handle(
        self: Arc<Self>,
        _ctx: PluginCtx,
        _command: CommandId,
        _msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

register_service_plugin! {
    namespace: "plugin-test",
    name: "counter",
    plugin: CounterPlugin,
    factory: CounterPlugin::default,
    dependencies: [],
    commands: [],
}

#[derive(Default)]
struct UndeclaredCallerPlugin;

impl ServicePlugin for UndeclaredCallerPlugin {
    fn handle(
        self: Arc<Self>,
        ctx: PluginCtx,
        _command: CommandId,
        _msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let _ = ctx.require::<CounterCapability>()?;
            Ok(())
        })
    }
}

register_service_plugin! {
    namespace: "plugin-test",
    name: "undeclared-caller",
    plugin: UndeclaredCallerPlugin,
    factory: UndeclaredCallerPlugin::default,
    dependencies: [],
    commands: [UNDECLARED_CAPABILITY],
}

#[derive(Default)]
struct CallerPlugin;

#[rskynet::service_plugin(
    namespace = "plugin-test",
    name = "caller",
    factory = CallerPlugin::default,
    dependencies = ["counter"],
    debug,
)]
impl CallerPlugin {
    #[msg(CALL_COUNTER)]
    async fn call_counter(&self, ctx: PluginCtx) -> String {
        let capability = ctx
            .require::<CounterCapability>()
            .expect("已声明 counter 依赖");
        capability
            .add(ctx.clone(), 7)
            .await
            .expect("counter capability")
            .to_string()
    }
}

struct Changed(u64);

#[derive(Deserialize, rskynet::MessageSchema, Serialize)]
struct DoubleRequest {
    value: u64,
}

#[derive(Deserialize, rskynet::MessageSchema, Serialize)]
struct DoubleResponse {
    value: u64,
}

#[derive(Deserialize, rskynet::MessageSchema, Serialize)]
enum VariantCommand {
    Double(DoubleRequest),
    Hidden(DoubleRequest),
}

rskynet::boxed_payload!(DoubleResponse, VariantCommand);

#[derive(Default)]
struct VariantPlugin;

#[rskynet::service_plugin(
    namespace = "plugin-test",
    name = "variant",
    factory = VariantPlugin::default,
    dependencies = [],
)]
impl VariantPlugin {
    #[debug(name = "double")]
    #[msg(VARIANT_COMMAND, variant = VariantCommand::Double)]
    async fn double(&self, _ctx: PluginCtx, request: DoubleRequest) -> DoubleResponse {
        DoubleResponse {
            value: request.value * 2,
        }
    }

    #[msg(VARIANT_COMMAND, variant = VariantCommand::Hidden)]
    async fn hidden(&self, _ctx: PluginCtx, _request: DoubleRequest) {
        HIDDEN_CALLS.fetch_add(1, SeqCst);
    }
}

#[derive(Default)]
struct EmitterPlugin;

impl ServicePlugin for EmitterPlugin {
    fn handle(
        self: Arc<Self>,
        ctx: PluginCtx,
        command: CommandId,
        msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            assert_eq!(command, EMIT_EVENT);
            ctx.emit(Changed(11))?;
            ctx.service().reply(&msg, Payload::None)?;
            Ok(())
        })
    }
}

register_service_plugin! {
    namespace: "plugin-test",
    name: "emitter",
    plugin: EmitterPlugin,
    factory: EmitterPlugin::default,
    dependencies: [],
    commands: [EMIT_EVENT],
}

#[derive(Default)]
struct ObserverPlugin;

impl ServicePlugin for ObserverPlugin {
    fn mount(self: Arc<Self>, mount: &mut PluginMount<'_>) -> Result<()> {
        mount.subscribe(|_ctx, event: Arc<Changed>| async move {
            EVENT_RESULT.fetch_add(event.0, SeqCst);
            Ok(())
        })
    }

    fn handle(
        self: Arc<Self>,
        _ctx: PluginCtx,
        _command: CommandId,
        _msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

register_service_plugin! {
    namespace: "plugin-test",
    name: "observer",
    plugin: ObserverPlugin,
    factory: ObserverPlugin::default,
    dependencies: [],
    commands: [],
}

struct HostService {
    plugins: PluginHost,
}

#[rskynet::service]
impl HostService {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        self.plugins
            .init(
                ctx.clone(),
                PluginHostConfig {
                    plugins: vec![
                        PluginSpec::new("caller"),
                        PluginSpec::new("counter"),
                        PluginSpec::new("emitter"),
                        PluginSpec::new("observer"),
                        PluginSpec::new("undeclared-caller"),
                        PluginSpec::new("variant"),
                    ],
                },
            )
            .await?;
        assert!(ctx.register_name("plugin-host"));
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, msg: Message) {
        self.plugins.dispatch(ctx, msg).await;
    }
}

#[derive(Default)]
struct Driver;

#[rskynet::service]
impl Driver {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let watchdog_ctx = ctx.clone();
        ctx.spawn(async move {
            watchdog_ctx.sleep(1_000).await;
            if HIDDEN_CALL_RESULT.load(SeqCst) == 0 {
                HIDDEN_CALL_RESULT.store(3, SeqCst);
                watchdog_ctx.abort();
            }
        });

        let task_ctx = ctx.clone();
        ctx.spawn(async move {
            let descriptor = task_ctx
                .node()
                .debug_messages(".plugin-host")
                .expect("宿主应公开动态插件消息")
                .into_iter()
                .find(|message| message.name() == "caller.call_counter")
                .expect("debug 全量注册应公开 caller 命令");
            assert!(descriptor.supports_call());
            let dashboard_payload = descriptor
                .decode(serde_json::Value::Null)
                .expect("Dashboard JSON 应包装成插件命令");
            let response = task_ctx
                .request(".plugin-host", dashboard_payload)
                .await
                .expect("Capability 命令应答");
            DIRECT_RESULT.store(
                response
                    .as_str()
                    .expect("文本应答")
                    .parse()
                    .expect("数字应答"),
                SeqCst,
            );

            let variant_messages = task_ctx
                .node()
                .debug_messages(".plugin-host")
                .expect("读取插件 variant 描述");
            assert!(
                variant_messages
                    .iter()
                    .any(|message| message.name() == "variant.double")
            );
            assert!(
                !variant_messages
                    .iter()
                    .any(|message| message.name() == "variant.hidden")
            );
            let double = variant_messages
                .into_iter()
                .find(|message| message.name() == "variant.double")
                .unwrap();
            let reply = task_ctx
                .request(
                    ".plugin-host",
                    double.decode(serde_json::json!({ "value": 6 })).unwrap(),
                )
                .await
                .expect("variant Dashboard call");
            assert_eq!(double.encode(reply).unwrap()["value"], 12);

            let hidden = task_ctx
                .request(
                    ".plugin-host",
                    Payload::of(CommandEnvelope::new(
                        VARIANT_COMMAND,
                        Payload::of(VariantCommand::Hidden(DoubleRequest { value: 1 })),
                    )),
                )
                .await;
            let rejected_without_running = hidden.is_err() && HIDDEN_CALLS.load(SeqCst) == 0;
            HIDDEN_CALL_RESULT.store(if rejected_without_running { 1 } else { 2 }, SeqCst);

            let unauthorized = task_ctx
                .request(
                    ".plugin-host",
                    Payload::of(CommandEnvelope::new(UNDECLARED_CAPABILITY, Payload::None)),
                )
                .await;
            assert!(unauthorized.is_err(), "未声明依赖不能取得 Capability");

            task_ctx
                .request(
                    ".plugin-host",
                    Payload::of(CommandEnvelope::new(EMIT_EVENT, Payload::None)),
                )
                .await
                .expect("广播命令应答");
            while EVENT_RESULT.load(SeqCst) == 0 {
                task_ctx.sleep(1).await;
            }
            task_ctx.abort();
        });
        Ok(())
    }
}

#[test]
fn plugins_register_call_and_broadcast_inside_one_service() {
    DIRECT_RESULT.store(0, SeqCst);
    EVENT_RESULT.store(0, SeqCst);
    HIDDEN_CALLS.store(0, SeqCst);
    HIDDEN_CALL_RESULT.store(0, SeqCst);

    let registry = Registry::new()
        .with("plugin-host", || HostService {
            plugins: PluginHost::from_auto("plugin-test").expect("自动插件注册表"),
        })
        .with("plugin-driver", Driver::default);
    let config = Config::default()
        .with_thread(2)
        .with_bootstrap(["plugin-host", "plugin-driver"]);

    rskynet::start(config, registry).expect("插件场景应正常完成");
    assert_eq!(DIRECT_RESULT.load(SeqCst), 7);
    assert_eq!(EVENT_RESULT.load(SeqCst), 11);
    assert_eq!(
        HIDDEN_CALL_RESULT.load(SeqCst),
        1,
        "无返回值 variant 的 call 必须立即失败且不能执行 handler",
    );
}
