//! Service 内的组合式插件。
//!
//! 插件仍然静态链接进最终程序；[`PluginRegistry::from_auto`] 收集描述符，
//! [`PluginHost`] 再按配置为每一个 service 实例分别创建插件对象。插件之间需要
//! 返回值的调用走类型化 Capability，不需要返回值的通知走同 service 邮箱上的
//! typed EventBus。

use std::any::{Any, TypeId, type_name};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use rskynet_core::{
    BoxFuture, Ctx, DebugMessageDescriptor, Error, Message, Payload, Result, SvcCell,
};
use serde::{Deserialize, Serialize};

/// 一个 namespace 内的业务命令号。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandId(pub u32);

/// 投给插件宿主的业务消息。
#[derive(Debug)]
pub struct CommandEnvelope {
    pub command: CommandId,
    pub payload: Payload,
}

impl CommandEnvelope {
    pub fn new(command: CommandId, payload: impl Into<Payload>) -> Self {
        Self {
            command,
            payload: payload.into(),
        }
    }
}

/// Service 内插件的生命周期与命令入口。
pub trait ServicePlugin: Send + Sync + 'static {
    /// 当前插件显式开放给 Dashboard 的命令。插件宿主会在装配成功后动态挂到
    /// 当前 service；默认不公开任何命令。
    fn debug_messages(&self) -> Vec<DebugMessageDescriptor> {
        Vec::new()
    }

    /// 同步装配阶段。这里只注册 Capability 与事件订阅，不做 IO。
    fn mount(self: Arc<Self>, mount: &mut PluginMount<'_>) -> Result<()> {
        let _ = mount;
        Ok(())
    }

    /// 异步初始化。依赖插件已经完成装配，可以通过 `ctx.require` 取得 Capability。
    fn init(
        self: Arc<Self>,
        ctx: PluginCtx,
        config: toml::Value,
    ) -> BoxFuture<'static, Result<()>> {
        let _ = (ctx, config);
        Box::pin(async { Ok(()) })
    }

    /// 处理该插件在描述符中声明的命令。
    ///
    /// 返回错误时宿主会记录日志；若原消息需要应答，宿主会发送错误应答。因此插件
    /// 返回 `Err` 之前不能已经自行应答。
    fn handle(
        self: Arc<Self>,
        ctx: PluginCtx,
        command: CommandId,
        msg: Message,
    ) -> BoxFuture<'static, Result<()>> {
        let _ = (self, ctx, msg);
        Box::pin(async move {
            Err(service_error(format!(
                "插件没有实现命令 {} 的处理函数",
                command.0,
            )))
        })
    }
}

pub type PluginFactory = fn() -> Arc<dyn ServicePlugin>;

/// 链接期插件描述符。名称与命令号只要求在同一个 namespace 内唯一。
#[derive(Clone, Copy)]
pub struct PluginDescriptor {
    pub namespace: &'static str,
    pub name: &'static str,
    pub dependencies: &'static [&'static str],
    pub commands: &'static [CommandId],
    pub factory: PluginFactory,
    pub source: &'static str,
}

impl PluginDescriptor {
    pub const fn new(
        namespace: &'static str,
        name: &'static str,
        dependencies: &'static [&'static str],
        commands: &'static [CommandId],
        factory: PluginFactory,
        source: &'static str,
    ) -> Self {
        Self {
            namespace,
            name,
            dependencies,
            commands,
            factory,
            source,
        }
    }
}

impl std::fmt::Debug for PluginDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginDescriptor")
            .field("namespace", &self.namespace)
            .field("name", &self.name)
            .field("dependencies", &self.dependencies)
            .field("commands", &self.commands)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

inventory::collect!(PluginDescriptor);

/// 提交一个静态链接插件。
#[macro_export]
macro_rules! register_service_plugin {
    (
        namespace: $namespace:literal,
        name: $name:literal,
        plugin: $plugin:ty,
        factory: $factory:path,
        dependencies: [$($dependency:literal),* $(,)?],
        commands: [$($command:expr),* $(,)?] $(,)?
    ) => {
        const _: () = {
            fn __rskynet_plugin_create() -> ::std::sync::Arc<dyn $crate::ServicePlugin> {
                let plugin: $plugin = $factory();
                ::std::sync::Arc::new(plugin)
            }

            $crate::__private::inventory::submit! {
                $crate::PluginDescriptor::new(
                    $namespace,
                    $name,
                    &[$($dependency),*],
                    &[$($command),*],
                    __rskynet_plugin_create,
                    ::core::concat!(::core::module_path!(), "::", ::core::stringify!($plugin)),
                )
            }
        };
    };
}

/// 当前二进制里可用的插件类型表。
#[derive(Clone, Default)]
pub struct PluginRegistry {
    descriptors: HashMap<(String, String), PluginDescriptor>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 收集所有链接进当前二进制的插件。
    pub fn from_auto() -> Result<Self> {
        let mut descriptors: Vec<_> = inventory::iter::<PluginDescriptor>
            .into_iter()
            .copied()
            .collect();
        descriptors.sort_unstable_by_key(|item| (item.namespace, item.name, item.source));
        Self::from_descriptors(descriptors)
    }

    pub fn from_descriptors(
        descriptors: impl IntoIterator<Item = PluginDescriptor>,
    ) -> Result<Self> {
        let mut registry = Self::new();
        for descriptor in descriptors {
            registry.register(descriptor)?;
        }
        Ok(registry)
    }

    pub fn register(&mut self, descriptor: PluginDescriptor) -> Result<&mut Self> {
        validate_identifier("namespace", descriptor.namespace)?;
        validate_identifier("插件名", descriptor.name)?;
        let key = (descriptor.namespace.to_owned(), descriptor.name.to_owned());
        if let Some(previous) = self.descriptors.get(&key) {
            return Err(config_error(format!(
                "namespace `{}` 的插件 `{}` 重复注册：{} 与 {}",
                descriptor.namespace, descriptor.name, previous.source, descriptor.source,
            )));
        }
        self.descriptors.insert(key, descriptor);
        Ok(self)
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<&PluginDescriptor> {
        self.descriptors
            .get(&(namespace.to_owned(), name.to_owned()))
    }

    pub fn plugins(&self, namespace: &str) -> Vec<&str> {
        let mut names: Vec<_> = self
            .descriptors
            .values()
            .filter(|item| item.namespace == namespace)
            .map(|item| item.name)
            .collect();
        names.sort_unstable();
        names
    }
}

impl std::fmt::Debug for PluginRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut plugins: Vec<_> = self.descriptors.keys().collect();
        plugins.sort_unstable();
        f.debug_struct("PluginRegistry")
            .field("plugins", &plugins)
            .finish()
    }
}

/// 配置中的一条插件启用项。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PluginSpec {
    pub name: String,
    pub config: toml::Value,
}

impl Default for PluginSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            config: toml::Value::Table(toml::Table::new()),
        }
    }
}

impl PluginSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: toml::Value) -> Self {
        self.config = config;
        self
    }
}

/// 可直接作为某个 service 配置段的插件部分。
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct PluginHostConfig {
    pub plugins: Vec<PluginSpec>,
}

struct LoadedPlugin {
    descriptor: PluginDescriptor,
    plugin: Arc<dyn ServicePlugin>,
    config: toml::Value,
}

type AnyCapability = Box<dyn Any + Send + Sync>;
type AnyEvent = Arc<dyn Any + Send + Sync>;
type EventHandler =
    Arc<dyn Fn(PluginCtx, AnyEvent) -> BoxFuture<'static, Result<()>> + Send + Sync>;

struct CapabilityEntry {
    provider: Arc<str>,
    type_name: &'static str,
    value: AnyCapability,
}

struct Subscriber {
    plugin: Arc<str>,
    handler: EventHandler,
}

#[derive(Default)]
struct BuildState {
    capabilities: HashMap<TypeId, CapabilityEntry>,
    subscribers: HashMap<TypeId, Vec<Subscriber>>,
    subscription_keys: HashSet<(String, TypeId)>,
}

/// 插件同步装配阶段的注册入口。
pub struct PluginMount<'a> {
    plugin: &'a str,
    state: &'a mut BuildState,
}

impl PluginMount<'_> {
    /// 发布一个可克隆的类型化 Capability。
    pub fn provide<T>(&mut self, capability: T) -> Result<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        let id = TypeId::of::<T>();
        if let Some(previous) = self.state.capabilities.get(&id) {
            return Err(config_error(format!(
                "Capability `{}` 被插件 `{}` 与 `{}` 重复提供",
                type_name::<T>(),
                previous.provider,
                self.plugin,
            )));
        }
        self.state.capabilities.insert(
            id,
            CapabilityEntry {
                provider: Arc::from(self.plugin),
                type_name: type_name::<T>(),
                value: Box::new(capability),
            },
        );
        Ok(())
    }

    /// 订阅一种不可变事件。闭包通常捕获 `Arc<Self>`。
    pub fn subscribe<E, F, Fut>(&mut self, handler: F) -> Result<()>
    where
        E: Send + Sync + 'static,
        F: Fn(PluginCtx, Arc<E>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let event = TypeId::of::<E>();
        let key = (self.plugin.to_owned(), event);
        if !self.state.subscription_keys.insert(key) {
            return Err(config_error(format!(
                "插件 `{}` 重复订阅事件 `{}`",
                self.plugin,
                type_name::<E>(),
            )));
        }
        let erased: EventHandler = Arc::new(move |ctx, event| {
            let event = event
                .downcast::<E>()
                .expect("EventBus 的 TypeId 与负载类型必须一致");
            Box::pin(handler(ctx, event))
        });
        self.state
            .subscribers
            .entry(event)
            .or_default()
            .push(Subscriber {
                plugin: Arc::from(self.plugin),
                handler: erased,
            });
        Ok(())
    }
}

struct Runtime {
    namespace: Arc<str>,
    routes: HashMap<CommandId, Arc<dyn ServicePlugin>>,
    plugin_names: HashMap<CommandId, Arc<str>>,
    dependencies: HashMap<Arc<str>, HashSet<Arc<str>>>,
    capabilities: HashMap<TypeId, CapabilityEntry>,
    subscribers: HashMap<TypeId, Vec<Subscriber>>,
}

/// 当前插件的 service 本地上下文。
#[derive(Clone)]
pub struct PluginCtx {
    service: Ctx,
    plugin: Arc<str>,
    runtime: Arc<Runtime>,
}

impl PluginCtx {
    fn new(service: Ctx, plugin: Arc<str>, runtime: Arc<Runtime>) -> Self {
        Self {
            service,
            plugin,
            runtime,
        }
    }

    pub fn service(&self) -> &Ctx {
        &self.service
    }

    pub fn namespace(&self) -> &str {
        &self.runtime.namespace
    }

    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// 取得当前插件自己或已声明依赖提供的 Capability。
    pub fn require<T>(&self) -> Result<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let Some(entry) = self.runtime.capabilities.get(&TypeId::of::<T>()) else {
            return Err(service_error(format!(
                "插件 `{}` 请求了未提供的 Capability `{}`",
                self.plugin,
                type_name::<T>(),
            )));
        };
        let allowed = entry.provider == self.plugin
            || self
                .runtime
                .dependencies
                .get(&self.plugin)
                .is_some_and(|items| items.contains(&entry.provider));
        if !allowed {
            return Err(service_error(format!(
                "插件 `{}` 未声明依赖 `{}`，不能取得 Capability `{}`",
                self.plugin, entry.provider, entry.type_name,
            )));
        }
        Ok(entry
            .value
            .downcast_ref::<T>()
            .expect("Capability 的 TypeId 与保存类型必须一致")
            .clone())
    }

    /// 把 typed event 异步投递到当前 service 自己的邮箱。
    pub fn emit<E>(&self, event: E) -> Result<()>
    where
        E: Send + Sync + 'static,
    {
        self.service.post(
            self.service.handle(),
            Payload::of(EventEnvelope {
                event_type: TypeId::of::<E>(),
                event_name: type_name::<E>(),
                payload: Arc::new(event),
            }),
        )
    }
}

impl std::fmt::Debug for PluginCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginCtx")
            .field("namespace", &self.runtime.namespace)
            .field("plugin", &self.plugin)
            .field("service", &format_args!(":{:08x}", self.service.handle()))
            .finish()
    }
}

struct EventEnvelope {
    event_type: TypeId,
    event_name: &'static str,
    payload: AnyEvent,
}

/// 一个 service 实例内的插件宿主。相同类型的多个 service 各自持有独立宿主，
/// 因而也各自创建独立插件实例与状态。
pub struct PluginHost {
    namespace: String,
    registry: PluginRegistry,
    runtime: SvcCell<Option<Arc<Runtime>>>,
}

impl PluginHost {
    pub fn new(namespace: impl Into<String>, registry: PluginRegistry) -> Result<Self> {
        let namespace = namespace.into();
        validate_identifier("namespace", &namespace)?;
        Ok(Self {
            namespace,
            registry,
            runtime: SvcCell::new(None),
        })
    }

    pub fn from_auto(namespace: impl Into<String>) -> Result<Self> {
        Self::new(namespace, PluginRegistry::from_auto()?)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn is_initialized(&self) -> bool {
        self.runtime.borrow().is_some()
    }

    /// 从当前节点的指定配置段读取 `plugins` 并初始化。
    pub async fn init_from_section(&self, ctx: Ctx, section: &str) -> Result<()> {
        let config = ctx
            .node()
            .section::<PluginHostConfig>(section)?
            .unwrap_or_default();
        self.init(ctx, config).await
    }

    /// 为这个 service 实例创建并初始化配置中启用的插件。
    pub async fn init(&self, ctx: Ctx, config: PluginHostConfig) -> Result<()> {
        if self.is_initialized() {
            return Err(service_error(format!(
                "namespace `{}` 已经初始化，不能重复装配插件",
                self.namespace,
            )));
        }

        let ordered = self.resolve(&config)?;
        let mut loaded = Vec::with_capacity(ordered.len());
        let mut routes = HashMap::new();
        let mut plugin_names = HashMap::new();
        let mut dependencies = HashMap::new();

        for (descriptor, plugin_config) in ordered {
            let plugin = (descriptor.factory)();
            let name: Arc<str> = Arc::from(descriptor.name);
            dependencies.insert(
                name.clone(),
                descriptor
                    .dependencies
                    .iter()
                    .map(|item| Arc::<str>::from(*item))
                    .collect(),
            );
            for command in descriptor.commands {
                if let Some(previous) = plugin_names.insert(*command, name.clone()) {
                    return Err(config_error(format!(
                        "namespace `{}` 的命令 {} 被插件 `{}` 与 `{}` 重复声明",
                        self.namespace, command.0, previous, descriptor.name,
                    )));
                }
                routes.insert(*command, plugin.clone());
            }
            loaded.push(LoadedPlugin {
                descriptor,
                plugin,
                config: plugin_config,
            });
        }

        let mut state = BuildState::default();
        for item in &loaded {
            let mut mount = PluginMount {
                plugin: item.descriptor.name,
                state: &mut state,
            };
            item.plugin.clone().mount(&mut mount)?;
        }

        let runtime = Arc::new(Runtime {
            namespace: Arc::from(self.namespace.as_str()),
            routes,
            plugin_names,
            dependencies,
            capabilities: state.capabilities,
            subscribers: state.subscribers,
        });

        let mut debug_messages = Vec::new();
        for item in loaded {
            let plugin_ctx = PluginCtx::new(
                ctx.clone(),
                Arc::from(item.descriptor.name),
                runtime.clone(),
            );
            item.plugin.clone().init(plugin_ctx, item.config).await?;
            debug_messages.extend(item.plugin.debug_messages());
        }

        ctx.register_debug_messages(debug_messages)?;

        *self.runtime.borrow_mut() = Some(runtime);
        Ok(())
    }

    /// 把 service 的消息交给内部事件总线或命令路由器。
    pub async fn dispatch(&self, ctx: Ctx, mut msg: Message) {
        let runtime = self.runtime.borrow().clone();
        let Some(runtime) = runtime else {
            ctx.log(format!("plugin namespace `{}` 尚未初始化", self.namespace));
            let _ = ctx.reply_error(&msg);
            return;
        };

        let payload = msg.take_payload();
        let payload = match payload.downcast::<EventEnvelope>() {
            Ok(event) => {
                Self::dispatch_event(ctx, runtime, *event);
                return;
            }
            Err(payload) => payload,
        };
        let command = match payload.downcast::<CommandEnvelope>() {
            Ok(command) => *command,
            Err(payload) => {
                msg.payload = payload;
                ctx.log(format!(
                    "plugin namespace `{}` 收到未封装为 CommandEnvelope 的消息",
                    self.namespace,
                ));
                let _ = ctx.reply_error(&msg);
                return;
            }
        };
        msg.payload = command.payload;

        let Some(plugin) = runtime.routes.get(&command.command).cloned() else {
            ctx.log(format!(
                "plugin namespace `{}` 收到未知命令 {}",
                self.namespace, command.command.0,
            ));
            let _ = ctx.reply_error(&msg);
            return;
        };
        let name = runtime
            .plugin_names
            .get(&command.command)
            .expect("命令路由与插件名必须成对建立")
            .clone();
        let error_request = Message::new(msg.source, msg.session, msg.mtype, Payload::None);
        let plugin_ctx = PluginCtx::new(ctx.clone(), name.clone(), runtime);
        if let Err(err) = plugin.handle(plugin_ctx, command.command, msg).await {
            ctx.log(format!("插件 `{name}` 处理命令失败：{err}"));
            let _ = ctx.reply_error(&error_request);
        }
    }

    fn dispatch_event(ctx: Ctx, runtime: Arc<Runtime>, event: EventEnvelope) {
        let Some(subscribers) = runtime.subscribers.get(&event.event_type) else {
            return;
        };
        for subscriber in subscribers {
            let plugin = subscriber.plugin.clone();
            let handler = subscriber.handler.clone();
            let payload = event.payload.clone();
            let event_name = event.event_name;
            let task_ctx = ctx.clone();
            let plugin_ctx = PluginCtx::new(ctx.clone(), plugin.clone(), runtime.clone());
            ctx.spawn(async move {
                if let Err(err) = handler(plugin_ctx, payload).await {
                    task_ctx.log(format!(
                        "插件 `{plugin}` 处理事件 `{event_name}` 失败：{err}",
                    ));
                }
            });
        }
    }

    fn resolve(&self, config: &PluginHostConfig) -> Result<Vec<(PluginDescriptor, toml::Value)>> {
        let mut specs = HashMap::new();
        let mut input_order = Vec::new();
        for (index, spec) in config.plugins.iter().enumerate() {
            validate_identifier(&format!("插件配置第 {} 项的 name", index + 1), &spec.name)?;
            if specs.insert(spec.name.as_str(), spec).is_some() {
                return Err(config_error(format!(
                    "namespace `{}` 的插件 `{}` 在配置中出现多次",
                    self.namespace, spec.name,
                )));
            }
            input_order.push(spec.name.as_str());
        }

        let mut descriptors = HashMap::new();
        for name in &input_order {
            let Some(descriptor) = self.registry.get(&self.namespace, name) else {
                return Err(config_error(format!(
                    "namespace `{}` 未注册插件 `{}`；已注册：{:?}",
                    self.namespace,
                    name,
                    self.registry.plugins(&self.namespace),
                )));
            };
            descriptors.insert(*name, *descriptor);
        }
        for descriptor in descriptors.values() {
            for dependency in descriptor.dependencies {
                if !descriptors.contains_key(dependency) {
                    return Err(config_error(format!(
                        "namespace `{}` 的插件 `{}` 依赖未启用的插件 `{}`",
                        self.namespace, descriptor.name, dependency,
                    )));
                }
            }
        }

        let mut commands = HashMap::new();
        for descriptor in descriptors.values() {
            for command in descriptor.commands {
                if let Some(previous) = commands.insert(*command, descriptor.name) {
                    return Err(config_error(format!(
                        "namespace `{}` 的命令 {} 被插件 `{}` 与 `{}` 重复声明",
                        self.namespace, command.0, previous, descriptor.name,
                    )));
                }
            }
        }

        let mut states = HashMap::new();
        let mut ordered = Vec::new();
        for name in input_order {
            visit_plugin(
                name,
                &descriptors,
                &mut states,
                &mut ordered,
                &self.namespace,
            )?;
        }
        Ok(ordered
            .into_iter()
            .map(|name| {
                let descriptor = descriptors[name];
                let config = specs[name].config.clone();
                (descriptor, config)
            })
            .collect())
    }
}

impl std::fmt::Debug for PluginHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginHost")
            .field("namespace", &self.namespace)
            .field("initialized", &self.is_initialized())
            .finish()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    Visiting,
    Done,
}

fn visit_plugin<'a>(
    name: &'a str,
    descriptors: &HashMap<&'a str, PluginDescriptor>,
    states: &mut HashMap<&'a str, Visit>,
    ordered: &mut Vec<&'a str>,
    namespace: &str,
) -> Result<()> {
    match states.get(name) {
        Some(Visit::Done) => return Ok(()),
        Some(Visit::Visiting) => {
            return Err(config_error(format!(
                "namespace `{namespace}` 的插件依赖存在环，回到 `{name}`",
            )));
        }
        None => {}
    }
    states.insert(name, Visit::Visiting);
    for dependency in descriptors[name].dependencies {
        visit_plugin(dependency, descriptors, states, ordered, namespace)?;
    }
    states.insert(name, Visit::Done);
    ordered.push(name);
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(config_error(format!("{label} 不能为空")));
    }
    Ok(())
}

fn config_error(message: impl Into<String>) -> Error {
    Error::Config(message.into())
}

fn service_error(message: impl Into<String>) -> Error {
    Error::service(message)
}

/// 宏展开所需的实现细节。
#[doc(hidden)]
pub mod __private {
    pub use inventory;
    pub use toml::Value as TomlValue;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NullPlugin;

    impl ServicePlugin for NullPlugin {
        fn handle(
            self: Arc<Self>,
            _ctx: PluginCtx,
            _command: CommandId,
            _msg: Message,
        ) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn null() -> Arc<dyn ServicePlugin> {
        Arc::new(NullPlugin)
    }

    fn descriptor(
        namespace: &'static str,
        name: &'static str,
        dependencies: &'static [&'static str],
    ) -> PluginDescriptor {
        PluginDescriptor::new(namespace, name, dependencies, &[], null, name)
    }

    fn descriptor_with_commands(
        namespace: &'static str,
        name: &'static str,
        commands: &'static [CommandId],
    ) -> PluginDescriptor {
        PluginDescriptor::new(namespace, name, &[], commands, null, name)
    }

    #[test]
    fn names_are_unique_only_inside_one_namespace() {
        let registry = PluginRegistry::from_descriptors([
            descriptor("alpha", "shared", &[]),
            descriptor("beta", "shared", &[]),
        ])
        .expect("不同 namespace 可以复用插件名");
        assert_eq!(registry.plugins("alpha"), vec!["shared"]);
        assert_eq!(registry.plugins("beta"), vec!["shared"]);

        let err = PluginRegistry::from_descriptors([
            descriptor("alpha", "shared", &[]),
            descriptor("alpha", "shared", &[]),
        ])
        .expect_err("同 namespace 同名必须失败");
        assert!(err.to_string().contains("重复注册"));
    }

    #[test]
    fn dependencies_are_topologically_ordered() {
        let registry = PluginRegistry::from_descriptors([
            descriptor("namespace", "leaf", &["middle"]),
            descriptor("namespace", "root", &[]),
            descriptor("namespace", "middle", &["root"]),
        ])
        .unwrap();
        let namespace = PluginHost::new("namespace", registry).unwrap();
        let config = PluginHostConfig {
            plugins: vec![
                PluginSpec::new("leaf"),
                PluginSpec::new("root"),
                PluginSpec::new("middle"),
            ],
        };
        let names: Vec<_> = namespace
            .resolve(&config)
            .unwrap()
            .into_iter()
            .map(|(item, _)| item.name)
            .collect();
        assert_eq!(names, ["root", "middle", "leaf"]);
    }

    #[test]
    fn missing_and_cyclic_dependencies_are_rejected() {
        let registry =
            PluginRegistry::from_descriptors([descriptor("namespace", "dependent", &["missing"])])
                .unwrap();
        let namespace = PluginHost::new("namespace", registry).unwrap();
        let err = namespace
            .resolve(&PluginHostConfig {
                plugins: vec![PluginSpec::new("dependent")],
            })
            .expect_err("缺失依赖必须失败");
        assert!(err.to_string().contains("未启用"));

        let registry = PluginRegistry::from_descriptors([
            descriptor("namespace", "left", &["right"]),
            descriptor("namespace", "right", &["left"]),
        ])
        .unwrap();
        let namespace = PluginHost::new("namespace", registry).unwrap();
        let err = namespace
            .resolve(&PluginHostConfig {
                plugins: vec![PluginSpec::new("left"), PluginSpec::new("right")],
            })
            .expect_err("依赖环必须失败");
        assert!(err.to_string().contains("依赖存在环"));
    }

    #[test]
    fn plugin_config_deserializes_inline_plugin_config() {
        let config: PluginHostConfig = toml::from_str(
            r#"
            plugins = [
                { name = "cache", config = { capacity = 1024 } },
            ]
            "#,
        )
        .unwrap();
        assert_eq!(config.plugins[0].name, "cache");
        assert_eq!(
            config.plugins[0].config["capacity"].as_integer(),
            Some(1024)
        );
    }

    #[test]
    fn duplicate_commands_capabilities_and_subscriptions_are_rejected() {
        const COMMANDS: &[CommandId] = &[CommandId(9)];
        let registry = PluginRegistry::from_descriptors([
            descriptor_with_commands("namespace", "left", COMMANDS),
            descriptor_with_commands("namespace", "right", COMMANDS),
        ])
        .unwrap();
        let namespace = PluginHost::new("namespace", registry).unwrap();
        let err = namespace
            .resolve(&PluginHostConfig {
                plugins: vec![PluginSpec::new("left"), PluginSpec::new("right")],
            })
            .expect_err("命令冲突必须失败");
        assert!(err.to_string().contains("重复声明"));

        let mut state = BuildState::default();
        PluginMount {
            plugin: "left",
            state: &mut state,
        }
        .provide(1u64)
        .unwrap();
        let err = PluginMount {
            plugin: "right",
            state: &mut state,
        }
        .provide(2u64)
        .expect_err("Capability 冲突必须失败");
        assert!(err.to_string().contains("重复提供"));

        let mut mount = PluginMount {
            plugin: "left",
            state: &mut state,
        };
        mount
            .subscribe(|_ctx, _event: Arc<u32>| async { Ok(()) })
            .unwrap();
        let err = mount
            .subscribe(|_ctx, _event: Arc<u32>| async { Ok(()) })
            .expect_err("同插件重复订阅必须失败");
        assert!(err.to_string().contains("重复订阅"));
    }
}
