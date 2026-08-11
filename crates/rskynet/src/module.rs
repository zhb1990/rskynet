//! 服务类型注册表，对照 `skynet-src/skynet_module.c`。
//!
//! skynet 从 `cpath` 里 `dlopen` 加载 `.so` 模块，Rust 走静态链接，
//! 所以改成启动前把「类型名 -> 构造函数」注册进表里，`launch` 时按名字取用。

use std::collections::HashMap;
use std::sync::Arc;

use crate::context::Service;

/// 服务构造函数。每次 `launch` 都会调用一次，产出一个全新的服务实例。
pub(crate) type ServiceFactory = Arc<dyn Fn() -> Arc<dyn Service> + Send + Sync>;

/// 服务类型表。
#[derive(Clone, Default)]
pub struct Registry {
    factories: HashMap<String, ServiceFactory>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一个服务类型。
    ///
    /// ```ignore
    /// let mut registry = Registry::new();
    /// registry.register("echo", Echo::default);
    /// ```
    pub fn register<S, F>(&mut self, kind: impl Into<String>, factory: F) -> &mut Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Service,
    {
        let factory: ServiceFactory = Arc::new(move || Arc::new(factory()) as Arc<dyn Service>);
        self.factories.insert(kind.into(), factory);
        self
    }

    /// 链式写法，方便一口气搭出注册表。
    #[must_use]
    pub fn with<S, F>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Service,
    {
        self.register(kind, factory);
        self
    }

    /// 挂上内置服务：`logger` 与 `bootstrap`。
    #[must_use]
    pub fn with_builtins(self) -> Self {
        self.with(crate::service::LOGGER, crate::service::Logger::default)
            .with(crate::service::BOOTSTRAP, crate::service::Bootstrap::default)
    }

    pub fn contains(&self, kind: &str) -> bool {
        self.factories.contains_key(kind)
    }

    pub fn kinds(&self) -> Vec<&str> {
        let mut kinds: Vec<&str> = self.factories.keys().map(String::as_str).collect();
        kinds.sort_unstable();
        kinds
    }

    pub(crate) fn get(&self, kind: &str) -> Option<ServiceFactory> {
        self.factories.get(kind).cloned()
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("kinds", &self.kinds())
            .finish()
    }
}
