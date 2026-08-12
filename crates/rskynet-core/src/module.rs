//! 服务类型注册表，对照 `skynet-src/skynet_module.c`。
//!
//! skynet 从 `cpath` 里 `dlopen` 加载 `.so` 模块，Rust 走静态链接，
//! 所以改成启动前把「类型名 -> 构造函数」注册进表里，`launch` 时按名字取用。
//!
//! 内核不预置任何服务类型，连日志与定时器都要自己挂：它们住在各自的 crate 里，
//! 由门面 crate `rskynet` 的 `with_builtins()` 按 feature 一次挂齐。

use std::collections::HashMap;
use std::sync::Arc;

use crate::context::Service;
use crate::exclusive::Exclusive;
use crate::{Error, Result};

/// 一个新造出来的服务实例。
///
/// 独占服务的两个字段指的是同一个对象，只是分别取了 [`Service`] 与
/// [`Exclusive`] 两副面孔——内核平时按前者调 `dispatch`，起线程时按后者调
/// `idle`。`exclusive` 为 `None` 就是普通服务，跑在共享 worker 池上。
pub(crate) struct Instance {
    pub(crate) service: Arc<dyn Service>,
    pub(crate) exclusive: Option<Arc<dyn Exclusive>>,
}

/// 服务构造函数。每次 `launch` 都会调用一次，产出一个全新的服务实例。
pub(crate) type ServiceFactory = Arc<dyn Fn() -> Instance + Send + Sync>;

/// 由 `#[service(name = "...")]` / `#[exclusive(name = "...")]` 提交的链接期服务描述。
///
/// 通常不需要手工构造；使用 [`Registry::from_auto`] 收集当前二进制中全部描述。
pub struct AutoService {
    pub name: &'static str,
    pub exclusive: bool,
    pub source: &'static str,
    register: fn(&mut Registry),
}

impl AutoService {
    #[doc(hidden)]
    pub const fn new(
        name: &'static str,
        exclusive: bool,
        source: &'static str,
        register: fn(&mut Registry),
    ) -> Self {
        Self {
            name,
            exclusive,
            source,
            register,
        }
    }
}

inventory::collect!(AutoService);

/// 服务类型表。
#[derive(Clone, Default)]
pub struct Registry {
    factories: HashMap<String, ServiceFactory>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 收集所有已经链接进当前二进制、且通过服务宏显式命名的服务类型。
    ///
    /// 名字重复时拒绝构造；链接顺序不会决定哪个实现胜出。
    pub fn from_auto() -> Result<Self> {
        let mut services: Vec<&'static AutoService> =
            inventory::iter::<AutoService>.into_iter().collect();
        services.sort_unstable_by_key(|service| (service.name, service.source));
        for pair in services.windows(2) {
            if pair[0].name == pair[1].name {
                return Err(Error::Config(format!(
                    "自动注册的服务类型 `{}` 重复：{} 与 {}",
                    pair[0].name, pair[0].source, pair[1].source
                )));
            }
        }

        let mut registry = Self::new();
        for service in services {
            (service.register)(&mut registry);
        }
        Ok(registry)
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
        let factory: ServiceFactory = Arc::new(move || Instance {
            service: Arc::new(factory()),
            exclusive: None,
        });
        self.factories.insert(kind.into(), factory);
        self
    }

    /// 注册一个独占线程的服务类型，见 [`Exclusive`]。
    ///
    /// 与 [`Registry::register`] 的唯一区别是：每 `launch` 一次就新起一条线程，
    /// 那条线程只跑这一个服务。日志、定时器、网络层用的都是这条路。
    pub fn register_exclusive<S, F>(&mut self, kind: impl Into<String>, factory: F) -> &mut Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Exclusive,
    {
        let factory: ServiceFactory = Arc::new(move || {
            let service = Arc::new(factory());
            Instance {
                service: service.clone(),
                exclusive: Some(service),
            }
        });
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

    /// [`Registry::register_exclusive`] 的链式写法。
    #[must_use]
    pub fn with_exclusive<S, F>(mut self, kind: impl Into<String>, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Exclusive,
    {
        self.register_exclusive(kind, factory);
        self
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
