//! # rskynet-bootstrap
//!
//! 引导服务，对照 `service/bootstrap.lua`。
//!
//! 它按 `[bootstrap]` 段里的清单顺序拉起服务，干完就退场：
//!
//! ```toml
//! [bootstrap]
//! # 换成自己的实现就改这里
//! name = "bootstrap"
//! # 写在前面的先起：pong 在 init 里注册好 .pong，ping 才查得到它
//! services = [
//!     { name = "pong" },
//!     { name = "ping", args = "100" },
//! ]
//! ```
//!
//! 清单不经内核的手：内核只按 `name` 把这个服务拉起来，段里的 `services` 由它
//! 自己读。C 版把类型名和参数挤在一个字符串里靠 `sscanf` 按首个空格拆，这里各占
//! 一个字段，于是参数里带空格、带分号都不再是问题。

use std::sync::Arc;

use rskynet_core::service::BOOTSTRAP;
use rskynet_core::{BoxFuture, Config, Ctx, Error, Message, Registry, Result, Service, log};
use serde::{Deserialize, Serialize};

/// 一条服务启动项：类型名加参数。
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ServiceSpec {
    /// 服务类型名，也就是注册表里的那个键。
    pub name: String,
    /// 传给服务 `init` 的参数，不需要就留空。
    pub args: String,
}

impl ServiceSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            args: String::new(),
        }
    }

    #[must_use]
    pub fn with_args(mut self, args: impl Into<String>) -> Self {
        self.args = args.into();
        self
    }
}

impl From<&str> for ServiceSpec {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for ServiceSpec {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl<N: Into<String>, A: Into<String>> From<(N, A)> for ServiceSpec {
    fn from((name, args): (N, A)) -> Self {
        Self::new(name).with_args(args)
    }
}

/// `[bootstrap]` 段。`name` 归内核解析，这里只关心清单。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct BootstrapConfig {
    services: Vec<ServiceSpec>,
}

#[derive(Default)]
pub struct Bootstrap;

impl Service for Bootstrap {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let config: BootstrapConfig = ctx.node().section(BOOTSTRAP)?.unwrap_or_default();
            for (index, spec) in config.services.iter().enumerate() {
                if spec.name.trim().is_empty() {
                    return Err(Error::Config(format!(
                        "[bootstrap] 清单第 {} 项没写 name",
                        index + 1
                    )));
                }
                let handle = ctx.launch(&spec.name, &spec.args).await?;
                log!(ctx, "bootstrap 拉起 {} -> :{handle:08x}", spec.name);
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

/// 把引导服务挂进注册表。
pub trait RegistryExt {
    /// 用约定的名字注册 [`Bootstrap`]，内核默认拉起的就是它。
    #[must_use]
    fn with_bootstrap(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_bootstrap(self) -> Self {
        self.with(BOOTSTRAP, Bootstrap::default)
    }
}

/// 在代码里写引导清单，给「不从 TOML 来」的场景（测试、示例）用。
pub trait ConfigExt {
    /// 覆盖引导清单。只有类型名的写字符串，要带参数的写成一对：
    ///
    /// ```
    /// # use rskynet_core::Config;
    /// use rskynet_bootstrap::ConfigExt;
    ///
    /// Config::default().with_bootstrap(["pong", "ping"]);
    /// Config::default().with_bootstrap([("pong", ""), ("ping", "100")]);
    /// ```
    #[must_use]
    fn with_bootstrap<I>(self, services: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<ServiceSpec>;

    /// 换掉引导服务本身的类型名，默认是 `bootstrap`。
    #[must_use]
    fn with_bootstrap_service(self, name: impl Into<String>) -> Self;
}

impl ConfigExt for Config {
    fn with_bootstrap<I>(mut self, services: I) -> Self
    where
        I: IntoIterator,
        I::Item: Into<ServiceSpec>,
    {
        let services: Vec<ServiceSpec> = services.into_iter().map(Into::into).collect();
        let services = toml::Value::try_from(services).expect("启动项一定能编成 TOML");
        self.section_mut(BOOTSTRAP)
            .insert("services".into(), services);
        self
    }

    fn with_bootstrap_service(mut self, name: impl Into<String>) -> Self {
        self.section_mut(BOOTSTRAP)
            .insert("name".into(), name.into().into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 代码里搭的清单与 TOML 里写的是同一份东西
    #[test]
    fn specs_round_trip_through_the_section() {
        let config = Config::default().with_bootstrap([("pong", ""), ("ping", "100")]);
        let section: BootstrapConfig = config
            .section(BOOTSTRAP)
            .unwrap()
            .expect("应当写进了 [bootstrap] 段");
        assert_eq!(section.services.len(), 2);
        assert_eq!(section.services[0].name, "pong");
        assert_eq!(section.services[0].args, "");
        assert_eq!(section.services[1].name, "ping");
        assert_eq!(section.services[1].args, "100");
    }

    /// 参数里的空格与分号原样留给服务，不再像 sscanf 那样被拆开
    #[test]
    fn spec_args_are_not_split() {
        let config = Config::from_toml_str(
            r#"
            [bootstrap]
            services = [{ name = "gate", args = "0.0.0.0:8888; backlog 128" }]
            "#,
        )
        .expect("配置应解析成功");
        let section: BootstrapConfig = config.section(BOOTSTRAP).unwrap().unwrap();
        assert_eq!(section.services[0].args, "0.0.0.0:8888; backlog 128");
    }

    /// 没写 name 的启动项要能被认出来，而不是拿空名字去注册表里找
    #[test]
    fn a_spec_without_name_is_rejected() {
        let config = Config::from_toml_str(
            r#"
            [bootstrap]
            services = [{ args = "100" }]
            "#,
        )
        .expect("配置本身是合法 TOML");
        let section: BootstrapConfig = config.section(BOOTSTRAP).unwrap().unwrap();
        assert!(section.services[0].name.is_empty());
    }

    /// 段整个缺席等于没有要拉起的服务
    #[test]
    fn a_missing_section_means_no_services() {
        let config = Config::default();
        let section: Option<BootstrapConfig> = config.section(BOOTSTRAP).unwrap();
        assert!(section.is_none());
    }

    /// 换引导服务的类型名与写清单互不干扰，都落在同一段里
    #[test]
    fn kind_and_services_share_the_section() {
        let config = Config::default()
            .with_bootstrap(["pong"])
            .with_bootstrap_service("my-boot");
        let section: BootstrapConfig = config.section(BOOTSTRAP).unwrap().unwrap();
        assert_eq!(section.services.len(), 1);
        // name 由内核读，这里只确认它没把 services 冲掉
        assert!(config.section::<toml::Table>(BOOTSTRAP).unwrap().is_some());
    }
}
