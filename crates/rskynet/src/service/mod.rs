//! 内置服务。
//!
//! 对照 skynet 的 `service-src/service_logger.c` 与 `service/bootstrap.lua`。

mod bootstrap;
mod logger;

pub use bootstrap::Bootstrap;
pub use logger::Logger;

/// 日志服务的注册名。
pub const LOGGER: &str = "logger";
/// 引导服务的注册名。
pub const BOOTSTRAP: &str = "bootstrap";
