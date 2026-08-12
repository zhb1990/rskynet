//! 内置服务。
//!
//! 对照 skynet 的 `service-src/service_logger.c` 与 `service/bootstrap.lua`，
//! 外加一个定时器——C 版那是内核里的一条专用线程，这里它也是个服务，
//! 只不过独占一条线程，见 [`crate::Exclusive`]。

mod bootstrap;
mod logger;
mod timer;

pub use bootstrap::Bootstrap;
pub use logger::Logger;
pub use timer::Timer;

/// 日志服务的注册名。
pub const LOGGER: &str = "logger";
/// 定时器服务的注册名。
pub const TIMER: &str = "timer";
/// 引导服务的注册名。
pub const BOOTSTRAP: &str = "bootstrap";
