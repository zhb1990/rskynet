//! 系统服务的约定名字。
//!
//! 实现一个都不在内核里：日志是 `rskynet-logger`、定时器是 `rskynet-timer`、
//! 引导是 `rskynet-bootstrap`，各自是一个独立 crate，接入方式与网络层没两样
//! ——用 [`crate::Registry`] 注册类型，在配置里占一段。
//!
//! 内核留着这三个名字是因为它得知道「默认拉起谁」：`[logger]` / `[timer]` /
//! `[bootstrap]` 三段里若没写 `name`，就按这里的常量去注册表里找。三个常量同时
//! 也是这些服务注册到 handle 表里的名字，于是别的服务可以直接 `.logger`、
//! `.timer` 地寻址。

/// 日志服务的注册名。
pub const LOGGER: &str = "logger";
/// 定时器服务的注册名。
pub const TIMER: &str = "timer";
/// 引导服务的注册名。
pub const BOOTSTRAP: &str = "bootstrap";
