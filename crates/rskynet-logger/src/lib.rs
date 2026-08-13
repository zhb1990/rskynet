//! # rskynet-logger
//!
//! 日志服务，对照 `service-src/service_logger.c`。
//!
//! 内核的 `Ctx::log` 并不直接写文件，而是把日志当成一条 `TEXT` 消息发给本服务
//! ——这样写日志就是一次投递，不会在业务线程上做 IO。
//!
//! 它是个[独占线程服务][rskynet_core::Exclusive]：写文件是阻塞 IO，让它占着共享
//! worker 不合适。两个钩子都走默认实现，也就是「没日志可写就阻塞在 park 上」。
//!
//! ## 配置
//!
//! ```toml
//! [logger]
//! # 换成自己的实现就改这里，内核按它去注册表里找
//! name = "logger"
//! # 日志文件路径，留空则只写标准输出
//! path = "run/rskynet.log"
//! # 最多保留多少条待写日志；超过后丢弃较旧日志，0 表示不限制
//! max_queue = 10000
//! ```

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use rskynet_core::service::LOGGER;
use rskynet_core::{Ctx, Message, MsgType, Registry, Result, SvcCell};
use serde::Deserialize;

const DEFAULT_MAX_QUEUE: usize = 10_000;

/// `[logger]` 段。`name` 归内核解析，这里只关心写到哪。
#[derive(Debug, Deserialize)]
#[serde(default)]
struct LoggerConfig {
    /// 日志文件路径，留空表示只写标准输出。
    path: String,
    /// 最多保留多少条待写日志，0 表示不限制。
    max_queue: usize,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            max_queue: DEFAULT_MAX_QUEUE,
        }
    }
}

#[derive(Debug)]
struct Backpressure {
    max_queue: usize,
    dropped: usize,
}

impl Default for Backpressure {
    fn default() -> Self {
        Self {
            max_queue: DEFAULT_MAX_QUEUE,
            dropped: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum LogDecision {
    Drop,
    Write { dropped: usize },
}

impl Backpressure {
    /// `queued` 不含当前这条已经出队的消息，因此 `queued >= max_queue`
    /// 正好表示出队前的总量超过限制。
    fn decide(&mut self, queued: usize) -> LogDecision {
        if self.max_queue != 0 && queued >= self.max_queue {
            self.dropped = self.dropped.saturating_add(1);
            return LogDecision::Drop;
        }

        LogDecision::Write {
            dropped: std::mem::take(&mut self.dropped),
        }
    }
}

#[derive(Default)]
pub struct Logger {
    /// 日志文件路径，空表示只写标准输出。
    path: SvcCell<String>,
    file: SvcCell<Option<File>>,
    backpressure: SvcCell<Backpressure>,
}

impl Logger {
    fn open(&self, path: &str) -> Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        *self.file.borrow_mut() = Some(file);
        Ok(())
    }

    fn write(&self, ctx: &Ctx, source: u32, text: &str) {
        // 时间戳用节点内相对时间（毫秒），免掉一个日期库依赖，排查问题也更直观
        let centis = ctx.now();
        let line = format!(
            "[{:>6}.{:02}] [:{:08x}] {}",
            centis / 100,
            centis % 100,
            source,
            text
        );
        println!("{line}");
        if let Some(file) = self.file.borrow_mut().as_mut() {
            let _ = writeln!(file, "{line}");
        }
    }
}

#[rskynet_macros::exclusive(crate = ::rskynet_core, name = "logger")]
impl Logger {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let config: LoggerConfig = ctx.node().section(LOGGER)?.unwrap_or_default();
        self.backpressure.borrow_mut().max_queue = config.max_queue;
        let path = config.path.trim().to_string();
        if !path.is_empty() {
            self.open(&path)?;
            *self.path.borrow_mut() = path;
        }
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, msg: Message) {
        match msg.mtype {
            MsgType::TEXT => {
                let decision = self.backpressure.borrow_mut().decide(ctx.mailbox_len());
                let LogDecision::Write { dropped } = decision else {
                    return;
                };
                if dropped != 0 {
                    self.write(
                        &ctx,
                        ctx.handle(),
                        &format!("日志队列积压，已丢弃 {dropped} 条较旧日志"),
                    );
                }
                self.write(
                    &ctx,
                    msg.source,
                    msg.payload.as_str().unwrap_or("<非法日志>"),
                );
            }
            // 对照 C 版收到 SIGHUP 后重开日志文件的做法
            MsgType::SYSTEM => {
                let path = self.path.borrow().clone();
                if !path.is_empty() {
                    *self.file.borrow_mut() = None;
                    if let Err(err) = self.open(&path) {
                        self.write(&ctx, ctx.handle(), &format!("重开日志文件失败：{err}"));
                    }
                }
            }
            _ => {}
        }
    }
}

/// 把日志服务挂进注册表。
pub trait RegistryExt {
    /// 用约定的名字注册 [`Logger`]，内核默认拉起的就是它。
    #[must_use]
    fn with_logger(self) -> Self;
}

impl RegistryExt for Registry {
    fn with_logger(self) -> Self {
        self.with_exclusive(LOGGER, Logger::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_uses_the_default_limit() {
        let config: LoggerConfig = toml::from_str("").unwrap();
        assert_eq!(config.max_queue, DEFAULT_MAX_QUEUE);

        let config: LoggerConfig = toml::from_str("max_queue = 0").unwrap();
        assert_eq!(config.max_queue, 0);

        let config: LoggerConfig = toml::from_str("max_queue = 42").unwrap();
        assert_eq!(config.max_queue, 42);
    }

    #[test]
    fn overload_drops_old_logs_and_reports_once_after_recovery() {
        let mut state = Backpressure {
            max_queue: 3,
            dropped: 0,
        };

        assert_eq!(state.decide(5), LogDecision::Drop);
        assert_eq!(state.decide(4), LogDecision::Drop);
        assert_eq!(state.decide(3), LogDecision::Drop);
        assert_eq!(state.decide(2), LogDecision::Write { dropped: 3 });
        assert_eq!(state.decide(1), LogDecision::Write { dropped: 0 });

        assert_eq!(state.decide(3), LogDecision::Drop);
        assert_eq!(state.decide(2), LogDecision::Write { dropped: 1 });
    }

    #[test]
    fn zero_limit_disables_dropping() {
        let mut state = Backpressure {
            max_queue: 0,
            dropped: 0,
        };

        assert_eq!(state.decide(usize::MAX), LogDecision::Write { dropped: 0 });
    }

    #[test]
    fn dropped_counter_saturates() {
        let mut state = Backpressure {
            max_queue: 1,
            dropped: usize::MAX,
        };

        assert_eq!(state.decide(1), LogDecision::Drop);
        assert_eq!(state.dropped, usize::MAX);
    }
}
