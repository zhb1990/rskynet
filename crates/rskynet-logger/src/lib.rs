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
//! ```

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use rskynet_core::service::LOGGER;
use rskynet_core::{Ctx, Message, MsgType, Registry, Result, SvcCell};
use serde::Deserialize;

/// `[logger]` 段。`name` 归内核解析，这里只关心写到哪。
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct LoggerConfig {
    /// 日志文件路径，留空表示只写标准输出。
    path: String,
}

#[derive(Default)]
pub struct Logger {
    /// 日志文件路径，空表示只写标准输出。
    path: SvcCell<String>,
    file: SvcCell<Option<File>>,
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
        // 时间戳用节点内相对时间（厘秒），免掉一个日期库依赖，排查问题也更直观
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
