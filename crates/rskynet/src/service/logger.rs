//! 日志服务，对照 `service-src/service_logger.c`。
//!
//! 内核的 `Ctx::log` 并不直接写文件，而是把日志当成一条 `TEXT` 消息发给本服务
//! ——这样写日志就是一次投递，不会在业务线程上做 IO。

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::context::{Ctx, Service};
use crate::error::Result;
use crate::message::{Message, MsgType};
use crate::task::SvcCell;

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

impl Service for Logger {
    fn init(self: Arc<Self>, _ctx: Ctx, args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            let path = args.trim().to_string();
            if !path.is_empty() {
                self.open(&path)?;
                *self.path.borrow_mut() = path;
            }
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match msg.mtype {
                MsgType::TEXT => {
                    self.write(&ctx, msg.source, msg.payload.as_str().unwrap_or("<非法日志>"));
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
        })
    }
}
