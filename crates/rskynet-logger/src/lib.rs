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
//! # 日志文件基础路径，留空则只写标准输出；实际文件名会带时间与序号
//! path = "run/rskynet.log"
//! # 本地日期变化时滚动日志文件
//! rotate_daily = true
//! # 单个活动日志文件最大字节数；0 表示不按大小滚动
//! max_file_size = 104857600
//! # 最多保留多少条待写日志；超过后丢弃较旧日志，0 表示不限制
//! max_queue = 10000
//! ```

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};
use rskynet_core::service::LOGGER;
use rskynet_core::{Ctx, Message, MsgType, Registry, Result, SvcCell};
use serde::Deserialize;

const DEFAULT_MAX_QUEUE: usize = 10_000;
const DEFAULT_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// `[logger]` 段。`name` 归内核解析，这里只关心写到哪。
#[derive(Debug, Deserialize)]
#[serde(default)]
struct LoggerConfig {
    /// 日志文件基础路径，留空表示只写标准输出。
    path: String,
    /// 本地日期变化时是否滚动日志文件。
    rotate_daily: bool,
    /// 单个活动日志文件的最大字节数，0 表示不按大小滚动。
    max_file_size: u64,
    /// 最多保留多少条待写日志，0 表示不限制。
    max_queue: usize,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            rotate_daily: true,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_queue: DEFAULT_MAX_QUEUE,
        }
    }
}

#[derive(Debug)]
struct LogFile {
    file: File,
    path: PathBuf,
    date: NaiveDate,
    len: u64,
}

impl LogFile {
    fn open(path: &Path, date: NaiveDate) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let len = file.metadata()?.len();
        Ok(Self {
            file,
            path: path.to_owned(),
            date,
            len,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct RotationConfig {
    rotate_daily: bool,
    max_file_size: u64,
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self {
            rotate_daily: true,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        }
    }
}

fn should_rotate(
    file: &LogFile,
    now: NaiveDate,
    next_line_len: u64,
    config: RotationConfig,
) -> bool {
    (config.rotate_daily && file.date != now)
        || (config.max_file_size != 0
            && file.len != 0
            && file.len.saturating_add(next_line_len) > config.max_file_size)
}

fn log_path(path: &Path, timestamp: NaiveDateTime, sequence: u64) -> PathBuf {
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let stem = path
        .file_stem()
        .filter(|stem| !stem.is_empty())
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_else(|| file_name.into_owned());
    let mut archive_name = format!("{stem}.{}.{sequence:03}", timestamp.format("%Y%m%d.%H%M%S"));
    if let Some(extension) = path.extension().filter(|extension| !extension.is_empty()) {
        let _ = write!(archive_name, ".{}", extension.to_string_lossy());
    }
    path.with_file_name(archive_name)
}

fn next_log_path(path: &Path, timestamp: NaiveDateTime) -> Result<PathBuf> {
    for sequence in 1.. {
        let candidate = log_path(path, timestamp, sequence);
        if !candidate.try_exists()? {
            return Ok(candidate);
        }
    }
    unreachable!("u64 序号不可能耗尽")
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
    /// 日志文件基础路径，空表示只写标准输出。
    path: SvcCell<String>,
    file: SvcCell<Option<LogFile>>,
    rotation: SvcCell<RotationConfig>,
    backpressure: SvcCell<Backpressure>,
}

impl Logger {
    fn open_new(&self, path: &Path, now: DateTime<Local>) -> Result<()> {
        let path = next_log_path(path, now.naive_local())?;
        let file = LogFile::open(&path, now.date_naive())?;
        *self.file.borrow_mut() = Some(file);
        Ok(())
    }

    fn rotate(&self, path: &Path, now: DateTime<Local>) -> Result<()> {
        // 每个活动文件从创建起就带时间和序号；打开新文件失败时保留旧句柄继续写入。
        self.open_new(path, now)
    }

    fn reopen(&self, base_path: &Path, now: DateTime<Local>) -> Result<()> {
        let active_path = self.file.borrow().as_ref().map(|file| file.path.clone());
        let Some(active_path) = active_path else {
            return self.open_new(base_path, now);
        };
        let file = LogFile::open(&active_path, now.date_naive())?;
        *self.file.borrow_mut() = Some(file);
        Ok(())
    }

    fn rotate_if_needed(&self, now: DateTime<Local>, next_line_len: u64) -> Result<()> {
        let needs_rotation = {
            let file = self.file.borrow();
            let config = *self.rotation.borrow();
            file.as_ref()
                .is_some_and(|file| should_rotate(file, now.date_naive(), next_line_len, config))
        };
        if needs_rotation {
            let path = self.path.borrow().clone();
            self.rotate(Path::new(&path), now)?;
        }
        Ok(())
    }

    fn write_unrotated(&self, line: &str) {
        if let Some(file) = self.file.borrow_mut().as_mut() {
            if writeln!(file.file, "{line}").is_ok() {
                file.len = file.len.saturating_add(line.len() as u64 + 1);
            }
        }
    }

    fn write(&self, ctx: &Ctx, source: rskynet_core::Handle, text: &str) {
        // 时间戳用节点内相对时间（毫秒），免掉一个日期库依赖，排查问题也更直观
        let elapsed_ms = ctx.now();
        let line = format!(
            "[{:>6}.{:03}] [:{:08x}] {}",
            elapsed_ms / 1_000,
            elapsed_ms % 1_000,
            source,
            text
        );
        println!("{line}");
        let now = Local::now();
        if let Err(error) = self.rotate_if_needed(now, line.len() as u64 + 1) {
            let error_line = format!(
                "[{:>6}.{:03}] [:{:08x}] 日志文件滚动失败：{error}",
                elapsed_ms / 1_000,
                elapsed_ms % 1_000,
                ctx.handle(),
            );
            println!("{error_line}");
            self.write_unrotated(&error_line);
        }
        self.write_unrotated(&line);
    }
}

#[rskynet_macros::exclusive(crate = ::rskynet_core, name = "logger")]
impl Logger {
    async fn init(&self, ctx: Ctx) -> Result<()> {
        let config: LoggerConfig = ctx.node().section(LOGGER)?.unwrap_or_default();
        self.backpressure.borrow_mut().max_queue = config.max_queue;
        *self.rotation.borrow_mut() = RotationConfig {
            rotate_daily: config.rotate_daily,
            max_file_size: config.max_file_size,
        };
        let path = config.path.trim().to_string();
        if !path.is_empty() {
            self.open_new(Path::new(&path), Local::now())?;
            *self.path.borrow_mut() = path;
        }
        Ok(())
    }

    async fn dispatch(&self, ctx: Ctx, msg: Message) {
        match msg.mtype {
            MsgType::TEXT => {
                let queued = ctx.node().mailbox_len(ctx.handle()).unwrap_or(0);
                let decision = self.backpressure.borrow_mut().decide(queued);
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
                    if let Err(err) = self.reopen(Path::new(&path), Local::now()) {
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
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::TimeZone;

    static NEXT_TEST_DIR: AtomicUsize = AtomicUsize::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("rskynet-logger-test-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn date(year: i32, month: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(year, month, day).unwrap()
    }

    fn local_datetime(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> DateTime<Local> {
        Local
            .from_local_datetime(
                &date(year, month, day)
                    .and_hms_opt(hour, minute, second)
                    .unwrap(),
            )
            .single()
            .unwrap()
    }

    #[test]
    fn config_uses_the_default_limit() {
        let config: LoggerConfig = toml::from_str("").unwrap();
        assert_eq!(config.max_queue, DEFAULT_MAX_QUEUE);
        assert!(config.rotate_daily);
        assert_eq!(config.max_file_size, DEFAULT_MAX_FILE_SIZE);

        let config: LoggerConfig =
            toml::from_str("max_queue = 0\nrotate_daily = false\nmax_file_size = 0").unwrap();
        assert_eq!(config.max_queue, 0);
        assert!(!config.rotate_daily);
        assert_eq!(config.max_file_size, 0);

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

    #[test]
    fn rotation_decision_handles_date_size_and_disabled_triggers() {
        let dir = TestDir::new();
        let path = dir.path().join("rskynet.log");
        let mut file = LogFile::open(&path, date(2026, 8, 22)).unwrap();

        let size_only = RotationConfig {
            rotate_daily: false,
            max_file_size: 10,
        };
        file.len = 9;
        assert!(
            !should_rotate(&file, date(2026, 8, 22), 1, size_only),
            "刚好达到阈值不滚动"
        );
        assert!(
            should_rotate(&file, date(2026, 8, 22), 2, size_only),
            "下一条会超过阈值时滚动"
        );
        file.len = 0;
        assert!(
            !should_rotate(&file, date(2026, 8, 22), 11, size_only),
            "空文件允许写入一条超阈值日志"
        );

        let daily_only = RotationConfig {
            rotate_daily: true,
            max_file_size: 0,
        };
        assert!(!should_rotate(&file, date(2026, 8, 22), 1, daily_only));
        assert!(should_rotate(&file, date(2026, 8, 23), 1, daily_only));

        let disabled = RotationConfig {
            rotate_daily: false,
            max_file_size: 0,
        };
        assert!(!should_rotate(&file, date(2026, 8, 23), u64::MAX, disabled));
    }

    #[test]
    fn log_names_include_timestamp_and_never_overwrite() {
        let dir = TestDir::new();
        let path = dir.path().join("rskynet.log");
        let timestamp = date(2026, 8, 22).and_hms_opt(15, 30, 45).unwrap();

        let first = next_log_path(&path, timestamp).unwrap();
        assert_eq!(
            first.file_name().unwrap().to_string_lossy(),
            "rskynet.20260822.153045.001.log"
        );
        fs::write(&first, []).unwrap();
        let second = next_log_path(&path, timestamp).unwrap();
        assert_eq!(
            second.file_name().unwrap().to_string_lossy(),
            "rskynet.20260822.153045.002.log"
        );
    }

    #[test]
    fn opening_existing_file_tracks_size() {
        let dir = TestDir::new();
        let path = dir.path().join("rskynet.log");
        fs::write(&path, "before\n").unwrap();

        let opened = LogFile::open(&path, date(2026, 8, 22)).unwrap();
        assert_eq!(opened.len, 7);
    }

    #[test]
    fn new_log_files_are_timestamped_from_start_and_after_rotation() {
        let dir = TestDir::new();
        let path = dir.path().join("rskynet.log");
        let now = local_datetime(2026, 8, 22, 15, 30, 45);

        let logger = Logger::default();
        logger.open_new(&path, now).unwrap();
        *logger.path.borrow_mut() = path.to_string_lossy().into_owned();
        logger.write_unrotated("first");
        logger.rotate(&path, now).unwrap();
        logger.write_unrotated("second");

        assert!(!path.exists(), "基础路径本身不应被创建为活动日志");
        assert_eq!(
            fs::read_to_string(dir.path().join("rskynet.20260822.153045.001.log")).unwrap(),
            "first\n"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("rskynet.20260822.153045.002.log")).unwrap(),
            "second\n"
        );
    }
}
