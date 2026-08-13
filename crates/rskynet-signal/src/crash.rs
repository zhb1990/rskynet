//! 独立崩溃报告进程。

use std::backtrace::Backtrace;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use minidump::{Minidump, MinidumpModuleList, MinidumpSystemInfo, Module};
use minidump_unwind::{
    FileError, FileKind, FillSymbolError, FrameSymbolizer, FrameWalker, MultiSymbolProvider,
    SymbolProvider,
};
use minidump_unwind::{Symbolizer, debuginfo::DebugInfoSymbolProvider, simple_symbol_supplier};
use minidumper::{Client, LoopAction, MinidumpBinary, Server, ServerHandler, SocketName};
use rskynet_core::{Error, Result};
use serde::{Deserialize, Serialize};

const HELPER_ARG: &str = "--rskynet-crash-helper";
const PANIC_MESSAGE: u32 = 1;
static PANIC_REPORTING: AtomicBool = AtomicBool::new(false);
static INSTALLED: AtomicBool = AtomicBool::new(false);

type PanicHook = dyn for<'a, 'b> Fn(&'a std::panic::PanicHookInfo<'b>) + Send + Sync + 'static;

/// 保持崩溃处理器、IPC 客户端和 helper 进程存活。
pub struct CrashGuard {
    previous_hook: Arc<PanicHook>,
    handler: Option<Arc<crash_handler::CrashHandler>>,
    client: Option<Arc<Client>>,
    child: Option<Child>,
}

/// 安装独立崩溃报告进程。
///
/// 标准 `rskynet::main::run()` 会自动调用；自定义入口应在解析业务参数之前调用，
/// 并把返回的 guard 留到进程结束。
pub fn install() -> Result<CrashGuard> {
    if let Some((socket, directory, pid)) = helper_args()? {
        let code = match run_helper(&socket, &directory, pid) {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("rskynet crash helper: {err}");
                1
            }
        };
        std::process::exit(code);
    }

    if INSTALLED.swap(true, Ordering::AcqRel) {
        return Err(Error::service("崩溃处理器已经安装"));
    }
    match install_parent() {
        Ok(guard) => Ok(guard),
        Err(err) => {
            INSTALLED.store(false, Ordering::Release);
            Err(err)
        }
    }
}

fn install_parent() -> Result<CrashGuard> {
    let directory = std::env::current_dir()?.join("crash");
    std::fs::create_dir_all(&directory)?;
    if !directory.is_dir() {
        return Err(Error::service(format!(
            "崩溃报告路径不是目录：{}",
            directory.display()
        )));
    }

    let pid = std::process::id();
    let socket = socket_path(&directory, pid);
    let executable = std::env::current_exe()?;
    let mut child = Command::new(executable)
        .arg(HELPER_ARG)
        .arg(&socket)
        .arg(&directory)
        .arg(pid.to_string())
        .spawn()
        .map_err(|err| Error::service(format!("启动崩溃 helper 失败：{err}")))?;

    let client = match connect(&socket, &mut child) {
        Ok(client) => Arc::new(client),
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(err);
        }
    };

    let crash_client = Arc::clone(&client);
    let event = unsafe {
        // SAFETY: 回调只调用 minidumper 专为崩溃上下文设计的 request_dump；所有
        // 分配、文件 IO 和符号化均在 helper 进程中执行。
        crash_handler::make_crash_event(move |context| {
            crash_handler::CrashEventResult::Handled(crash_client.request_dump(context).is_ok())
        })
    };
    let handler = match crash_handler::CrashHandler::attach(event) {
        Ok(handler) => Arc::new(handler),
        Err(err) => {
            drop(client);
            stop_helper(&mut child);
            return Err(Error::service(format!("安装崩溃处理器失败：{err}")));
        }
    };

    #[cfg(any(target_os = "linux", target_os = "android"))]
    handler.set_ptracer(Some(child.id()));

    let previous: Arc<PanicHook> = Arc::from(std::panic::take_hook());
    let hook_previous = Arc::clone(&previous);
    let hook_handler = Arc::clone(&handler);
    let hook_client = Arc::clone(&client);
    std::panic::set_hook(Box::new(move |info| {
        hook_previous(info);
        if PANIC_REPORTING.swap(true, Ordering::AcqRel) {
            return;
        }
        let metadata = PanicMetadata::capture(info);
        if let Ok(bytes) = serde_json::to_vec(&metadata) {
            let _ = hook_client.send_message(PANIC_MESSAGE, bytes);
            let _ = hook_client.ping();
        }
        simulate_dump(&hook_handler);
        PANIC_REPORTING.store(false, Ordering::Release);
    }));

    Ok(CrashGuard {
        previous_hook: previous,
        handler: Some(handler),
        client: Some(client),
        child: Some(child),
    })
}

fn connect(socket: &Path, child: &mut Child) -> Result<Client> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(client) = Client::with_name(socket_name(socket)) {
            return Ok(client);
        }
        if let Some(status) = child.try_wait()? {
            return Err(Error::service(format!(
                "崩溃 helper 在 IPC 握手前退出：{status}"
            )));
        }
        if std::time::Instant::now() >= deadline {
            return Err(Error::service("等待崩溃 helper IPC 握手超时"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn socket_path(directory: &Path, pid: u32) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        // Linux 抽象 socket 无需清理文件，也规避 sockaddr_un 的短路径上限。
        return PathBuf::from(format!("rskynet-crash-{pid}-{}", unix_millis()));
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = directory;
        std::env::temp_dir().join(format!("rsc-{pid}-{:x}.sock", unix_millis()))
    }
}

fn helper_args() -> Result<Option<(PathBuf, PathBuf, u32)>> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new(HELPER_ARG)) {
        return Ok(None);
    }
    let socket = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::service("崩溃 helper 缺少 socket 参数"))?;
    let directory = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::service("崩溃 helper 缺少目录参数"))?;
    let pid = args
        .next()
        .and_then(|value| value.to_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| Error::service("崩溃 helper 的 pid 参数非法"))?;
    Ok(Some((socket, directory, pid)))
}

fn run_helper(socket: &Path, directory: &Path, pid: u32) -> Result<()> {
    std::fs::create_dir_all(directory)?;
    // Minidumps commonly store the main module and its PDB/DWARF companion as
    // relative names. Resolve those names beside this executable rather than
    // against the crashed application's working directory.
    if let Some(executable_directory) = std::env::current_exe()?.parent() {
        std::env::set_current_dir(executable_directory)?;
    }
    let mut server = Server::with_name(socket_name(socket))
        .map_err(|err| Error::service(format!("创建崩溃 helper IPC 失败：{err}")))?;
    let shutdown = AtomicBool::new(false);
    server
        .run(
            Box::new(ReportHandler::new(directory.to_path_buf(), pid)),
            &shutdown,
            Some(Duration::from_secs(30)),
        )
        .map_err(|err| Error::service(format!("崩溃 helper 运行失败：{err}")))
}

#[derive(Serialize, Deserialize)]
struct PanicMetadata {
    payload: String,
    location: Option<String>,
    thread: Option<String>,
    backtrace: String,
}

impl PanicMetadata {
    fn capture(info: &std::panic::PanicHookInfo<'_>) -> Self {
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .map(|value| (*value).to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "<非字符串 panic payload>".into());
        let location = info.location().map(|location| {
            format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        });
        let mut metadata = Self {
            payload,
            location,
            thread: std::thread::current().name().map(str::to_string),
            backtrace: Backtrace::force_capture().to_string(),
        };
        truncate_utf8(&mut metadata.payload, 8 * 1024);
        truncate_utf8(&mut metadata.backtrace, 48 * 1024);
        metadata
    }
}

fn truncate_utf8(text: &mut String, limit: usize) {
    if text.len() <= limit {
        return;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str("\n<truncated>");
}

fn simulate_dump(handler: &crash_handler::CrashHandler) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        let _ = handler.simulate_signal(libc::SIGALRM as u32);
    }
    #[cfg(windows)]
    {
        let _ = handler.simulate_exception(None);
    }
    #[cfg(target_os = "macos")]
    {
        let _ = handler.simulate_exception(None);
    }
}

struct ReportHandler {
    directory: PathBuf,
    pid: u32,
    metadata: Mutex<Option<PanicMetadata>>,
}

impl ReportHandler {
    fn new(directory: PathBuf, pid: u32) -> Self {
        Self {
            directory,
            pid,
            metadata: Mutex::new(None),
        }
    }

    fn create_file(&self) -> std::io::Result<(File, PathBuf)> {
        loop {
            let path = self
                .directory
                .join(format!("{}-{}.dmp", self.pid, unix_millis()));
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => return Ok((file, path)),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::yield_now();
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl ServerHandler for ReportHandler {
    fn create_minidump_file(&self) -> std::io::Result<(File, PathBuf)> {
        self.create_file()
    }

    fn on_minidump_created(
        &self,
        result: std::result::Result<MinidumpBinary, minidumper::Error>,
    ) -> LoopAction {
        match result {
            Ok(mut dump) => {
                let _ = dump.file.flush();
                let metadata = self
                    .metadata
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .take();
                if let Err(err) = write_report(&dump.path, metadata) {
                    eprintln!("rskynet crash helper: 写崩溃报告失败：{err}");
                }
            }
            Err(err) => eprintln!("rskynet crash helper: 生成 minidump 失败：{err}"),
        }
        LoopAction::Continue
    }

    fn on_message(&self, kind: u32, buffer: Vec<u8>) {
        if kind == PANIC_MESSAGE {
            if let Ok(metadata) = serde_json::from_slice(&buffer) {
                *self.metadata.lock().unwrap_or_else(|err| err.into_inner()) = Some(metadata);
            }
        }
    }

    fn on_client_disconnected(&self, clients: usize) -> LoopAction {
        if clients == 0 {
            LoopAction::Exit
        } else {
            LoopAction::Continue
        }
    }
}

fn write_report(dump_path: &Path, metadata: Option<PanicMetadata>) -> std::io::Result<()> {
    let mut report = Vec::new();
    write_report_preamble(&mut report, dump_path, metadata.as_ref())?;

    match Minidump::read_path(dump_path) {
        Ok(dump) => {
            let state = futures_executor::block_on(async {
                let system_info = dump.get_stream::<MinidumpSystemInfo>();
                let modules = dump.get_stream::<MinidumpModuleList>();
                if let (Ok(system_info), Ok(modules)) = (system_info, modules)
                    && matches!(
                        system_info.cpu,
                        minidump::system_info::Cpu::X86_64 | minidump::system_info::Cpu::Arm64
                    )
                {
                    let local_symbols = LocalDebugSymbolProvider::new(&modules);
                    let unwind = DebugInfoSymbolProvider::builder()
                        .build(&system_info, &modules)
                        .await;
                    let mut symbols = MultiSymbolProvider::new();
                    symbols.add(Box::new(local_symbols));
                    symbols.add(Box::new(unwind));
                    minidump_processor::process_minidump(&dump, &symbols).await
                } else {
                    let symbols = Symbolizer::new(simple_symbol_supplier(Vec::new()));
                    minidump_processor::process_minidump(&dump, &symbols).await
                }
            });
            match state {
                Ok(state) => {
                    if metadata.is_some() {
                        writeln!(
                            report,
                            "\nPost-panic minidump snapshot (captured after the panic hook; \
                             not the panic origin):"
                        )?;
                    } else {
                        writeln!(
                            report,
                            "\nNative crash minidump stackwalk (crashing thread first):"
                        )?;
                    }
                    state.print(&mut report)?;
                }
                Err(err) => writeln!(report, "\nMinidump stackwalk failed: {err}")?,
            }
        }
        Err(err) => writeln!(report, "\nMinidump parse failed: {err}")?,
    }

    let log_path = dump_path.with_extension("log");
    std::fs::write(&log_path, &report)?;
    eprintln!("{}", String::from_utf8_lossy(&report));
    Ok(())
}

struct LocalDebugSymbolProvider {
    dump_base: Option<u64>,
    local_base: Option<usize>,
}

impl LocalDebugSymbolProvider {
    fn new(modules: &MinidumpModuleList) -> Self {
        Self {
            dump_base: modules.main_module().map(Module::base_address),
            local_base: local_module_base(),
        }
    }
}

#[async_trait::async_trait]
impl SymbolProvider for LocalDebugSymbolProvider {
    async fn fill_symbol(
        &self,
        module: &(dyn Module + Sync),
        frame: &mut (dyn FrameSymbolizer + Send),
    ) -> std::result::Result<(), FillSymbolError> {
        let dump_base = self.dump_base.ok_or(FillSymbolError {})?;
        let local_base = self.local_base.ok_or(FillSymbolError {})?;
        if module.base_address() != dump_base {
            return Err(FillSymbolError {});
        }
        let relative_address: usize = frame
            .get_instruction()
            .checked_sub(dump_base)
            .and_then(|address| address.try_into().ok())
            .ok_or(FillSymbolError {})?;
        let local_address = (local_base + relative_address) as *mut std::ffi::c_void;
        let mut resolved = None;
        backtrace::resolve(local_address, |symbol| {
            if resolved.is_none() {
                resolved = Some((
                    symbol.name().map(|name| name.to_string()),
                    symbol.addr().map(|address| address as usize),
                    symbol.filename().map(Path::to_path_buf),
                    symbol.lineno(),
                ));
            }
        });
        let (name, symbol_address, filename, line) = resolved.ok_or(FillSymbolError {})?;
        let name = name.ok_or(FillSymbolError {})?;
        let function_base = symbol_address
            .and_then(|address| address.checked_sub(local_base))
            .map(|offset| dump_base + offset as u64)
            .unwrap_or(frame.get_instruction());
        frame.set_function(&name, function_base, 0);
        if let Some(filename) = filename {
            frame.set_source_file(
                filename.to_string_lossy().as_ref(),
                line.unwrap_or(0),
                function_base,
            );
        }
        Ok(())
    }

    async fn walk_frame(
        &self,
        _module: &(dyn Module + Sync),
        _walker: &mut (dyn FrameWalker + Send),
    ) -> Option<()> {
        None
    }

    async fn get_file_path(
        &self,
        _module: &(dyn Module + Sync),
        _file_kind: FileKind,
    ) -> std::result::Result<PathBuf, FileError> {
        Err(FileError::NotFound)
    }
}

#[cfg(windows)]
fn local_module_base() -> Option<usize> {
    let module = unsafe { GetModuleHandleW(std::ptr::null()) };
    (!module.is_null()).then_some(module as usize)
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetModuleHandleW(name: *const u16) -> *mut std::ffi::c_void;
}

#[cfg(unix)]
fn local_module_base() -> Option<usize> {
    unsafe {
        let mut info = std::mem::zeroed::<libc::Dl_info>();
        (libc::dladdr(
            local_module_base as *const () as *const libc::c_void,
            &mut info,
        ) != 0)
            .then_some(info.dli_fbase as usize)
    }
}

fn write_report_preamble(
    report: &mut Vec<u8>,
    dump_path: &Path,
    metadata: Option<&PanicMetadata>,
) -> std::io::Result<()> {
    writeln!(report, "rskynet crash report")?;
    writeln!(report, "minidump: {}", dump_path.display())?;
    if let Some(metadata) = metadata {
        writeln!(report, "kind: rust panic")?;
        writeln!(report, "payload: {}", metadata.payload)?;
        if let Some(location) = &metadata.location {
            writeln!(report, "location: {location}")?;
        }
        if let Some(thread) = &metadata.thread {
            writeln!(report, "thread: {thread}")?;
        }
        writeln!(
            report,
            "\nRust panic backtrace (captured by the panic hook):\n{}",
            metadata.backtrace
        )?;
    } else {
        writeln!(report, "kind: native crash")?;
    }
    Ok(())
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn stop_helper(child: &mut Child) {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn socket_name(path: &Path) -> SocketName<'_> {
    SocketName::abstract_namespace(path.to_str().expect("内部 socket 名必须是 UTF-8"))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn socket_name(path: &Path) -> SocketName<'_> {
    path.into()
}

impl Drop for CrashGuard {
    fn drop(&mut self) {
        // panic 展开期间 std 明确禁止 take_hook/set_hook。此时钩子自己还持有 handler
        // 与 client，进程退出会关闭 IPC，helper 随后自行收工；不要为了清理再制造
        // 第二次 panic，把原始崩溃变成 abort。
        if std::thread::panicking() {
            return;
        }
        let ours = std::panic::take_hook();
        let previous = Arc::clone(&self.previous_hook);
        std::panic::set_hook(Box::new(move |info| previous(info)));
        // 旧闭包持有 handler/client；先释放它，关闭 IPC 后 helper 才会自然退出。
        drop(ours);
        self.handler.take();
        self.client.take();
        if let Some(mut child) = self.child.take() {
            stop_helper(&mut child);
        }
        INSTALLED.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_metadata_truncation_keeps_utf8_valid() {
        let mut text = "崩".repeat(20_000);
        truncate_utf8(&mut text, 1024);
        assert!(text.is_char_boundary(text.len()));
        assert!(text.ends_with("<truncated>"));
        assert!(text.len() < 1100);
    }

    #[test]
    fn dump_name_contains_pid_and_millisecond_timestamp() {
        let handler = ReportHandler::new(std::env::temp_dir(), 1234);
        let (_file, path) = handler.create_file().expect("应能创建临时 dump");
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        std::fs::remove_file(&path).unwrap();
        assert!(name.starts_with("1234-"));
        assert!(name.ends_with(".dmp"));
        let stamp = name.trim_start_matches("1234-").trim_end_matches(".dmp");
        assert_eq!(stamp.len(), 13);
        assert!(stamp.parse::<u64>().is_ok());
    }

    #[test]
    fn panic_report_distinguishes_backtrace_from_later_snapshot() {
        let metadata = PanicMetadata {
            payload: "probe".into(),
            location: Some("src/main.rs:7:9".into()),
            thread: Some("main".into()),
            backtrace: "trigger_panic\ncaller".into(),
        };
        let mut report = Vec::new();
        write_report_preamble(&mut report, Path::new("probe.dmp"), Some(&metadata)).unwrap();
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("kind: rust panic"));
        assert!(report.contains("Rust panic backtrace (captured by the panic hook):"));
        assert!(report.contains("trigger_panic\ncaller"));
        assert!(!report.contains("kind: native crash"));
    }

    #[test]
    fn native_report_has_no_panic_metadata() {
        let mut report = Vec::new();
        write_report_preamble(&mut report, Path::new("probe.dmp"), None).unwrap();
        let report = String::from_utf8(report).unwrap();
        assert!(report.contains("kind: native crash"));
        assert!(!report.contains("Rust panic backtrace"));
    }
}
