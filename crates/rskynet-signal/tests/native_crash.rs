use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RUN_ENV: &str = "RSKYNET_RUN_NATIVE_CRASH_TEST";
const KEEP_ENV: &str = "RSKYNET_KEEP_NATIVE_CRASH_TEST_DIR";
const CHILD_ENV: &str = "RSKYNET_NATIVE_CRASH_CHILD";
const HELPER_ARG: &str = "--rskynet-crash-helper";

fn main() {
    if std::env::var_os(CHILD_ENV).is_some()
        || std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new(HELPER_ARG))
    {
        run_crashing_process();
    }

    if std::env::var_os(RUN_ENV).as_deref() != Some(std::ffi::OsStr::new("1")) {
        eprintln!("native crash test skipped; set {RUN_ENV}=1 to run it");
        return;
    }

    run_parent_test();
}

fn run_crashing_process() -> ! {
    #[cfg(windows)]
    unsafe {
        // Suppress the Windows Error Reporting dialog in unattended test runs.
        let _ = SetErrorMode(SEM_NOGPFAULTERRORBOX);
    }

    let _guard = rskynet_signal::crash::install().expect("应能安装崩溃处理器");
    unsafe { trigger_native_crash() }
}

fn run_parent_test() {
    let root = unique_temp_directory();
    std::fs::create_dir_all(&root).expect("应能创建原生崩溃测试目录");
    let stderr_path = root.join("child.stderr");
    let stderr = std::fs::File::create(&stderr_path).expect("应能创建子进程诊断文件");

    let mut child = Command::new(std::env::current_exe().expect("应能获取测试可执行文件"))
        .env(CHILD_ENV, "1")
        .current_dir(&root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("应能启动原生崩溃子进程");

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        match child.try_wait().expect("应能查询原生崩溃子进程") {
            Some(status) => break status,
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            None => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("原生崩溃子进程在 20 秒内没有退出");
            }
        }
    };
    assert!(!status.success(), "原生崩溃子进程必须异常退出");

    let crash_dir = root.join("crash");
    let log_path = wait_for_report(
        &crash_dir,
        Instant::now() + Duration::from_secs(60),
        &stderr_path,
    );
    let dump_path = log_path.with_extension("dmp");
    assert!(dump_path.is_file(), "崩溃报告必须包含同名 minidump");

    let report = std::fs::read_to_string(&log_path).expect("原生崩溃日志应为 UTF-8");
    assert!(report.contains("kind: native crash"));
    assert!(report.contains("Native crash minidump stackwalk (crashing thread first):"));
    assert!(
        report.contains("trigger_native_crash"),
        "崩溃线程应符号化到真实触发函数，报告如下：\n{report}"
    );
    assert!(!report.contains("Rust panic backtrace"));
    assert!(!report.contains("Post-panic minidump snapshot"));

    if std::env::var_os(KEEP_ENV).as_deref() == Some(std::ffi::OsStr::new("1")) {
        eprintln!("native crash test artifacts retained at {}", root.display());
    } else {
        std::fs::remove_dir_all(&root).expect("应能清理原生崩溃测试目录");
    }
}

fn wait_for_report(crash_dir: &Path, deadline: Instant, stderr_path: &Path) -> PathBuf {
    loop {
        if let Ok(entries) = std::fs::read_dir(crash_dir) {
            if let Some(path) = entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .find(|path| path.extension().and_then(|value| value.to_str()) == Some("log"))
            {
                return path;
            }
        }
        if Instant::now() >= deadline {
            let diagnostic = std::fs::read_to_string(stderr_path).unwrap_or_default();
            panic!("崩溃 helper 在超时前没有生成文本报告；子进程输出：\n{diagnostic}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_directory() -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rskynet-signal 应位于 workspace 的 crates 目录下")
        .join("crash")
        .join(format!(
            "rskynet-native-crash-{}-{stamp}",
            std::process::id()
        ))
}

#[cfg(unix)]
#[inline(never)]
unsafe fn trigger_native_crash() -> ! {
    let page = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    assert_ne!(page, libc::MAP_FAILED, "应能创建不可访问的内存页");
    let _ = unsafe { std::ptr::read_volatile(page.cast::<u8>()) };
    std::process::abort()
}

#[cfg(windows)]
#[inline(never)]
unsafe fn trigger_native_crash() -> ! {
    let page = unsafe {
        VirtualAlloc(
            std::ptr::null_mut(),
            4096,
            MEM_RESERVE | MEM_COMMIT,
            PAGE_NOACCESS,
        )
    };
    assert!(!page.is_null(), "应能创建不可访问的内存页");
    let _ = unsafe { std::ptr::read_volatile(page.cast::<u8>()) };
    std::process::abort()
}

#[cfg(windows)]
const MEM_COMMIT: u32 = 0x0000_1000;
#[cfg(windows)]
const MEM_RESERVE: u32 = 0x0000_2000;
#[cfg(windows)]
const PAGE_NOACCESS: u32 = 0x01;
#[cfg(windows)]
const SEM_NOGPFAULTERRORBOX: u32 = 0x0002;

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn VirtualAlloc(
        address: *mut std::ffi::c_void,
        size: usize,
        allocation_type: u32,
        protect: u32,
    ) -> *mut std::ffi::c_void;
    fn SetErrorMode(mode: u32) -> u32;
}
