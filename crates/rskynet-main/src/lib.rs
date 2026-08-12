//! rskynet 的可选标准命令行启动器。
//!
//! 最终应用只需链接包含自动注册服务的 crate，再从 `main` 调用 [`run`]。

use std::ffi::OsString;
use std::process::ExitCode;

use rskynet::{Config, Error, Registry, Result};

/// 使用进程参数启动节点。要求且只接受一个 TOML 配置路径。
pub fn run() -> ExitCode {
    match run_from(std::env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("rskynet: {err}");
            ExitCode::FAILURE
        }
    }
}

/// 可测试、可嵌入的参数入口。第一项按程序名处理。
pub fn run_from<I>(args: I) -> Result<()>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let program = args
        .next()
        .unwrap_or_else(|| OsString::from("rskynet-main"));
    let Some(path) = args.next() else {
        return Err(usage(&program));
    };
    if args.next().is_some() {
        return Err(usage(&program));
    }

    let config = Config::from_toml_file(path)?;
    let registry = Registry::from_auto()?;
    rskynet::start(config, registry)
}

fn usage(program: &OsString) -> Error {
    Error::Config(format!(
        "用法：{} <config.toml>",
        std::path::Path::new(program).display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_config_path_reports_usage() {
        let err = run_from([OsString::from("node")]).expect_err("缺参数应失败");
        assert!(err.to_string().contains("用法：node <config.toml>"));
    }

    #[test]
    fn extra_arguments_report_usage() {
        let err = run_from([
            OsString::from("node"),
            OsString::from("one.toml"),
            OsString::from("two.toml"),
        ])
        .expect_err("多余参数应失败");
        assert!(err.to_string().contains("用法：node <config.toml>"));
    }
}
