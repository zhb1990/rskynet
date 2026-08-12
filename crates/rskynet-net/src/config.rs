//! `[net]` 段，对照 `socket_server.c` 顶上那几个编译期常量。
//!
//! C 版把 `MAX_SOCKET_P`、`MIN_READ_BUFFER`、`WARNING_SIZE` 写成宏，改一下要重新
//! 编译整个内核。这里它们是配置项，各服务实例可以不一样。

use serde::Deserialize;

use rskynet_core::{Error, Result};

/// 一次 `poll` 最多取回多少个就绪事件。
const DEFAULT_EVENTS: usize = 1024;
/// 槽位数，对照 C 版 `MAX_SOCKET`（`1 << MAX_SOCKET_P`，默认 65536）。
const DEFAULT_MAX_SOCKET: usize = 65536;
/// 读缓冲的下限，对照 C 版 `MIN_READ_BUFFER`。
const DEFAULT_MIN_READ_BUFFER: usize = 64;
/// 写缓冲堆到这么多字节就给属主报一次警，对照 C 版 `WARNING_SIZE`。
const DEFAULT_WARN_SIZE: usize = 1024 * 1024;

/// 网络层那一段配置。
///
/// ```toml
/// [net]
/// events = 1024
/// max_socket = 65536
/// min_read_buffer = 64
/// warn_size = 1048576
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NetConfig {
    /// 一次 `poll` 最多取回多少个事件。取小了只是多醒几次，不会丢事件。
    pub events: usize,
    /// 同时最多多少个连接。id 按 `id % max_socket` 落槽，所以这个数也决定了
    /// 同一个槽位被复用的间隔。
    pub max_socket: usize,
    /// 读缓冲的起始大小，之后按实际读到的量翻倍或减半。
    pub min_read_buffer: usize,
    /// 写缓冲堆到这么多字节就投一条 [`crate::SocketEvent::Warning`]，之后每翻倍
    /// 再报一次。属主据此决定是限流还是踢人。
    pub warn_size: usize,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            events: DEFAULT_EVENTS,
            max_socket: DEFAULT_MAX_SOCKET,
            min_read_buffer: DEFAULT_MIN_READ_BUFFER,
            warn_size: DEFAULT_WARN_SIZE,
        }
    }
}

impl NetConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.events == 0 {
            return Err(Error::Config("[net] events 必须大于 0".into()));
        }
        if self.max_socket == 0 {
            return Err(Error::Config("[net] max_socket 必须大于 0".into()));
        }
        if self.min_read_buffer == 0 {
            return Err(Error::Config("[net] min_read_buffer 必须大于 0".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 默认值要与 C 版那几个常量对得上
    #[test]
    fn defaults_match_the_c_constants() {
        let config = NetConfig::default();
        assert_eq!(config.max_socket, 65536);
        assert_eq!(config.min_read_buffer, 64);
        assert_eq!(config.warn_size, 1024 * 1024);
        config.validate().expect("默认值应当合法");
    }

    /// 零值都要在启动前拦下来，而不是留到运行期除零
    #[test]
    fn zero_values_are_rejected() {
        for broken in [
            NetConfig {
                events: 0,
                ..Default::default()
            },
            NetConfig {
                max_socket: 0,
                ..Default::default()
            },
            NetConfig {
                min_read_buffer: 0,
                ..Default::default()
            },
        ] {
            assert!(broken.validate().is_err());
        }
    }
}
