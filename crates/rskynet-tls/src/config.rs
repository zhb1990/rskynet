use serde::Deserialize;

use rskynet_core::{Error, Result};

const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_BUFFER_LIMIT: usize = 256 * 1024;
const DEFAULT_MAX_CONNECTIONS: usize = 65_536;
const DEFAULT_MAX_HANDSHAKES: usize = 4_096;

/// TLS 服务的运行限制。证书、私钥和信任策略属于每次 `listen` / `connect` 的参数，
/// 不放在这里。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub handshake_timeout_ms: u64,
    pub buffer_limit: usize,
    pub max_connections: usize,
    pub max_handshakes: usize,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
            buffer_limit: DEFAULT_BUFFER_LIMIT,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_handshakes: DEFAULT_MAX_HANDSHAKES,
        }
    }
}

impl TlsConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.handshake_timeout_ms == 0 {
            return Err(Error::Config(
                "[tls] handshake_timeout_ms 必须大于 0".into(),
            ));
        }
        if self.buffer_limit == 0 {
            return Err(Error::Config("[tls] buffer_limit 必须大于 0".into()));
        }
        if self.max_connections == 0 {
            return Err(Error::Config("[tls] max_connections 必须大于 0".into()));
        }
        if self.max_handshakes == 0 || self.max_handshakes > self.max_connections {
            return Err(Error::Config(
                "[tls] max_handshakes 必须大于 0 且不超过 max_connections".into(),
            ));
        }
        Ok(())
    }
}
