use std::sync::Arc;
use std::time::Duration;

use quinn_proto::{IdleTimeout, TransportConfig, VarInt};
use serde::Deserialize;

use rskynet_core::{Error, Result};

const DEFAULT_MAX_CONNECTIONS: usize = 65_536;
const DEFAULT_MAX_HANDSHAKES: usize = 4_096;
const DEFAULT_WRITE_HIGH_WATER: usize = 4 * 1024 * 1024;
const DEFAULT_WRITE_LOW_WATER: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_CHUNK_SIZE: usize = 64 * 1024;
const DEFAULT_HANDSHAKE_TIMEOUT_MS: u64 = 10_000;

/// QUIC 服务的节点级运行限制。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct QuicConfig {
    pub max_connections: usize,
    pub max_handshakes: usize,
    pub write_high_water: usize,
    pub write_low_water: usize,
    pub max_chunk_size: usize,
    pub handshake_timeout_ms: u64,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_handshakes: DEFAULT_MAX_HANDSHAKES,
            write_high_water: DEFAULT_WRITE_HIGH_WATER,
            write_low_water: DEFAULT_WRITE_LOW_WATER,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            handshake_timeout_ms: DEFAULT_HANDSHAKE_TIMEOUT_MS,
        }
    }
}

impl QuicConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_connections == 0
            || self.max_handshakes == 0
            || self.max_handshakes > self.max_connections
        {
            return Err(Error::Config("[quic] 连接/握手上限无效".into()));
        }
        if self.write_low_water == 0 || self.write_low_water >= self.write_high_water {
            return Err(Error::Config(
                "[quic] 必须满足 0 < write_low_water < write_high_water".into(),
            ));
        }
        if self.max_chunk_size == 0 || self.handshake_timeout_ms == 0 {
            return Err(Error::Config(
                "[quic] max_chunk_size 和 handshake_timeout_ms 必须大于 0".into(),
            ));
        }
        Ok(())
    }
}

/// 每个 QUIC endpoint 的传输参数。
#[derive(Debug, Clone)]
pub struct QuicTransportOptions {
    pub max_idle_timeout_ms: u64,
    pub max_concurrent_bidi_streams: u32,
    pub max_concurrent_uni_streams: u32,
    pub stream_receive_window: u32,
    pub receive_window: u32,
    pub datagrams: bool,
    pub datagram_receive_buffer_size: usize,
    pub datagram_send_buffer_size: usize,
}

impl Default for QuicTransportOptions {
    fn default() -> Self {
        Self {
            max_idle_timeout_ms: 30_000,
            max_concurrent_bidi_streams: 100,
            max_concurrent_uni_streams: 100,
            stream_receive_window: 1024 * 1024,
            receive_window: 8 * 1024 * 1024,
            datagrams: true,
            datagram_receive_buffer_size: 1024 * 1024,
            datagram_send_buffer_size: 1024 * 1024,
        }
    }
}

impl QuicTransportOptions {
    pub(crate) fn build(&self) -> Result<Arc<TransportConfig>> {
        if self.max_idle_timeout_ms == 0
            || self.stream_receive_window == 0
            || self.receive_window == 0
        {
            return Err(Error::Config("QUIC transport 流控/超时参数无效".into()));
        }
        let mut config = TransportConfig::default();
        let idle = IdleTimeout::try_from(Duration::from_millis(self.max_idle_timeout_ms))
            .map_err(|_| Error::Config("QUIC idle timeout 超出可表示范围".into()))?;
        config.max_idle_timeout(Some(idle));
        config.max_concurrent_bidi_streams(VarInt::from_u32(self.max_concurrent_bidi_streams));
        config.max_concurrent_uni_streams(VarInt::from_u32(self.max_concurrent_uni_streams));
        config.stream_receive_window(VarInt::from_u32(self.stream_receive_window));
        config.receive_window(VarInt::from_u32(self.receive_window));
        if self.datagrams {
            config.datagram_receive_buffer_size(Some(self.datagram_receive_buffer_size));
            config.datagram_send_buffer_size(self.datagram_send_buffer_size);
        } else {
            config.datagram_receive_buffer_size(None);
            config.datagram_send_buffer_size(0);
        }
        Ok(Arc::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        QuicConfig::default().validate().unwrap();
        QuicTransportOptions::default().build().unwrap();
    }

    #[test]
    fn invalid_watermarks_are_rejected() {
        let mut config = QuicConfig::default();
        config.write_low_water = config.write_high_water;
        assert!(config.validate().is_err());
    }

    #[test]
    fn datagrams_can_be_disabled() {
        let options = QuicTransportOptions {
            datagrams: false,
            ..Default::default()
        };
        options.build().unwrap();
    }
}
