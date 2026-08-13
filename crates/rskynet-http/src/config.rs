use serde::Deserialize;

use crate::{HttpError, Result};

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HttpClientConfig {
    pub connect_timeout_ms: u64,
    pub response_head_timeout_ms: u64,
    pub body_idle_timeout_ms: u64,
    pub continue_timeout_ms: u64,
    pub pool_idle_timeout_ms: u64,
    pub max_connections: usize,
    pub max_connections_per_origin: usize,
    pub max_idle_connections: usize,
    pub max_idle_connections_per_origin: usize,
    pub input_high_water: usize,
    pub input_low_water: usize,
    pub max_chunk_size: usize,
    pub max_header_size: usize,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            response_head_timeout_ms: 30_000,
            body_idle_timeout_ms: 30_000,
            continue_timeout_ms: 1_000,
            pool_idle_timeout_ms: 60_000,
            max_connections: 256,
            max_connections_per_origin: 16,
            max_idle_connections: 64,
            max_idle_connections_per_origin: 8,
            input_high_water: 256 * 1024,
            input_low_water: 128 * 1024,
            max_chunk_size: 64 * 1024,
            max_header_size: 64 * 1024,
        }
    }
}

impl HttpClientConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_common(
            self.input_low_water,
            self.input_high_water,
            self.max_chunk_size,
            self.max_header_size,
        )?;
        if self.connect_timeout_ms == 0
            || self.response_head_timeout_ms == 0
            || self.body_idle_timeout_ms == 0
            || self.continue_timeout_ms == 0
            || self.pool_idle_timeout_ms == 0
        {
            return Err(HttpError::Config("客户端超时必须大于 0".into()));
        }
        if self.max_connections == 0
            || self.max_connections_per_origin == 0
            || self.max_connections_per_origin > self.max_connections
        {
            return Err(HttpError::Config("客户端连接上限无效".into()));
        }
        if self.max_idle_connections > self.max_connections
            || self.max_idle_connections_per_origin > self.max_connections_per_origin
        {
            return Err(HttpError::Config("客户端空闲连接上限无效".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub header_timeout_ms: u64,
    pub body_idle_timeout_ms: u64,
    pub input_high_water: usize,
    pub input_low_water: usize,
    pub max_chunk_size: usize,
    pub max_header_size: usize,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            header_timeout_ms: 30_000,
            body_idle_timeout_ms: 30_000,
            input_high_water: 256 * 1024,
            input_low_water: 128 * 1024,
            max_chunk_size: 64 * 1024,
            max_header_size: 64 * 1024,
        }
    }
}

impl HttpServerConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_common(
            self.input_low_water,
            self.input_high_water,
            self.max_chunk_size,
            self.max_header_size,
        )?;
        if self.header_timeout_ms == 0 || self.body_idle_timeout_ms == 0 {
            return Err(HttpError::Config("服务端超时必须大于 0".into()));
        }
        Ok(())
    }
}

fn validate_common(low: usize, high: usize, chunk: usize, header: usize) -> Result<()> {
    if low == 0 || low >= high || chunk == 0 || header == 0 {
        return Err(HttpError::Config(
            "必须满足 0 < input_low_water < input_high_water，且块/头上限非零".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_watermarks_are_ordered() {
        HttpClientConfig::default().validate().unwrap();
        HttpServerConfig::default().validate().unwrap();
        let mut bad = HttpClientConfig::default();
        bad.input_low_water = bad.input_high_water;
        assert!(bad.validate().is_err());
    }
}
