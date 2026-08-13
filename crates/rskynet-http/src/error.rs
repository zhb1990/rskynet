#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HttpError {
    #[error("HTTP 配置无效：{0}")]
    Config(String),
    #[error("URI 无效：{0}")]
    InvalidUri(String),
    #[error("不支持的 URI scheme：{0}")]
    UnsupportedScheme(String),
    #[error("HTTP 协议错误：{0}")]
    Protocol(String),
    #[error("传输错误：{0}")]
    Transport(String),
    #[error("TLS 错误：{0}")]
    Tls(String),
    #[error("{0} 阶段超时")]
    Timeout(&'static str),
    #[error("body 长度不符：期待 {expected} 字节，实际 {actual} 字节")]
    BodyLengthMismatch { expected: u64, actual: u64 },
    #[error("body 已关闭")]
    BodyClosed,
    #[error("请求体已被最终响应拒绝")]
    RequestBodyRejected,
    #[error("块大小 {actual} 超过上限 {limit}")]
    BackpressureLimit { actual: usize, limit: usize },
    #[error("操作已取消")]
    Canceled,
    #[error("HTTP 客户端服务不可用：{0}")]
    ServiceUnavailable(String),
    #[error("当前状态不允许该操作：{0}")]
    InvalidState(&'static str),
    #[cfg(feature = "websocket")]
    #[error("WebSocket 错误：{0}")]
    WebSocket(#[from] tungstenite::Error),
}

impl From<ureq_proto::Error> for HttpError {
    fn from(value: ureq_proto::Error) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl From<rskynet_core::Error> for HttpError {
    fn from(value: rskynet_core::Error) -> Self {
        Self::ServiceUnavailable(value.to_string())
    }
}
