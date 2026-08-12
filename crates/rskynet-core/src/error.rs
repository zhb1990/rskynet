//! 内核错误类型。
//!
//! handle 一律按 skynet 的习惯打印成 `:0100000a` 的十六进制形式。

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 目标地址上没有活着的服务，对应 skynet 里 `skynet_context_push` 返回 -1 的情况。
    #[error("服务 :{0:08x} 不存在")]
    NoService(u32),

    /// 服务类型没有在 `Registry` 中注册，对应 skynet 找不到 `.so` 模块。
    #[error("未注册的服务类型 `{0}`")]
    UnknownService(String),

    /// 按名字寻址失败。
    #[error("名字 `{0}` 未注册")]
    NameNotFound(String),

    /// 对端以 `MsgType::ERROR` 应答，或对端在处理请求期间退出。
    #[error("请求被服务 :{0:08x} 拒绝，或对端已退出")]
    CallFailed(u32),

    /// 等待中的 session 随服务销毁一起消失。
    #[error("请求已取消")]
    Canceled,

    /// 服务初始化失败。
    #[error("服务 `{kind}` 初始化失败：{reason}")]
    Init { kind: String, reason: String },

    /// 服务在初始化或运行中主动报错。
    #[error("{0}")]
    Service(String),

    /// 负载不是期待的类型，见 [`crate::FromPayload`]。分发时撞上它意味着两边对
    /// 协议号的约定不一致——发送方按一个类型装，接收方按另一个类型取。
    #[error("负载类型不符：{0}")]
    Payload(String),

    #[error("配置错误：{0}")]
    Config(String),

    #[error("读取配置 `{path}` 失败：{source}")]
    ConfigIo {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("解析配置失败：{0}")]
    ConfigParse(#[from] toml::de::Error),

    /// 启动前没有注入时间来源，见 [`crate::Builder::timer`]。
    #[error("启动前必须注入 timer 对象，现成的实现见 rskynet-timer")]
    MissingTimer,

    /// 服务参数用 JSON 承载时的编解码失败。内核自己不用 JSON，这个变体是留给
    /// 服务包的（网络层的协议、跨节点的负载都可能用得上）。
    #[error("服务参数不是合法的 JSON：{0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    /// 便于服务实现里快速构造错误：`Error::service(format!(...))`。
    pub fn service(msg: impl Into<String>) -> Self {
        Error::Service(msg.into())
    }
}
