use std::fmt;
use std::net::SocketAddr;

use rskynet_core::boxed_payload;

/// TLS 监听口或连接的逻辑编号。它与底层网络层的 `SocketId` 有意分离。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TlsId(pub u32);

impl TlsId {
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TlsId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tls#{}", self.0)
    }
}

/// TLS 服务投给连接 owner 的事件。
#[derive(Debug)]
pub enum TlsEvent {
    Connected {
        id: TlsId,
        /// 入站连接对应的 TLS 监听口；主动连接为 `None`。
        listen: Option<TlsId>,
        peer: SocketAddr,
        version: Option<&'static str>,
        cipher_suite: Option<String>,
        alpn: Option<Vec<u8>>,
    },
    Data {
        id: TlsId,
        data: Vec<u8>,
    },
    Close {
        id: TlsId,
    },
    Error {
        id: TlsId,
        reason: String,
    },
    Warning {
        id: TlsId,
        kilobytes: usize,
    },
}

impl TlsEvent {
    pub const fn id(&self) -> TlsId {
        match self {
            Self::Connected { id, .. }
            | Self::Data { id, .. }
            | Self::Close { id }
            | Self::Error { id, .. }
            | Self::Warning { id, .. } => *id,
        }
    }
}

boxed_payload!(TlsEvent);

#[derive(Debug, Clone)]
pub struct TlsInfo {
    pub id: TlsId,
    pub owner: u32,
    pub kind: &'static str,
    pub state: &'static str,
    pub local: Option<SocketAddr>,
    pub peer: Option<SocketAddr>,
    pub version: Option<&'static str>,
    pub cipher_suite: Option<String>,
    pub alpn: Option<Vec<u8>>,
}

boxed_payload!(TlsInfo);
