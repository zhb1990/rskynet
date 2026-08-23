use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use rskynet_core::boxed_payload;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct QuicListenerId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct QuicConnectionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct QuicStreamId(pub u64);

impl fmt::Display for QuicListenerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "quic-listener#{}", self.0)
    }
}

impl fmt::Display for QuicConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "quic#{}", self.0)
    }
}

impl fmt::Display for QuicStreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "stream#{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamDirection {
    Bidirectional,
    Unidirectional,
}

#[derive(Debug)]
pub enum QuicEvent {
    Connected {
        id: QuicConnectionId,
        listener: Option<QuicListenerId>,
        peer: SocketAddr,
        alpn: Option<Vec<u8>>,
    },
    StreamOpened {
        id: QuicConnectionId,
        stream: QuicStreamId,
        direction: StreamDirection,
    },
    StreamData {
        id: QuicConnectionId,
        stream: QuicStreamId,
        data: Vec<u8>,
    },
    /// 本地发送方向的全部数据已得到确认。
    SendFinished {
        id: QuicConnectionId,
        stream: QuicStreamId,
    },
    /// 对端已经结束发送方向，接收侧不会再有数据。
    ReceiveFinished {
        id: QuicConnectionId,
        stream: QuicStreamId,
    },
    StreamReset {
        id: QuicConnectionId,
        stream: QuicStreamId,
        error_code: u64,
    },
    StreamStopped {
        id: QuicConnectionId,
        stream: QuicStreamId,
        error_code: u64,
    },
    Datagram {
        id: QuicConnectionId,
        data: Vec<u8>,
    },
    Close {
        id: QuicConnectionId,
        error_code: Option<u64>,
        reason: Vec<u8>,
    },
    Error {
        id: QuicConnectionId,
        reason: String,
    },
    Warning {
        id: QuicConnectionId,
        buffered_bytes: usize,
    },
}

impl QuicEvent {
    pub const fn id(&self) -> QuicConnectionId {
        match self {
            Self::Connected { id, .. }
            | Self::StreamOpened { id, .. }
            | Self::StreamData { id, .. }
            | Self::SendFinished { id, .. }
            | Self::ReceiveFinished { id, .. }
            | Self::StreamReset { id, .. }
            | Self::StreamStopped { id, .. }
            | Self::Datagram { id, .. }
            | Self::Close { id, .. }
            | Self::Error { id, .. }
            | Self::Warning { id, .. } => *id,
        }
    }
}

boxed_payload!(QuicEvent);

#[derive(Debug, Clone, Serialize)]
pub struct QuicInfo {
    pub id: QuicConnectionId,
    pub owner: rskynet_core::Handle,
    pub listener: Option<QuicListenerId>,
    pub state: &'static str,
    pub started: bool,
    pub local: Option<SocketAddr>,
    pub peer: SocketAddr,
    pub alpn: Option<Vec<u8>>,
    pub rtt_micros: u64,
    pub congestion_window: u64,
    pub open_send_streams: usize,
    pub buffered_bytes: usize,
}

impl QuicInfo {
    pub(crate) fn rtt_micros(duration: Duration) -> u64 {
        duration.as_micros().min(u128::from(u64::MAX)) as u64
    }
}

boxed_payload!(QuicInfo);
