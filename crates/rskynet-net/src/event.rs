//! 网络层投给属主服务的事件，对照 `socket_server.h` 的 `SOCKET_DATA` 那一组常量。
//!
//! 事件以 [`MsgType::SOCKET`][rskynet_core::MsgType::SOCKET]（协议号 5）投给
//! 持有该连接的服务，与定时器回包走同一条路径。

use std::fmt;
use std::net::SocketAddr;

use rskynet_core::boxed_payload;

/// 一个连接、监听口或 UDP 端口的编号，对照 C 版的 socket id。
///
/// 单调递增，`id % max_socket` 就是它的槽位（照搬 C 版的 `HASH_ID`），所以同一个
/// 槽位被复用时 id 一定变了——迟到的事件因此不会张冠李戴。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SocketId(pub u32);

impl SocketId {
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl fmt::Display for SocketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// 网络层投给属主服务的一件事。
///
/// 与 C 版的差别只有一处：地址直接是 [`SocketAddr`]，不做 C 版那套
/// 「1 字节协议族 + IP + 端口」的字节编码——那是为了让 Lua 能把地址当字符串拿着。
#[derive(Debug)]
pub enum SocketEvent {
    /// 监听口上来了新连接，对照 `SOCKET_ACCEPT`。
    ///
    /// 新连接此刻还没进 poll，属主得调 [`start`][crate::start] 才开始收数据——
    /// 这个空档正是把连接转交给别的服务（C 版的 agent）的时机。
    Accept {
        id: SocketId,
        /// 哪个监听口收下的。
        listen: SocketId,
        /// 对端地址。
        peer: SocketAddr,
    },
    /// 连接上读到了数据，对照 `SOCKET_DATA`。
    Data { id: SocketId, data: Vec<u8> },
    /// UDP 端口收到一个包，对照 `SOCKET_UDP`。
    Udp {
        id: SocketId,
        from: SocketAddr,
        data: Vec<u8>,
    },
    /// 连接没了，对照 `SOCKET_CLOSE`。自己调 `close` 关掉的也会收到这一条。
    Close { id: SocketId },
    /// 出错了，连接已经关掉，对照 `SOCKET_ERROR`。
    Error { id: SocketId, reason: String },
    /// 写缓冲堆得太多了，对照 `SOCKET_WARNING`。
    ///
    /// 第一次在越过 `warn_size` 时报，之后每翻一倍再报一次。属主该据此限流或踢人
    /// ——网络层自己不会丢数据，堆到内存耗尽也是它照堆。
    Warning { id: SocketId, kilobytes: usize },
}

boxed_payload!(SocketEvent);

impl SocketEvent {
    /// 事件说的是哪个 socket。
    pub fn id(&self) -> SocketId {
        match self {
            SocketEvent::Accept { id, .. }
            | SocketEvent::Data { id, .. }
            | SocketEvent::Udp { id, .. }
            | SocketEvent::Close { id }
            | SocketEvent::Error { id, .. }
            | SocketEvent::Warning { id, .. } => *id,
        }
    }

    /// 这个事件是不是意味着「socket 已经没了」。收到之后 id 不再有效。
    pub fn is_gone(&self) -> bool {
        matches!(self, SocketEvent::Close { .. } | SocketEvent::Error { .. })
    }
}

/// 一个 socket 的现状，[`info`][crate::info] 的返回值。对照 C 版 `'Q'` 那条命令。
#[derive(Debug, Clone)]
pub struct SocketInfo {
    pub id: SocketId,
    /// 属主服务的 handle，事件投给它。对照 C 版的 `opaque`。
    pub owner: u32,
    /// `listener` / `stream` / `udp`。
    pub kind: &'static str,
    /// 状态名，与 C 版的 `SOCKET_TYPE_*` 对齐。
    pub state: &'static str,
    /// 读被暂停了（[`pause`][crate::pause] 过且还没 [`start`][crate::start]）。
    pub paused: bool,
    pub local: Option<SocketAddr>,
    pub peer: Option<SocketAddr>,
    /// 写缓冲里还压着多少字节。
    pub write_pending: usize,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

boxed_payload!(SocketInfo);
