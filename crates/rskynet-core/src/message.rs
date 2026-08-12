//! 消息、消息类型与地址。对照 `skynet-src/skynet_mq.h` 的 `struct skynet_message`。
//!
//! C 版为了省内存把消息类型藏在 `sz` 的高 8 位里（`MESSAGE_TYPE_SHIFT`），
//! Rust 版直接用独立字段，语义不变；`Payload` 则取代了 `void* data + size_t sz`。

use std::any::Any;
use std::fmt;

/// 消息类型。数值与 skynet 的 `PTYPE_*` 保持一致，方便对照与将来接入网络层。
///
/// 用 newtype 而不是 enum，是为了让使用方能像 skynet 那样自定义协议号
/// （skynet 里 `PTYPE_RESERVED_LUA = 10` 就是 Lua 服务自己约定的业务协议）。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MsgType(pub u8);

impl MsgType {
    /// 文本消息，logger 服务收的就是这种。
    pub const TEXT: MsgType = MsgType(0);
    /// 应答消息，session 与请求配对；定时器到期也用它。
    pub const RESPONSE: MsgType = MsgType(1);
    pub const MULTICAST: MsgType = MsgType(2);
    pub const CLIENT: MsgType = MsgType(3);
    pub const SYSTEM: MsgType = MsgType(4);
    pub const HARBOR: MsgType = MsgType(5);
    pub const SOCKET: MsgType = MsgType(6);
    /// 错误应答，`call` 收到它会得到 `Error::CallFailed`。
    pub const ERROR: MsgType = MsgType(7);
    /// 业务消息的默认协议号，对应 skynet 的 `PTYPE_RESERVED_LUA`。
    pub const USER: MsgType = MsgType(10);

    pub const fn raw(self) -> u8 {
        self.0
    }

    /// 应答类消息（`RESPONSE` / `ERROR`）由内核直接唤醒等待中的 session，
    /// 不会进入服务的 `dispatch`。
    pub const fn is_reply(self) -> bool {
        self.0 == Self::RESPONSE.0 || self.0 == Self::ERROR.0
    }

    fn name(self) -> Option<&'static str> {
        Some(match self {
            Self::TEXT => "TEXT",
            Self::RESPONSE => "RESPONSE",
            Self::MULTICAST => "MULTICAST",
            Self::CLIENT => "CLIENT",
            Self::SYSTEM => "SYSTEM",
            Self::HARBOR => "HARBOR",
            Self::SOCKET => "SOCKET",
            Self::ERROR => "ERROR",
            Self::USER => "USER",
            _ => return None,
        })
    }
}

impl fmt::Debug for MsgType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => f.write_str(name),
            None => write!(f, "MsgType({})", self.0),
        }
    }
}

/// 消息负载。
///
/// 同进程内传对象走 `Boxed`，零拷贝、无需序列化；`Bytes` 留给文本日志与
/// 将来的网络层（跨节点时只有字节流能过网）。
pub enum Payload {
    None,
    Bytes(Vec<u8>),
    Boxed(Box<dyn Any + Send>),
}

impl Payload {
    /// 把任意类型装进消息，接收方用 [`Payload::downcast`] 取回。
    pub fn of<T: Any + Send>(value: T) -> Self {
        Payload::Boxed(Box::new(value))
    }

    pub fn text(text: impl Into<String>) -> Self {
        Payload::Bytes(text.into().into_bytes())
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Payload::None)
    }

    /// 取回 `Boxed` 里的具体类型；类型不匹配时把原负载还给调用方。
    pub fn downcast<T: Any + Send>(self) -> std::result::Result<Box<T>, Payload> {
        match self {
            Payload::Boxed(any) => any.downcast::<T>().map_err(Payload::Boxed),
            other => Err(other),
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Payload::Bytes(bytes) => Some(bytes),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// 字节负载的长度；对象负载无法计量，返回 0。
    pub fn len(&self) -> usize {
        match self {
            Payload::Bytes(bytes) => bytes.len(),
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Payload::None => f.write_str("None"),
            Payload::Bytes(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => write!(f, "Bytes({text:?})"),
                Err(_) => write!(f, "Bytes({} 字节)", bytes.len()),
            },
            Payload::Boxed(_) => f.write_str("Boxed(..)"),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(value: Vec<u8>) -> Self {
        Payload::Bytes(value)
    }
}

impl From<String> for Payload {
    fn from(value: String) -> Self {
        Payload::Bytes(value.into_bytes())
    }
}

impl From<&str> for Payload {
    fn from(value: &str) -> Self {
        Payload::Bytes(value.as_bytes().to_vec())
    }
}

/// 一条服务间消息。
#[derive(Debug)]
pub struct Message {
    /// 发送方 handle，0 表示来自内核（定时器等）。
    pub source: u32,
    /// 会话号。请求方分配的正数，应答时原样带回；0 表示不需要应答。
    pub session: i32,
    pub mtype: MsgType,
    pub payload: Payload,
}

impl Message {
    pub fn new(source: u32, session: i32, mtype: MsgType, payload: Payload) -> Self {
        Self {
            source,
            session,
            mtype,
            payload,
        }
    }

    /// 取走负载，原地留下 `Payload::None`。
    pub fn take_payload(&mut self) -> Payload {
        std::mem::replace(&mut self.payload, Payload::None)
    }

    /// 发送方是否在等待应答。
    pub fn needs_reply(&self) -> bool {
        self.session != 0
    }
}

/// 寻址方式，对照 skynet 的 `:handle` 与 `.name` 两种写法。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Addr {
    Handle(u32),
    Name(String),
}

impl From<u32> for Addr {
    fn from(value: u32) -> Self {
        Addr::Handle(value)
    }
}

impl From<&str> for Addr {
    fn from(value: &str) -> Self {
        // `:0100000a` 是十六进制 handle，`.foo` 与 `foo` 都按本地名字解析
        if let Some(hex) = value.strip_prefix(':') {
            if let Ok(handle) = u32::from_str_radix(hex, 16) {
                return Addr::Handle(handle);
            }
        }
        Addr::Name(value.trim_start_matches('.').to_string())
    }
}

impl From<&String> for Addr {
    fn from(value: &String) -> Self {
        Addr::from(value.as_str())
    }
}

impl From<String> for Addr {
    fn from(value: String) -> Self {
        Addr::from(value.as_str())
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Addr::Handle(handle) => write!(f, ":{handle:08x}"),
            Addr::Name(name) => write!(f, ".{name}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 地址字符串要能区分十六进制句柄与本地名字
    #[test]
    fn addr_parsing_distinguishes_handle_and_name() {
        assert_eq!(Addr::from(":0100000a"), Addr::Handle(0x0100_000a));
        assert_eq!(Addr::from(".logger"), Addr::Name("logger".into()));
        assert_eq!(Addr::from("logger"), Addr::Name("logger".into()));
        assert_eq!(Addr::from(7u32), Addr::Handle(7));
        assert_eq!(Addr::Handle(0x0100_000a).to_string(), ":0100000a");
    }

    /// 对象负载能原样取回，取错类型时也不会把负载弄丢
    #[test]
    fn boxed_payload_round_trips() {
        let payload = Payload::of(vec![1u32, 2, 3]);
        let value = payload.downcast::<Vec<u32>>().expect("类型应匹配");
        assert_eq!(*value, vec![1, 2, 3]);

        // 类型不匹配时负载不会丢
        let payload = Payload::of(42u8);
        let back = payload.downcast::<String>().expect_err("类型不该匹配");
        assert_eq!(*back.downcast::<u8>().unwrap(), 42);
    }

    /// 协议号必须与 skynet 的 PTYPE_* 逐一对齐
    #[test]
    fn msg_type_numbers_match_skynet() {
        assert_eq!(MsgType::TEXT.raw(), 0);
        assert_eq!(MsgType::RESPONSE.raw(), 1);
        assert_eq!(MsgType::ERROR.raw(), 7);
        assert_eq!(MsgType::USER.raw(), 10);
        assert!(MsgType::RESPONSE.is_reply() && MsgType::ERROR.is_reply());
        assert!(!MsgType::USER.is_reply());
        assert_eq!(format!("{:?}", MsgType::RESPONSE), "RESPONSE");
        assert_eq!(format!("{:?}", MsgType(99)), "MsgType(99)");
    }
}
