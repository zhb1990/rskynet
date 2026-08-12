//! 负载与具体类型之间的转换约定。C 版没有对应物——skynet 里这件事由 Lua 的
//! `skynet.dispatch(proto, ...)` 承担：每个协议注册一对 `pack` / `unpack`，
//! 分发前先把字节流解成 Lua 值。
//!
//! 这里的对应物是两个 trait。它们只服务于一个目的：让 [`crate::Message`] 的
//! 分发能按**声明的类型**收发，而不是每个处理函数自己写一遍 `downcast`：
//!
//! ```ignore
//! #[rskynet::service]
//! impl Echo {
//!     #[msg(MsgType::USER)]
//!     async fn on_ask(&self, ctx: Ctx, ask: Ask) -> Answer { .. }
//! }
//! ```
//!
//! 那句 `ask: Ask` 靠 [`FromPayload`] 取出来，`-> Answer` 靠 [`IntoPayload`]
//! 装回去。手写 `dispatch` 的服务用不着这两个 trait。
//!
//! # 为什么对象负载要写一句宏
//!
//! `Payload::Boxed` 装的是 `Box<dyn Any + Send>`，理论上「凡 `T: Any + Send`
//! 都能 downcast」，可这个 blanket impl 会与 `Vec<u8>` / `String` 那两个撞车
//! （它们同样满足 `Any + Send`），而一致性规则不允许。所以对象负载改成显式声明：
//!
//! ```
//! # use rskynet_core::boxed_payload;
//! struct Ask(u32);
//! boxed_payload!(Ask);
//! ```
//!
//! 显式也有好处：一个类型到底是走字节还是走对象，看一眼声明就知道，不会因为
//! 改了参数类型就悄悄换了传输方式。

use crate::error::{Error, Result};
use crate::message::Payload;

/// 从负载里取出自己。分发时用它把 [`Payload`] 变成处理函数声明的参数类型。
///
/// 内置实现只有四个：[`Payload`] 本身（原样拿走）、`()`（不关心负载）、
/// `Vec<u8>` 与 `String`（要求 `Payload::Bytes`）。自己的类型走
/// [`boxed_payload!`][crate::boxed_payload] 声明。
pub trait FromPayload: Sized {
    /// 类型不匹配时返回 [`Error::Payload`]，分发方据此给对方回一个错误应答。
    fn from_payload(payload: Payload) -> Result<Self>;
}

/// 把自己装进负载。处理函数的返回值靠它变成应答。
pub trait IntoPayload {
    fn into_payload(self) -> Payload;
}

impl FromPayload for Payload {
    fn from_payload(payload: Payload) -> Result<Self> {
        Ok(payload)
    }
}

impl IntoPayload for Payload {
    fn into_payload(self) -> Payload {
        self
    }
}

/// 不关心负载的处理函数，也是「没有返回值就不回包」那条路的标记。
impl FromPayload for () {
    fn from_payload(_payload: Payload) -> Result<Self> {
        Ok(())
    }
}

impl IntoPayload for () {
    fn into_payload(self) -> Payload {
        Payload::None
    }
}

impl FromPayload for Vec<u8> {
    fn from_payload(payload: Payload) -> Result<Self> {
        match payload {
            Payload::Bytes(bytes) => Ok(bytes),
            // 空负载当空字节看：发送方写 Payload::None 与写空 Vec 是一个意思
            Payload::None => Ok(Vec::new()),
            Payload::Boxed(_) => Err(Error::Payload("期待字节负载，收到的是对象".into())),
        }
    }
}

impl IntoPayload for Vec<u8> {
    fn into_payload(self) -> Payload {
        Payload::Bytes(self)
    }
}

impl FromPayload for String {
    fn from_payload(payload: Payload) -> Result<Self> {
        let bytes = Vec::<u8>::from_payload(payload)?;
        String::from_utf8(bytes).map_err(|_| Error::Payload("字节负载不是合法 UTF-8".into()))
    }
}

impl IntoPayload for String {
    fn into_payload(self) -> Payload {
        Payload::Bytes(self.into_bytes())
    }
}

/// 声明若干类型走对象负载（`Payload::Boxed`），理由见[模块文档][self]。
///
/// 生成的是 [`FromPayload`] 与 [`IntoPayload`] 两个实现，所以声明过的类型既能
/// 当处理函数的参数，也能当返回值：
///
/// ```
/// use rskynet_core::{FromPayload, Payload, boxed_payload};
///
/// #[derive(Debug, PartialEq)]
/// struct Ask(u32);
/// #[derive(Debug, PartialEq)]
/// struct Answer(u32);
/// boxed_payload!(Ask, Answer);
///
/// let ask = Ask::from_payload(Payload::of(Ask(7))).unwrap();
/// assert_eq!(ask, Ask(7));
/// ```
#[macro_export]
macro_rules! boxed_payload {
    ($($t:ty),+ $(,)?) => {
        $(
            impl $crate::FromPayload for $t {
                fn from_payload(payload: $crate::Payload) -> $crate::Result<Self> {
                    match payload.downcast::<$t>() {
                        Ok(value) => Ok(*value),
                        Err(_) => Err($crate::Error::Payload(::std::format!(
                            "负载不是 {}",
                            ::std::stringify!($t)
                        ))),
                    }
                }
            }

            impl $crate::IntoPayload for $t {
                fn into_payload(self) -> $crate::Payload {
                    $crate::Payload::of(self)
                }
            }
        )+
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Ask(u32);
    crate::boxed_payload!(Ask);

    /// 字节负载与文本负载能来回转，空负载当成空字节
    #[test]
    fn byte_payloads_round_trip() {
        let payload = "你好".to_string().into_payload();
        assert_eq!(String::from_payload(payload).unwrap(), "你好");

        let payload = vec![1u8, 2, 3].into_payload();
        assert_eq!(Vec::<u8>::from_payload(payload).unwrap(), vec![1, 2, 3]);

        assert!(Vec::<u8>::from_payload(Payload::None).unwrap().is_empty());
    }

    /// 声明过的类型走对象负载，取错类型要拿到错误而不是 panic
    #[test]
    fn boxed_payloads_report_type_mismatch() {
        let ask = Ask::from_payload(Payload::of(Ask(7))).expect("类型应匹配");
        assert_eq!(ask.0, 7);

        let err = Ask::from_payload(Payload::text("并不是 Ask")).expect_err("类型不该匹配");
        assert!(matches!(err, Error::Payload(_)));

        let err = Vec::<u8>::from_payload(Payload::of(Ask(1))).expect_err("对象不是字节");
        assert!(matches!(err, Error::Payload(_)));
    }

    /// 不合法的 UTF-8 要被认出来，而不是悄悄丢字符
    #[test]
    fn invalid_utf8_is_rejected() {
        let err = String::from_payload(Payload::Bytes(vec![0xff, 0xfe])).expect_err("不是 UTF-8");
        assert!(matches!(err, Error::Payload(_)));
    }
}
