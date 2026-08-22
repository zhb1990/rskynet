//! 显式开放给 Dashboard 调试控制台的强类型消息描述。
//!
//! 内核不主动猜测 `Payload::Boxed` 里的具体类型。服务宏只为标有 `#[debug]`
//! 的 `#[msg]` 处理器生成这里的描述，把 JSON 与处理器声明的 Rust 类型连接起来。

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{FromPayload, IntoPayload, MessageSchema, MessageSchemaType, MsgType, Payload, Result};

type DecodePayload = fn(Value) -> Result<Payload>;
type EncodePayload = fn(Payload) -> Result<Value>;

/// 一条可由调试控制台发送的强类型消息。
///
/// 转换函数只留在进程内；Dashboard 对外只序列化名称、协议号与 Rust 类型名。
#[derive(Clone, Copy)]
pub struct DebugMessageDescriptor {
    name: &'static str,
    mtype: MsgType,
    request_type: &'static str,
    response_type: Option<&'static str>,
    request_schema: fn() -> MessageSchema,
    response_schema: Option<fn() -> MessageSchema>,
    decode: DecodePayload,
    encode: Option<EncodePayload>,
}

impl DebugMessageDescriptor {
    /// 构造一条只支持 `send` 的消息描述。
    pub fn send<Request>(name: &'static str, mtype: MsgType) -> Self
    where
        Request: DeserializeOwned + IntoPayload + MessageSchemaType,
    {
        Self {
            name,
            mtype,
            request_type: std::any::type_name::<Request>(),
            response_type: None,
            request_schema: Request::message_schema,
            response_schema: None,
            decode: decode_payload::<Request>,
            encode: None,
        }
    }

    /// 宏展开 variant 调试消息时使用：请求 schema 仍属于 `Request`，但 decoder
    /// 会先把它包装成外层 enum 再放进 `Payload`。
    #[doc(hidden)]
    pub fn send_decoded<Request>(
        name: &'static str,
        mtype: MsgType,
        decode: fn(Value) -> Result<Payload>,
    ) -> Self
    where
        Request: DeserializeOwned + MessageSchemaType,
    {
        Self {
            name,
            mtype,
            request_type: std::any::type_name::<Request>(),
            response_type: None,
            request_schema: Request::message_schema,
            response_schema: None,
            decode,
            encode: None,
        }
    }

    /// 构造一条同时支持 `send` 与 `call` 的消息描述。
    pub fn call<Request, Response>(name: &'static str, mtype: MsgType) -> Self
    where
        Request: DeserializeOwned + IntoPayload + MessageSchemaType,
        Response: FromPayload + Serialize + MessageSchemaType,
    {
        Self {
            name,
            mtype,
            request_type: std::any::type_name::<Request>(),
            response_type: Some(std::any::type_name::<Response>()),
            request_schema: Request::message_schema,
            response_schema: Some(Response::message_schema),
            decode: decode_payload::<Request>,
            encode: Some(encode_payload::<Response>),
        }
    }

    /// [`Self::send_decoded`] 的 call 版本。
    #[doc(hidden)]
    pub fn call_decoded<Request, Response>(
        name: &'static str,
        mtype: MsgType,
        decode: fn(Value) -> Result<Payload>,
    ) -> Self
    where
        Request: DeserializeOwned + MessageSchemaType,
        Response: FromPayload + Serialize + MessageSchemaType,
    {
        Self {
            name,
            mtype,
            request_type: std::any::type_name::<Request>(),
            response_type: Some(std::any::type_name::<Response>()),
            request_schema: Request::message_schema,
            response_schema: Some(Response::message_schema),
            decode,
            encode: Some(encode_payload::<Response>),
        }
    }

    pub fn name(self) -> &'static str {
        self.name
    }

    pub fn mtype(self) -> MsgType {
        self.mtype
    }

    pub fn request_type(self) -> &'static str {
        self.request_type
    }

    pub fn response_type(self) -> Option<&'static str> {
        self.response_type
    }

    pub fn supports_call(self) -> bool {
        self.encode.is_some()
    }

    pub fn request_schema(self) -> MessageSchema {
        (self.request_schema)()
    }

    pub fn response_schema(self) -> Option<MessageSchema> {
        self.response_schema.map(|schema| schema())
    }

    pub fn decode(self, value: Value) -> Result<Payload> {
        (self.decode)(value)
    }

    pub fn encode(self, payload: Payload) -> Result<Value> {
        match self.encode {
            Some(encode) => encode(payload),
            None => Err(crate::Error::service(format!(
                "调试消息 `{}` 不支持 call",
                self.name
            ))),
        }
    }
}

/// 过程宏生成的 variant decoder 通过这个入口反序列化内部字段，避免要求使用方
/// 直接依赖 `serde_json`。
#[doc(hidden)]
pub fn decode_json<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(Into::into)
}

impl std::fmt::Debug for DebugMessageDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebugMessageDescriptor")
            .field("name", &self.name)
            .field("mtype", &self.mtype)
            .field("request_type", &self.request_type)
            .field("response_type", &self.response_type)
            .finish_non_exhaustive()
    }
}

fn decode_payload<T>(value: Value) -> Result<Payload>
where
    T: DeserializeOwned + IntoPayload,
{
    Ok(serde_json::from_value::<T>(value)?.into_payload())
}

fn encode_payload<T>(payload: Payload) -> Result<Value>
where
    T: FromPayload + Serialize,
{
    serde_json::to_value(T::from_payload(payload)?).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Deserialize, PartialEq, Serialize, rskynet_macros::MessageSchema)]
    #[schema(crate = crate)]
    struct Number {
        value: u32,
    }

    crate::boxed_payload!(Number);

    #[test]
    fn typed_descriptor_converts_json_and_boxed_payloads() {
        let descriptor = DebugMessageDescriptor::call::<Number, Number>("number", MsgType::USER);
        let payload = descriptor
            .decode(serde_json::json!({ "value": 42 }))
            .unwrap();
        assert_eq!(Number::from_payload(payload).unwrap(), Number { value: 42 });
        assert_eq!(
            descriptor.encode(Payload::of(Number { value: 7 })).unwrap(),
            serde_json::json!({ "value": 7 })
        );
        assert!(descriptor.supports_call());
    }

    #[test]
    fn send_descriptor_rejects_response_encoding() {
        let descriptor = DebugMessageDescriptor::send::<String>("notice", MsgType(42));
        assert_eq!(
            descriptor
                .decode(Value::String("hello".into()))
                .unwrap()
                .as_str(),
            Some("hello")
        );
        assert!(descriptor.encode(Payload::None).is_err());
        assert!(!descriptor.supports_call());
        assert_eq!(descriptor.request_schema()["type"], "string");
    }
}
