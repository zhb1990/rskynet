//! Dashboard 消息结构描述。

use serde_json::{Map, Value, json};

/// 可由 Dashboard 展示的 JSON Schema。
pub type MessageSchema = Value;

/// 为消息类型提供结构描述。业务类型通常用 `#[derive(rskynet::MessageSchema)]` 生成。
pub trait MessageSchemaType {
    fn message_schema() -> MessageSchema;
}

macro_rules! scalar_schema {
    ($kind:literal => $($ty:ty),+ $(,)?) => {$ (
        impl MessageSchemaType for $ty {
            fn message_schema() -> MessageSchema { json!({ "type": $kind }) }
        }
    )+ };
}

scalar_schema!("boolean" => bool);
scalar_schema!("integer" => i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);
scalar_schema!("number" => f32, f64);
scalar_schema!("string" => String, char);

impl MessageSchemaType for () {
    fn message_schema() -> MessageSchema {
        json!({ "type": "null" })
    }
}

impl<T: MessageSchemaType> MessageSchemaType for Option<T> {
    fn message_schema() -> MessageSchema {
        json!({ "anyOf": [T::message_schema(), { "type": "null" }] })
    }
}

impl<T: MessageSchemaType> MessageSchemaType for Vec<T> {
    fn message_schema() -> MessageSchema {
        json!({ "type": "array", "items": T::message_schema() })
    }
}

impl<T: MessageSchemaType, const N: usize> MessageSchemaType for [T; N] {
    fn message_schema() -> MessageSchema {
        json!({ "type": "array", "items": T::message_schema(), "minItems": N, "maxItems": N })
    }
}

impl<T: MessageSchemaType> MessageSchemaType for Box<T> {
    fn message_schema() -> MessageSchema {
        T::message_schema()
    }
}

impl<T: MessageSchemaType> MessageSchemaType for std::sync::Arc<T> {
    fn message_schema() -> MessageSchema {
        T::message_schema()
    }
}

impl<T: MessageSchemaType> MessageSchemaType for std::collections::HashMap<String, T> {
    fn message_schema() -> MessageSchema {
        json!({ "type": "object", "additionalProperties": T::message_schema() })
    }
}

impl<T: MessageSchemaType> MessageSchemaType for std::collections::BTreeMap<String, T> {
    fn message_schema() -> MessageSchema {
        json!({ "type": "object", "additionalProperties": T::message_schema() })
    }
}

macro_rules! tuple_schema {
    ($($name:ident),+ $(,)?) => {
        impl<$($name: MessageSchemaType),+> MessageSchemaType for ($($name,)+) {
            fn message_schema() -> MessageSchema {
                let items = vec![$($name::message_schema()),+];
                json!({ "type": "array", "minItems": items.len(), "maxItems": items.len(), "prefixItems": items })
            }
        }
    };
}

tuple_schema!(A);
tuple_schema!(A, B);
tuple_schema!(A, B, C);
tuple_schema!(A, B, C, D);
tuple_schema!(A, B, C, D, E);
tuple_schema!(A, B, C, D, E, F);

#[doc(hidden)]
pub fn schema_object(
    title: &'static str,
    description: Option<&'static str>,
    fields: Vec<(&'static str, MessageSchema, bool)>,
) -> MessageSchema {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for (name, schema, is_required) in fields {
        properties.insert(name.into(), schema);
        if is_required {
            required.push(Value::String(name.into()));
        }
    }
    let mut schema = Map::from_iter([
        ("type".into(), Value::String("object".into())),
        ("title".into(), Value::String(title.into())),
        ("properties".into(), Value::Object(properties)),
    ]);
    if !required.is_empty() {
        schema.insert("required".into(), Value::Array(required));
    }
    if let Some(description) = description {
        schema.insert("description".into(), description.into());
    }
    Value::Object(schema)
}

#[doc(hidden)]
pub fn schema_tuple(
    title: &'static str,
    description: Option<&'static str>,
    items: Vec<MessageSchema>,
) -> MessageSchema {
    let len = items.len();
    let mut schema = json!({ "type": "array", "title": title, "prefixItems": items, "minItems": len, "maxItems": len });
    if let Some(description) = description {
        schema["description"] = description.into();
    }
    schema
}

#[doc(hidden)]
pub fn schema_enum(
    title: &'static str,
    description: Option<&'static str>,
    variants: Vec<MessageSchema>,
) -> MessageSchema {
    let mut schema = json!({ "title": title, "oneOf": variants });
    if let Some(description) = description {
        schema["description"] = description.into();
    }
    schema
}

#[doc(hidden)]
pub fn schema_enum_unit(name: &'static str) -> MessageSchema {
    json!({ "type": "string", "const": name })
}

#[doc(hidden)]
pub fn schema_enum_newtype(name: &'static str, value: MessageSchema) -> MessageSchema {
    schema_named_variant(name, value)
}

#[doc(hidden)]
pub fn schema_enum_struct(name: &'static str, value: MessageSchema) -> MessageSchema {
    schema_named_variant(name, value)
}

fn schema_named_variant(name: &'static str, value: MessageSchema) -> MessageSchema {
    let properties = Map::from_iter([(name.into(), value)]);
    json!({ "type": "object", "properties": properties, "required": [name] })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(rskynet_macros::MessageSchema)]
    #[schema(crate = crate)]
    #[allow(dead_code)]
    struct Request {
        /// 用户可读的字段说明。
        text: String,
        optional: Option<u32>,
        #[serde(default, rename = "page_size")]
        limit: u16,
    }

    #[derive(rskynet_macros::MessageSchema)]
    #[schema(crate = crate)]
    #[serde(rename_all = "snake_case")]
    #[allow(dead_code)]
    enum Command {
        StartJob { request: Request },
        Stop,
    }

    #[test]
    fn derive_describes_fields_requiredness_docs_and_serde_names() {
        let schema = Request::message_schema();
        assert_eq!(schema["properties"]["text"]["type"], "string");
        assert_eq!(
            schema["properties"]["text"]["description"],
            "用户可读的字段说明。"
        );
        assert_eq!(schema["properties"]["page_size"]["type"], "integer");
        assert_eq!(schema["required"], json!(["text"]));

        let schema = Command::message_schema();
        assert_eq!(
            schema["oneOf"][0]["properties"]["start_job"]["type"], "object",
            "{schema}"
        );
        assert_eq!(schema["oneOf"][1]["const"], "stop");
    }
}
