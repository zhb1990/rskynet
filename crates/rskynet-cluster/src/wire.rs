use prost::{Enumeration, Message, Oneof};

pub(crate) const PROTOCOL_VERSION: u32 = 2;

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Envelope {
    #[prost(uint32, tag = "1")]
    pub version: u32,
    #[prost(enumeration = "Kind", tag = "2")]
    pub kind: i32,
    #[prost(uint32, tag = "3")]
    pub source_node: u32,
    #[prost(uint64, tag = "4")]
    pub request_id: u64,
    #[prost(uint32, tag = "5")]
    pub type_id: u32,
    #[prost(message, optional, tag = "6")]
    pub target: Option<Target>,
    #[prost(bytes = "vec", tag = "7")]
    pub payload: Vec<u8>,
    #[prost(string, tag = "8")]
    pub error: String,
    #[prost(string, tag = "9")]
    pub advertise: String,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct Target {
    #[prost(oneof = "target::Value", tags = "1, 2")]
    pub value: Option<target::Value>,
}

pub(crate) mod target {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Value {
        #[prost(uint64, tag = "1")]
        Handle(u64),
        #[prost(string, tag = "2")]
        Name(String),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub(crate) enum Kind {
    Hello = 0,
    Send = 1,
    Request = 2,
    Response = 3,
    Error = 4,
}

pub(crate) fn frame(envelope: &Envelope, max: usize) -> Result<Vec<u8>, &'static str> {
    let body = envelope.encode_to_vec();
    if body.len() > max || body.len() > u32::MAX as usize {
        return Err("消息帧超过长度上限");
    }
    let mut bytes = Vec::with_capacity(4 + body.len());
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub(crate) fn drain(buffer: &mut Vec<u8>, max: usize) -> Result<Vec<Envelope>, &'static str> {
    let mut result = Vec::new();
    let mut consumed = 0;
    while buffer.len().saturating_sub(consumed) >= 4 {
        let len = u32::from_be_bytes(buffer[consumed..consumed + 4].try_into().unwrap()) as usize;
        if len > max {
            return Err("消息帧超过长度上限");
        }
        if buffer.len() - consumed < 4 + len {
            break;
        }
        let start = consumed + 4;
        let envelope = Envelope::decode(&buffer[start..start + len])
            .map_err(|_| "Protobuf envelope 不合法")?;
        result.push(envelope);
        consumed = start + len;
    }
    if consumed != 0 {
        buffer.drain(..consumed);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_frames_are_reassembled() {
        let envelope = Envelope {
            version: PROTOCOL_VERSION,
            kind: Kind::Send as i32,
            payload: vec![1, 2, 3],
            ..Default::default()
        };
        let bytes = frame(&envelope, 1024).unwrap();
        let mut buffer = bytes[..3].to_vec();
        assert!(drain(&mut buffer, 1024).unwrap().is_empty());
        buffer.extend_from_slice(&bytes[3..]);
        assert_eq!(drain(&mut buffer, 1024).unwrap()[0].payload, vec![1, 2, 3]);
        assert!(buffer.is_empty());
    }

    #[test]
    fn oversized_and_malformed_frames_are_rejected() {
        assert!(drain(&mut vec![0, 0, 4, 1], 1024).is_err());
        let mut malformed = vec![0, 0, 0, 1, 0xff];
        assert!(drain(&mut malformed, 1024).is_err());
    }
}
