use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub const MAX_TOPIC_NAME: usize = 249;
pub const MAX_KEY_SIZE: usize = u16::MAX as usize;
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
pub const MAX_FRAME_SIZE: usize = 1 + 2 + MAX_TOPIC_NAME + 2 + MAX_KEY_SIZE + 4 + MAX_MESSAGE_SIZE;

const FRAME_HEADER_SIZE: usize = size_of::<u32>();
const PRODUCE: u8 = 0x01;
const PRODUCE_ACK: u8 = 0x02;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Produce(ProduceRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    topic: String,
    key: Bytes,
    payload: Bytes,
}

impl ProduceRequest {
    pub fn new(topic: String, key: Bytes, payload: Bytes) -> Result<Self, ProtocolError> {
        validate_topic(&topic)?;
        validate_size(key.len(), MAX_KEY_SIZE, ProtocolError::KeyTooLarge)?;
        validate_size(
            payload.len(),
            MAX_MESSAGE_SIZE,
            ProtocolError::MessageTooLarge,
        )?;
        Ok(Self {
            topic,
            key,
            payload,
        })
    }

    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[must_use]
    pub const fn key(&self) -> &Bytes {
        &self.key
    }

    #[must_use]
    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Response {
    ProduceAck(ProduceAck),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceAck {
    pub partition: u32,
    pub offset: u64,
}

pub fn decode_request(buffer: &mut BytesMut) -> Result<Option<Request>, ProtocolError> {
    if buffer.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let frame_len = usize::try_from(u32::from_be_bytes(
        buffer[..FRAME_HEADER_SIZE]
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame)?,
    ))
    .map_err(|_| ProtocolError::FrameTooLarge)?;

    if frame_len == 0 {
        return Err(ProtocolError::InvalidFrame);
    }
    if frame_len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge);
    }
    if buffer.len() < FRAME_HEADER_SIZE + frame_len {
        return Ok(None);
    }

    buffer.advance(FRAME_HEADER_SIZE);
    let mut frame = buffer.split_to(frame_len);
    let opcode = frame.get_u8();

    match opcode {
        PRODUCE => decode_produce(frame).map(|request| Some(Request::Produce(request))),
        value => Err(ProtocolError::UnknownOpcode(value)),
    }
}

pub fn encode_request(request: &Request, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    match request {
        Request::Produce(request) => encode_produce(request, buffer),
    }
}

pub fn encode_response(response: Response, buffer: &mut BytesMut) {
    match response {
        Response::ProduceAck(ack) => {
            buffer.put_u32(1 + 4 + 8);
            buffer.put_u8(PRODUCE_ACK);
            buffer.put_u32(ack.partition);
            buffer.put_u64(ack.offset);
        }
    }
}

pub fn decode_response(buffer: &mut BytesMut) -> Result<Option<Response>, ProtocolError> {
    if buffer.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let frame_len = usize::try_from(u32::from_be_bytes(
        buffer[..FRAME_HEADER_SIZE]
            .try_into()
            .map_err(|_| ProtocolError::InvalidFrame)?,
    ))
    .map_err(|_| ProtocolError::FrameTooLarge)?;

    if frame_len == 0 {
        return Err(ProtocolError::InvalidFrame);
    }
    if frame_len > MAX_FRAME_SIZE {
        return Err(ProtocolError::FrameTooLarge);
    }
    if buffer.len() < FRAME_HEADER_SIZE + frame_len {
        return Ok(None);
    }

    buffer.advance(FRAME_HEADER_SIZE);
    let mut frame = buffer.split_to(frame_len);
    let opcode = frame.get_u8();
    match opcode {
        PRODUCE_ACK => {
            if frame.remaining() != size_of::<u32>() + size_of::<u64>() {
                return Err(ProtocolError::InvalidFrame);
            }
            Ok(Some(Response::ProduceAck(ProduceAck {
                partition: frame.get_u32(),
                offset: frame.get_u64(),
            })))
        }
        value => Err(ProtocolError::UnknownOpcode(value)),
    }
}

fn decode_produce(mut frame: BytesMut) -> Result<ProduceRequest, ProtocolError> {
    let topic_len = read_u16_len(&mut frame)?;
    validate_size(topic_len, MAX_TOPIC_NAME, ProtocolError::TopicTooLarge)?;
    let topic = read_bytes(&mut frame, topic_len)?;
    let topic = String::from_utf8(topic.to_vec()).map_err(|_| ProtocolError::InvalidTopic)?;
    if topic.is_empty() {
        return Err(ProtocolError::InvalidTopic);
    }

    let key_len = read_u16_len(&mut frame)?;
    validate_size(key_len, MAX_KEY_SIZE, ProtocolError::KeyTooLarge)?;
    let key = read_bytes(&mut frame, key_len)?;

    let payload_len = read_u32_len(&mut frame)?;
    validate_size(
        payload_len,
        MAX_MESSAGE_SIZE,
        ProtocolError::MessageTooLarge,
    )?;
    let payload = read_bytes(&mut frame, payload_len)?;

    if frame.has_remaining() {
        return Err(ProtocolError::InvalidFrame);
    }

    ProduceRequest::new(topic, key, payload)
}

fn encode_produce(request: &ProduceRequest, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    let frame_len = 1 + 2 + request.topic.len() + 2 + request.key.len() + 4 + request.payload.len();
    buffer.put_u32(u32::try_from(frame_len).map_err(|_| ProtocolError::FrameTooLarge)?);
    buffer.put_u8(PRODUCE);
    buffer.put_u16(u16::try_from(request.topic.len()).map_err(|_| ProtocolError::TopicTooLarge)?);
    buffer.extend_from_slice(request.topic.as_bytes());
    buffer.put_u16(u16::try_from(request.key.len()).map_err(|_| ProtocolError::KeyTooLarge)?);
    buffer.extend_from_slice(&request.key);
    buffer
        .put_u32(u32::try_from(request.payload.len()).map_err(|_| ProtocolError::MessageTooLarge)?);
    buffer.extend_from_slice(&request.payload);
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), ProtocolError> {
    validate_size(topic.len(), MAX_TOPIC_NAME, ProtocolError::TopicTooLarge)?;
    if topic.is_empty() {
        return Err(ProtocolError::InvalidTopic);
    }
    Ok(())
}

fn read_u16_len(buffer: &mut BytesMut) -> Result<usize, ProtocolError> {
    ensure_remaining(buffer, size_of::<u16>())?;
    Ok(usize::from(buffer.get_u16()))
}

fn read_u32_len(buffer: &mut BytesMut) -> Result<usize, ProtocolError> {
    ensure_remaining(buffer, size_of::<u32>())?;
    usize::try_from(buffer.get_u32()).map_err(|_| ProtocolError::FrameTooLarge)
}

fn read_bytes(buffer: &mut BytesMut, len: usize) -> Result<Bytes, ProtocolError> {
    ensure_remaining(buffer, len)?;
    Ok(buffer.split_to(len).freeze())
}

fn ensure_remaining(buffer: &BytesMut, required: usize) -> Result<(), ProtocolError> {
    if buffer.remaining() < required {
        return Err(ProtocolError::InvalidFrame);
    }
    Ok(())
}

const fn validate_size(
    actual: usize,
    maximum: usize,
    error: ProtocolError,
) -> Result<(), ProtocolError> {
    if actual > maximum {
        return Err(error);
    }
    Ok(())
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("frame is malformed or truncated")]
    InvalidFrame,
    #[error("frame exceeds the configured limit")]
    FrameTooLarge,
    #[error("unknown opcode {0:#04x}")]
    UnknownOpcode(u8),
    #[error("topic is empty or not valid UTF-8")]
    InvalidTopic,
    #[error("topic exceeds the configured limit")]
    TopicTooLarge,
    #[error("key exceeds the configured limit")]
    KeyTooLarge,
    #[error("message exceeds the configured limit")]
    MessageTooLarge,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::{
        MAX_FRAME_SIZE, ProduceRequest, ProtocolError, Request, decode_request, encode_request,
    };

    fn produce_request() -> Request {
        Request::Produce(
            ProduceRequest::new(
                "payments".to_owned(),
                Bytes::from_static(b"customer-123"),
                Bytes::from_static(br#"{"amount":150}"#),
            )
            .expect("request should be valid"),
        )
    }

    #[test]
    fn round_trips_produce_request() {
        let request = produce_request();
        let mut buffer = BytesMut::new();
        encode_request(&request, &mut buffer).expect("request should encode");

        assert_eq!(
            decode_request(&mut buffer).expect("frame should decode"),
            Some(request)
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn waits_for_complete_frame() {
        let mut encoded = BytesMut::new();
        encode_request(&produce_request(), &mut encoded).expect("request should encode");
        let split_at = encoded.len() - 1;
        let tail = encoded.split_off(split_at);

        assert_eq!(decode_request(&mut encoded).expect("prefix is valid"), None);

        encoded.extend_from_slice(&tail);
        assert!(
            decode_request(&mut encoded)
                .expect("frame should decode")
                .is_some()
        );
    }

    #[test]
    fn decodes_multiple_frames_from_one_buffer() {
        let request = produce_request();
        let mut buffer = BytesMut::new();
        encode_request(&request, &mut buffer).expect("first request should encode");
        encode_request(&request, &mut buffer).expect("second request should encode");

        assert_eq!(
            decode_request(&mut buffer).expect("first frame"),
            Some(request.clone())
        );
        assert_eq!(
            decode_request(&mut buffer).expect("second frame"),
            Some(request)
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn rejects_oversized_frame_before_receiving_payload() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(u32::try_from(MAX_FRAME_SIZE + 1).expect("limit should fit in u32"));

        assert_eq!(
            decode_request(&mut buffer),
            Err(ProtocolError::FrameTooLarge)
        );
    }
}
