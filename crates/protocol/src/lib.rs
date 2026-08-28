use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

pub const MAX_TOPIC_NAME: usize = 249;
pub const MAX_KEY_SIZE: usize = u16::MAX as usize;
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
pub const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;
pub const MAX_FETCH_BYTES: usize = 3 * 1024 * 1024;
pub const MAX_FETCH_WAIT_MS: u32 = 30_000;

const FRAME_HEADER_SIZE: usize = size_of::<u32>();
const PRODUCE: u8 = 0x01;
const PRODUCE_ACK: u8 = 0x02;
const FETCH: u8 = 0x10;
const FETCH_RESPONSE: u8 = 0x11;
const JOIN_GROUP: u8 = 0x20;
const JOIN_GROUP_RESPONSE: u8 = 0x21;
const HEARTBEAT: u8 = 0x22;
const LEAVE_GROUP: u8 = 0x23;
const COMMIT_OFFSET: u8 = 0x24;
const FETCH_COMMITTED_OFFSET: u8 = 0x25;
const GROUP_ACK: u8 = 0x26;
const COMMITTED_OFFSET_RESPONSE: u8 = 0x27;
const ERROR_RESPONSE: u8 = 0x7f;
const GROUP_FETCH: u8 = 0x28;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    JoinGroup(GroupMemberRequest),
    Heartbeat(GroupGenerationRequest),
    LeaveGroup(GroupGenerationRequest),
    CommitOffset(CommitOffsetRequest),
    FetchCommittedOffset(FetchCommittedOffsetRequest),
    GroupFetch(GroupFetchRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberRequest {
    pub group: String,
    pub topic: String,
    pub member: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupGenerationRequest {
    pub group: String,
    pub topic: String,
    pub member: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetRequest {
    pub group: String,
    pub topic: String,
    pub member: String,
    pub generation: u64,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchCommittedOffsetRequest {
    pub group: String,
    pub topic: String,
    pub partition: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupFetchRequest {
    pub group: String,
    pub topic: String,
    pub member: String,
    pub generation: u64,
    pub partition: u32,
    pub offset: u64,
    pub max_bytes: u32,
    pub max_wait_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    topic: String,
    partition: u32,
    offset: u64,
    max_bytes: u32,
    max_wait_ms: u32,
}

impl FetchRequest {
    pub fn new(
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<Self, ProtocolError> {
        validate_topic(&topic)?;
        let max_bytes_usize =
            usize::try_from(max_bytes).map_err(|_| ProtocolError::FetchTooLarge)?;
        if max_bytes == 0 || max_bytes_usize > MAX_FETCH_BYTES {
            return Err(ProtocolError::FetchTooLarge);
        }
        if max_wait_ms > MAX_FETCH_WAIT_MS {
            return Err(ProtocolError::FetchWaitTooLong);
        }
        Ok(Self {
            topic,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        })
    }

    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[must_use]
    pub const fn partition(&self) -> u32 {
        self.partition
    }

    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn max_bytes(&self) -> u32 {
        self.max_bytes
    }

    #[must_use]
    pub const fn max_wait_ms(&self) -> u32 {
        self.max_wait_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    topic: String,
    key: Bytes,
    payload: Bytes,
    ack_mode: AckMode,
    producer: Option<ProducerIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub id: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    Leader,
    Durable,
}

impl AckMode {
    const fn code(self) -> u8 {
        match self {
            Self::Leader => 1,
            Self::Durable => 2,
        }
    }

    const fn from_code(code: u8) -> Result<Self, ProtocolError> {
        match code {
            1 => Ok(Self::Leader),
            2 => Ok(Self::Durable),
            _ => Err(ProtocolError::InvalidAckMode(code)),
        }
    }
}

impl ProduceRequest {
    pub fn new(
        topic: String,
        key: Bytes,
        payload: Bytes,
        ack_mode: AckMode,
        producer: Option<ProducerIdentity>,
    ) -> Result<Self, ProtocolError> {
        validate_topic(&topic)?;
        validate_size(key.len(), MAX_KEY_SIZE, ProtocolError::KeyTooLarge)?;
        validate_size(
            payload.len(),
            MAX_MESSAGE_SIZE,
            ProtocolError::MessageTooLarge,
        )?;
        if let Some(producer) = &producer {
            validate_identifier(&producer.id)?;
        }
        Ok(Self {
            topic,
            key,
            payload,
            ack_mode,
            producer,
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

    #[must_use]
    pub const fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    #[must_use]
    pub const fn producer(&self) -> Option<&ProducerIdentity> {
        self.producer.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    ProduceAck(ProduceAck),
    Fetch(FetchResponse),
    JoinGroup(JoinGroupResponse),
    GroupAck,
    CommittedOffset(Option<u64>),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponse {
    generation: u64,
    partitions: Vec<u32>,
}

impl JoinGroupResponse {
    #[must_use]
    pub const fn new(generation: u64, partitions: Vec<u32>) -> Self {
        Self {
            generation,
            partitions,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn partitions(&self) -> &[u32] {
        &self.partitions
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProduceAck {
    pub partition: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    records: Vec<FetchedRecord>,
}

impl FetchResponse {
    #[must_use]
    pub const fn new(records: Vec<FetchedRecord>) -> Self {
        Self { records }
    }

    #[must_use]
    pub fn records(&self) -> &[FetchedRecord] {
        &self.records
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRecord {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Option<Bytes>,
    pub value: Bytes,
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
        FETCH => decode_fetch(frame).map(|request| Some(Request::Fetch(request))),
        JOIN_GROUP => decode_group_member(frame).map(|request| Some(Request::JoinGroup(request))),
        HEARTBEAT => {
            decode_group_generation(frame).map(|request| Some(Request::Heartbeat(request)))
        }
        LEAVE_GROUP => {
            decode_group_generation(frame).map(|request| Some(Request::LeaveGroup(request)))
        }
        COMMIT_OFFSET => {
            decode_commit_offset(frame).map(|request| Some(Request::CommitOffset(request)))
        }
        FETCH_COMMITTED_OFFSET => decode_fetch_committed(frame)
            .map(|request| Some(Request::FetchCommittedOffset(request))),
        GROUP_FETCH => decode_group_fetch(frame).map(|request| Some(Request::GroupFetch(request))),
        value => Err(ProtocolError::UnknownOpcode(value)),
    }
}

pub fn encode_request(request: &Request, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    match request {
        Request::Produce(request) => encode_produce(request, buffer),
        Request::Fetch(request) => encode_fetch(request, buffer),
        Request::JoinGroup(request) => encode_group_member(JOIN_GROUP, request, buffer),
        Request::Heartbeat(request) => encode_group_generation(HEARTBEAT, request, buffer),
        Request::LeaveGroup(request) => encode_group_generation(LEAVE_GROUP, request, buffer),
        Request::CommitOffset(request) => encode_commit_offset(request, buffer),
        Request::FetchCommittedOffset(request) => encode_fetch_committed(request, buffer),
        Request::GroupFetch(request) => encode_group_fetch(request, buffer),
    }
}

pub fn encode_response(response: &Response, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    match response {
        Response::ProduceAck(ack) => {
            buffer.put_u32(1 + 4 + 8);
            buffer.put_u8(PRODUCE_ACK);
            buffer.put_u32(ack.partition);
            buffer.put_u64(ack.offset);
            Ok(())
        }
        Response::Fetch(response) => encode_fetch_response(response, buffer),
        Response::JoinGroup(response) => {
            buffer.put_u32(
                1 + 8
                    + 4
                    + u32::try_from(response.partitions.len())
                        .map_err(|_| ProtocolError::FrameTooLarge)?
                        * 4,
            );
            buffer.put_u8(JOIN_GROUP_RESPONSE);
            buffer.put_u64(response.generation);
            buffer.put_u32(
                u32::try_from(response.partitions.len())
                    .map_err(|_| ProtocolError::FrameTooLarge)?,
            );
            for partition in &response.partitions {
                buffer.put_u32(*partition);
            }
            Ok(())
        }
        Response::GroupAck => {
            buffer.put_u32(1);
            buffer.put_u8(GROUP_ACK);
            Ok(())
        }
        Response::CommittedOffset(offset) => {
            buffer.put_u32(1 + 1 + 8);
            buffer.put_u8(COMMITTED_OFFSET_RESPONSE);
            buffer.put_u8(u8::from(offset.is_some()));
            buffer.put_u64(offset.unwrap_or_default());
            Ok(())
        }
        Response::Error(message) => encode_string_response(ERROR_RESPONSE, message, buffer),
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
        FETCH_RESPONSE => {
            decode_fetch_response(frame).map(|response| Some(Response::Fetch(response)))
        }
        JOIN_GROUP_RESPONSE => {
            ensure_remaining(&frame, 12)?;
            let generation = frame.get_u64();
            let count =
                usize::try_from(frame.get_u32()).map_err(|_| ProtocolError::FrameTooLarge)?;
            ensure_remaining(
                &frame,
                count.checked_mul(4).ok_or(ProtocolError::FrameTooLarge)?,
            )?;
            let partitions = (0..count).map(|_| frame.get_u32()).collect();
            if frame.has_remaining() {
                return Err(ProtocolError::InvalidFrame);
            }
            Ok(Some(Response::JoinGroup(JoinGroupResponse::new(
                generation, partitions,
            ))))
        }
        GROUP_ACK if !frame.has_remaining() => Ok(Some(Response::GroupAck)),
        COMMITTED_OFFSET_RESPONSE => {
            ensure_remaining(&frame, 9)?;
            let found = frame.get_u8();
            let offset = frame.get_u64();
            if frame.has_remaining() || found > 1 {
                return Err(ProtocolError::InvalidFrame);
            }
            Ok(Some(Response::CommittedOffset(
                (found == 1).then_some(offset),
            )))
        }
        ERROR_RESPONSE => {
            let message = decode_string(&mut frame)?;
            ensure_empty(&frame)?;
            Ok(Some(Response::Error(message)))
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
    ensure_remaining(&frame, 1)?;
    let ack_mode = AckMode::from_code(frame.get_u8())?;
    let producer_len = read_u16_len(&mut frame)?;
    let producer = if producer_len == 0 {
        ensure_remaining(&frame, size_of::<u64>())?;
        let _ = frame.get_u64();
        None
    } else {
        let id = read_bytes(&mut frame, producer_len)?;
        let id = String::from_utf8(id.to_vec()).map_err(|_| ProtocolError::InvalidProducerId)?;
        ensure_remaining(&frame, size_of::<u64>())?;
        Some(ProducerIdentity {
            id,
            sequence: frame.get_u64(),
        })
    };

    if frame.has_remaining() {
        return Err(ProtocolError::InvalidFrame);
    }

    ProduceRequest::new(topic, key, payload, ack_mode, producer)
}

fn decode_fetch(mut frame: BytesMut) -> Result<FetchRequest, ProtocolError> {
    let topic_len = read_u16_len(&mut frame)?;
    validate_size(topic_len, MAX_TOPIC_NAME, ProtocolError::TopicTooLarge)?;
    let topic = read_bytes(&mut frame, topic_len)?;
    let topic = String::from_utf8(topic.to_vec()).map_err(|_| ProtocolError::InvalidTopic)?;
    ensure_remaining(
        &frame,
        size_of::<u32>() + size_of::<u64>() + size_of::<u32>() * 2,
    )?;
    let partition = frame.get_u32();
    let offset = frame.get_u64();
    let max_bytes = frame.get_u32();
    let max_wait_ms = frame.get_u32();
    if frame.has_remaining() {
        return Err(ProtocolError::InvalidFrame);
    }
    FetchRequest::new(topic, partition, offset, max_bytes, max_wait_ms)
}

fn encode_produce(request: &ProduceRequest, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    let producer_len = request
        .producer
        .as_ref()
        .map_or(0, |producer| producer.id.len());
    let frame_len = 1
        + 2
        + request.topic.len()
        + 2
        + request.key.len()
        + 4
        + request.payload.len()
        + 1
        + 2
        + producer_len
        + 8;
    buffer.put_u32(u32::try_from(frame_len).map_err(|_| ProtocolError::FrameTooLarge)?);
    buffer.put_u8(PRODUCE);
    buffer.put_u16(u16::try_from(request.topic.len()).map_err(|_| ProtocolError::TopicTooLarge)?);
    buffer.extend_from_slice(request.topic.as_bytes());
    buffer.put_u16(u16::try_from(request.key.len()).map_err(|_| ProtocolError::KeyTooLarge)?);
    buffer.extend_from_slice(&request.key);
    buffer
        .put_u32(u32::try_from(request.payload.len()).map_err(|_| ProtocolError::MessageTooLarge)?);
    buffer.extend_from_slice(&request.payload);
    buffer.put_u8(request.ack_mode.code());
    buffer.put_u16(u16::try_from(producer_len).map_err(|_| ProtocolError::InvalidProducerId)?);
    if let Some(producer) = &request.producer {
        buffer.extend_from_slice(producer.id.as_bytes());
        buffer.put_u64(producer.sequence);
    } else {
        buffer.put_u64(0);
    }
    Ok(())
}

fn encode_fetch(request: &FetchRequest, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    let frame_len = 1 + 2 + request.topic.len() + 4 + 8 + 4 + 4;
    buffer.put_u32(u32::try_from(frame_len).map_err(|_| ProtocolError::FrameTooLarge)?);
    buffer.put_u8(FETCH);
    buffer.put_u16(u16::try_from(request.topic.len()).map_err(|_| ProtocolError::TopicTooLarge)?);
    buffer.extend_from_slice(request.topic.as_bytes());
    buffer.put_u32(request.partition);
    buffer.put_u64(request.offset);
    buffer.put_u32(request.max_bytes);
    buffer.put_u32(request.max_wait_ms);
    Ok(())
}

fn encode_fetch_response(
    response: &FetchResponse,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    let frame_start = buffer.len();
    buffer.put_u32(0);
    buffer.put_u8(FETCH_RESPONSE);
    buffer
        .put_u32(u32::try_from(response.records.len()).map_err(|_| ProtocolError::FrameTooLarge)?);
    for record in &response.records {
        buffer.put_u64(record.offset);
        buffer.put_u64(record.timestamp_ms);
        match &record.key {
            Some(key) => {
                buffer.put_i32(i32::try_from(key.len()).map_err(|_| ProtocolError::KeyTooLarge)?);
            }
            None => buffer.put_i32(-1),
        }
        buffer.put_u32(
            u32::try_from(record.value.len()).map_err(|_| ProtocolError::MessageTooLarge)?,
        );
        if let Some(key) = &record.key {
            buffer.extend_from_slice(key);
        }
        buffer.extend_from_slice(&record.value);
    }

    let frame_len = buffer.len() - frame_start - FRAME_HEADER_SIZE;
    if frame_len > MAX_FRAME_SIZE {
        buffer.truncate(frame_start);
        return Err(ProtocolError::FrameTooLarge);
    }
    let encoded_len = u32::try_from(frame_len).map_err(|_| ProtocolError::FrameTooLarge)?;
    buffer[frame_start..frame_start + FRAME_HEADER_SIZE]
        .copy_from_slice(&encoded_len.to_be_bytes());
    Ok(())
}

fn decode_fetch_response(mut frame: BytesMut) -> Result<FetchResponse, ProtocolError> {
    ensure_remaining(&frame, size_of::<u32>())?;
    let record_count =
        usize::try_from(frame.get_u32()).map_err(|_| ProtocolError::FrameTooLarge)?;
    let mut records = Vec::with_capacity(record_count.min(1024));

    for _ in 0..record_count {
        ensure_remaining(
            &frame,
            size_of::<u64>() * 2 + size_of::<i32>() + size_of::<u32>(),
        )?;
        let offset = frame.get_u64();
        let timestamp_ms = frame.get_u64();
        let key_len = frame.get_i32();
        let value_len =
            usize::try_from(frame.get_u32()).map_err(|_| ProtocolError::FrameTooLarge)?;
        let key = match key_len {
            -1 => None,
            0.. => {
                let len = usize::try_from(key_len).map_err(|_| ProtocolError::InvalidFrame)?;
                Some(read_bytes(&mut frame, len)?)
            }
            _ => return Err(ProtocolError::InvalidFrame),
        };
        let value = read_bytes(&mut frame, value_len)?;
        records.push(FetchedRecord {
            offset,
            timestamp_ms,
            key,
            value,
        });
    }
    if frame.has_remaining() {
        return Err(ProtocolError::InvalidFrame);
    }
    Ok(FetchResponse::new(records))
}

fn decode_group_member(mut frame: BytesMut) -> Result<GroupMemberRequest, ProtocolError> {
    let group = decode_string(&mut frame)?;
    let topic = decode_string(&mut frame)?;
    let member = decode_string(&mut frame)?;
    ensure_empty(&frame)?;
    Ok(GroupMemberRequest {
        group,
        topic,
        member,
    })
}

fn decode_group_generation(mut frame: BytesMut) -> Result<GroupGenerationRequest, ProtocolError> {
    let group = decode_string(&mut frame)?;
    let topic = decode_string(&mut frame)?;
    let member = decode_string(&mut frame)?;
    ensure_remaining(&frame, 8)?;
    let generation = frame.get_u64();
    ensure_empty(&frame)?;
    Ok(GroupGenerationRequest {
        group,
        topic,
        member,
        generation,
    })
}

fn decode_commit_offset(mut frame: BytesMut) -> Result<CommitOffsetRequest, ProtocolError> {
    let group = decode_string(&mut frame)?;
    let topic = decode_string(&mut frame)?;
    let member = decode_string(&mut frame)?;
    ensure_remaining(&frame, 20)?;
    let generation = frame.get_u64();
    let partition = frame.get_u32();
    let offset = frame.get_u64();
    ensure_empty(&frame)?;
    Ok(CommitOffsetRequest {
        group,
        topic,
        member,
        generation,
        partition,
        offset,
    })
}

fn decode_fetch_committed(
    mut frame: BytesMut,
) -> Result<FetchCommittedOffsetRequest, ProtocolError> {
    let group = decode_string(&mut frame)?;
    let topic = decode_string(&mut frame)?;
    ensure_remaining(&frame, 4)?;
    let partition = frame.get_u32();
    ensure_empty(&frame)?;
    Ok(FetchCommittedOffsetRequest {
        group,
        topic,
        partition,
    })
}

fn decode_group_fetch(mut frame: BytesMut) -> Result<GroupFetchRequest, ProtocolError> {
    let group = decode_string(&mut frame)?;
    let topic = decode_string(&mut frame)?;
    let member = decode_string(&mut frame)?;
    ensure_remaining(&frame, 28)?;
    let generation = frame.get_u64();
    let partition = frame.get_u32();
    let offset = frame.get_u64();
    let max_bytes = frame.get_u32();
    let max_wait_ms = frame.get_u32();
    ensure_empty(&frame)?;
    FetchRequest::new(topic.clone(), partition, offset, max_bytes, max_wait_ms)?;
    Ok(GroupFetchRequest {
        group,
        topic,
        member,
        generation,
        partition,
        offset,
        max_bytes,
        max_wait_ms,
    })
}

fn encode_group_member(
    opcode: u8,
    request: &GroupMemberRequest,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    encode_string_request(
        opcode,
        [&request.group, &request.topic, &request.member],
        buffer,
    )
    .map(|_| ())
}

fn encode_group_generation(
    opcode: u8,
    request: &GroupGenerationRequest,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    let start = encode_string_request(
        opcode,
        [&request.group, &request.topic, &request.member],
        buffer,
    )?;
    buffer.put_u64(request.generation);
    set_frame_len(buffer, start)
}

fn encode_commit_offset(
    request: &CommitOffsetRequest,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    let start = encode_string_request(
        COMMIT_OFFSET,
        [&request.group, &request.topic, &request.member],
        buffer,
    )?;
    buffer.put_u64(request.generation);
    buffer.put_u32(request.partition);
    buffer.put_u64(request.offset);
    set_frame_len(buffer, start)
}

fn encode_fetch_committed(
    request: &FetchCommittedOffsetRequest,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    let start = encode_string_request(
        FETCH_COMMITTED_OFFSET,
        [&request.group, &request.topic],
        buffer,
    )?;
    buffer.put_u32(request.partition);
    set_frame_len(buffer, start)
}

fn encode_group_fetch(
    request: &GroupFetchRequest,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    FetchRequest::new(
        request.topic.clone(),
        request.partition,
        request.offset,
        request.max_bytes,
        request.max_wait_ms,
    )?;
    let start = encode_string_request(
        GROUP_FETCH,
        [&request.group, &request.topic, &request.member],
        buffer,
    )?;
    buffer.put_u64(request.generation);
    buffer.put_u32(request.partition);
    buffer.put_u64(request.offset);
    buffer.put_u32(request.max_bytes);
    buffer.put_u32(request.max_wait_ms);
    set_frame_len(buffer, start)
}

fn encode_string_request<const N: usize>(
    opcode: u8,
    values: [&String; N],
    buffer: &mut BytesMut,
) -> Result<usize, ProtocolError> {
    let start = buffer.len();
    buffer.put_u32(0);
    buffer.put_u8(opcode);
    for value in values {
        encode_string(value, buffer)?;
    }
    set_frame_len(buffer, start)?;
    Ok(start)
}

fn encode_string_response(
    opcode: u8,
    value: &str,
    buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    let start = buffer.len();
    buffer.put_u32(0);
    buffer.put_u8(opcode);
    encode_string(value, buffer)?;
    set_frame_len(buffer, start)
}

fn encode_string(value: &str, buffer: &mut BytesMut) -> Result<(), ProtocolError> {
    buffer.put_u16(u16::try_from(value.len()).map_err(|_| ProtocolError::InvalidFrame)?);
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_string(buffer: &mut BytesMut) -> Result<String, ProtocolError> {
    let len = read_u16_len(buffer)?;
    let bytes = read_bytes(buffer, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError::InvalidFrame)
}

fn set_frame_len(buffer: &mut BytesMut, start: usize) -> Result<(), ProtocolError> {
    let len = buffer.len() - start - FRAME_HEADER_SIZE;
    buffer[start..start + FRAME_HEADER_SIZE].copy_from_slice(
        &u32::try_from(len)
            .map_err(|_| ProtocolError::FrameTooLarge)?
            .to_be_bytes(),
    );
    Ok(())
}

fn ensure_empty(buffer: &BytesMut) -> Result<(), ProtocolError> {
    if buffer.has_remaining() {
        return Err(ProtocolError::InvalidFrame);
    }
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), ProtocolError> {
    validate_size(topic.len(), MAX_TOPIC_NAME, ProtocolError::TopicTooLarge)?;
    if topic.is_empty() || topic.starts_with("__") {
        return Err(ProtocolError::InvalidTopic);
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty()
        || value.len() > MAX_TOPIC_NAME
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ProtocolError::InvalidProducerId);
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
    #[error("fetch byte limit is zero or exceeds the configured limit")]
    FetchTooLarge,
    #[error("fetch wait exceeds the configured limit")]
    FetchWaitTooLong,
    #[error("produce ack mode {0} is not supported")]
    InvalidAckMode(u8),
    #[error("producer id is invalid")]
    InvalidProducerId,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};

    use super::{
        FetchRequest, FetchResponse, FetchedRecord, MAX_FRAME_SIZE, ProduceRequest, ProtocolError,
        Request, Response, decode_request, decode_response, encode_request, encode_response,
    };

    fn produce_request() -> Request {
        Request::Produce(
            ProduceRequest::new(
                "payments".to_owned(),
                Bytes::from_static(b"customer-123"),
                Bytes::from_static(br#"{"amount":150}"#),
                super::AckMode::Leader,
                Some(super::ProducerIdentity {
                    id: "producer-a".to_owned(),
                    sequence: 0,
                }),
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

    #[test]
    fn round_trips_fetch_request() {
        let request = Request::Fetch(
            FetchRequest::new("payments".to_owned(), 0, 42, 4096, 5_000)
                .expect("fetch should be valid"),
        );
        let mut buffer = BytesMut::new();
        encode_request(&request, &mut buffer).expect("request should encode");

        assert_eq!(
            decode_request(&mut buffer).expect("request should decode"),
            Some(request)
        );
    }

    #[test]
    fn round_trips_fetch_response() {
        let response = Response::Fetch(FetchResponse::new(vec![FetchedRecord {
            offset: 42,
            timestamp_ms: 1_700_000_000_000,
            key: Some(Bytes::from_static(b"customer-123")),
            value: Bytes::from_static(b"hello"),
        }]));
        let mut buffer = BytesMut::new();
        encode_response(&response, &mut buffer).expect("response should encode");

        assert_eq!(
            decode_response(&mut buffer).expect("response should decode"),
            Some(response)
        );
    }

    #[test]
    fn round_trips_consumer_group_frames() {
        let request = Request::CommitOffset(super::CommitOffsetRequest {
            group: "workers".to_owned(),
            topic: "payments".to_owned(),
            member: "worker-a".to_owned(),
            generation: 7,
            partition: 2,
            offset: 42,
        });
        let mut buffer = BytesMut::new();
        encode_request(&request, &mut buffer).expect("group request should encode");
        assert_eq!(
            decode_request(&mut buffer).expect("group request should decode"),
            Some(request)
        );

        let response = Response::JoinGroup(super::JoinGroupResponse::new(7, vec![0, 2]));
        encode_response(&response, &mut buffer).expect("group response should encode");
        assert_eq!(
            decode_response(&mut buffer).expect("group response should decode"),
            Some(response)
        );
    }
}
