use std::{
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use sevlamq_client::Client;
use sevlamq_protocol::{AckMode, FetchedRecord, ProducerIdentity};
use tokio::io::AsyncWriteExt;

use crate::error::CliError;

const MAGIC: &[u8; 4] = b"SVR1";
const MAX_ERROR_BYTES: usize = 4 * 1024;

pub(super) struct MessageContext {
    pub original_topic: String,
    pub original_partition: u32,
    pub original_offset: u64,
    pub original_timestamp_ms: u64,
    pub attempt: u32,
    pub available_at_ms: u64,
    pub key: Bytes,
    pub payload: Bytes,
}

pub(super) fn context(
    consumed_topic: &str,
    partition: u32,
    record: &FetchedRecord,
) -> Result<MessageContext, CliError> {
    if !record.value.starts_with(MAGIC) {
        return Ok(MessageContext {
            original_topic: consumed_topic.to_owned(),
            original_partition: partition,
            original_offset: record.offset,
            original_timestamp_ms: record.timestamp_ms,
            attempt: 0,
            available_at_ms: 0,
            key: record.key.clone().unwrap_or_default(),
            payload: record.value.clone(),
        });
    }
    decode_envelope(record)
}

pub(super) async fn wait_until_available(context: &MessageContext) -> Result<(), CliError> {
    let now = now_ms()?;
    if context.available_at_ms > now {
        tokio::time::sleep(Duration::from_millis(context.available_at_ms - now)).await;
    }
    Ok(())
}

pub(super) async fn run_handler(
    handler: Option<&str>,
    timeout_ms: u64,
    payload: &Bytes,
) -> Result<Result<(), String>, CliError> {
    let Some(handler) = handler else {
        return Ok(Ok(()));
    };
    let mut command = tokio::process::Command::new(handler);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let mut stdin = child.stdin.take().ok_or(CliError::MissingHandlerStdin)?;
    stdin.write_all(payload).await?;
    stdin.shutdown().await?;
    drop(stdin);
    let output =
        match tokio::time::timeout(Duration::from_millis(timeout_ms), child.wait_with_output())
            .await
        {
            Ok(output) => output?,
            Err(_) => return Ok(Err(format!("handler timed out after {timeout_ms}ms"))),
        };
    if output.status.success() {
        return Ok(Ok(()));
    }
    let error_len = output.stderr.len().min(MAX_ERROR_BYTES);
    let error = String::from_utf8_lossy(&output.stderr[..error_len])
        .trim()
        .to_owned();
    if error.is_empty() {
        Ok(Err(format!("handler exited with status {}", output.status)))
    } else {
        Ok(Err(error))
    }
}

pub(super) async fn publish_failure(
    client: &mut Client,
    group: &str,
    context: &MessageContext,
    error: &str,
    retry_delays_ms: &[u64],
) -> Result<String, CliError> {
    let attempt_index = usize::try_from(context.attempt).map_err(|_| CliError::AttemptOverflow)?;
    let (topic, next_attempt, available_at_ms) =
        if let Some(delay) = retry_delays_ms.get(attempt_index) {
            (
                format!("{}.retry.{}", context.original_topic, format_delay(*delay)),
                context
                    .attempt
                    .checked_add(1)
                    .ok_or(CliError::AttemptOverflow)?,
                now_ms()?.saturating_add(*delay),
            )
        } else {
            (
                format!("{}.DLQ", context.original_topic),
                context.attempt,
                now_ms()?,
            )
        };
    let envelope = encode_envelope(context, next_attempt, available_at_ms, error)?;
    let producer_id = retry_producer_id(group, context, next_attempt, &topic);
    let routing_key = if context.key.is_empty() {
        Bytes::copy_from_slice(producer_id.as_bytes())
    } else {
        context.key.clone()
    };
    client
        .produce(
            topic.clone(),
            routing_key,
            envelope,
            AckMode::Durable,
            Some(ProducerIdentity {
                id: producer_id,
                sequence: 0,
            }),
        )
        .await?;
    Ok(topic)
}

fn encode_envelope(
    context: &MessageContext,
    attempt: u32,
    available_at_ms: u64,
    error: &str,
) -> Result<Bytes, CliError> {
    let error = error.as_bytes();
    let error = &error[..error.len().min(MAX_ERROR_BYTES)];
    let topic_len =
        u16::try_from(context.original_topic.len()).map_err(|_| CliError::RetryEnvelopeTooLarge)?;
    let error_len = u16::try_from(error.len()).map_err(|_| CliError::RetryEnvelopeTooLarge)?;
    let payload_len =
        u32::try_from(context.payload.len()).map_err(|_| CliError::RetryEnvelopeTooLarge)?;
    let key_len = u32::try_from(context.key.len()).map_err(|_| CliError::RetryEnvelopeTooLarge)?;
    let mut encoded = BytesMut::new();
    encoded.extend_from_slice(MAGIC);
    encoded.put_u32(attempt);
    encoded.put_u64(available_at_ms);
    encoded.put_u32(context.original_partition);
    encoded.put_u64(context.original_offset);
    encoded.put_u64(context.original_timestamp_ms);
    encoded.put_u16(topic_len);
    encoded.put_u16(error_len);
    encoded.put_u32(key_len);
    encoded.put_u32(payload_len);
    encoded.extend_from_slice(context.original_topic.as_bytes());
    encoded.extend_from_slice(error);
    encoded.extend_from_slice(&context.key);
    encoded.extend_from_slice(&context.payload);
    if encoded.len() > sevlamq_protocol::MAX_MESSAGE_SIZE {
        return Err(CliError::RetryEnvelopeTooLarge);
    }
    Ok(encoded.freeze())
}

fn decode_envelope(record: &FetchedRecord) -> Result<MessageContext, CliError> {
    let mut encoded = record.value.clone();
    if encoded.remaining() < 48 {
        return Err(CliError::InvalidRetryEnvelope);
    }
    encoded.advance(MAGIC.len());
    let attempt = encoded.get_u32();
    let available_at_ms = encoded.get_u64();
    let original_partition = encoded.get_u32();
    let original_offset = encoded.get_u64();
    let original_timestamp_ms = encoded.get_u64();
    let topic_len = usize::from(encoded.get_u16());
    let error_len = usize::from(encoded.get_u16());
    let key_len = usize::try_from(encoded.get_u32()).map_err(|_| CliError::InvalidRetryEnvelope)?;
    let payload_len =
        usize::try_from(encoded.get_u32()).map_err(|_| CliError::InvalidRetryEnvelope)?;
    let required = topic_len
        .checked_add(error_len)
        .and_then(|size| size.checked_add(key_len))
        .and_then(|size| size.checked_add(payload_len))
        .ok_or(CliError::InvalidRetryEnvelope)?;
    if encoded.remaining() != required {
        return Err(CliError::InvalidRetryEnvelope);
    }
    let topic = encoded.copy_to_bytes(topic_len);
    let original_topic =
        String::from_utf8(topic.to_vec()).map_err(|_| CliError::InvalidRetryEnvelope)?;
    encoded.advance(error_len);
    let key = encoded.copy_to_bytes(key_len);
    let payload = encoded.copy_to_bytes(payload_len);
    Ok(MessageContext {
        original_topic,
        original_partition,
        original_offset,
        original_timestamp_ms,
        attempt,
        available_at_ms,
        key,
        payload,
    })
}

fn retry_producer_id(
    group: &str,
    context: &MessageContext,
    attempt: u32,
    destination: &str,
) -> String {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(group.as_bytes());
    hasher.update(context.original_topic.as_bytes());
    hasher.update(&context.original_partition.to_be_bytes());
    hasher.update(&context.original_offset.to_be_bytes());
    hasher.update(&attempt.to_be_bytes());
    hasher.update(destination.as_bytes());
    format!("retry-{:08x}-{attempt}", hasher.finalize())
}

fn format_delay(delay_ms: u64) -> String {
    if delay_ms.is_multiple_of(1_000) {
        format!("{}s", delay_ms / 1_000)
    } else {
        format!("{delay_ms}ms")
    }
}

fn now_ms() -> Result<u64, CliError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CliError::InvalidSystemTime)?
            .as_millis(),
    )
    .map_err(|_| CliError::InvalidSystemTime)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn preserves_original_message_through_retry_envelope() {
        let original = MessageContext {
            original_topic: "payments".to_owned(),
            original_partition: 2,
            original_offset: 41,
            original_timestamp_ms: 1_234,
            attempt: 0,
            available_at_ms: 0,
            key: Bytes::from_static(b"customer-123"),
            payload: Bytes::from_static(b"hello"),
        };
        let encoded = encode_envelope(&original, 1, 9_999, "temporary failure")
            .expect("retry envelope should encode");
        let retry_record = FetchedRecord {
            offset: 7,
            timestamp_ms: 5_678,
            key: None,
            value: encoded,
        };

        let decoded =
            context("payments.retry.1s", 0, &retry_record).expect("retry envelope should decode");

        assert_eq!(decoded.original_topic, "payments");
        assert_eq!(decoded.original_partition, 2);
        assert_eq!(decoded.original_offset, 41);
        assert_eq!(decoded.original_timestamp_ms, 1_234);
        assert_eq!(decoded.attempt, 1);
        assert_eq!(decoded.available_at_ms, 9_999);
        assert_eq!(decoded.key, Bytes::from_static(b"customer-123"));
        assert_eq!(decoded.payload, Bytes::from_static(b"hello"));
    }
}
