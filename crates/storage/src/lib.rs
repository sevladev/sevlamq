use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::hash;
use thiserror::Error;

pub const MAX_RECORD_SIZE: usize = 16 * 1024 * 1024;

const RECORD_VERSION: u8 = 1;
const LENGTH_SIZE: usize = size_of::<u32>();
const CRC_SIZE: usize = size_of::<u32>();
const BODY_FIXED_SIZE: usize =
    size_of::<u8>() + size_of::<u64>() * 2 + size_of::<i32>() + size_of::<u32>();
const MIN_RECORD_SIZE: usize = CRC_SIZE + BODY_FIXED_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    key: Option<Bytes>,
    value: Bytes,
    timestamp_ms: u64,
}

impl Record {
    #[must_use]
    pub const fn new(key: Option<Bytes>, value: Bytes, timestamp_ms: u64) -> Self {
        Self {
            key,
            value,
            timestamp_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    offset: u64,
    timestamp_ms: u64,
    key: Option<Bytes>,
    value: Bytes,
}

impl StoredRecord {
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    #[must_use]
    pub const fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    #[must_use]
    pub const fn value(&self) -> &Bytes {
        &self.value
    }
}

pub struct PartitionLog {
    next_offset: u64,
    active_path: PathBuf,
    active_file: File,
}

impl PartitionLog {
    pub fn open(
        data_dir: impl AsRef<Path>,
        topic: &str,
        partition: u32,
    ) -> Result<Self, StorageError> {
        validate_topic(topic)?;
        let partition_dir = data_dir.as_ref().join(topic).join(partition.to_string());
        fs::create_dir_all(&partition_dir)?;
        let active_path = partition_dir.join(format!("{:020}.log", 0));
        ensure_supported_segments(&partition_dir, &active_path)?;
        let active_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&active_path)?;
        let (next_offset, valid_len) = recover_segment(&active_path)?;
        let actual_len = active_file.metadata()?.len();
        if valid_len < actual_len {
            active_file.set_len(valid_len)?;
        }

        Ok(Self {
            next_offset,
            active_path,
            active_file,
        })
    }

    pub fn create(
        data_dir: impl AsRef<Path>,
        topic: &str,
        partition: u32,
    ) -> Result<Self, StorageError> {
        validate_topic(topic)?;
        let partition_dir = data_dir.as_ref().join(topic).join(partition.to_string());
        fs::create_dir_all(&partition_dir)?;
        let active_path = partition_dir.join(format!("{:020}.log", 0));
        let active_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&active_path)?;

        Ok(Self {
            next_offset: 0,
            active_path,
            active_file,
        })
    }

    pub fn append(&mut self, record: &Record) -> Result<u64, StorageError> {
        let offset = self.next_offset;
        let encoded = encode_record(record, offset)?;
        self.active_file.write_all(&encoded)?;
        self.next_offset = self
            .next_offset
            .checked_add(1)
            .ok_or(StorageError::OffsetOverflow)?;
        Ok(offset)
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.active_file.flush()?;
        Ok(())
    }

    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<StoredRecord>, StorageError> {
        if max_bytes == 0 {
            return Err(StorageError::InvalidReadLimit);
        }

        let contents = fs::read(&self.active_path)?;
        let mut buffer = BytesMut::from(contents.as_slice());
        let mut records = Vec::new();
        let mut bytes_read = 0_usize;

        while !buffer.is_empty() {
            let before = buffer.len();
            let record = decode_record(&mut buffer)?.ok_or(StorageError::InvalidRecord)?;
            let encoded_size = before - buffer.len();
            if record.offset() < offset {
                continue;
            }
            if !records.is_empty() && bytes_read + encoded_size > max_bytes {
                break;
            }
            bytes_read += encoded_size;
            records.push(record);
        }

        Ok(records)
    }

    #[must_use]
    pub fn active_path(&self) -> &Path {
        &self.active_path
    }

    #[must_use]
    pub const fn next_offset(&self) -> u64 {
        self.next_offset
    }
}

fn recover_segment(path: &Path) -> Result<(u64, u64), StorageError> {
    let contents = fs::read(path)?;
    let total_len = contents.len();
    let mut buffer = BytesMut::from(contents.as_slice());
    let mut next_offset = 0_u64;

    loop {
        if let Some(record) = decode_record(&mut buffer)? {
            if record.offset() != next_offset {
                return Err(StorageError::UnexpectedOffset {
                    expected: next_offset,
                    actual: record.offset(),
                });
            }
            next_offset = next_offset
                .checked_add(1)
                .ok_or(StorageError::OffsetOverflow)?;
        } else {
            let valid_len = total_len - buffer.len();
            return Ok((
                next_offset,
                u64::try_from(valid_len).map_err(|_| StorageError::RecordTooLarge)?,
            ));
        }
    }
}

fn ensure_supported_segments(partition_dir: &Path, active_path: &Path) -> Result<(), StorageError> {
    for entry in fs::read_dir(partition_dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "log") && path != active_path {
            return Err(StorageError::MultipleSegmentsNotSupported);
        }
    }
    Ok(())
}

pub fn decode_record(buffer: &mut BytesMut) -> Result<Option<StoredRecord>, StorageError> {
    if buffer.len() < LENGTH_SIZE {
        return Ok(None);
    }

    let record_len = usize::try_from(u32::from_be_bytes(
        buffer[..LENGTH_SIZE]
            .try_into()
            .map_err(|_| StorageError::InvalidRecord)?,
    ))
    .map_err(|_| StorageError::RecordTooLarge)?;

    if record_len < MIN_RECORD_SIZE {
        return Err(StorageError::InvalidRecord);
    }
    if record_len > MAX_RECORD_SIZE {
        return Err(StorageError::RecordTooLarge);
    }
    if buffer.len() < LENGTH_SIZE + record_len {
        return Ok(None);
    }

    buffer.advance(LENGTH_SIZE);
    let mut encoded = buffer.split_to(record_len);
    let expected_crc = encoded.get_u32();
    if hash(&encoded) != expected_crc {
        return Err(StorageError::ChecksumMismatch);
    }

    let version = encoded.get_u8();
    if version != RECORD_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }

    let offset = encoded.get_u64();
    let timestamp_ms = encoded.get_u64();
    let key_len = encoded.get_i32();
    let value_len = usize::try_from(encoded.get_u32()).map_err(|_| StorageError::RecordTooLarge)?;
    let key = match key_len {
        -1 => None,
        0.. => {
            let len = usize::try_from(key_len).map_err(|_| StorageError::InvalidRecord)?;
            Some(read_bytes(&mut encoded, len)?)
        }
        _ => return Err(StorageError::InvalidRecord),
    };
    let value = read_bytes(&mut encoded, value_len)?;

    if encoded.has_remaining() {
        return Err(StorageError::InvalidRecord);
    }

    Ok(Some(StoredRecord {
        offset,
        timestamp_ms,
        key,
        value,
    }))
}

fn encode_record(record: &Record, offset: u64) -> Result<BytesMut, StorageError> {
    let key_len = record.key.as_ref().map_or(0, Bytes::len);
    let body_len = BODY_FIXED_SIZE + key_len + record.value.len();
    let record_len = CRC_SIZE + body_len;
    if record_len > MAX_RECORD_SIZE {
        return Err(StorageError::RecordTooLarge);
    }

    let mut body = BytesMut::with_capacity(body_len);
    body.put_u8(RECORD_VERSION);
    body.put_u64(offset);
    body.put_u64(record.timestamp_ms);
    match &record.key {
        Some(key) => {
            body.put_i32(i32::try_from(key.len()).map_err(|_| StorageError::RecordTooLarge)?);
        }
        None => body.put_i32(-1),
    }
    body.put_u32(u32::try_from(record.value.len()).map_err(|_| StorageError::RecordTooLarge)?);
    if let Some(key) = &record.key {
        body.extend_from_slice(key);
    }
    body.extend_from_slice(&record.value);

    let mut encoded = BytesMut::with_capacity(LENGTH_SIZE + record_len);
    encoded.put_u32(u32::try_from(record_len).map_err(|_| StorageError::RecordTooLarge)?);
    encoded.put_u32(hash(&body));
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

fn read_bytes(buffer: &mut BytesMut, len: usize) -> Result<Bytes, StorageError> {
    if buffer.remaining() < len {
        return Err(StorageError::InvalidRecord);
    }
    Ok(buffer.split_to(len).freeze())
}

fn validate_topic(topic: &str) -> Result<(), StorageError> {
    if topic.is_empty()
        || !topic
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(StorageError::InvalidTopic);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("record is malformed or truncated")]
    InvalidRecord,
    #[error("record exceeds the configured limit")]
    RecordTooLarge,
    #[error("record checksum does not match its contents")]
    ChecksumMismatch,
    #[error("record version {0} is not supported")]
    UnsupportedVersion(u8),
    #[error("topic name is invalid")]
    InvalidTopic,
    #[error("offset space is exhausted")]
    OffsetOverflow,
    #[error("expected offset {expected}, found {actual}")]
    UnexpectedOffset { expected: u64, actual: u64 },
    #[error("multiple segments are not supported yet")]
    MultipleSegmentsNotSupported,
    #[error("record timestamp is outside the supported range")]
    InvalidTimestamp,
    #[error("read limit must be greater than zero")]
    InvalidReadLimit,
    #[error("partition {0} does not exist")]
    UnknownPartition(u32),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
    };

    use bytes::{Bytes, BytesMut};
    use tempfile::tempdir;

    use super::{PartitionLog, Record, decode_record};

    #[test]
    fn appends_records_with_monotonic_offsets() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let mut log = PartitionLog::create(data_dir.path(), "payments", 0)
            .expect("partition log should be created");

        let first = Record::new(
            Some(Bytes::from_static(b"customer-123")),
            Bytes::from_static(br#"{"amount":150}"#),
            1_700_000_000_000,
        );
        let second = Record::new(None, Bytes::from_static(b"confirmed"), 1_700_000_000_001);

        assert_eq!(log.append(&first).expect("first append should work"), 0);
        assert_eq!(log.append(&second).expect("second append should work"), 1);
        log.flush().expect("log should flush");

        let mut bytes = BytesMut::from(
            fs::read(log.active_path())
                .expect("segment should be readable")
                .as_slice(),
        );
        let first = decode_record(&mut bytes)
            .expect("first record should decode")
            .expect("first record should be complete");
        let second = decode_record(&mut bytes)
            .expect("second record should decode")
            .expect("second record should be complete");

        assert_eq!(first.offset(), 0);
        assert_eq!(first.key(), Some(&Bytes::from_static(b"customer-123")));
        assert_eq!(second.offset(), 1);
        assert_eq!(second.key(), None);
        assert!(bytes.is_empty());
    }

    #[test]
    fn detects_corrupted_record() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let mut log = PartitionLog::create(data_dir.path(), "payments", 0)
            .expect("partition log should be created");
        log.append(&Record::new(None, Bytes::from_static(b"hello"), 1))
            .expect("append should work");
        log.flush().expect("log should flush");

        let mut bytes = fs::read(log.active_path()).expect("segment should be readable");
        let last = bytes.last_mut().expect("record should not be empty");
        *last ^= 0xff;

        assert!(matches!(
            decode_record(&mut BytesMut::from(bytes.as_slice())),
            Err(super::StorageError::ChecksumMismatch)
        ));
    }

    #[test]
    fn restores_next_offset_when_reopened() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let path = {
            let mut log = PartitionLog::create(data_dir.path(), "payments", 0)
                .expect("partition log should be created");
            log.append(&Record::new(None, Bytes::from_static(b"first"), 1))
                .expect("first append should work");
            log.append(&Record::new(None, Bytes::from_static(b"second"), 2))
                .expect("second append should work");
            log.flush().expect("log should flush");
            log.active_path().to_owned()
        };

        let mut log = PartitionLog::open(data_dir.path(), "payments", 0)
            .expect("partition log should recover");
        assert_eq!(log.next_offset(), 2);
        assert_eq!(
            log.append(&Record::new(None, Bytes::from_static(b"third"), 3))
                .expect("append after recovery should work"),
            2
        );
        log.flush().expect("log should flush");

        let mut bytes = BytesMut::from(fs::read(path).expect("segment should exist").as_slice());
        let offsets: Vec<u64> = std::iter::from_fn(|| {
            decode_record(&mut bytes)
                .expect("record should decode")
                .map(|record| record.offset())
        })
        .collect();
        assert_eq!(offsets, [0, 1, 2]);
    }

    #[test]
    fn truncates_an_incomplete_tail_when_reopened() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let path = {
            let mut log = PartitionLog::create(data_dir.path(), "payments", 0)
                .expect("partition log should be created");
            log.append(&Record::new(None, Bytes::from_static(b"complete"), 1))
                .expect("append should work");
            log.flush().expect("log should flush");
            log.active_path().to_owned()
        };
        let valid_len = fs::metadata(&path).expect("metadata should exist").len();
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("segment should open");
        file.write_all(&[0, 0, 0, 100, 1, 2, 3])
            .expect("partial record should be written");
        drop(file);

        let log = PartitionLog::open(data_dir.path(), "payments", 0)
            .expect("partition log should recover");

        assert_eq!(log.next_offset(), 1);
        assert_eq!(
            fs::metadata(log.active_path())
                .expect("metadata should exist")
                .len(),
            valid_len
        );
    }

    #[test]
    fn reads_records_from_requested_offset() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let mut log = PartitionLog::create(data_dir.path(), "payments", 0)
            .expect("partition log should be created");
        for value in ["zero", "one", "two"] {
            log.append(&Record::new(
                None,
                Bytes::copy_from_slice(value.as_bytes()),
                1,
            ))
            .expect("append should work");
        }
        log.flush().expect("log should flush");

        let records = log.read(1, 1024).expect("read should work");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset(), 1);
        assert_eq!(records[0].value(), &Bytes::from_static(b"one"));
        assert_eq!(records[1].offset(), 2);
        assert_eq!(records[1].value(), &Bytes::from_static(b"two"));
    }
}
