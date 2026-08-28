use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use sevlamq_common::BrokerConfig;
use sevlamq_protocol::{
    FetchResponse, FetchedRecord, ProduceAck, Request, Response, decode_request, encode_response,
};
use sevlamq_storage::{PartitionLog, Record, discover_partitions};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, info, warn};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
const STORAGE_QUEUE_CAPACITY: usize = 1024;

pub async fn run(config: &BrokerConfig) -> Result<(), BrokerError> {
    let address = config.socket_addr()?;
    let (storage, storage_worker) = start_storage_worker(
        PathBuf::from(&config.data_dir),
        config.max_segment_bytes,
        config.index_interval_bytes,
        config.default_partition_count,
    )
    .await?;
    let listener = TcpListener::bind(address).await?;

    info!(%address, data_dir = %config.data_dir, "broker started");
    accept_connections(&listener, storage.clone()).await?;
    info!("shutdown signal received");

    drop(storage);
    storage_worker.await??;
    drop(listener);
    info!("broker stopped");
    Ok(())
}

async fn accept_connections(
    listener: &TcpListener,
    storage: StorageHandle,
) -> Result<(), BrokerError> {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            signal = shutdown_signal() => {
                signal?;
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                connections.spawn(handle_connection(stream, peer, connection_id, storage.clone()));
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                report_connection_result(result);
            }
        }
    }

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        report_connection_result(result);
    }
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    connection_id: u64,
    storage: StorageHandle,
) -> Result<(), ConnectionError> {
    let mut read_buffer = BytesMut::with_capacity(8 * 1024);
    let mut write_buffer = BytesMut::with_capacity(128);
    debug!(connection_id, %peer, "connection opened");

    loop {
        let bytes_read = stream.read_buf(&mut read_buffer).await?;
        if bytes_read == 0 {
            if !read_buffer.is_empty() {
                return Err(ConnectionError::TruncatedFrame);
            }
            break;
        }

        while let Some(request) = decode_request(&mut read_buffer)? {
            match request {
                Request::Produce(request) => {
                    debug!(
                        connection_id,
                        %peer,
                        topic = %request.topic(),
                        payload_bytes = request.payload().len(),
                        "produce received"
                    );
                    let ack = storage
                        .append(
                            request.topic().to_owned(),
                            request.key().clone(),
                            request.payload().clone(),
                        )
                        .await?;
                    debug!(
                        connection_id,
                        %peer,
                        topic = %request.topic(),
                        partition = ack.partition,
                        offset = ack.offset,
                        "produce appended"
                    );
                    encode_response(&Response::ProduceAck(ack), &mut write_buffer)?;
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
                Request::Fetch(request) => {
                    debug!(
                        connection_id,
                        %peer,
                        topic = %request.topic(),
                        partition = request.partition(),
                        offset = request.offset(),
                        max_bytes = request.max_bytes(),
                        "fetch received"
                    );
                    let records = storage
                        .read(
                            request.topic().to_owned(),
                            request.partition(),
                            request.offset(),
                            request.max_bytes(),
                        )
                        .await?;
                    encode_response(
                        &Response::Fetch(FetchResponse::new(records)),
                        &mut write_buffer,
                    )?;
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
            }
        }
    }

    debug!(connection_id, %peer, "connection closed");
    Ok(())
}

#[derive(Clone)]
struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
}

impl StorageHandle {
    async fn append(
        &self,
        topic: String,
        key: Bytes,
        value: Bytes,
    ) -> Result<ProduceAck, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StorageCommand::Append {
                topic,
                key,
                value,
                reply,
            })
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)
    }

    async fn read(
        &self,
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> Result<Vec<FetchedRecord>, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StorageCommand::Read {
                topic,
                partition,
                offset,
                max_bytes,
                reply,
            })
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)
    }
}

enum StorageCommand {
    Append {
        topic: String,
        key: Bytes,
        value: Bytes,
        reply: oneshot::Sender<Result<ProduceAck, sevlamq_storage::StorageError>>,
    },
    Read {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        reply: oneshot::Sender<Result<Vec<FetchedRecord>, sevlamq_storage::StorageError>>,
    },
}

struct StorageSettings {
    data_dir: PathBuf,
    max_segment_bytes: u64,
    index_interval_bytes: u64,
    default_partition_count: u32,
}

type PartitionKey = (String, u32);
type PartitionLogs = HashMap<PartitionKey, PartitionLog>;

async fn start_storage_worker(
    data_dir: PathBuf,
    max_segment_bytes: u64,
    index_interval_bytes: u64,
    default_partition_count: u32,
) -> Result<
    (
        StorageHandle,
        JoinHandle<Result<(), sevlamq_storage::StorageError>>,
    ),
    BrokerError,
> {
    if default_partition_count == 0 {
        return Err(BrokerError::InvalidPartitionCount);
    }
    let settings = StorageSettings {
        data_dir,
        max_segment_bytes,
        index_interval_bytes,
        default_partition_count,
    };
    let (settings, mut logs) =
        tokio::task::spawn_blocking(move || recover_storage(settings)).await??;
    let (sender, mut receiver) = mpsc::channel(STORAGE_QUEUE_CAPACITY);
    let worker = tokio::task::spawn_blocking(move || {
        let mut round_robin = HashMap::<String, u32>::new();
        while let Some(command) = receiver.blocking_recv() {
            match command {
                StorageCommand::Append {
                    topic,
                    key,
                    value,
                    reply,
                } => {
                    let result =
                        append_record(&settings, &mut logs, &mut round_robin, &topic, key, value);
                    let _ = reply.send(result);
                }
                StorageCommand::Read {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                    reply,
                } => {
                    let result =
                        read_records(&settings, &mut logs, &topic, partition, offset, max_bytes);
                    let _ = reply.send(result);
                }
            }
        }
        Ok(())
    });
    Ok((StorageHandle { sender }, worker))
}

fn recover_storage(
    settings: StorageSettings,
) -> Result<(StorageSettings, PartitionLogs), BrokerError> {
    let identities = discover_partitions(&settings.data_dir)?;
    let mut logs = HashMap::with_capacity(identities.len());

    for identity in identities {
        let topic = identity.topic().to_owned();
        let partition = identity.partition();
        if partition >= settings.default_partition_count {
            return Err(BrokerError::PartitionOutsideConfiguredRange {
                topic,
                partition,
                partition_count: settings.default_partition_count,
            });
        }
        let log = PartitionLog::open(
            &settings.data_dir,
            &topic,
            partition,
            settings.max_segment_bytes,
            settings.index_interval_bytes,
        )?;
        info!(
            topic,
            partition,
            segments = log.segments().len(),
            next_offset = log.next_offset(),
            "partition recovered"
        );
        logs.insert((topic, partition), log);
    }

    info!(partitions = logs.len(), "storage recovery completed");
    Ok((settings, logs))
}

fn append_record(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    round_robin: &mut HashMap<String, u32>,
    topic: &str,
    key: Bytes,
    value: Bytes,
) -> Result<ProduceAck, sevlamq_storage::StorageError> {
    ensure_topic_partitions(settings, logs, topic)?;
    let partition = select_partition(settings.default_partition_count, round_robin, topic, &key);
    let partition_key = (topic.to_owned(), partition);
    let log = logs
        .get_mut(&partition_key)
        .ok_or(sevlamq_storage::StorageError::InvalidTopic)?;
    let timestamp_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| sevlamq_storage::StorageError::InvalidTimestamp)?
            .as_millis(),
    )
    .map_err(|_| sevlamq_storage::StorageError::InvalidTimestamp)?;
    let key = (!key.is_empty()).then_some(key);
    let offset = log.append(&Record::new(key, value, timestamp_ms))?;
    Ok(ProduceAck { partition, offset })
}

fn ensure_topic_partitions(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topic: &str,
) -> Result<(), sevlamq_storage::StorageError> {
    for partition in 0..settings.default_partition_count {
        let partition_key = (topic.to_owned(), partition);
        if let std::collections::hash_map::Entry::Vacant(entry) = logs.entry(partition_key) {
            entry.insert(PartitionLog::open(
                &settings.data_dir,
                topic,
                partition,
                settings.max_segment_bytes,
                settings.index_interval_bytes,
            )?);
        }
    }
    Ok(())
}

fn select_partition(
    partition_count: u32,
    round_robin: &mut HashMap<String, u32>,
    topic: &str,
    key: &Bytes,
) -> u32 {
    if !key.is_empty() {
        return crc32fast::hash(key) % partition_count;
    }

    let next = round_robin.entry(topic.to_owned()).or_default();
    let partition = *next;
    *next = (*next + 1) % partition_count;
    partition
}

fn read_records(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topic: &str,
    partition: u32,
    offset: u64,
    max_bytes: u32,
) -> Result<Vec<FetchedRecord>, sevlamq_storage::StorageError> {
    let partition_key = (topic.to_owned(), partition);
    if !logs.contains_key(&partition_key) {
        if partition != 0 {
            return Err(sevlamq_storage::StorageError::UnknownPartition(partition));
        }
        logs.insert(
            partition_key.clone(),
            PartitionLog::open(
                &settings.data_dir,
                topic,
                0,
                settings.max_segment_bytes,
                settings.index_interval_bytes,
            )?,
        );
    }
    let log = logs
        .get(&partition_key)
        .ok_or(sevlamq_storage::StorageError::InvalidTopic)?;
    let max_bytes =
        usize::try_from(max_bytes).map_err(|_| sevlamq_storage::StorageError::InvalidReadLimit)?;
    log.read(offset, max_bytes).map(|records| {
        records
            .into_iter()
            .map(|record| FetchedRecord {
                offset: record.offset(),
                timestamp_ms: record.timestamp_ms(),
                key: record.key().cloned(),
                value: record.value().clone(),
            })
            .collect()
    })
}

fn report_connection_result(result: Result<Result<(), ConnectionError>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "connection closed with error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!(%error, "connection task failed"),
    }
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error(transparent)]
    Config(#[from] sevlamq_common::ConfigError),
    #[error("broker I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Storage(#[from] sevlamq_storage::StorageError),
    #[error("storage worker failed: {0}")]
    StorageWorker(#[from] tokio::task::JoinError),
    #[error("default partition count must be greater than zero")]
    InvalidPartitionCount,
    #[error(
        "topic {topic} has partition {partition}, outside configured partition count {partition_count}"
    )]
    PartitionOutsideConfiguredRange {
        topic: String,
        partition: u32,
        partition_count: u32,
    },
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error("connection I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] sevlamq_protocol::ProtocolError),
    #[error("connection closed with a partial frame")]
    TruncatedFrame,
    #[error(transparent)]
    Storage(sevlamq_storage::StorageError),
    #[error("storage worker is unavailable")]
    StorageUnavailable,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, fs, time::Duration};

    use bytes::Bytes;
    use sevlamq_client::Client;
    use sevlamq_storage::{PartitionLog, Record};
    use tempfile::tempdir;
    use tokio::{net::TcpListener, time::timeout};

    use super::{handle_connection, select_partition, start_storage_worker};

    #[test]
    fn selects_partitions_deterministically_and_round_robins_unkeyed_messages() {
        let mut round_robin = HashMap::new();
        let key = Bytes::from_static(b"customer-123");

        assert_eq!(select_partition(3, &mut round_robin, "payments", &key), 1);
        assert_eq!(select_partition(3, &mut round_robin, "payments", &key), 1);

        let selected: Vec<u32> = (0..4)
            .map(|_| select_partition(3, &mut round_robin, "payments", &Bytes::new()))
            .collect();
        assert_eq!(selected, [0, 1, 2, 0]);
    }

    #[tokio::test]
    async fn recovers_existing_partitions_before_starting_worker() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let index_path = {
            let mut log = PartitionLog::open(data_dir.path(), "payments", 0, 1024, 64)
                .expect("partition log should be created");
            log.append(&Record::new(None, Bytes::from_static(b"hello"), 1))
                .expect("record should append");
            log.flush().expect("record should flush");
            log.active_path().with_extension("idx")
        };
        fs::write(&index_path, b"broken index").expect("index should be corrupted");

        let (storage, storage_worker) =
            start_storage_worker(data_dir.path().to_owned(), 1024, 64, 3)
                .await
                .expect("storage recovery should complete");

        assert_eq!(
            fs::metadata(index_path)
                .expect("index metadata should exist")
                .len(),
            16
        );
        drop(storage);
        storage_worker
            .await
            .expect("storage worker should finish")
            .expect("storage should close cleanly");
    }

    #[tokio::test]
    async fn produces_and_fetches_persisted_records() {
        timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let address = listener
                .local_addr()
                .expect("listener should have an address");
            let data_dir = tempdir().expect("temporary directory should be created");
            let (storage, storage_worker) =
                start_storage_worker(data_dir.path().to_owned(), 1024, 64, 3)
                    .await
                    .expect("storage worker should start");
            let connection_storage = storage.clone();
            let server = tokio::spawn(async move {
                let (stream, peer) = listener.accept().await.expect("connection should arrive");
                handle_connection(stream, peer, 1, connection_storage).await
            });

            let mut client = Client::connect(address)
                .await
                .expect("client should connect");
            let mut selected_partition = None;
            for (expected_offset, value) in ["zero", "one", "two"].into_iter().enumerate() {
                let ack = client
                    .produce(
                        "payments".to_owned(),
                        Bytes::from_static(b"customer-123"),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                    .await
                    .expect("broker should acknowledge produce");
                assert_eq!(
                    *selected_partition.get_or_insert(ack.partition),
                    ack.partition
                );
                assert_eq!(
                    ack.offset,
                    u64::try_from(expected_offset).expect("offset should fit in u64")
                );
            }

            for partition in 0..3 {
                assert!(
                    data_dir
                        .path()
                        .join("payments")
                        .join(partition.to_string())
                        .is_dir()
                );
            }
            let response = client
                .fetch(
                    "payments".to_owned(),
                    selected_partition.expect("partition should be selected"),
                    1,
                    1024,
                )
                .await
                .expect("broker should return records");
            assert_eq!(response.records().len(), 2);
            assert_eq!(response.records()[0].offset, 1);
            assert_eq!(response.records()[0].value, Bytes::from_static(b"one"));
            assert_eq!(response.records()[1].offset, 2);
            assert_eq!(response.records()[1].value, Bytes::from_static(b"two"));

            drop(client);
            server
                .await
                .expect("connection task should finish")
                .expect("connection should close cleanly");
            drop(storage);
            storage_worker
                .await
                .expect("storage worker should finish")
                .expect("storage should close cleanly");
        })
        .await
        .expect("test should not time out");
    }
}
