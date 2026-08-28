use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use sevlamq_common::BrokerConfig;
use sevlamq_protocol::{
    FetchResponse, FetchedRecord, ProduceAck, Request, Response, decode_request, encode_response,
};
use sevlamq_storage::{PartitionLog, Record};
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
    let listener = TcpListener::bind(address).await?;
    let (storage, storage_worker) =
        start_storage_worker(PathBuf::from(&config.data_dir), config.max_segment_bytes);

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
                    let offset = storage
                        .append(
                            request.topic().to_owned(),
                            request.key().clone(),
                            request.payload().clone(),
                        )
                        .await?;
                    encode_response(
                        &Response::ProduceAck(ProduceAck {
                            partition: 0,
                            offset,
                        }),
                        &mut write_buffer,
                    )?;
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
    ) -> Result<u64, ConnectionError> {
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
        reply: oneshot::Sender<Result<u64, sevlamq_storage::StorageError>>,
    },
    Read {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        reply: oneshot::Sender<Result<Vec<FetchedRecord>, sevlamq_storage::StorageError>>,
    },
}

fn start_storage_worker(
    data_dir: PathBuf,
    max_segment_bytes: u64,
) -> (
    StorageHandle,
    JoinHandle<Result<(), sevlamq_storage::StorageError>>,
) {
    let (sender, mut receiver) = mpsc::channel(STORAGE_QUEUE_CAPACITY);
    let worker = tokio::task::spawn_blocking(move || {
        let mut logs = HashMap::<String, PartitionLog>::new();
        while let Some(command) = receiver.blocking_recv() {
            match command {
                StorageCommand::Append {
                    topic,
                    key,
                    value,
                    reply,
                } => {
                    let result =
                        append_record(&data_dir, max_segment_bytes, &mut logs, &topic, key, value);
                    let _ = reply.send(result);
                }
                StorageCommand::Read {
                    topic,
                    partition,
                    offset,
                    max_bytes,
                    reply,
                } => {
                    let result = read_records(
                        &data_dir,
                        max_segment_bytes,
                        &mut logs,
                        &topic,
                        partition,
                        offset,
                        max_bytes,
                    );
                    let _ = reply.send(result);
                }
            }
        }
        Ok(())
    });
    (StorageHandle { sender }, worker)
}

fn append_record(
    data_dir: &Path,
    max_segment_bytes: u64,
    logs: &mut HashMap<String, PartitionLog>,
    topic: &str,
    key: Bytes,
    value: Bytes,
) -> Result<u64, sevlamq_storage::StorageError> {
    if !logs.contains_key(topic) {
        logs.insert(
            topic.to_owned(),
            PartitionLog::open(data_dir, topic, 0, max_segment_bytes)?,
        );
    }
    let log = logs
        .get_mut(topic)
        .ok_or(sevlamq_storage::StorageError::InvalidTopic)?;
    let timestamp_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| sevlamq_storage::StorageError::InvalidTimestamp)?
            .as_millis(),
    )
    .map_err(|_| sevlamq_storage::StorageError::InvalidTimestamp)?;
    let key = (!key.is_empty()).then_some(key);
    log.append(&Record::new(key, value, timestamp_ms))
}

fn read_records(
    data_dir: &Path,
    max_segment_bytes: u64,
    logs: &mut HashMap<String, PartitionLog>,
    topic: &str,
    partition: u32,
    offset: u64,
    max_bytes: u32,
) -> Result<Vec<FetchedRecord>, sevlamq_storage::StorageError> {
    if partition != 0 {
        return Err(sevlamq_storage::StorageError::UnknownPartition(partition));
    }
    if !logs.contains_key(topic) {
        logs.insert(
            topic.to_owned(),
            PartitionLog::open(data_dir, topic, 0, max_segment_bytes)?,
        );
    }
    let log = logs
        .get(topic)
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
    use std::time::Duration;

    use bytes::Bytes;
    use sevlamq_client::Client;
    use tempfile::tempdir;
    use tokio::{net::TcpListener, time::timeout};

    use super::{handle_connection, start_storage_worker};

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
            let (storage, storage_worker) = start_storage_worker(data_dir.path().to_owned(), 1024);
            let connection_storage = storage.clone();
            let server = tokio::spawn(async move {
                let (stream, peer) = listener.accept().await.expect("connection should arrive");
                handle_connection(stream, peer, 1, connection_storage).await
            });

            let mut client = Client::connect(address)
                .await
                .expect("client should connect");
            for (expected_offset, value) in ["zero", "one", "two"].into_iter().enumerate() {
                let ack = client
                    .produce(
                        "payments".to_owned(),
                        Bytes::from_static(b"customer-123"),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                    .await
                    .expect("broker should acknowledge produce");
                assert_eq!(ack.partition, 0);
                assert_eq!(
                    ack.offset,
                    u64::try_from(expected_offset).expect("offset should fit in u64")
                );
            }

            let response = client
                .fetch("payments".to_owned(), 0, 1, 1024)
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
