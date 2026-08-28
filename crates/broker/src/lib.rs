use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use sevlamq_common::BrokerConfig;
use sevlamq_protocol::{
    AckMode, FetchResponse, FetchedRecord, GroupFetchRequest, ProduceAck, Request, Response,
    decode_request, encode_response,
};
use sevlamq_storage::{
    PartitionLog, ProducerIdentity as StorageProducerIdentity, Record, discover_partitions,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout_at},
};
use tracing::{debug, info, warn};

mod groups;

use groups::{GroupCoordinator, GroupIdentity};

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
    let groups = GroupHandle::open(
        Path::new(&config.data_dir),
        config.default_partition_count,
        Duration::from_millis(config.group_session_timeout_ms),
    )?;

    info!(%address, data_dir = %config.data_dir, "broker started");
    accept_connections(&listener, storage.clone(), groups).await?;
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
    groups: GroupHandle,
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
                connections.spawn(handle_connection(stream, peer, connection_id, storage.clone(), groups.clone()));
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
    groups: GroupHandle,
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
                            request.ack_mode(),
                            request.producer().map(|producer| {
                                StorageProducerIdentity::new(producer.id.clone(), producer.sequence)
                            }),
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
                            request.max_wait_ms(),
                        )
                        .await?;
                    encode_response(
                        &Response::Fetch(FetchResponse::new(records)),
                        &mut write_buffer,
                    )?;
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
                Request::GroupFetch(request) => {
                    let response = group_fetch(&storage, &groups, request).await?;
                    encode_response(&response, &mut write_buffer)?;
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
                request @ (Request::JoinGroup(_)
                | Request::Heartbeat(_)
                | Request::LeaveGroup(_)
                | Request::CommitOffset(_)
                | Request::FetchCommittedOffset(_)) => {
                    let response = groups.execute(request).await;
                    encode_response(&response, &mut write_buffer)?;
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
            }
        }
    }

    debug!(connection_id, %peer, "connection closed");
    Ok(())
}

async fn group_fetch(
    storage: &StorageHandle,
    groups: &GroupHandle,
    request: GroupFetchRequest,
) -> Result<Response, ConnectionError> {
    match groups.execute(Request::GroupFetch(request.clone())).await {
        Response::GroupAck => storage
            .read(
                request.topic,
                request.partition,
                request.offset,
                request.max_bytes,
                request.max_wait_ms,
            )
            .await
            .map(|records| Response::Fetch(FetchResponse::new(records))),
        error @ Response::Error(_) => Ok(error),
        _ => Ok(Response::Error(
            "invalid group authorization response".to_owned(),
        )),
    }
}

#[derive(Clone)]
struct GroupHandle {
    coordinator: Arc<std::sync::Mutex<GroupCoordinator>>,
}

impl GroupHandle {
    fn open(
        data_dir: &Path,
        partition_count: u32,
        session_timeout: Duration,
    ) -> Result<Self, BrokerError> {
        let coordinator = Arc::new(std::sync::Mutex::new(GroupCoordinator::open(
            data_dir,
            partition_count,
            session_timeout,
        )?));
        start_group_session_sweeper(&coordinator);
        Ok(Self { coordinator })
    }

    async fn execute(&self, request: Request) -> Response {
        let coordinator = Arc::clone(&self.coordinator);
        match tokio::task::spawn_blocking(move || execute_group_request(&coordinator, request))
            .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => Response::Error(error.to_string()),
            Err(error) => Response::Error(format!("group coordinator task failed: {error}")),
        }
    }
}

fn start_group_session_sweeper(coordinator: &Arc<std::sync::Mutex<GroupCoordinator>>) {
    let coordinator = Arc::downgrade(coordinator);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(coordinator) = coordinator.upgrade() else {
                break;
            };
            let result = tokio::task::spawn_blocking(move || {
                coordinator
                    .lock()
                    .map_err(|_| groups::GroupError::CoordinatorPoisoned)?
                    .expire_sessions()
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(%error, "consumer group session sweep failed"),
                Err(error) => warn!(%error, "consumer group session sweep task failed"),
            }
        }
    });
}

fn execute_group_request(
    coordinator: &std::sync::Mutex<GroupCoordinator>,
    request: Request,
) -> Result<Response, groups::GroupError> {
    let mut coordinator = coordinator
        .lock()
        .map_err(|_| groups::GroupError::CoordinatorPoisoned)?;
    match request {
        Request::JoinGroup(request) => coordinator
            .join(&request.group, &request.topic, &request.member)
            .map(Response::JoinGroup),
        Request::Heartbeat(request) => coordinator
            .heartbeat(
                &request.group,
                &request.topic,
                &request.member,
                request.generation,
            )
            .map(|()| Response::GroupAck),
        Request::LeaveGroup(request) => coordinator
            .leave(
                &request.group,
                &request.topic,
                &request.member,
                request.generation,
            )
            .map(|()| Response::GroupAck),
        Request::CommitOffset(request) => coordinator
            .commit(
                &GroupIdentity {
                    group: &request.group,
                    topic: &request.topic,
                    member: &request.member,
                    generation: request.generation,
                },
                request.partition,
                request.offset,
            )
            .map(|()| Response::GroupAck),
        Request::FetchCommittedOffset(request) => coordinator
            .committed_offset(&request.group, &request.topic, request.partition)
            .map(Response::CommittedOffset),
        Request::GroupFetch(request) => coordinator
            .authorize_fetch(
                &GroupIdentity {
                    group: &request.group,
                    topic: &request.topic,
                    member: &request.member,
                    generation: request.generation,
                },
                request.partition,
            )
            .map(|()| Response::GroupAck),
        Request::Produce(_) | Request::Fetch(_) => unreachable!("group requests are filtered"),
    }
}

#[derive(Clone)]
struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
    new_records: Arc<Notify>,
}

impl StorageHandle {
    async fn append(
        &self,
        topic: String,
        key: Bytes,
        value: Bytes,
        ack_mode: AckMode,
        producer: Option<StorageProducerIdentity>,
    ) -> Result<ProduceAck, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StorageCommand::Append {
                topic,
                key,
                value,
                ack_mode,
                producer,
                reply,
            })
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?;
        let ack = response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)?;
        self.new_records.notify_waiters();
        Ok(ack)
    }

    async fn read(
        &self,
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<Vec<FetchedRecord>, ConnectionError> {
        let deadline = Instant::now() + Duration::from_millis(u64::from(max_wait_ms));

        loop {
            let notified = self.new_records.notified();
            tokio::pin!(notified);
            let _ = notified.as_mut().enable();
            let records = self.read_once(&topic, partition, offset, max_bytes).await?;
            if !records.is_empty() || max_wait_ms == 0 {
                return Ok(records);
            }
            if timeout_at(deadline, notified).await.is_err() {
                return Ok(records);
            }
        }
    }

    async fn read_once(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> Result<Vec<FetchedRecord>, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(StorageCommand::Read {
                topic: topic.to_owned(),
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
        ack_mode: AckMode,
        producer: Option<StorageProducerIdentity>,
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

struct AppendInput {
    topic: String,
    key: Bytes,
    value: Bytes,
    ack_mode: AckMode,
    producer: Option<StorageProducerIdentity>,
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
    let new_records = Arc::new(Notify::new());
    let worker = tokio::task::spawn_blocking(move || {
        let mut round_robin = HashMap::<String, u32>::new();
        while let Some(command) = receiver.blocking_recv() {
            match command {
                StorageCommand::Append {
                    topic,
                    key,
                    value,
                    ack_mode,
                    producer,
                    reply,
                } => {
                    let input = AppendInput {
                        topic,
                        key,
                        value,
                        ack_mode,
                        producer,
                    };
                    let result = append_record(&settings, &mut logs, &mut round_robin, input);
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
    Ok((
        StorageHandle {
            sender,
            new_records,
        },
        worker,
    ))
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
    input: AppendInput,
) -> Result<ProduceAck, sevlamq_storage::StorageError> {
    let AppendInput {
        topic,
        key,
        value,
        ack_mode,
        producer,
    } = input;
    ensure_topic_partitions(settings, logs, &topic)?;
    let routing_key = if key.is_empty() {
        producer.as_ref().map(|producer| producer.id().as_bytes())
    } else {
        Some(key.as_ref())
    };
    let partition = select_partition(
        settings.default_partition_count,
        round_robin,
        &topic,
        routing_key,
    );
    let partition_key = (topic, partition);
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
    let mut record = Record::new(key, value, timestamp_ms);
    if let Some(producer) = producer {
        record = record.with_producer(producer);
    }
    let offset = log.append(&record)?;
    if ack_mode == AckMode::Durable {
        log.sync_data()?;
    }
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
    key: Option<&[u8]>,
) -> u32 {
    if let Some(key) = key {
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
        if partition >= settings.default_partition_count {
            return Err(sevlamq_storage::StorageError::UnknownPartition(partition));
        }
        logs.insert(
            partition_key.clone(),
            PartitionLog::open(
                &settings.data_dir,
                topic,
                partition,
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
    #[error(transparent)]
    Group(#[from] groups::GroupError),
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
    use sevlamq_protocol::AckMode;
    use sevlamq_storage::{PartitionLog, Record};
    use tempfile::tempdir;
    use tokio::{net::TcpListener, time::timeout};

    use super::{GroupHandle, handle_connection, select_partition, start_storage_worker};

    #[test]
    fn selects_partitions_deterministically_and_round_robins_unkeyed_messages() {
        let mut round_robin = HashMap::new();
        let key = Bytes::from_static(b"customer-123");

        assert_eq!(
            select_partition(3, &mut round_robin, "payments", Some(&key)),
            1
        );
        assert_eq!(
            select_partition(3, &mut round_robin, "payments", Some(&key)),
            1
        );

        let selected: Vec<u32> = (0..4)
            .map(|_| select_partition(3, &mut round_robin, "payments", None))
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
    async fn wakes_long_poll_when_a_record_is_appended() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let (storage, storage_worker) =
            start_storage_worker(data_dir.path().to_owned(), 1024, 64, 1)
                .await
                .expect("storage worker should start");
        let waiting_storage = storage.clone();
        let waiting = tokio::spawn(async move {
            waiting_storage
                .read("payments".to_owned(), 0, 0, 1024, 1_000)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        storage
            .append(
                "payments".to_owned(),
                Bytes::new(),
                Bytes::from_static(b"hello"),
                AckMode::Durable,
                None,
            )
            .await
            .expect("append should wake waiting fetch");
        let records = waiting
            .await
            .expect("fetch task should finish")
            .expect("fetch should succeed");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value, Bytes::from_static(b"hello"));
        drop(storage);
        storage_worker
            .await
            .expect("storage worker should finish")
            .expect("storage should close cleanly");
    }

    #[tokio::test]
    async fn fetches_an_empty_partition_before_the_first_produce() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let (storage, storage_worker) =
            start_storage_worker(data_dir.path().to_owned(), 1024, 64, 3)
                .await
                .expect("storage worker should start");

        let records = storage
            .read("payments".to_owned(), 2, 0, 1024, 0)
            .await
            .expect("configured partition should open lazily");

        assert!(records.is_empty());
        assert!(data_dir.path().join("payments/2").is_dir());
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
            let groups = GroupHandle::open(data_dir.path(), 3, Duration::from_secs(10))
                .expect("group coordinator should start");
            let server = tokio::spawn(async move {
                let (stream, peer) = listener.accept().await.expect("connection should arrive");
                handle_connection(stream, peer, 1, connection_storage, groups).await
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
                        AckMode::Leader,
                        None,
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
                    0,
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
