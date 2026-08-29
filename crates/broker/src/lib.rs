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
use sevlamq_common::Config;
use sevlamq_protocol::{
    AckMode, BatchRecord, ClusterMetadata, ClusterNode, FetchResponse, FetchedRecord,
    GroupFetchRequest, ProduceAck, ReplicateAck, ReplicateRecord, Request, Response, TopicMetadata,
    decode_replicate_ack, decode_replicate_record, decode_request, encode_replicate_ack,
    encode_replicate_record, encode_response,
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
mod observability;

use groups::{GroupCoordinator, GroupIdentity};
use observability::RuntimeMetrics;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub async fn run(config: &Config) -> Result<(), BrokerError> {
    let broker_config = &config.broker;
    let address = broker_config.socket_addr()?;
    let cluster = Arc::new(ClusterMetadata {
        responding_broker_id: config.cluster.broker_id,
        nodes: config
            .cluster_nodes()?
            .into_iter()
            .map(|node| ClusterNode {
                id: node.id,
                host: node.host,
                port: node.port,
                admin_port: node.admin_port,
                replication_port: node.replication_port,
            })
            .collect(),
    });
    let metrics = Arc::new(RuntimeMetrics::default());
    let storage_settings = StorageSettings {
        data_dir: PathBuf::from(&broker_config.data_dir),
        max_segment_bytes: broker_config.max_segment_bytes,
        index_interval_bytes: broker_config.index_interval_bytes,
        default_partition_count: broker_config.default_partition_count,
        retention_bytes: (broker_config.retention_bytes > 0)
            .then_some(broker_config.retention_bytes),
        retention_age: (broker_config.retention_ms > 0)
            .then(|| Duration::from_millis(broker_config.retention_ms)),
        auto_create_topics: broker_config.auto_create_topics,
        queue_capacity: broker_config.storage_queue_capacity,
        enqueue_timeout: Duration::from_millis(broker_config.storage_enqueue_timeout_ms),
    };
    let (storage, storage_worker) =
        start_storage_worker(storage_settings, Arc::clone(&metrics)).await?;
    let listener = TcpListener::bind(address).await?;
    let admin_address = config.admin.socket_addr()?;
    let admin_listener = TcpListener::bind(admin_address).await?;
    let local_node = cluster
        .nodes
        .iter()
        .find(|node| node.id == cluster.responding_broker_id)
        .ok_or(BrokerError::InvalidClusterMetadata)?;
    let replication_address: SocketAddr =
        format!("{}:{}", local_node.host, local_node.replication_port).parse()?;
    let replication_listener = TcpListener::bind(replication_address).await?;
    let cluster_runtime = ClusterRuntime {
        replicator: Replicator::new(Arc::clone(&cluster), Arc::clone(&metrics)),
        metadata: Arc::clone(&cluster),
    };
    let groups = GroupHandle::open(
        Path::new(&broker_config.data_dir),
        broker_config.default_partition_count,
        Duration::from_millis(broker_config.group_session_timeout_ms),
    )?;
    for topic in storage
        .list_topics()
        .await
        .map_err(|_| BrokerError::StorageUnavailable)?
    {
        groups.set_topic_partitions(&topic.topic, topic.partitions)?;
    }

    info!(broker_id = cluster.responding_broker_id, %address, %admin_address, %replication_address, data_dir = %broker_config.data_dir, "broker started");
    accept_connections(
        &listener,
        &admin_listener,
        &replication_listener,
        storage.clone(),
        groups,
        metrics,
        cluster_runtime,
    )
    .await?;
    info!("shutdown signal received");

    drop(storage);
    storage_worker.await??;
    drop(listener);
    info!("broker stopped");
    Ok(())
}

async fn accept_connections(
    listener: &TcpListener,
    admin_listener: &TcpListener,
    replication_listener: &TcpListener,
    storage: StorageHandle,
    groups: GroupHandle,
    metrics: Arc<RuntimeMetrics>,
    cluster: ClusterRuntime,
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
                connections.spawn(handle_connection(stream, peer, connection_id, storage.clone(), groups.clone(), Arc::clone(&metrics), cluster.clone()));
            }
            accepted = admin_listener.accept() => {
                let (stream, _) = accepted?;
                connections.spawn(handle_admin_connection(stream, storage.clone(), groups.clone(), Arc::clone(&metrics), Arc::clone(&cluster.metadata)));
            }
            accepted = replication_listener.accept() => {
                let (stream, peer) = accepted?;
                connections.spawn(handle_replication_connection(stream, peer, storage.clone(), Arc::clone(&metrics)));
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
    metrics: Arc<RuntimeMetrics>,
    cluster: ClusterRuntime,
) -> Result<(), ConnectionError> {
    let mut read_buffer = BytesMut::with_capacity(8 * 1024);
    let mut write_buffer = BytesMut::with_capacity(128);
    debug!(connection_id, %peer, "connection opened");
    metrics.connection_opened();
    let _connection_guard = ConnectionGuard(Arc::clone(&metrics));

    loop {
        let bytes_read = stream.read_buf(&mut read_buffer).await?;
        if bytes_read == 0 {
            if !read_buffer.is_empty() {
                return Err(ConnectionError::TruncatedFrame);
            }
            break;
        }

        while let Some(request) = decode_request(&mut read_buffer)? {
            let operation = match &request {
                Request::Produce(_) | Request::ProduceBatch(_) => Some("produce"),
                Request::Fetch(_) | Request::GroupFetch(_) => Some("fetch"),
                _ => None,
            };
            let started = Instant::now();
            let response = match process_request(
                request,
                &storage,
                &groups,
                &metrics,
                connection_id,
                peer,
                &cluster,
            )
            .await
            {
                Ok(response) => response,
                Err(ConnectionError::BrokerBusy) => {
                    metrics.broker_busy();
                    Response::Error("broker busy: storage queue timeout".to_owned())
                }
                Err(ConnectionError::Storage(error)) => Response::Error(error.to_string()),
                Err(error) => return Err(error),
            };
            match operation {
                Some("produce") => metrics.observe_produce(started.elapsed()),
                Some("fetch") => metrics.observe_fetch(started.elapsed()),
                _ => {}
            }
            encode_response(&response, &mut write_buffer)?;
            stream.write_all(&write_buffer).await?;
            write_buffer.clear();
        }
    }

    debug!(connection_id, %peer, "connection closed");
    Ok(())
}

async fn process_request(
    request: Request,
    storage: &StorageHandle,
    groups: &GroupHandle,
    metrics: &RuntimeMetrics,
    connection_id: u64,
    peer: SocketAddr,
    cluster: &ClusterRuntime,
) -> Result<Response, ConnectionError> {
    match request {
        Request::Produce(request) => {
            if let Some(error) = leadership_error(&cluster.metadata, request.partition()) {
                return Ok(error);
            }
            debug!(connection_id, %peer, topic = %request.topic(), "produce received");
            let ack = storage
                .append(
                    request.topic().to_owned(),
                    request.partition(),
                    request.key().clone(),
                    request.payload().clone(),
                    request.ack_mode(),
                    request.producer().map(|producer| {
                        StorageProducerIdentity::new(producer.id.clone(), producer.sequence)
                    }),
                )
                .await?;
            let record = storage
                .record_at(request.topic(), ack.partition, ack.offset)
                .await?;
            dispatch_replication(
                storage.clone(),
                cluster.replicator.clone(),
                request.topic().to_owned(),
                vec![(ack.partition, record)],
                request.ack_mode(),
            )
            .await?;
            metrics.produced(1, request.payload().len());
            Ok(Response::ProduceAck(ack))
        }
        Request::ProduceBatch(request) => {
            if let Some(error) = leadership_error(&cluster.metadata, request.partition()) {
                return Ok(error);
            }
            let response =
                produce_batch(storage, &cluster.replicator, connection_id, peer, &request).await?;
            metrics.produced(
                request.records().len(),
                request
                    .records()
                    .iter()
                    .map(|record| record.payload.len())
                    .sum(),
            );
            Ok(response)
        }
        Request::Fetch(request) => {
            if let Some(error) = leadership_error(&cluster.metadata, Some(request.partition())) {
                return Ok(error);
            }
            let records = storage
                .read(
                    request.topic().to_owned(),
                    request.partition(),
                    request.offset(),
                    request.max_bytes(),
                    request.max_wait_ms(),
                )
                .await?;
            metrics.fetched(
                records.len(),
                records.iter().map(|record| record.value.len()).sum(),
            );
            Ok(Response::Fetch(FetchResponse::new(records)))
        }
        Request::GroupFetch(request) => {
            if let Some(error) = leadership_error(&cluster.metadata, Some(request.partition)) {
                return Ok(error);
            }
            let response = group_fetch(storage, groups, request).await?;
            if let Response::Fetch(fetch) = &response {
                metrics.fetched(
                    fetch.records().len(),
                    fetch
                        .records()
                        .iter()
                        .map(|record| record.value.len())
                        .sum(),
                );
            }
            Ok(response)
        }
        request @ (Request::JoinGroup(_)
        | Request::Heartbeat(_)
        | Request::LeaveGroup(_)
        | Request::CommitOffset(_)
        | Request::FetchCommittedOffset(_)) => Ok(groups.execute(request).await),
        request @ (Request::CreateTopic(_)
        | Request::ListTopics
        | Request::DescribeTopic(_)
        | Request::ClusterMetadata) => {
            process_topic_request(request, storage, groups, &cluster.metadata).await
        }
    }
}

async fn process_topic_request(
    request: Request,
    storage: &StorageHandle,
    groups: &GroupHandle,
    cluster: &ClusterMetadata,
) -> Result<Response, ConnectionError> {
    match request {
        Request::CreateTopic(request) => {
            let metadata = match storage
                .create_topic(request.topic, request.partitions)
                .await
            {
                Ok(metadata) => metadata,
                Err(ConnectionError::Storage(error)) => {
                    return Ok(Response::Error(error.to_string()));
                }
                Err(error) => return Err(error),
            };
            groups
                .set_topic_partitions(&metadata.topic, metadata.partitions)
                .map_err(|_| ConnectionError::StorageUnavailable)?;
            Ok(Response::Topics(vec![with_leaders(metadata, cluster)]))
        }
        Request::ListTopics => storage.list_topics().await.map(|topics| {
            Response::Topics(
                topics
                    .into_iter()
                    .map(|metadata| with_leaders(metadata, cluster))
                    .collect(),
            )
        }),
        Request::DescribeTopic(topic) => match storage.describe_topic(topic).await {
            Ok(metadata) => Ok(Response::Topics(vec![with_leaders(metadata, cluster)])),
            Err(ConnectionError::Storage(error)) => Ok(Response::Error(error.to_string())),
            Err(error) => Err(error),
        },
        Request::ClusterMetadata => Ok(Response::ClusterMetadata(cluster.clone())),
        _ => unreachable!("only topic and cluster requests are dispatched here"),
    }
}

fn with_leaders(mut metadata: TopicMetadata, cluster: &ClusterMetadata) -> TopicMetadata {
    metadata.leaders = (0..metadata.partitions)
        .map(|partition| leader_for_partition(cluster, partition).id)
        .collect();
    metadata
}

fn leadership_error(cluster: &ClusterMetadata, partition: Option<u32>) -> Option<Response> {
    if cluster.nodes.len() == 1 {
        return None;
    }
    let Some(partition) = partition else {
        return Some(Response::Error(
            "partition is required for clustered produce".to_owned(),
        ));
    };
    let leader = leader_for_partition(cluster, partition);
    (leader.id != cluster.responding_broker_id).then(|| {
        Response::Error(format!(
            "not partition leader: partition={partition} leader_id={} leader={}:{}",
            leader.id, leader.host, leader.port
        ))
    })
}

fn leader_for_partition(cluster: &ClusterMetadata, partition: u32) -> &ClusterNode {
    let index = usize::try_from(partition).map_or(0, |value| value % cluster.nodes.len());
    &cluster.nodes[index]
}

#[derive(Clone)]
struct ClusterRuntime {
    metadata: Arc<ClusterMetadata>,
    replicator: Replicator,
}

#[derive(Clone)]
struct Replicator {
    cluster: Arc<ClusterMetadata>,
    metrics: Arc<RuntimeMetrics>,
}

impl Replicator {
    const fn new(cluster: Arc<ClusterMetadata>, metrics: Arc<RuntimeMetrics>) -> Self {
        Self { cluster, metrics }
    }

    async fn replicate(
        &self,
        storage: &StorageHandle,
        topic: &str,
        partition: u32,
        record: &FetchedRecord,
        ack_mode: AckMode,
    ) -> Result<(), ConnectionError> {
        for follower in self
            .cluster
            .nodes
            .iter()
            .filter(|node| node.id != self.cluster.responding_broker_id)
        {
            let request = ReplicateRecord {
                topic: topic.to_owned(),
                partition,
                offset: record.offset,
                timestamp_ms: record.timestamp_ms,
                key: record.key.clone(),
                value: record.value.clone(),
                durable: ack_mode == AckMode::Durable,
            };
            let mut next_offset = send_replica(follower, &request).await?;
            self.metrics.replication_sent(
                follower.id,
                topic,
                partition,
                next_offset,
                next_offset == request.offset.saturating_add(1),
            );
            if next_offset > record.offset.saturating_add(1) {
                return Err(ConnectionError::InvalidReplicationAck);
            }
            while next_offset <= record.offset {
                let missing = storage.record_at(topic, partition, next_offset).await?;
                let request = ReplicateRecord {
                    topic: topic.to_owned(),
                    partition,
                    offset: missing.offset,
                    timestamp_ms: missing.timestamp_ms,
                    key: missing.key,
                    value: missing.value,
                    durable: ack_mode == AckMode::Durable,
                };
                next_offset = send_replica(follower, &request).await?;
                self.metrics.replication_sent(
                    follower.id,
                    topic,
                    partition,
                    next_offset,
                    next_offset == request.offset.saturating_add(1),
                );
            }
        }
        Ok(())
    }
}

async fn send_replica(
    follower: &ClusterNode,
    request: &ReplicateRecord,
) -> Result<u64, ConnectionError> {
    let address = format!("{}:{}", follower.host, follower.replication_port);
    let mut stream = TcpStream::connect(address).await?;
    let mut buffer = BytesMut::new();
    encode_replicate_record(request, &mut buffer)?;
    stream.write_all(&buffer).await?;
    buffer.clear();
    loop {
        if let Some(ack) = decode_replicate_ack(&mut buffer)? {
            if ack.partition != request.partition {
                return Err(ConnectionError::InvalidReplicationAck);
            }
            return Ok(ack.next_offset);
        }
        if stream.read_buf(&mut buffer).await? == 0 {
            return Err(ConnectionError::ReplicationClosed);
        }
    }
}

async fn handle_replication_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    storage: StorageHandle,
    metrics: Arc<RuntimeMetrics>,
) -> Result<(), ConnectionError> {
    let mut read_buffer = BytesMut::with_capacity(8 * 1024);
    let mut write_buffer = BytesMut::with_capacity(32);
    loop {
        if stream.read_buf(&mut read_buffer).await? == 0 {
            if read_buffer.is_empty() {
                return Ok(());
            }
            return Err(ConnectionError::TruncatedFrame);
        }
        while let Some(record) = decode_replicate_record(&mut read_buffer)? {
            let expected = storage
                .partition_offset(&record.topic, record.partition)
                .await?;
            let next_offset = if record.offset > expected {
                expected
            } else {
                storage.replicate(record.clone()).await?
            };
            if next_offset > expected {
                metrics.replication_received();
            }
            encode_replicate_ack(
                ReplicateAck {
                    partition: record.partition,
                    next_offset,
                },
                &mut write_buffer,
            )?;
            stream.write_all(&write_buffer).await?;
            write_buffer.clear();
            debug!(%peer, topic = %record.topic, partition = record.partition, offset = record.offset, "record replicated");
        }
    }
}

struct ConnectionGuard(Arc<RuntimeMetrics>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connection_closed();
    }
}

async fn produce_batch(
    storage: &StorageHandle,
    replicator: &Replicator,
    connection_id: u64,
    peer: SocketAddr,
    request: &sevlamq_protocol::ProduceBatchRequest,
) -> Result<Response, ConnectionError> {
    debug!(
        connection_id,
        %peer,
        topic = %request.topic(),
        records = request.records().len(),
        compression = ?request.compression(),
        "produce batch received"
    );
    let acks = storage
        .append_batch(
            request.topic().to_owned(),
            request.partition(),
            request.records().to_vec(),
            request.ack_mode(),
        )
        .await?;
    let mut records = Vec::with_capacity(acks.len());
    for ack in &acks {
        records.push((
            ack.partition,
            storage
                .record_at(request.topic(), ack.partition, ack.offset)
                .await?,
        ));
    }
    dispatch_replication(
        storage.clone(),
        replicator.clone(),
        request.topic().to_owned(),
        records,
        request.ack_mode(),
    )
    .await?;
    Ok(Response::ProduceBatchAck(acks))
}

async fn dispatch_replication(
    storage: StorageHandle,
    replicator: Replicator,
    topic: String,
    records: Vec<(u32, FetchedRecord)>,
    ack_mode: AckMode,
) -> Result<(), ConnectionError> {
    if ack_mode == AckMode::Leader {
        tokio::spawn(async move {
            if let Err(error) =
                replicate_records(&storage, &replicator, &topic, &records, ack_mode).await
            {
                replicator.metrics.replication_failed();
                warn!(%error, %topic, "asynchronous replication failed");
            }
        });
        Ok(())
    } else {
        let result = replicate_records(&storage, &replicator, &topic, &records, ack_mode).await;
        if result.is_err() {
            replicator.metrics.replication_failed();
        }
        result
    }
}

async fn replicate_records(
    storage: &StorageHandle,
    replicator: &Replicator,
    topic: &str,
    records: &[(u32, FetchedRecord)],
    ack_mode: AckMode,
) -> Result<(), ConnectionError> {
    for (partition, record) in records {
        replicator
            .replicate(storage, topic, *partition, record, ack_mode)
            .await?;
    }
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

async fn handle_admin_connection(
    mut stream: TcpStream,
    storage: StorageHandle,
    groups: GroupHandle,
    metrics: Arc<RuntimeMetrics>,
    cluster: Arc<ClusterMetadata>,
) -> Result<(), ConnectionError> {
    const MAX_REQUEST_BYTES: usize = 8 * 1024;
    let mut request = BytesMut::with_capacity(1024);
    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() >= MAX_REQUEST_BYTES {
            write_http_response(&mut stream, "413 Payload Too Large", "text/plain", "").await?;
            return Ok(());
        }
        if stream.read_buf(&mut request).await? == 0 {
            return Ok(());
        }
    }
    let request_line = request
        .as_ref()
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| std::str::from_utf8(line).ok())
        .unwrap_or_default();
    let path = request_line.split_ascii_whitespace().nth(1);
    match path {
        Some("/health/live") => {
            write_http_response(&mut stream, "200 OK", "text/plain", "ok\n").await
        }
        Some("/health/ready") => {
            let ready = storage.snapshot().await.is_ok() && groups.snapshot().await.is_ok();
            if ready {
                write_http_response(&mut stream, "200 OK", "text/plain", "ready\n").await
            } else {
                write_http_response(
                    &mut stream,
                    "503 Service Unavailable",
                    "text/plain",
                    "not ready\n",
                )
                .await
            }
        }
        Some("/metrics") => {
            let queue_depth = storage.queue_depth();
            let partitions = storage.snapshot().await?;
            let group_snapshots = groups.snapshot().await?;
            let body = render_metrics(
                &metrics,
                queue_depth,
                &partitions,
                &group_snapshots,
                &cluster,
            );
            write_http_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
            )
            .await
        }
        _ => write_http_response(&mut stream, "404 Not Found", "text/plain", "not found\n").await,
    }
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<(), ConnectionError> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    Ok(())
}

fn render_metrics(
    metrics: &RuntimeMetrics,
    storage_queue_depth: usize,
    partitions: &[PartitionSnapshot],
    groups: &[groups::GroupSnapshot],
    cluster: &ClusterMetadata,
) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(8 * 1024);
    metrics.render(&mut output);
    output.push_str("# HELP sevlamq_broker_info Static broker identity and endpoint.\n");
    output.push_str("# TYPE sevlamq_broker_info gauge\n");
    for node in &cluster.nodes {
        let host = observability::escape_label(&node.host);
        let current = u8::from(node.id == cluster.responding_broker_id);
        let _ = writeln!(
            output,
            "sevlamq_broker_info{{broker_id=\"{}\",host=\"{host}\",port=\"{}\",current=\"{current}\"}} 1",
            node.id, node.port
        );
    }
    output.push_str(
        "# HELP sevlamq_storage_queue_depth Commands waiting in the storage worker queue.\n",
    );
    output.push_str("# TYPE sevlamq_storage_queue_depth gauge\n");
    let _ = writeln!(output, "sevlamq_storage_queue_depth {storage_queue_depth}");
    output.push_str("# HELP sevlamq_topic_partitions Number of partitions in a topic.\n");
    output.push_str("# TYPE sevlamq_topic_partitions gauge\n");
    let mut topic_counts = std::collections::BTreeMap::<&str, usize>::new();
    for partition in partitions {
        *topic_counts.entry(&partition.topic).or_default() += 1;
    }
    for (topic, count) in topic_counts {
        let topic = observability::escape_label(topic);
        let _ = writeln!(
            output,
            "sevlamq_topic_partitions{{topic=\"{topic}\"}} {count}"
        );
    }
    output.push_str("# HELP sevlamq_topic_high_watermark_sum Sum of partition high watermarks.\n");
    output.push_str("# TYPE sevlamq_topic_high_watermark_sum gauge\n");
    output.push_str(
        "# HELP sevlamq_topic_log_size_bytes Bytes stored by all partitions of a topic.\n",
    );
    output.push_str("# TYPE sevlamq_topic_log_size_bytes gauge\n");
    let mut topic_totals = std::collections::BTreeMap::<&str, (u64, u64)>::new();
    for partition in partitions {
        let totals = topic_totals.entry(&partition.topic).or_default();
        totals.0 = totals.0.saturating_add(partition.high_watermark);
        totals.1 = totals.1.saturating_add(partition.log_size_bytes);
    }
    for (topic, (high_watermark, size)) in topic_totals {
        let topic = observability::escape_label(topic);
        let _ = writeln!(
            output,
            "sevlamq_topic_high_watermark_sum{{topic=\"{topic}\"}} {high_watermark}"
        );
        let _ = writeln!(
            output,
            "sevlamq_topic_log_size_bytes{{topic=\"{topic}\"}} {size}"
        );
    }
    render_partition_metrics(&mut output, partitions);
    render_replication_metrics(&mut output, metrics, partitions);
    render_group_metrics(&mut output, partitions, groups);
    output
}

fn render_replication_metrics(
    output: &mut String,
    metrics: &RuntimeMetrics,
    partitions: &[PartitionSnapshot],
) {
    use std::fmt::Write as _;
    output.push_str("# HELP sevlamq_replica_offset Next offset acknowledged by a follower.\n");
    output.push_str("# TYPE sevlamq_replica_offset gauge\n");
    output.push_str(
        "# HELP sevlamq_replication_lag_records Records not yet acknowledged by a follower.\n",
    );
    output.push_str("# TYPE sevlamq_replication_lag_records gauge\n");
    for (follower, topic, partition, offset) in metrics.replica_offsets() {
        let high_watermark = partitions
            .iter()
            .find(|snapshot| snapshot.topic == topic && snapshot.partition == partition)
            .map_or(0, |snapshot| snapshot.high_watermark);
        let topic = observability::escape_label(&topic);
        let labels =
            format!("follower_id=\"{follower}\",topic=\"{topic}\",partition=\"{partition}\"");
        let _ = writeln!(output, "sevlamq_replica_offset{{{labels}}} {offset}");
        let _ = writeln!(
            output,
            "sevlamq_replication_lag_records{{{labels}}} {}",
            high_watermark.saturating_sub(offset)
        );
    }
}

fn render_partition_metrics(output: &mut String, partitions: &[PartitionSnapshot]) {
    use std::fmt::Write as _;
    output.push_str("# HELP sevlamq_partition_high_watermark Next offset of a partition.\n");
    output.push_str("# TYPE sevlamq_partition_high_watermark gauge\n");
    output.push_str("# HELP sevlamq_partition_low_watermark Earliest available offset.\n");
    output.push_str("# TYPE sevlamq_partition_low_watermark gauge\n");
    output
        .push_str("# HELP sevlamq_partition_records Records currently retained in a partition.\n");
    output.push_str("# TYPE sevlamq_partition_records gauge\n");
    output.push_str("# HELP sevlamq_partition_log_size_bytes Bytes stored in partition logs.\n");
    output.push_str("# TYPE sevlamq_partition_log_size_bytes gauge\n");
    output.push_str("# HELP sevlamq_partition_segments Number of log segments.\n");
    output.push_str("# TYPE sevlamq_partition_segments gauge\n");
    for snapshot in partitions {
        let topic = observability::escape_label(&snapshot.topic);
        let labels = format!("topic=\"{topic}\",partition=\"{}\"", snapshot.partition);
        let _ = writeln!(
            output,
            "sevlamq_partition_high_watermark{{{labels}}} {}",
            snapshot.high_watermark
        );
        let _ = writeln!(
            output,
            "sevlamq_partition_low_watermark{{{labels}}} {}",
            snapshot.low_watermark
        );
        let _ = writeln!(
            output,
            "sevlamq_partition_records{{{labels}}} {}",
            snapshot
                .high_watermark
                .saturating_sub(snapshot.low_watermark)
        );
        let _ = writeln!(
            output,
            "sevlamq_partition_log_size_bytes{{{labels}}} {}",
            snapshot.log_size_bytes
        );
        let _ = writeln!(
            output,
            "sevlamq_partition_segments{{{labels}}} {}",
            snapshot.segments
        );
    }
}

fn render_group_metrics(
    output: &mut String,
    partitions: &[PartitionSnapshot],
    groups: &[groups::GroupSnapshot],
) {
    use std::fmt::Write as _;
    for (name, help) in [
        (
            "sevlamq_consumer_group_members",
            "Active consumer group members.",
        ),
        (
            "sevlamq_consumer_group_generation",
            "Current consumer group generation.",
        ),
        (
            "sevlamq_consumer_group_assigned_partitions",
            "Partitions assigned to active members.",
        ),
        (
            "sevlamq_consumer_group_committed_offset",
            "Committed next offset for a partition.",
        ),
        (
            "sevlamq_consumer_group_lag",
            "Records between the committed offset and high watermark.",
        ),
        (
            "sevlamq_consumer_group_lag_total",
            "Total consumer group lag across a topic.",
        ),
    ] {
        let _ = writeln!(output, "# HELP {name} {help}");
        let _ = writeln!(output, "# TYPE {name} gauge");
    }
    for group in groups {
        let group_label = observability::escape_label(&group.group);
        let topic_label = observability::escape_label(&group.topic);
        let labels = format!("group=\"{group_label}\",topic=\"{topic_label}\"");
        let _ = writeln!(
            output,
            "sevlamq_consumer_group_members{{{labels}}} {}",
            group.members
        );
        let _ = writeln!(
            output,
            "sevlamq_consumer_group_generation{{{labels}}} {}",
            group.generation
        );
        let _ = writeln!(
            output,
            "sevlamq_consumer_group_assigned_partitions{{{labels}}} {}",
            group.assigned_partitions
        );
        let mut total_lag = 0_u64;
        for partition in partitions.iter().filter(|item| item.topic == group.topic) {
            let committed = group
                .committed_offsets
                .iter()
                .find_map(|(id, offset)| (*id == partition.partition).then_some(*offset))
                .unwrap_or_default();
            let lag = partition
                .high_watermark
                .saturating_sub(committed.max(partition.low_watermark));
            total_lag = total_lag.saturating_add(lag);
            let partition_labels = format!("{labels},partition=\"{}\"", partition.partition);
            let _ = writeln!(
                output,
                "sevlamq_consumer_group_committed_offset{{{partition_labels}}} {committed}"
            );
            let _ = writeln!(
                output,
                "sevlamq_consumer_group_lag{{{partition_labels}}} {lag}"
            );
        }
        let _ = writeln!(
            output,
            "sevlamq_consumer_group_lag_total{{{labels}}} {total_lag}"
        );
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

    async fn snapshot(&self) -> Result<Vec<groups::GroupSnapshot>, ConnectionError> {
        let coordinator = Arc::clone(&self.coordinator);
        tokio::task::spawn_blocking(move || {
            coordinator
                .lock()
                .map_err(|_| groups::GroupError::CoordinatorPoisoned)
                .map(|coordinator| coordinator.snapshot())
        })
        .await
        .map_err(|_| ConnectionError::StorageUnavailable)?
        .map_err(|_| ConnectionError::StorageUnavailable)
    }

    fn set_topic_partitions(&self, topic: &str, partitions: u32) -> Result<(), groups::GroupError> {
        self.coordinator
            .lock()
            .map_err(|_| groups::GroupError::CoordinatorPoisoned)?
            .set_topic_partitions(topic, partitions)
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
        Request::Produce(_)
        | Request::ProduceBatch(_)
        | Request::Fetch(_)
        | Request::CreateTopic(_)
        | Request::ListTopics
        | Request::DescribeTopic(_)
        | Request::ClusterMetadata => {
            unreachable!("group requests are filtered")
        }
    }
}

#[derive(Clone)]
struct StorageHandle {
    sender: mpsc::Sender<StorageCommand>,
    new_records: Arc<Notify>,
    enqueue_timeout: Duration,
}

#[derive(Debug, Clone)]
struct PartitionSnapshot {
    topic: String,
    partition: u32,
    high_watermark: u64,
    low_watermark: u64,
    log_size_bytes: u64,
    segments: usize,
}

impl StorageHandle {
    async fn enqueue(&self, command: StorageCommand) -> Result<(), ConnectionError> {
        match tokio::time::timeout(self.enqueue_timeout, self.sender.send(command)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ConnectionError::StorageUnavailable),
            Err(_) => Err(ConnectionError::BrokerBusy),
        }
    }
    fn queue_depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    async fn create_topic(
        &self,
        topic: String,
        partitions: u32,
    ) -> Result<TopicMetadata, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::CreateTopic {
            topic,
            partitions,
            reply,
        })
        .await?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)
    }

    async fn list_topics(&self) -> Result<Vec<TopicMetadata>, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::ListTopics { reply }).await?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)
    }

    async fn describe_topic(&self, topic: String) -> Result<TopicMetadata, ConnectionError> {
        let topics = self.list_topics().await?;
        topics
            .into_iter()
            .find(|metadata| metadata.topic == topic)
            .ok_or(ConnectionError::Storage(
                sevlamq_storage::StorageError::UnknownTopic,
            ))
    }

    async fn snapshot(&self) -> Result<Vec<PartitionSnapshot>, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::Snapshot { reply }).await?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)
    }

    async fn append(
        &self,
        topic: String,
        partition: Option<u32>,
        key: Bytes,
        value: Bytes,
        ack_mode: AckMode,
        producer: Option<StorageProducerIdentity>,
    ) -> Result<ProduceAck, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::Append {
            topic,
            partition,
            key,
            value,
            ack_mode,
            producer,
            reply,
        })
        .await?;
        let ack = response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)?;
        self.new_records.notify_waiters();
        Ok(ack)
    }

    async fn append_batch(
        &self,
        topic: String,
        partition: Option<u32>,
        records: Vec<BatchRecord>,
        ack_mode: AckMode,
    ) -> Result<Vec<ProduceAck>, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::AppendBatch {
            topic,
            partition,
            records,
            ack_mode,
            reply,
        })
        .await?;
        let acks = response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)?;
        self.new_records.notify_waiters();
        Ok(acks)
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
        self.enqueue(StorageCommand::Read {
            topic: topic.to_owned(),
            partition,
            offset,
            max_bytes,
            reply,
        })
        .await?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)
    }

    async fn record_at(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<FetchedRecord, ConnectionError> {
        self.read_once(topic, partition, offset, 16 * 1024 * 1024)
            .await?
            .into_iter()
            .find(|record| record.offset == offset)
            .ok_or(ConnectionError::MissingAppendedRecord)
    }

    async fn replicate(&self, record: ReplicateRecord) -> Result<u64, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::Replicate { record, reply })
            .await?;
        let next_offset = response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)?;
        self.new_records.notify_waiters();
        Ok(next_offset)
    }

    async fn partition_offset(&self, topic: &str, partition: u32) -> Result<u64, ConnectionError> {
        let (reply, response) = oneshot::channel();
        self.enqueue(StorageCommand::PartitionOffset {
            topic: topic.to_owned(),
            partition,
            reply,
        })
        .await?;
        response
            .await
            .map_err(|_| ConnectionError::StorageUnavailable)?
            .map_err(ConnectionError::Storage)
    }
}

enum StorageCommand {
    CreateTopic {
        topic: String,
        partitions: u32,
        reply: oneshot::Sender<Result<TopicMetadata, sevlamq_storage::StorageError>>,
    },
    ListTopics {
        reply: oneshot::Sender<Vec<TopicMetadata>>,
    },
    Append {
        topic: String,
        partition: Option<u32>,
        key: Bytes,
        value: Bytes,
        ack_mode: AckMode,
        producer: Option<StorageProducerIdentity>,
        reply: oneshot::Sender<Result<ProduceAck, sevlamq_storage::StorageError>>,
    },
    AppendBatch {
        topic: String,
        partition: Option<u32>,
        records: Vec<BatchRecord>,
        ack_mode: AckMode,
        reply: oneshot::Sender<Result<Vec<ProduceAck>, sevlamq_storage::StorageError>>,
    },
    Read {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        reply: oneshot::Sender<Result<Vec<FetchedRecord>, sevlamq_storage::StorageError>>,
    },
    Replicate {
        record: ReplicateRecord,
        reply: oneshot::Sender<Result<u64, sevlamq_storage::StorageError>>,
    },
    PartitionOffset {
        topic: String,
        partition: u32,
        reply: oneshot::Sender<Result<u64, sevlamq_storage::StorageError>>,
    },
    Snapshot {
        reply: oneshot::Sender<Vec<PartitionSnapshot>>,
    },
    ApplyRetention,
}

struct StorageSettings {
    data_dir: PathBuf,
    max_segment_bytes: u64,
    index_interval_bytes: u64,
    default_partition_count: u32,
    retention_bytes: Option<u64>,
    retention_age: Option<Duration>,
    auto_create_topics: bool,
    queue_capacity: usize,
    enqueue_timeout: Duration,
}

struct AppendInput {
    topic: String,
    partition: Option<u32>,
    key: Bytes,
    value: Bytes,
    ack_mode: AckMode,
    producer: Option<StorageProducerIdentity>,
}

struct BatchAppendInput {
    topic: String,
    partition: Option<u32>,
    records: Vec<BatchRecord>,
    ack_mode: AckMode,
}

type PartitionKey = (String, u32);
type PartitionLogs = HashMap<PartitionKey, PartitionLog>;

async fn start_storage_worker(
    settings: StorageSettings,
    metrics: Arc<RuntimeMetrics>,
) -> Result<
    (
        StorageHandle,
        JoinHandle<Result<(), sevlamq_storage::StorageError>>,
    ),
    BrokerError,
> {
    if settings.default_partition_count == 0 {
        return Err(BrokerError::InvalidPartitionCount);
    }
    if settings.queue_capacity == 0 || settings.enqueue_timeout.is_zero() {
        return Err(BrokerError::InvalidStorageQueueConfig);
    }
    let queue_capacity = settings.queue_capacity;
    let enqueue_timeout = settings.enqueue_timeout;
    let (settings, mut logs, mut topics) =
        tokio::task::spawn_blocking(move || recover_storage(settings)).await??;
    let (sender, mut receiver) = mpsc::channel(queue_capacity);
    let retention_sender = sender.downgrade();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(sender) = retention_sender.upgrade() else {
                break;
            };
            if sender.send(StorageCommand::ApplyRetention).await.is_err() {
                break;
            }
        }
    });
    let new_records = Arc::new(Notify::new());
    let worker = tokio::task::spawn_blocking(move || {
        run_storage_worker(&settings, &mut logs, &mut topics, &mut receiver, &metrics);
        Ok(())
    });
    Ok((
        StorageHandle {
            sender,
            new_records,
            enqueue_timeout,
        },
        worker,
    ))
}

fn run_storage_worker(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topics: &mut HashMap<String, u32>,
    receiver: &mut mpsc::Receiver<StorageCommand>,
    metrics: &RuntimeMetrics,
) {
    let mut round_robin = HashMap::<String, u32>::new();
    while let Some(command) = receiver.blocking_recv() {
        match command {
            StorageCommand::CreateTopic {
                topic,
                partitions,
                reply,
            } => {
                let result = create_topic(settings, logs, topics, &topic, partitions);
                let _ = reply.send(result);
            }
            StorageCommand::ListTopics { reply } => {
                let mut metadata: Vec<_> = topics
                    .iter()
                    .map(|(topic, partitions)| TopicMetadata {
                        topic: topic.clone(),
                        partitions: *partitions,
                        leaders: Vec::new(),
                    })
                    .collect();
                metadata.sort_unstable_by(|left, right| left.topic.cmp(&right.topic));
                let _ = reply.send(metadata);
            }
            StorageCommand::Append {
                topic,
                partition,
                key,
                value,
                ack_mode,
                producer,
                reply,
            } => {
                let input = AppendInput {
                    topic,
                    partition,
                    key,
                    value,
                    ack_mode,
                    producer,
                };
                let result =
                    append_record(settings, logs, topics, &mut round_robin, input, metrics);
                let _ = reply.send(result);
            }
            StorageCommand::AppendBatch {
                topic,
                partition,
                records,
                ack_mode,
                reply,
            } => {
                let result = append_batch_records(
                    settings,
                    logs,
                    topics,
                    &mut round_robin,
                    BatchAppendInput {
                        topic,
                        partition,
                        records,
                        ack_mode,
                    },
                    metrics,
                );
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
                    read_records(settings, logs, topics, &topic, partition, offset, max_bytes);
                let _ = reply.send(result);
            }
            StorageCommand::Replicate { record, reply } => {
                let result = append_replica(logs, topics, record);
                let _ = reply.send(result);
            }
            StorageCommand::PartitionOffset {
                topic,
                partition,
                reply,
            } => {
                let result = logs
                    .get(&(topic, partition))
                    .map(PartitionLog::next_offset)
                    .ok_or(sevlamq_storage::StorageError::UnknownPartition(partition));
                let _ = reply.send(result);
            }
            StorageCommand::Snapshot { reply } => {
                let _ = reply.send(snapshot_logs(logs));
            }
            StorageCommand::ApplyRetention => {
                apply_retention(settings, logs, metrics);
            }
        }
    }
}

fn snapshot_logs(logs: &PartitionLogs) -> Vec<PartitionSnapshot> {
    let mut snapshots: Vec<_> = logs
        .iter()
        .map(|((topic, partition), log)| PartitionSnapshot {
            topic: topic.clone(),
            partition: *partition,
            high_watermark: log.next_offset(),
            low_watermark: log.low_watermark(),
            log_size_bytes: log.log_size_bytes(),
            segments: log.segments().len(),
        })
        .collect();
    snapshots.sort_unstable_by(|left, right| {
        (&left.topic, left.partition).cmp(&(&right.topic, right.partition))
    });
    snapshots
}

fn apply_retention(settings: &StorageSettings, logs: &mut PartitionLogs, metrics: &RuntimeMetrics) {
    for ((topic, partition), log) in logs {
        match log.apply_retention(settings.retention_bytes, settings.retention_age) {
            Ok(removed) if removed > 0 => {
                metrics.retention_removed(removed);
                info!(topic, partition, removed, "expired log segments removed");
            }
            Ok(_) => {}
            Err(error) => warn!(topic, partition, %error, "retention failed"),
        }
    }
}

fn recover_storage(
    settings: StorageSettings,
) -> Result<(StorageSettings, PartitionLogs, HashMap<String, u32>), BrokerError> {
    let identities = discover_partitions(&settings.data_dir)?;
    let mut logs = HashMap::with_capacity(identities.len());
    let mut topics = HashMap::<String, u32>::new();

    for identity in identities {
        let topic = identity.topic().to_owned();
        let partition = identity.partition();
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
        topics
            .entry(identity.topic().to_owned())
            .and_modify(|count| *count = (*count).max(partition + 1))
            .or_insert(partition + 1);
    }

    info!(partitions = logs.len(), "storage recovery completed");
    for (topic, partitions) in &topics {
        persist_topic_metadata(&settings.data_dir, topic, *partitions)?;
    }
    Ok((settings, logs, topics))
}

fn append_record(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topics: &mut HashMap<String, u32>,
    round_robin: &mut HashMap<String, u32>,
    input: AppendInput,
    metrics: &RuntimeMetrics,
) -> Result<ProduceAck, sevlamq_storage::StorageError> {
    let AppendInput {
        topic,
        partition,
        key,
        value,
        ack_mode,
        producer,
    } = input;
    let partition_count = ensure_topic(settings, logs, topics, &topic)?;
    let routing_key = if key.is_empty() {
        producer.as_ref().map(|producer| producer.id().as_bytes())
    } else {
        Some(key.as_ref())
    };
    let partition = if let Some(partition) = partition {
        if partition >= partition_count {
            return Err(sevlamq_storage::StorageError::UnknownPartition(partition));
        }
        partition
    } else {
        select_partition(partition_count, round_robin, &topic, routing_key)
    };
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
    let append_started = Instant::now();
    let offset = log.append(&record)?;
    metrics.observe_append(append_started.elapsed());
    if ack_mode == AckMode::Durable {
        let flush_started = Instant::now();
        log.sync_data()?;
        metrics.observe_flush(flush_started.elapsed());
    }
    Ok(ProduceAck { partition, offset })
}

fn append_replica(
    logs: &mut PartitionLogs,
    topics: &HashMap<String, u32>,
    replicated: ReplicateRecord,
) -> Result<u64, sevlamq_storage::StorageError> {
    let partition_count = topics
        .get(&replicated.topic)
        .copied()
        .ok_or(sevlamq_storage::StorageError::UnknownTopic)?;
    if replicated.partition >= partition_count {
        return Err(sevlamq_storage::StorageError::UnknownPartition(
            replicated.partition,
        ));
    }
    let log = logs
        .get_mut(&(replicated.topic, replicated.partition))
        .ok_or(sevlamq_storage::StorageError::InvalidTopic)?;
    let record = Record::new(replicated.key, replicated.value, replicated.timestamp_ms);
    log.append_replica(replicated.offset, &record)?;
    if replicated.durable {
        log.sync_data()?;
    }
    Ok(log.next_offset())
}

fn append_batch_records(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topics: &mut HashMap<String, u32>,
    round_robin: &mut HashMap<String, u32>,
    input: BatchAppendInput,
    metrics: &RuntimeMetrics,
) -> Result<Vec<ProduceAck>, sevlamq_storage::StorageError> {
    let BatchAppendInput {
        topic,
        partition,
        records,
        ack_mode,
    } = input;
    let mut acks = Vec::with_capacity(records.len());
    for record in records {
        acks.push(append_record(
            settings,
            logs,
            topics,
            round_robin,
            AppendInput {
                topic: topic.clone(),
                partition,
                key: record.key,
                value: record.payload,
                ack_mode: AckMode::Leader,
                producer: None,
            },
            metrics,
        )?);
    }
    if ack_mode == AckMode::Durable {
        let mut partitions: Vec<u32> = acks.iter().map(|ack| ack.partition).collect();
        partitions.sort_unstable();
        partitions.dedup();
        for partition in partitions {
            let flush_started = Instant::now();
            logs.get_mut(&(topic.clone(), partition))
                .ok_or(sevlamq_storage::StorageError::InvalidTopic)?
                .sync_data()?;
            metrics.observe_flush(flush_started.elapsed());
        }
    }
    Ok(acks)
}

fn ensure_topic(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topics: &mut HashMap<String, u32>,
    topic: &str,
) -> Result<u32, sevlamq_storage::StorageError> {
    if let Some(partitions) = topics.get(topic) {
        return Ok(*partitions);
    }
    if !settings.auto_create_topics {
        return Err(sevlamq_storage::StorageError::UnknownTopic);
    }
    create_topic(
        settings,
        logs,
        topics,
        topic,
        settings.default_partition_count,
    )
    .map(|metadata| metadata.partitions)
}

fn create_topic(
    settings: &StorageSettings,
    logs: &mut PartitionLogs,
    topics: &mut HashMap<String, u32>,
    topic: &str,
    partitions: u32,
) -> Result<TopicMetadata, sevlamq_storage::StorageError> {
    if partitions == 0 {
        return Err(sevlamq_storage::StorageError::InvalidTopic);
    }
    if topics.contains_key(topic) {
        return Err(sevlamq_storage::StorageError::TopicAlreadyExists);
    }
    for partition in 0..partitions {
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
    persist_topic_metadata(&settings.data_dir, topic, partitions)?;
    topics.insert(topic.to_owned(), partitions);
    Ok(TopicMetadata {
        topic: topic.to_owned(),
        partitions,
        leaders: Vec::new(),
    })
}

fn persist_topic_metadata(
    data_dir: &Path,
    topic: &str,
    partitions: u32,
) -> Result<(), sevlamq_storage::StorageError> {
    use std::io::Write as _;
    let topic_dir = data_dir.join(topic);
    let path = topic_dir.join("topic.meta");
    let temporary = topic_dir.join("topic.meta.tmp");
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "partitions={partitions}")?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    std::fs::File::open(topic_dir)?.sync_all()?;
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
    topics: &mut HashMap<String, u32>,
    topic: &str,
    partition: u32,
    offset: u64,
    max_bytes: u32,
) -> Result<Vec<FetchedRecord>, sevlamq_storage::StorageError> {
    let partition_count = ensure_topic(settings, logs, topics, topic)?;
    let partition_key = (topic.to_owned(), partition);
    if !logs.contains_key(&partition_key) {
        if partition >= partition_count {
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
    #[error("invalid replication address: {0}")]
    ReplicationAddress(#[from] std::net::AddrParseError),
    #[error(transparent)]
    Storage(#[from] sevlamq_storage::StorageError),
    #[error("storage worker failed: {0}")]
    StorageWorker(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Group(#[from] groups::GroupError),
    #[error("default partition count must be greater than zero")]
    InvalidPartitionCount,
    #[error("storage queue capacity and enqueue timeout must be greater than zero")]
    InvalidStorageQueueConfig,
    #[error("storage worker is unavailable during broker startup")]
    StorageUnavailable,
    #[error("local broker is missing from cluster metadata")]
    InvalidClusterMetadata,
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
    #[error("broker busy: storage queue timeout")]
    BrokerBusy,
    #[error("replication connection closed before acknowledgement")]
    ReplicationClosed,
    #[error("follower returned an invalid replication acknowledgement")]
    InvalidReplicationAck,
    #[error("newly appended record could not be read back")]
    MissingAppendedRecord,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc, time::Duration};

    use bytes::Bytes;
    use sevlamq_client::Client;
    use sevlamq_protocol::{AckMode, BatchRecord, ClusterMetadata, ClusterNode};
    use sevlamq_storage::{PartitionLog, Record};
    use tempfile::tempdir;
    use tokio::{
        net::TcpListener,
        sync::{Notify, mpsc, oneshot},
        time::timeout,
    };

    use super::{
        ClusterRuntime, ConnectionError, GroupHandle, PartitionSnapshot, Replicator,
        RuntimeMetrics, StorageCommand, StorageHandle, StorageSettings, handle_connection,
        leader_for_partition, leadership_error, render_metrics, select_partition,
        start_storage_worker,
    };

    fn test_storage_settings(data_dir: &std::path::Path, partitions: u32) -> StorageSettings {
        StorageSettings {
            data_dir: data_dir.to_owned(),
            max_segment_bytes: 1024,
            index_interval_bytes: 64,
            default_partition_count: partitions,
            retention_bytes: None,
            retention_age: None,
            auto_create_topics: true,
            queue_capacity: 1024,
            enqueue_timeout: Duration::from_millis(100),
        }
    }

    fn test_cluster() -> Arc<ClusterMetadata> {
        Arc::new(ClusterMetadata {
            responding_broker_id: 1,
            nodes: vec![ClusterNode {
                id: 1,
                host: "127.0.0.1".to_owned(),
                port: 7400,
                admin_port: 7401,
                replication_port: 7500,
            }],
        })
    }

    fn three_node_cluster() -> ClusterMetadata {
        ClusterMetadata {
            responding_broker_id: 1,
            nodes: (1..=3)
                .map(|id| ClusterNode {
                    id,
                    host: "127.0.0.1".to_owned(),
                    port: u16::try_from(7_300 + id * 100).expect("test port should fit"),
                    admin_port: u16::try_from(7_301 + id * 100).expect("test port should fit"),
                    replication_port: u16::try_from(7_302 + id * 100)
                        .expect("test port should fit"),
                })
                .collect(),
        }
    }

    #[test]
    fn assigns_partition_leaders_deterministically() {
        let cluster = three_node_cluster();

        let leaders: Vec<u32> = (0..6)
            .map(|partition| leader_for_partition(&cluster, partition).id)
            .collect();

        assert_eq!(leaders, [1, 2, 3, 1, 2, 3]);
        assert!(leadership_error(&cluster, Some(0)).is_none());
        assert!(leadership_error(&cluster, Some(1)).is_some());
        assert!(leadership_error(&cluster, None).is_some());
    }

    #[test]
    fn renders_partition_and_consumer_group_lag_metrics() {
        let metrics = RuntimeMetrics::default();
        metrics.produced(2, 10);
        metrics.broker_busy();
        metrics.retention_removed(3);
        metrics.observe_produce(Duration::from_millis(2));
        let partitions = vec![PartitionSnapshot {
            topic: "payments".to_owned(),
            partition: 0,
            high_watermark: 13,
            low_watermark: 0,
            log_size_bytes: 512,
            segments: 2,
        }];
        let groups = vec![super::groups::GroupSnapshot {
            group: "workers".to_owned(),
            topic: "payments".to_owned(),
            generation: 3,
            members: 1,
            assigned_partitions: 1,
            committed_offsets: vec![(0, 8)],
        }];

        let rendered = render_metrics(&metrics, 0, &partitions, &groups, &test_cluster());

        assert!(rendered.contains("sevlamq_messages_produced_total 2"));
        assert!(rendered.contains("sevlamq_broker_busy_total 1"));
        assert!(rendered.contains("sevlamq_retention_segments_removed_total 3"));
        assert!(rendered.contains("sevlamq_produce_duration_seconds_count 1"));
        assert!(
            rendered.contains(
                "sevlamq_partition_log_size_bytes{topic=\"payments\",partition=\"0\"} 512"
            )
        );
        assert!(rendered.contains(
            "sevlamq_consumer_group_lag{group=\"workers\",topic=\"payments\",partition=\"0\"} 5"
        ));
    }

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
    async fn rejects_commands_when_storage_queue_timeout_expires() {
        let (sender, _receiver) = mpsc::channel(1);
        let (reply, _response) = oneshot::channel();
        assert!(
            sender
                .try_send(StorageCommand::ListTopics { reply })
                .is_ok()
        );
        let storage = StorageHandle {
            sender,
            new_records: Arc::new(Notify::new()),
            enqueue_timeout: Duration::from_millis(1),
        };

        assert!(matches!(
            storage.list_topics().await,
            Err(ConnectionError::BrokerBusy)
        ));
    }

    #[tokio::test]
    async fn rejects_unknown_topics_when_auto_create_is_disabled() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let mut settings = test_storage_settings(data_dir.path(), 3);
        settings.auto_create_topics = false;
        let (storage, storage_worker) =
            start_storage_worker(settings, Arc::new(RuntimeMetrics::default()))
                .await
                .expect("storage worker should start");

        assert!(matches!(
            storage.read("missing".to_owned(), 0, 0, 1024, 0).await,
            Err(ConnectionError::Storage(
                sevlamq_storage::StorageError::UnknownTopic
            ))
        ));
        drop(storage);
        storage_worker
            .await
            .expect("storage worker should finish")
            .expect("storage should close cleanly");
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

        let (storage, storage_worker) = start_storage_worker(
            test_storage_settings(data_dir.path(), 3),
            Arc::new(RuntimeMetrics::default()),
        )
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
        let (storage, storage_worker) = start_storage_worker(
            test_storage_settings(data_dir.path(), 1),
            Arc::new(RuntimeMetrics::default()),
        )
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
                None,
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
    async fn appends_a_durable_batch_and_returns_each_offset() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let (storage, storage_worker) = start_storage_worker(
            test_storage_settings(data_dir.path(), 1),
            Arc::new(RuntimeMetrics::default()),
        )
        .await
        .expect("storage worker should start");

        let acks = storage
            .append_batch(
                "payments".to_owned(),
                None,
                vec![
                    BatchRecord {
                        key: Bytes::new(),
                        payload: Bytes::from_static(b"hello"),
                    },
                    BatchRecord {
                        key: Bytes::new(),
                        payload: Bytes::from_static(b"world"),
                    },
                ],
                AckMode::Durable,
            )
            .await
            .expect("batch should append");

        assert_eq!(acks.len(), 2);
        assert_eq!(acks[0].offset, 0);
        assert_eq!(acks[1].offset, 1);
        drop(storage);
        storage_worker
            .await
            .expect("storage worker should finish")
            .expect("storage should close cleanly");
    }

    #[tokio::test]
    async fn catches_up_a_follower_without_creating_offset_gaps() {
        let leader_dir = tempdir().expect("leader directory should be created");
        let follower_dir = tempdir().expect("follower directory should be created");
        let (leader, leader_worker) = start_storage_worker(
            test_storage_settings(leader_dir.path(), 1),
            Arc::new(RuntimeMetrics::default()),
        )
        .await
        .expect("leader storage should start");
        let (follower, follower_worker) = start_storage_worker(
            test_storage_settings(follower_dir.path(), 1),
            Arc::new(RuntimeMetrics::default()),
        )
        .await
        .expect("follower storage should start");
        leader
            .create_topic("payments".to_owned(), 1)
            .await
            .expect("leader topic should be created");
        follower
            .create_topic("payments".to_owned(), 1)
            .await
            .expect("follower topic should be created");
        for value in ["first", "second", "third"] {
            leader
                .append(
                    "payments".to_owned(),
                    Some(0),
                    Bytes::new(),
                    Bytes::from(value.to_owned()),
                    AckMode::Durable,
                    None,
                )
                .await
                .expect("leader append should succeed");
        }

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("replication listener should bind");
        let address = listener
            .local_addr()
            .expect("replication listener should have an address");
        let follower_server = follower.clone();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (stream, peer) = listener.accept().await.expect("replica should connect");
                super::handle_replication_connection(
                    stream,
                    peer,
                    follower_server.clone(),
                    Arc::new(RuntimeMetrics::default()),
                )
                .await
                .expect("replication request should succeed");
            }
        });
        let cluster = Arc::new(ClusterMetadata {
            responding_broker_id: 1,
            nodes: vec![
                ClusterNode {
                    id: 1,
                    host: "127.0.0.1".to_owned(),
                    port: 1,
                    admin_port: 2,
                    replication_port: 3,
                },
                ClusterNode {
                    id: 2,
                    host: "127.0.0.1".to_owned(),
                    port: 4,
                    admin_port: 5,
                    replication_port: address.port(),
                },
            ],
        });
        let replicator = Replicator::new(cluster, Arc::new(RuntimeMetrics::default()));
        let latest = leader
            .record_at("payments", 0, 2)
            .await
            .expect("latest leader record should exist");

        replicator
            .replicate(&leader, "payments", 0, &latest, AckMode::Durable)
            .await
            .expect("follower should catch up");
        server.await.expect("replication server should finish");
        let records = follower
            .read_once("payments", 0, 0, 1024)
            .await
            .expect("follower records should be readable");
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].offset, 2);

        drop(leader);
        drop(follower);
        leader_worker
            .await
            .expect("leader worker should finish")
            .expect("leader storage should close");
        follower_worker
            .await
            .expect("follower worker should finish")
            .expect("follower storage should close");
    }

    #[tokio::test]
    async fn fetches_an_empty_partition_before_the_first_produce() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let (storage, storage_worker) = start_storage_worker(
            test_storage_settings(data_dir.path(), 3),
            Arc::new(RuntimeMetrics::default()),
        )
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
            let (storage, storage_worker) = start_storage_worker(
                test_storage_settings(data_dir.path(), 3),
                Arc::new(RuntimeMetrics::default()),
            )
            .await
            .expect("storage worker should start");
            let connection_storage = storage.clone();
            let groups = GroupHandle::open(data_dir.path(), 3, Duration::from_secs(10))
                .expect("group coordinator should start");
            let server = tokio::spawn(async move {
                let (stream, peer) = listener.accept().await.expect("connection should arrive");
                handle_connection(
                    stream,
                    peer,
                    1,
                    connection_storage,
                    groups,
                    Arc::new(RuntimeMetrics::default()),
                    ClusterRuntime {
                        metadata: test_cluster(),
                        replicator: Replicator::new(
                            test_cluster(),
                            Arc::new(RuntimeMetrics::default()),
                        ),
                    },
                )
                .await
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
