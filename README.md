# SevlaMQ

SevlaMQ is a compact distributed message broker written in Rust. It was built as a
portfolio and learning project to explore commit logs, binary protocols, consumer
groups, replication, quorum acknowledgements, leader epochs, and automatic
partition failover without hiding the mechanics behind a large framework.

It is an educational MVP, not a production replacement for Kafka, NATS, or
RabbitMQ.

## Features

- Append-only partition logs with CRC32 checksums
- Sparse indexes, segment rotation, retention, and startup recovery
- Custom length-prefixed binary protocol over TCP
- Key-based partitioning and round-robin routing for unkeyed messages
- Batched produce requests with optional Zstandard compression
- Long-poll fetches
- Consumer groups with membership, heartbeats, rebalancing, and durable offsets
- At-most-once and at-least-once consumption helpers
- Idempotent producers using producer IDs and sequence numbers
- Retry topics and dead-letter queues through the CLI consumer
- Static three-broker clusters with configurable replication factor and minimum ISR
- Durable quorum acknowledgements
- Persisted leader epochs and committed high watermarks
- Manual and automatic safe partition-leader promotion
- Structured logs and a Prometheus-compatible metrics endpoint
- Strict Rust and Clippy linting with `unsafe` forbidden

## Architecture

```text
Producer / Consumer / CLI
           │
           ▼
   custom TCP protocol
           │
           ▼
  partition leader ─────► follower
           │             follower
           ▼
  append-only segments
    + sparse indexes
```

Topics are divided into partitions. Each partition has one leader and a static
replica set. Producers and consumers retrieve metadata from the controller and
connect to the current partition leader.

The broker with the lowest configured ID is the static controller. It owns topic
and leadership metadata, monitors partition leaders, and performs safe automatic
promotion after three consecutive failed probes. A promoted replica must belong to
the replica set, contain the committed high watermark, and be reachable alongside
enough replicas to satisfy `min_in_sync_replicas`.

The workspace is split into focused crates:

```text
bin/sevlamqd       broker executable
bin/sevlamq-cli    command-line client
crates/broker      networking, groups, replication, and coordination
crates/client      protocol client and leader routing
crates/common      configuration
crates/protocol    wire types and binary codec
crates/storage     logs, segments, indexes, retention, and recovery
```

## Requirements

- Rust 1.98 or newer
- `make` for the cluster convenience commands

## Quick start

Start a single broker:

```bash
cargo run --bin sevlamqd
```

In another terminal, produce and fetch messages:

```bash
cargo run --bin sevlamq -- produce payments \
  --key customer-123 \
  --message '{"amount":150}' \
  --acks durable

cargo run --bin sevlamq -- fetch payments \
  --partition 1 \
  --offset 0
```

The selected partition is printed by `produce`. Use that partition in the fetch
command when following a keyed message.

## Running a three-broker cluster

The repository includes three local configurations under `config/cluster/`:

```bash
make cluster
```

This starts the brokers on:

| Broker | Client | Admin | Replication |
| --- | --- | --- | --- |
| 1 | `127.0.0.1:7400` | `127.0.0.1:7401` | `127.0.0.1:7402` |
| 2 | `127.0.0.1:7500` | `127.0.0.1:7501` | `127.0.0.1:7502` |
| 3 | `127.0.0.1:7600` | `127.0.0.1:7601` | `127.0.0.1:7602` |

Create and inspect a topic:

```bash
cargo run --bin sevlamq -- topic create orders --partitions 6
cargo run --bin sevlamq -- topic describe orders
cargo run --bin sevlamq -- cluster status
```

Example metadata:

```text
topic=orders partitions=6 leaders=[0:1@0/hw=0,1:2@0/hw=0,2:3@0/hw=0,3:1@0/hw=0,4:2@0/hw=0,5:3@0/hw=0]
```

The format is `partition:leader@epoch/hw=high-watermark`.

You can also run each broker in a separate terminal, which makes failure testing
easier:

```bash
make broker-1
make broker-2
make broker-3
```

Stop a partition leader and wait approximately three seconds. The controller will
promote a safe replica when the remaining brokers can still form the configured
quorum. New client operations refresh metadata and route to the promoted leader.

Manual promotion is also available:

```bash
cargo run --bin sevlamq -- cluster promote orders \
  --partition 1 \
  --broker-id 3
```

## Batches and compression

```bash
cargo run --bin sevlamq -- produce-batch events \
  --message first \
  --message second \
  --message third \
  --key customer-123 \
  --compression zstd \
  --acks durable
```

Records sharing a key are routed to the same partition, preserving their order
within that partition.

## Consumer groups

The high-level consumer joins a group, maintains heartbeats, fetches its assigned
partitions, and commits offsets:

```bash
cargo run --bin sevlamq -- consume payments \
  --group payment-workers \
  --member worker-a \
  --delivery at-least-once
```

Run another member with a different member ID to observe partition rebalancing.

A handler command can process each message. Failed messages are routed through
retry topics and eventually to a dead-letter topic:

```bash
cargo run --bin sevlamq -- consume payments \
  --group payment-workers \
  --member worker-a \
  --handler ./process-payment \
  --retry-delays-ms 1000,5000,30000
```

The retry scheduler currently lives in the CLI consumer rather than in the broker.

## Delivery and durability

`--acks leader` acknowledges after the leader appends locally and replicates in the
background. Such a record may be lost during failover if it never reaches quorum.

`--acks durable` only succeeds after the configured minimum number of in-sync
replicas confirms the append. The committed high watermark advances after quorum,
and fetch operations never expose records beyond it.

Leader changes increment a persisted epoch. Replication from stale leaders is
rejected, and an uncommitted divergent tail is truncated when a replica follows a
new epoch.

## Observability

The admin server exposes liveness, readiness, and Prometheus metrics:

```bash
curl http://127.0.0.1:7401/health/live
curl http://127.0.0.1:7401/health/ready
curl http://127.0.0.1:7401/metrics
```

Selected metrics include:

```text
sevlamq_messages_produced_total
sevlamq_messages_fetched_total
sevlamq_consumer_group_lag
sevlamq_partition_log_size_bytes
sevlamq_partition_log_end_offset
sevlamq_partition_high_watermark
sevlamq_partition_isr
sevlamq_replication_lag_records
sevlamq_automatic_leader_promotions_total
```

Set `json = true` under `[logging]` for JSON logs suitable for collection by Loki or
another log backend. SevlaMQ deliberately does not require Prometheus, Grafana, or
Loki to operate; deployments can integrate their preferred observability stack.

## Configuration

The default configuration is `config/sevlamq.toml`. A different file can be passed
to the daemon:

```bash
cargo run --bin sevlamqd -- config/cluster/broker-1.toml
```

Important cluster settings:

```toml
[cluster]
broker_id = 1
replication_factor = 3
min_in_sync_replicas = 2
```

Storage settings include segment size, sparse-index interval, retention by age and
bytes, queue capacity, and automatic topic creation.

## Development

Run the complete quality gate:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

The workspace forbids `unsafe` code and enables Clippy's `all`, `pedantic`, and
`nursery` lint groups.

## Next steps

The current feature set is the MVP. Future work can be added incrementally in this
order:

### Short term

- Add transparent client retries after `not partition leader` responses.
- Add metadata caching, refresh intervals, connection pooling, and backoff.
- Add reproducible throughput and latency benchmarks, including Zstd comparisons.
- Add chaos tests that kill leaders during produce and verify that committed records
  survive.
- Provide an optional example stack with Prometheus, Grafana, and Loki dashboards.
- Move retry scheduling from the CLI consumer into a persistent broker-side
  scheduler.

### Cluster hardening

- Make ISR membership persistent and configurable by replica lag and timeout.
- Expose under-replicated partitions and failed-election metrics.
- Replicate consumer-group coordination and committed offsets across brokers.
- Support broker membership changes and partition reassignment.
- Add persistent replication connections and internal replication batches.

### V2 ideas

- Use Raft for controller election and metadata replication.
- Add TLS, broker/client authentication, and topic/group ACLs.
- Add log compaction by key.
- Version and negotiate the wire protocol.
- Add fuzzing, property tests, and network-partition simulations.

## Current limitations

- The controller is static. If broker 1 fails, partition leadership metadata cannot
  change until it returns.
- Cluster membership and replica placement are static; there is no partition
  reassignment or Raft-based metadata quorum.
- Internal and client connections do not yet use TLS, authentication, or ACLs.
- Replication uses short-lived TCP connections rather than pooled persistent links.
- The failure detector uses fixed probe intervals and thresholds.
- The wire protocol is project-specific and not compatible with Kafka clients.

These boundaries are intentional for the MVP and keep the implementation small
enough to study.

## License

MIT
