use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

const LATENCY_BUCKETS_US: [u64; 9] = [
    100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];

struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_US.len() + 1],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    fn observe(&self, duration: Duration) {
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        for (index, bound) in LATENCY_BUCKETS_US.iter().enumerate() {
            if micros <= *bound {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.buckets[LATENCY_BUCKETS_US.len()].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    fn render(&self, output: &mut String, name: &str, help: &str) {
        use std::fmt::Write as _;
        let _ = writeln!(output, "# HELP {name} {help}");
        let _ = writeln!(output, "# TYPE {name} histogram");
        for (index, bound) in LATENCY_BUCKETS_US.iter().enumerate() {
            let seconds = format_seconds(*bound);
            let value = self.buckets[index].load(Ordering::Relaxed);
            let _ = writeln!(output, "{name}_bucket{{le=\"{seconds}\"}} {value}");
        }
        let infinity = self.buckets[LATENCY_BUCKETS_US.len()].load(Ordering::Relaxed);
        let _ = writeln!(output, "{name}_bucket{{le=\"+Inf\"}} {infinity}");
        let _ = writeln!(
            output,
            "{name}_count {}",
            self.count.load(Ordering::Relaxed)
        );
        let sum_seconds = format_seconds(self.sum_us.load(Ordering::Relaxed));
        let _ = writeln!(output, "{name}_sum {sum_seconds}");
    }
}

fn format_seconds(micros: u64) -> String {
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

#[derive(Default)]
pub struct RuntimeMetrics {
    active_connections: AtomicU64,
    produced_messages: AtomicU64,
    produced_bytes: AtomicU64,
    fetched_messages: AtomicU64,
    fetched_bytes: AtomicU64,
    broker_busy: AtomicU64,
    retention_segments_removed: AtomicU64,
    replication_records_sent: AtomicU64,
    replication_records_received: AtomicU64,
    replication_failures: AtomicU64,
    replica_offsets: Mutex<HashMap<(u32, String, u32), u64>>,
    produce_latency: Histogram,
    fetch_latency: Histogram,
    append_latency: Histogram,
    flush_latency: Histogram,
}

impl RuntimeMetrics {
    pub fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn produced(&self, messages: usize, bytes: usize) {
        self.produced_messages.fetch_add(
            u64::try_from(messages).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.produced_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub fn fetched(&self, messages: usize, bytes: usize) {
        self.fetched_messages.fetch_add(
            u64::try_from(messages).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.fetched_bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub fn broker_busy(&self) {
        self.broker_busy.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retention_removed(&self, segments: usize) {
        self.retention_segments_removed.fetch_add(
            u64::try_from(segments).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn replication_sent(
        &self,
        follower_id: u32,
        topic: &str,
        partition: u32,
        next_offset: u64,
        record_accepted: bool,
    ) {
        if record_accepted {
            self.replication_records_sent
                .fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut offsets) = self.replica_offsets.lock() {
            offsets.insert((follower_id, topic.to_owned(), partition), next_offset);
        }
    }

    pub fn replication_received(&self) {
        self.replication_records_received
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn replication_failed(&self) {
        self.replication_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn replica_offsets(&self) -> Vec<(u32, String, u32, u64)> {
        self.replica_offsets.lock().map_or_else(
            |_| Vec::new(),
            |offsets| {
                offsets
                    .iter()
                    .map(|((broker, topic, partition), offset)| {
                        (*broker, topic.clone(), *partition, *offset)
                    })
                    .collect()
            },
        )
    }

    pub fn observe_produce(&self, duration: Duration) {
        self.produce_latency.observe(duration);
    }

    pub fn observe_fetch(&self, duration: Duration) {
        self.fetch_latency.observe(duration);
    }

    pub fn observe_append(&self, duration: Duration) {
        self.append_latency.observe(duration);
    }

    pub fn observe_flush(&self, duration: Duration) {
        self.flush_latency.observe(duration);
    }

    pub fn render(&self, output: &mut String) {
        metric(
            output,
            "sevlamq_active_connections",
            "Current client TCP connections.",
            "gauge",
            self.active_connections.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_messages_produced_total",
            "Messages accepted by the broker.",
            "counter",
            self.produced_messages.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_bytes_produced_total",
            "Uncompressed payload bytes accepted by the broker.",
            "counter",
            self.produced_bytes.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_messages_fetched_total",
            "Messages returned by fetch requests.",
            "counter",
            self.fetched_messages.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_bytes_fetched_total",
            "Payload bytes returned by fetch requests.",
            "counter",
            self.fetched_bytes.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_broker_busy_total",
            "Requests rejected because the storage queue remained full.",
            "counter",
            self.broker_busy.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_retention_segments_removed_total",
            "Closed log segments removed by retention.",
            "counter",
            self.retention_segments_removed.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_replication_records_sent_total",
            "Records acknowledged by followers.",
            "counter",
            self.replication_records_sent.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_replication_records_received_total",
            "Records accepted through the replication listener.",
            "counter",
            self.replication_records_received.load(Ordering::Relaxed),
        );
        metric(
            output,
            "sevlamq_replication_failures_total",
            "Replication operations that failed before acknowledgement.",
            "counter",
            self.replication_failures.load(Ordering::Relaxed),
        );
        self.produce_latency.render(
            output,
            "sevlamq_produce_duration_seconds",
            "End-to-end produce request latency.",
        );
        self.fetch_latency.render(
            output,
            "sevlamq_fetch_duration_seconds",
            "End-to-end fetch request latency.",
        );
        self.append_latency.render(
            output,
            "sevlamq_append_duration_seconds",
            "Storage append latency.",
        );
        self.flush_latency.render(
            output,
            "sevlamq_flush_duration_seconds",
            "Durable storage synchronization latency.",
        );
    }
}

fn metric(output: &mut String, name: &str, help: &str, kind: &str, value: u64) {
    use std::fmt::Write as _;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} {kind}");
    let _ = writeln!(output, "{name} {value}");
}

pub fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
