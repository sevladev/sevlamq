use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct RuntimeMetrics {
    active_connections: AtomicU64,
    produced_messages: AtomicU64,
    produced_bytes: AtomicU64,
    fetched_messages: AtomicU64,
    fetched_bytes: AtomicU64,
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
