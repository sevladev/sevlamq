use std::net::SocketAddr;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "sevlamq", version, about = "Command-line client for SevlaMQ")]
pub struct Cli {
    #[arg(long, default_value = "127.0.0.1:7400", global = true)]
    pub broker: SocketAddr,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Publishes one message and prints its partition and offset.
    Produce {
        topic: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long, value_enum, default_value_t = ProduceAcks::Leader)]
        acks: ProduceAcks,
        #[arg(long, requires = "sequence")]
        producer_id: Option<String>,
        #[arg(long, requires = "producer_id")]
        sequence: Option<u64>,
    },
    /// Publishes multiple messages in one optionally compressed request.
    ProduceBatch {
        topic: String,
        #[arg(long, required = true)]
        message: Vec<String>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long, value_enum, default_value_t = ProduceAcks::Leader)]
        acks: ProduceAcks,
        #[arg(long, value_enum, default_value_t = BatchCompression::None)]
        compression: BatchCompression,
    },
    /// Reads persisted records starting at an offset.
    Fetch {
        topic: String,
        #[arg(long, default_value_t = 0)]
        partition: u32,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_bytes: u32,
        #[arg(long, default_value_t = 0)]
        wait_ms: u32,
    },
    /// Manages consumer-group membership and committed offsets.
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Creates and inspects topic metadata.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    /// Inspects static cluster membership.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommand,
    },
    /// Runs a coordinated consumer with automatic heartbeat and commits.
    Consume {
        topic: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        member: String,
        #[arg(long, value_enum, default_value_t = DeliveryMode::AtLeastOnce)]
        delivery: DeliveryMode,
        #[arg(long, default_value_t = 1_000)]
        wait_ms: u32,
        #[arg(
            long,
            default_value_t = 3_000,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        heartbeat_ms: u64,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_bytes: u32,
        #[arg(long)]
        handler: Option<String>,
        #[arg(long, default_value_t = 30_000)]
        handler_timeout_ms: u64,
        #[arg(long, value_delimiter = ',', default_value = "1000,5000,30000")]
        retry_delays_ms: Vec<u64>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ProduceAcks {
    Leader,
    Durable,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum BatchCompression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DeliveryMode {
    AtMostOnce,
    AtLeastOnce,
}

#[derive(Debug, Subcommand)]
pub enum GroupCommand {
    Join {
        group: String,
        topic: String,
        member: String,
    },
    Heartbeat {
        group: String,
        topic: String,
        member: String,
        #[arg(long)]
        generation: u64,
    },
    Leave {
        group: String,
        topic: String,
        member: String,
        #[arg(long)]
        generation: u64,
    },
    Commit {
        group: String,
        topic: String,
        member: String,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        offset: u64,
    },
    Offset {
        group: String,
        topic: String,
        #[arg(long)]
        partition: u32,
    },
    Fetch {
        group: String,
        topic: String,
        member: String,
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        offset: u64,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_bytes: u32,
        #[arg(long, default_value_t = 0)]
        wait_ms: u32,
    },
}

#[derive(Debug, Subcommand)]
pub enum TopicCommand {
    Create {
        topic: String,
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
        partitions: u32,
    },
    List,
    Describe {
        topic: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum ClusterCommand {
    Status,
    /// Promotes an in-sync replica through the static controller.
    Promote {
        topic: String,
        #[arg(long)]
        partition: u32,
        #[arg(long = "broker-id")]
        broker_id: u32,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::Parser;

    use super::{BatchCompression, Cli, ClusterCommand, Command, GroupCommand, TopicCommand};

    #[test]
    fn parses_produce_command() {
        let cli = Cli::try_parse_from([
            "sevlamq",
            "produce",
            "payments",
            "--message",
            "hello",
            "--key",
            "customer-123",
        ])
        .expect("produce command should parse");
        assert!(matches!(
            cli.command,
            Command::Produce { topic, message, key, .. }
                if topic == "payments" && message == "hello"
                    && key.as_deref() == Some("customer-123")
        ));
    }

    #[test]
    fn parses_zstd_produce_batch() {
        let cli = Cli::try_parse_from([
            "sevlamq",
            "produce-batch",
            "payments",
            "--message",
            "hello",
            "--message",
            "world",
            "--compression",
            "zstd",
        ])
        .expect("produce batch command should parse");

        assert!(matches!(
            cli.command,
            Command::ProduceBatch {
                message,
                compression: BatchCompression::Zstd,
                ..
            } if message == ["hello", "world"]
        ));
    }

    #[test]
    fn parses_fetch_defaults() {
        let cli = Cli::try_parse_from(["sevlamq", "fetch", "payments"])
            .expect("fetch command should parse");
        assert!(matches!(
            cli.command,
            Command::Fetch {
                topic,
                partition: 0,
                offset: 0,
                max_bytes: 1_048_576,
                wait_ms: 0,
            } if topic == "payments"
        ));
    }

    #[test]
    fn parses_topic_create() {
        let cli =
            Cli::try_parse_from(["sevlamq", "topic", "create", "orders", "--partitions", "6"])
                .expect("topic create should parse");

        assert!(matches!(
            cli.command,
            Command::Topic {
                command: TopicCommand::Create {
                    topic,
                    partitions: 6,
                }
            } if topic == "orders"
        ));
    }

    #[test]
    fn parses_cluster_status() {
        let cli = Cli::try_parse_from(["sevlamq", "cluster", "status"])
            .expect("cluster status should parse");
        assert!(matches!(
            cli.command,
            Command::Cluster {
                command: ClusterCommand::Status
            }
        ));
    }

    #[test]
    fn parses_manual_leader_promotion() {
        let cli = Cli::try_parse_from([
            "sevlamq",
            "cluster",
            "promote",
            "payments",
            "--partition",
            "1",
            "--broker-id",
            "3",
        ])
        .expect("cluster promotion should parse");
        assert!(matches!(
            cli.command,
            Command::Cluster {
                command: ClusterCommand::Promote {
                    topic,
                    partition: 1,
                    broker_id: 3,
                }
            } if topic == "payments"
        ));
    }

    #[test]
    fn parses_group_fetch_with_named_offsets() {
        let cli = Cli::try_parse_from([
            "sevlamq",
            "group",
            "fetch",
            "workers",
            "payments",
            "worker-a",
            "--generation",
            "5",
            "--partition",
            "0",
            "--offset",
            "13",
        ])
        .expect("group fetch should parse");
        assert!(matches!(
            cli.command,
            Command::Group {
                command: GroupCommand::Fetch {
                    generation: 5,
                    partition: 0,
                    offset: 13,
                    ..
                }
            }
        ));
    }

    #[test]
    fn parses_high_level_consumer_defaults() {
        let cli = Cli::try_parse_from([
            "sevlamq", "consume", "payments", "--group", "workers", "--member", "worker-a",
        ])
        .expect("consumer command should parse");

        assert!(matches!(
            cli.command,
            Command::Consume {
                delivery: super::DeliveryMode::AtLeastOnce,
                wait_ms: 1_000,
                heartbeat_ms: 3_000,
                handler: None,
                ..
            }
        ));
    }
}
