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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command, GroupCommand};

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
