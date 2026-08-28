use std::net::SocketAddr;

use clap::{Parser, Subcommand};

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
            Command::Produce { topic, message, key }
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
}
