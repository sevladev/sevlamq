use std::net::SocketAddr;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use sevlamq_client::Client;
use sevlamq_protocol::{CommitOffsetRequest, GroupFetchRequest};
use thiserror::Error;

#[derive(Debug, Parser)]
#[command(name = "sevlamq", version, about = "Command-line client for SevlaMQ")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:7400", global = true)]
    broker: SocketAddr,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
enum GroupCommand {
    Join {
        group: String,
        topic: String,
        member: String,
    },
    Heartbeat {
        group: String,
        topic: String,
        member: String,
        generation: u64,
    },
    Leave {
        group: String,
        topic: String,
        member: String,
        generation: u64,
    },
    Commit {
        group: String,
        topic: String,
        member: String,
        generation: u64,
        partition: u32,
        offset: u64,
    },
    Offset {
        group: String,
        topic: String,
        partition: u32,
    },
    Fetch {
        group: String,
        topic: String,
        member: String,
        generation: u64,
        partition: u32,
        offset: u64,
        #[arg(long, default_value_t = 1024 * 1024)]
        max_bytes: u32,
        #[arg(long, default_value_t = 0)]
        wait_ms: u32,
    },
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let cli = Cli::parse();
    let mut client = Client::connect(cli.broker).await?;

    match cli.command {
        Command::Produce {
            topic,
            message,
            key,
        } => {
            let key = key.map_or_else(Bytes::new, Bytes::from);
            let ack = client.produce(topic, key, Bytes::from(message)).await?;
            println!("partition={} offset={}", ack.partition, ack.offset);
        }
        Command::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => {
            let response = client
                .fetch(topic, partition, offset, max_bytes, wait_ms)
                .await?;
            for record in response.records() {
                let key = record
                    .key
                    .as_ref()
                    .map_or_else(|| "-".into(), |key| String::from_utf8_lossy(key));
                println!(
                    "offset={} timestamp_ms={} key={} value={}",
                    record.offset,
                    record.timestamp_ms,
                    key,
                    String::from_utf8_lossy(&record.value)
                );
            }
        }
        Command::Group { command } => run_group(&mut client, command).await?,
    }

    Ok(())
}

async fn run_group(client: &mut Client, command: GroupCommand) -> Result<(), CliError> {
    match command {
        GroupCommand::Join {
            group,
            topic,
            member,
        } => {
            let response = client.join_group(group, topic, member).await?;
            let partitions = response
                .partitions()
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "generation={} partitions={partitions}",
                response.generation()
            );
        }
        GroupCommand::Heartbeat {
            group,
            topic,
            member,
            generation,
        } => {
            client.heartbeat(group, topic, member, generation).await?;
            println!("heartbeat accepted");
        }
        GroupCommand::Leave {
            group,
            topic,
            member,
            generation,
        } => {
            client.leave_group(group, topic, member, generation).await?;
            println!("member left group");
        }
        GroupCommand::Commit {
            group,
            topic,
            member,
            generation,
            partition,
            offset,
        } => {
            client
                .commit_offset(CommitOffsetRequest {
                    group,
                    topic,
                    member,
                    generation,
                    partition,
                    offset,
                })
                .await?;
            println!("offset committed");
        }
        GroupCommand::Offset {
            group,
            topic,
            partition,
        } => match client.committed_offset(group, topic, partition).await? {
            Some(offset) => println!("offset={offset}"),
            None => println!("offset=-"),
        },
        GroupCommand::Fetch {
            group,
            topic,
            member,
            generation,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => {
            print_group_fetch(
                client,
                GroupFetchRequest {
                    group,
                    topic,
                    member,
                    generation,
                    partition,
                    offset,
                    max_bytes,
                    max_wait_ms: wait_ms,
                },
            )
            .await?;
        }
    }

    Ok(())
}

async fn print_group_fetch(
    client: &mut Client,
    request: GroupFetchRequest,
) -> Result<(), CliError> {
    let response = client.group_fetch(request).await?;
    for record in response.records() {
        println!(
            "offset={} key={} value={}",
            record.offset,
            record
                .key
                .as_ref()
                .map_or_else(|| "-".into(), |key| String::from_utf8_lossy(key)),
            String::from_utf8_lossy(&record.value)
        );
    }
    Ok(())
}

#[derive(Debug, Error)]
enum CliError {
    #[error(transparent)]
    Client(#[from] sevlamq_client::ClientError),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

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
                if topic == "payments"
                    && message == "hello"
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
}
