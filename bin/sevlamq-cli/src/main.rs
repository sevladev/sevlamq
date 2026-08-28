use std::net::SocketAddr;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use sevlamq_client::Client;
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
