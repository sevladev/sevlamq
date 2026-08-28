use sevlamq_client::Client;

use crate::{
    cli::{Cli, Command},
    error::CliError,
};

mod consume;
mod fetch;
mod group;
mod produce;
mod retry;
mod topic;

pub async fn execute(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Consume {
            topic,
            group,
            member,
            delivery,
            wait_ms,
            heartbeat_ms,
            max_bytes,
            handler,
            handler_timeout_ms,
            retry_delays_ms,
        } => {
            consume::execute(
                cli.broker,
                consume::Options {
                    topic,
                    group,
                    member,
                    delivery,
                    wait_ms,
                    heartbeat_ms,
                    max_bytes,
                    handler,
                    handler_timeout_ms,
                    retry_delays_ms,
                },
            )
            .await
        }
        command => execute_single(cli.broker, command).await,
    }
}

async fn execute_single(broker: std::net::SocketAddr, command: Command) -> Result<(), CliError> {
    let mut client = Client::connect(broker).await?;
    match command {
        Command::Produce {
            topic,
            message,
            key,
            acks,
            producer_id,
            sequence,
        } => {
            produce::execute(
                &mut client,
                produce::Options {
                    topic,
                    message,
                    key,
                    acks,
                    producer_id,
                    sequence,
                },
            )
            .await
        }
        Command::ProduceBatch {
            topic,
            message,
            key,
            acks,
            compression,
        } => {
            produce::execute_batch(
                &mut client,
                produce::BatchOptions {
                    topic,
                    messages: message,
                    key,
                    acks,
                    compression,
                },
            )
            .await
        }
        Command::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => fetch::execute(&mut client, topic, partition, offset, max_bytes, wait_ms).await,
        Command::Group { command } => group::execute(&mut client, command).await,
        Command::Topic { command } => topic::execute(&mut client, command).await,
        Command::Consume { .. } => unreachable!("consume has a dedicated execution path"),
    }
}
