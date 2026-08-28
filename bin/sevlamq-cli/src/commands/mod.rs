use sevlamq_client::Client;

use crate::{
    cli::{Cli, Command},
    error::CliError,
};

mod consume;
mod fetch;
mod group;
mod produce;

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
        } => produce::execute(&mut client, topic, message, key, acks).await,
        Command::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => fetch::execute(&mut client, topic, partition, offset, max_bytes, wait_ms).await,
        Command::Group { command } => group::execute(&mut client, command).await,
        Command::Consume { .. } => unreachable!("consume has a dedicated execution path"),
    }
}
