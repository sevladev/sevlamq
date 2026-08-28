use sevlamq_client::Client;

use crate::{
    cli::{Cli, Command},
    error::CliError,
};

mod fetch;
mod group;
mod produce;

pub async fn execute(cli: Cli) -> Result<(), CliError> {
    let mut client = Client::connect(cli.broker).await?;
    match cli.command {
        Command::Produce {
            topic,
            message,
            key,
        } => produce::execute(&mut client, topic, message, key).await,
        Command::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => fetch::execute(&mut client, topic, partition, offset, max_bytes, wait_ms).await,
        Command::Group { command } => group::execute(&mut client, command).await,
    }
}
