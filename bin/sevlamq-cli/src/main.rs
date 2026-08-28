use clap::Parser;

use crate::{cli::Cli, error::CliError};

mod cli;
mod commands;
mod error;

#[tokio::main]
async fn main() -> Result<(), CliError> {
    commands::execute(Cli::parse()).await
}
