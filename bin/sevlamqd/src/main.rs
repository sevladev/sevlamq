use std::{env, path::PathBuf};

use sevlamq_common::Config;
use thiserror::Error;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config_path = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("config/sevlamq.toml"), PathBuf::from);
    let config = Config::from_file(config_path)?;
    init_tracing(config.logging.json);

    sevlamq_broker::run(&config).await?;
    Ok(())
}

fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if json {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[derive(Debug, Error)]
enum AppError {
    #[error(transparent)]
    Config(#[from] sevlamq_common::ConfigError),
    #[error(transparent)]
    Broker(#[from] sevlamq_broker::BrokerError),
}
