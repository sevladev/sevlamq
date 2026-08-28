use sevlamq_common::BrokerConfig;
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::info;

pub async fn run(config: &BrokerConfig) -> Result<(), BrokerError> {
    let address = config.socket_addr()?;
    let listener = TcpListener::bind(address).await?;

    info!(%address, data_dir = %config.data_dir, "broker started");
    shutdown_signal().await?;
    info!("shutdown signal received");

    drop(listener);
    info!("broker stopped");
    Ok(())
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error(transparent)]
    Config(#[from] sevlamq_common::ConfigError),
    #[error("broker I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
