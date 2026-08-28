use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Client(#[from] sevlamq_client::ClientError),
    #[error("record offset space is exhausted")]
    OffsetOverflow,
}
