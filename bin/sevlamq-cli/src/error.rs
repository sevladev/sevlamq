use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error(transparent)]
    Client(#[from] sevlamq_client::ClientError),
    #[error("record offset space is exhausted")]
    OffsetOverflow,
    #[error("retry attempt space is exhausted")]
    AttemptOverflow,
    #[error("retry envelope exceeds the message size limit")]
    RetryEnvelopeTooLarge,
    #[error("retry envelope is malformed")]
    InvalidRetryEnvelope,
    #[error("system time is outside the supported range")]
    InvalidSystemTime,
    #[error("handler process did not expose stdin")]
    MissingHandlerStdin,
    #[error("handler I/O failed: {0}")]
    Io(#[from] std::io::Error),
}
