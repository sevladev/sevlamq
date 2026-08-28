use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use bytes::BytesMut;
use sevlamq_common::BrokerConfig;
use sevlamq_protocol::{Request, Response, decode_request, encode_response};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
};
use tracing::{debug, info, warn};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub async fn run(config: &BrokerConfig) -> Result<(), BrokerError> {
    let address = config.socket_addr()?;
    let listener = TcpListener::bind(address).await?;

    info!(%address, data_dir = %config.data_dir, "broker started");
    accept_connections(&listener).await?;
    info!("shutdown signal received");

    drop(listener);
    info!("broker stopped");
    Ok(())
}

async fn accept_connections(listener: &TcpListener) -> Result<(), BrokerError> {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            signal = shutdown_signal() => {
                signal?;
                break;
            }
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
                connections.spawn(handle_connection(stream, peer, connection_id));
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                report_connection_result(result);
            }
        }
    }

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        report_connection_result(result);
    }
    Ok(())
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    connection_id: u64,
) -> Result<(), ConnectionError> {
    let mut read_buffer = BytesMut::with_capacity(8 * 1024);
    let mut write_buffer = BytesMut::with_capacity(128);
    debug!(connection_id, %peer, "connection opened");

    loop {
        let bytes_read = stream.read_buf(&mut read_buffer).await?;
        if bytes_read == 0 {
            if !read_buffer.is_empty() {
                return Err(ConnectionError::TruncatedFrame);
            }
            break;
        }

        while let Some(request) = decode_request(&mut read_buffer)? {
            match request {
                Request::Produce(request) => {
                    debug!(
                        connection_id,
                        %peer,
                        topic = %request.topic(),
                        payload_bytes = request.payload().len(),
                        "produce received"
                    );
                    encode_response(Response::ProduceAck, &mut write_buffer);
                    stream.write_all(&write_buffer).await?;
                    write_buffer.clear();
                }
            }
        }
    }

    debug!(connection_id, %peer, "connection closed");
    Ok(())
}

fn report_connection_result(result: Result<Result<(), ConnectionError>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "connection closed with error"),
        Err(error) if error.is_cancelled() => {}
        Err(error) => warn!(%error, "connection task failed"),
    }
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

#[derive(Debug, Error)]
enum ConnectionError {
    #[error("connection I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] sevlamq_protocol::ProtocolError),
    #[error("connection closed with a partial frame")]
    TruncatedFrame,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use sevlamq_client::Client;
    use tokio::{net::TcpListener, time::timeout};

    use super::handle_connection;

    #[tokio::test]
    async fn acknowledges_a_produce_request() {
        timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let address = listener
                .local_addr()
                .expect("listener should have an address");
            let server = tokio::spawn(async move {
                let (stream, peer) = listener.accept().await.expect("connection should arrive");
                handle_connection(stream, peer, 1).await
            });

            let mut client = Client::connect(address)
                .await
                .expect("client should connect");
            client
                .produce(
                    "payments".to_owned(),
                    Bytes::from_static(b"customer-123"),
                    Bytes::from_static(br#"{"amount":150}"#),
                )
                .await
                .expect("broker should acknowledge produce");

            drop(client);
            server
                .await
                .expect("connection task should finish")
                .expect("connection should close cleanly");
        })
        .await
        .expect("test should not time out");
    }
}
