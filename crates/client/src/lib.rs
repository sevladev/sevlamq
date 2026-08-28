use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use sevlamq_protocol::{
    FetchRequest, FetchResponse, ProduceAck, ProduceRequest, Request, Response, decode_response,
    encode_request,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub struct Client {
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl Client {
    pub async fn connect(address: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self {
            stream,
            read_buffer: BytesMut::with_capacity(8 * 1024),
            write_buffer: BytesMut::with_capacity(8 * 1024),
        })
    }

    pub async fn produce(
        &mut self,
        topic: String,
        key: Bytes,
        payload: Bytes,
    ) -> Result<ProduceAck, ClientError> {
        let request = Request::Produce(ProduceRequest::new(topic, key, payload)?);
        encode_request(&request, &mut self.write_buffer)?;
        self.stream.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();

        loop {
            if let Some(response) = decode_response(&mut self.read_buffer)? {
                return match response {
                    Response::ProduceAck(ack) => Ok(ack),
                    Response::Fetch(_) => Err(ClientError::UnexpectedResponse),
                };
            }

            if self.stream.read_buf(&mut self.read_buffer).await? == 0 {
                return Err(ClientError::ConnectionClosed);
            }
        }
    }

    pub async fn fetch(
        &mut self,
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
    ) -> Result<FetchResponse, ClientError> {
        let request = Request::Fetch(FetchRequest::new(topic, partition, offset, max_bytes)?);
        encode_request(&request, &mut self.write_buffer)?;
        self.stream.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();

        loop {
            if let Some(response) = decode_response(&mut self.read_buffer)? {
                return match response {
                    Response::Fetch(response) => Ok(response),
                    Response::ProduceAck(_) => Err(ClientError::UnexpectedResponse),
                };
            }

            if self.stream.read_buf(&mut self.read_buffer).await? == 0 {
                return Err(ClientError::ConnectionClosed);
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("client I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] sevlamq_protocol::ProtocolError),
    #[error("broker closed the connection before sending a response")]
    ConnectionClosed,
    #[error("broker returned a response for a different operation")]
    UnexpectedResponse,
}
