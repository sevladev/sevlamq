use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use sevlamq_protocol::{
    AckMode, CommitOffsetRequest, FetchCommittedOffsetRequest, FetchRequest, FetchResponse,
    GroupFetchRequest, GroupGenerationRequest, GroupMemberRequest, JoinGroupResponse, ProduceAck,
    ProduceRequest, Request, Response, decode_response, encode_request,
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
        ack_mode: AckMode,
    ) -> Result<ProduceAck, ClientError> {
        let request = Request::Produce(ProduceRequest::new(topic, key, payload, ack_mode)?);
        match self.request(request).await? {
            Response::ProduceAck(ack) => Ok(ack),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn fetch(
        &mut self,
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<FetchResponse, ClientError> {
        let request = Request::Fetch(FetchRequest::new(
            topic,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        )?);
        match self.request(request).await? {
            Response::Fetch(response) => Ok(response),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn join_group(
        &mut self,
        group: String,
        topic: String,
        member: String,
    ) -> Result<JoinGroupResponse, ClientError> {
        match self
            .request(Request::JoinGroup(GroupMemberRequest {
                group,
                topic,
                member,
            }))
            .await?
        {
            Response::JoinGroup(response) => Ok(response),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn heartbeat(
        &mut self,
        group: String,
        topic: String,
        member: String,
        generation: u64,
    ) -> Result<(), ClientError> {
        self.group_ack(Request::Heartbeat(GroupGenerationRequest {
            group,
            topic,
            member,
            generation,
        }))
        .await
    }

    pub async fn leave_group(
        &mut self,
        group: String,
        topic: String,
        member: String,
        generation: u64,
    ) -> Result<(), ClientError> {
        self.group_ack(Request::LeaveGroup(GroupGenerationRequest {
            group,
            topic,
            member,
            generation,
        }))
        .await
    }

    pub async fn commit_offset(&mut self, request: CommitOffsetRequest) -> Result<(), ClientError> {
        self.group_ack(Request::CommitOffset(request)).await
    }

    pub async fn committed_offset(
        &mut self,
        group: String,
        topic: String,
        partition: u32,
    ) -> Result<Option<u64>, ClientError> {
        match self
            .request(Request::FetchCommittedOffset(FetchCommittedOffsetRequest {
                group,
                topic,
                partition,
            }))
            .await?
        {
            Response::CommittedOffset(offset) => Ok(offset),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn group_fetch(
        &mut self,
        request: GroupFetchRequest,
    ) -> Result<FetchResponse, ClientError> {
        match self.request(Request::GroupFetch(request)).await? {
            Response::Fetch(response) => Ok(response),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    async fn group_ack(&mut self, request: Request) -> Result<(), ClientError> {
        match self.request(request).await? {
            Response::GroupAck => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    async fn request(&mut self, request: Request) -> Result<Response, ClientError> {
        encode_request(&request, &mut self.write_buffer)?;
        self.stream.write_all(&self.write_buffer).await?;
        self.write_buffer.clear();

        loop {
            if let Some(response) = decode_response(&mut self.read_buffer)? {
                return match response {
                    Response::Error(message) => Err(ClientError::Server(message)),
                    response => Ok(response),
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
    #[error("broker rejected the request: {0}")]
    Server(String),
}
