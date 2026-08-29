use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use sevlamq_protocol::{
    AckMode, ClusterMetadata, ClusterNode, CommitOffsetRequest, CreateTopicRequest,
    FetchCommittedOffsetRequest, FetchRequest, FetchResponse, GroupFetchRequest,
    GroupGenerationRequest, GroupMemberRequest, JoinGroupResponse, ProduceAck, ProduceBatchRequest,
    ProduceRequest, ProducerIdentity, Request, Response, TopicMetadata, decode_response,
    encode_request,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub struct Client {
    address: SocketAddr,
    stream: TcpStream,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl Client {
    pub async fn connect(address: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self {
            address,
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
        producer: Option<ProducerIdentity>,
    ) -> Result<ProduceAck, ClientError> {
        let cluster = self.cluster_metadata().await?;
        let mut request = ProduceRequest::new(
            topic.clone(),
            key.clone(),
            payload,
            ack_mode,
            producer.clone(),
        )?;
        let address = if cluster.nodes.len() == 1 {
            self.address
        } else {
            let topic_metadata = self.describe_topic(topic).await?;
            let routing_key = if key.is_empty() {
                producer.as_ref().map(|identity| identity.id.as_bytes())
            } else {
                Some(key.as_ref())
            };
            let partition =
                routing_key.map_or(0, |key| crc32fast::hash(key) % topic_metadata.partitions);
            request = request.with_partition(partition);
            leader_address(&cluster, partition)?
        };
        match self.request_at(address, Request::Produce(request)).await? {
            Response::ProduceAck(ack) => Ok(ack),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn produce_batch(
        &mut self,
        request: ProduceBatchRequest,
    ) -> Result<Vec<ProduceAck>, ClientError> {
        let cluster = self.cluster_metadata().await?;
        let (address, request) = if cluster.nodes.len() == 1 {
            (self.address, request)
        } else {
            let topic = self.describe_topic(request.topic().to_owned()).await?;
            let partition = request
                .records()
                .first()
                .filter(|record| !record.key.is_empty())
                .map_or(0, |record| crc32fast::hash(&record.key) % topic.partitions);
            if request.records().iter().any(|record| {
                !record.key.is_empty()
                    && crc32fast::hash(&record.key) % topic.partitions != partition
            }) {
                return Err(ClientError::BatchSpansPartitions);
            }
            (
                leader_address(&cluster, partition)?,
                request.with_partition(partition),
            )
        };
        match self
            .request_at(address, Request::ProduceBatch(request))
            .await?
        {
            Response::ProduceBatchAck(acks) => Ok(acks),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn create_topic(
        &mut self,
        topic: String,
        partitions: u32,
    ) -> Result<TopicMetadata, ClientError> {
        let cluster = self.cluster_metadata().await?;
        let mut created = None;
        for node in &cluster.nodes {
            let address = node_address(node)?;
            match self
                .request_at(
                    address,
                    Request::CreateTopic(CreateTopicRequest {
                        topic: topic.clone(),
                        partitions,
                    }),
                )
                .await?
            {
                Response::Topics(mut topics) if topics.len() == 1 => {
                    created.get_or_insert_with(|| topics.remove(0));
                }
                _ => return Err(ClientError::UnexpectedResponse),
            }
        }
        created.ok_or(ClientError::UnexpectedResponse)
    }

    pub async fn list_topics(&mut self) -> Result<Vec<TopicMetadata>, ClientError> {
        match self.request(Request::ListTopics).await? {
            Response::Topics(topics) => Ok(topics),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn describe_topic(&mut self, topic: String) -> Result<TopicMetadata, ClientError> {
        match self.request(Request::DescribeTopic(topic)).await? {
            Response::Topics(mut topics) if topics.len() == 1 => Ok(topics.remove(0)),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub async fn cluster_metadata(&mut self) -> Result<ClusterMetadata, ClientError> {
        match self.request(Request::ClusterMetadata).await? {
            Response::ClusterMetadata(metadata) => Ok(metadata),
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
        let cluster = self.cluster_metadata().await?;
        let address = if cluster.nodes.len() == 1 {
            self.address
        } else {
            leader_address(&cluster, partition)?
        };
        match self.request_at(address, request).await? {
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

    async fn request_at(
        &mut self,
        address: SocketAddr,
        request: Request,
    ) -> Result<Response, ClientError> {
        if address == self.address {
            self.request(request).await
        } else {
            Self::connect(address).await?.request(request).await
        }
    }
}

fn leader_address(cluster: &ClusterMetadata, partition: u32) -> Result<SocketAddr, ClientError> {
    let partition = usize::try_from(partition).map_err(|_| ClientError::InvalidPartition)?;
    if cluster.nodes.is_empty() {
        return Err(ClientError::EmptyCluster);
    }
    let node = cluster
        .nodes
        .get(partition % cluster.nodes.len())
        .ok_or(ClientError::EmptyCluster)?;
    node_address(node)
}

fn node_address(node: &ClusterNode) -> Result<SocketAddr, ClientError> {
    format!("{}:{}", node.host, node.port)
        .parse()
        .map_err(ClientError::InvalidBrokerAddress)
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
    #[error("broker advertised an invalid client address: {0}")]
    InvalidBrokerAddress(std::net::AddrParseError),
    #[error("broker returned an empty cluster membership")]
    EmptyCluster,
    #[error("partition does not fit this platform's address space")]
    InvalidPartition,
    #[error("a compressed batch cannot span multiple partitions")]
    BatchSpansPartitions,
    #[error("broker rejected the request: {0}")]
    Server(String),
}
