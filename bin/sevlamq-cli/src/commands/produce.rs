use bytes::Bytes;
use sevlamq_client::Client;

use crate::{cli::ProduceAcks, error::CliError};

pub(super) struct Options {
    pub topic: String,
    pub message: String,
    pub key: Option<String>,
    pub acks: ProduceAcks,
    pub producer_id: Option<String>,
    pub sequence: Option<u64>,
}

pub(super) async fn execute(client: &mut Client, options: Options) -> Result<(), CliError> {
    let key = options.key.map_or_else(Bytes::new, Bytes::from);
    let ack_mode = match options.acks {
        ProduceAcks::Leader => sevlamq_protocol::AckMode::Leader,
        ProduceAcks::Durable => sevlamq_protocol::AckMode::Durable,
    };
    let producer = options
        .producer_id
        .zip(options.sequence)
        .map(|(id, sequence)| sevlamq_protocol::ProducerIdentity { id, sequence });
    let ack = client
        .produce(
            options.topic,
            key,
            Bytes::from(options.message),
            ack_mode,
            producer,
        )
        .await?;
    println!("partition={} offset={}", ack.partition, ack.offset);
    Ok(())
}
