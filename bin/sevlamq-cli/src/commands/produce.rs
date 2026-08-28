use bytes::Bytes;
use sevlamq_client::Client;

use crate::{
    cli::{BatchCompression, ProduceAcks},
    error::CliError,
};

pub(super) struct Options {
    pub topic: String,
    pub message: String,
    pub key: Option<String>,
    pub acks: ProduceAcks,
    pub producer_id: Option<String>,
    pub sequence: Option<u64>,
}

pub(super) struct BatchOptions {
    pub topic: String,
    pub messages: Vec<String>,
    pub key: Option<String>,
    pub acks: ProduceAcks,
    pub compression: BatchCompression,
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

pub(super) async fn execute_batch(
    client: &mut Client,
    options: BatchOptions,
) -> Result<(), CliError> {
    let key = options.key.map_or_else(Bytes::new, Bytes::from);
    let records = options
        .messages
        .into_iter()
        .map(|message| sevlamq_protocol::BatchRecord {
            key: key.clone(),
            payload: Bytes::from(message),
        })
        .collect();
    let ack_mode = match options.acks {
        ProduceAcks::Leader => sevlamq_protocol::AckMode::Leader,
        ProduceAcks::Durable => sevlamq_protocol::AckMode::Durable,
    };
    let compression = match options.compression {
        BatchCompression::None => sevlamq_protocol::Compression::None,
        BatchCompression::Zstd => sevlamq_protocol::Compression::Zstd,
    };
    let request =
        sevlamq_protocol::ProduceBatchRequest::new(options.topic, records, ack_mode, compression)?;
    let acks = client.produce_batch(request).await?;
    for ack in acks {
        println!("partition={} offset={}", ack.partition, ack.offset);
    }
    Ok(())
}
