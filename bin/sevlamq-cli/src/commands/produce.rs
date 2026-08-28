use bytes::Bytes;
use sevlamq_client::Client;

use crate::{cli::ProduceAcks, error::CliError};

pub(super) async fn execute(
    client: &mut Client,
    topic: String,
    message: String,
    key: Option<String>,
    acks: ProduceAcks,
) -> Result<(), CliError> {
    let key = key.map_or_else(Bytes::new, Bytes::from);
    let ack_mode = match acks {
        ProduceAcks::Leader => sevlamq_protocol::AckMode::Leader,
        ProduceAcks::Durable => sevlamq_protocol::AckMode::Durable,
    };
    let ack = client
        .produce(topic, key, Bytes::from(message), ack_mode)
        .await?;
    println!("partition={} offset={}", ack.partition, ack.offset);
    Ok(())
}
