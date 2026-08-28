use bytes::Bytes;
use sevlamq_client::Client;

use crate::error::CliError;

pub(super) async fn execute(
    client: &mut Client,
    topic: String,
    message: String,
    key: Option<String>,
) -> Result<(), CliError> {
    let key = key.map_or_else(Bytes::new, Bytes::from);
    let ack = client.produce(topic, key, Bytes::from(message)).await?;
    println!("partition={} offset={}", ack.partition, ack.offset);
    Ok(())
}
