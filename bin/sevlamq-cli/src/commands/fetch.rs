use sevlamq_client::Client;
use sevlamq_protocol::FetchResponse;

use crate::error::CliError;

pub(super) async fn execute(
    client: &mut Client,
    topic: String,
    partition: u32,
    offset: u64,
    max_bytes: u32,
    wait_ms: u32,
) -> Result<(), CliError> {
    let response = client
        .fetch(topic, partition, offset, max_bytes, wait_ms)
        .await?;
    print_records(&response);
    Ok(())
}

pub(super) fn print_records(response: &FetchResponse) {
    for record in response.records() {
        let key = record
            .key
            .as_ref()
            .map_or_else(|| "-".into(), |key| String::from_utf8_lossy(key));
        println!(
            "offset={} timestamp_ms={} key={} value={}",
            record.offset,
            record.timestamp_ms,
            key,
            String::from_utf8_lossy(&record.value)
        );
    }
}

pub(super) fn print_partition_records(partition: u32, response: &FetchResponse) {
    for record in response.records() {
        let key = record
            .key
            .as_ref()
            .map_or_else(|| "-".into(), |key| String::from_utf8_lossy(key));
        println!(
            "partition={} offset={} timestamp_ms={} key={} value={}",
            partition,
            record.offset,
            record.timestamp_ms,
            key,
            String::from_utf8_lossy(&record.value)
        );
    }
}
