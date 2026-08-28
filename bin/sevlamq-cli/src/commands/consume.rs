use std::{collections::HashMap, net::SocketAddr, time::Duration};

use sevlamq_client::{Client, ClientError};
use sevlamq_protocol::{CommitOffsetRequest, GroupFetchRequest};
use tokio::{sync::watch, task::JoinHandle};

use crate::{cli::DeliveryMode, error::CliError};

pub(super) struct Options {
    pub topic: String,
    pub group: String,
    pub member: String,
    pub delivery: DeliveryMode,
    pub wait_ms: u32,
    pub heartbeat_ms: u64,
    pub max_bytes: u32,
}

pub(super) async fn execute(broker: SocketAddr, options: Options) -> Result<(), CliError> {
    loop {
        let mut client = Client::connect(broker).await?;
        let joined = client
            .join_group(
                options.group.clone(),
                options.topic.clone(),
                options.member.clone(),
            )
            .await?;
        let generation = joined.generation();
        let partitions = joined.partitions().to_vec();
        println!("generation={generation} partitions={partitions:?}");

        let (stop, heartbeat) = start_heartbeat(broker, &options, generation);
        let outcome =
            consume_generation(&mut client, &options, generation, &partitions, &heartbeat).await?;
        let _ = stop.send(true);
        let _ = heartbeat.await;

        match outcome {
            GenerationOutcome::Shutdown => {
                if let Ok(mut leaving) = Client::connect(broker).await {
                    let _ = leaving
                        .leave_group(
                            options.group.clone(),
                            options.topic.clone(),
                            options.member.clone(),
                            generation,
                        )
                        .await;
                }
                return Ok(());
            }
            GenerationOutcome::Rejoin => {
                eprintln!("group generation changed; rejoining");
            }
        }
    }
}

async fn consume_generation(
    client: &mut Client,
    options: &Options,
    generation: u64,
    partitions: &[u32],
    heartbeat: &JoinHandle<Result<(), ClientError>>,
) -> Result<GenerationOutcome, CliError> {
    let mut offsets = load_offsets(client, options, partitions).await?;
    loop {
        if heartbeat.is_finished() {
            return Ok(GenerationOutcome::Rejoin);
        }
        if partitions.is_empty() {
            tokio::time::sleep(Duration::from_millis(250)).await;
            continue;
        }
        for partition in partitions {
            let offset = offsets.get(partition).copied().unwrap_or_default();
            let fetched = tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    signal.map_err(ClientError::Io)?;
                    return Ok(GenerationOutcome::Shutdown);
                }
                response = client.group_fetch(GroupFetchRequest {
                    group: options.group.clone(),
                    topic: options.topic.clone(),
                    member: options.member.clone(),
                    generation,
                    partition: *partition,
                    offset,
                    max_bytes: options.max_bytes,
                    max_wait_ms: options.wait_ms,
                }) => response,
            };
            let response = match fetched {
                Ok(response) => response,
                Err(ClientError::Server(_)) => return Ok(GenerationOutcome::Rejoin),
                Err(error) => return Err(error.into()),
            };
            let Some(last) = response.records().last() else {
                continue;
            };
            let next_offset = last.offset.checked_add(1).ok_or(CliError::OffsetOverflow)?;
            if matches!(options.delivery, DeliveryMode::AtMostOnce)
                && commit_requires_rejoin(
                    commit(client, options, generation, *partition, next_offset).await,
                )?
            {
                return Ok(GenerationOutcome::Rejoin);
            }
            super::fetch::print_partition_records(*partition, &response);
            if matches!(options.delivery, DeliveryMode::AtLeastOnce)
                && commit_requires_rejoin(
                    commit(client, options, generation, *partition, next_offset).await,
                )?
            {
                return Ok(GenerationOutcome::Rejoin);
            }
            offsets.insert(*partition, next_offset);
        }
    }
}

async fn load_offsets(
    client: &mut Client,
    options: &Options,
    partitions: &[u32],
) -> Result<HashMap<u32, u64>, CliError> {
    let mut offsets = HashMap::with_capacity(partitions.len());
    for partition in partitions {
        let offset = client
            .committed_offset(options.group.clone(), options.topic.clone(), *partition)
            .await?
            .unwrap_or_default();
        offsets.insert(*partition, offset);
    }
    Ok(offsets)
}

async fn commit(
    client: &mut Client,
    options: &Options,
    generation: u64,
    partition: u32,
    offset: u64,
) -> Result<(), ClientError> {
    client
        .commit_offset(CommitOffsetRequest {
            group: options.group.clone(),
            topic: options.topic.clone(),
            member: options.member.clone(),
            generation,
            partition,
            offset,
        })
        .await
}

fn commit_requires_rejoin(result: Result<(), ClientError>) -> Result<bool, CliError> {
    match result {
        Ok(()) => Ok(false),
        Err(ClientError::Server(_)) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn start_heartbeat(
    broker: SocketAddr,
    options: &Options,
    generation: u64,
) -> (watch::Sender<bool>, JoinHandle<Result<(), ClientError>>) {
    let (stop, mut stopped) = watch::channel(false);
    let group = options.group.clone();
    let topic = options.topic.clone();
    let member = options.member.clone();
    let heartbeat_ms = options.heartbeat_ms;
    let heartbeat = tokio::spawn(async move {
        let mut client = Client::connect(broker).await?;
        let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_ms));
        interval.tick().await;
        loop {
            tokio::select! {
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    client
                        .heartbeat(group.clone(), topic.clone(), member.clone(), generation)
                        .await?;
                }
            }
        }
    });
    (stop, heartbeat)
}

enum GenerationOutcome {
    Shutdown,
    Rejoin,
}
