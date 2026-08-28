use sevlamq_client::Client;
use sevlamq_protocol::{CommitOffsetRequest, GroupFetchRequest};

use crate::{cli::GroupCommand, error::CliError};

pub(super) async fn execute(client: &mut Client, command: GroupCommand) -> Result<(), CliError> {
    match command {
        GroupCommand::Join {
            group,
            topic,
            member,
        } => join(client, group, topic, member).await?,
        GroupCommand::Heartbeat {
            group,
            topic,
            member,
            generation,
        } => {
            client.heartbeat(group, topic, member, generation).await?;
            println!("heartbeat accepted");
        }
        GroupCommand::Leave {
            group,
            topic,
            member,
            generation,
        } => {
            client.leave_group(group, topic, member, generation).await?;
            println!("member left group");
        }
        GroupCommand::Commit {
            group,
            topic,
            member,
            generation,
            partition,
            offset,
        } => {
            client
                .commit_offset(CommitOffsetRequest {
                    group,
                    topic,
                    member,
                    generation,
                    partition,
                    offset,
                })
                .await?;
            println!("offset committed");
        }
        GroupCommand::Offset {
            group,
            topic,
            partition,
        } => match client.committed_offset(group, topic, partition).await? {
            Some(offset) => println!("offset={offset}"),
            None => println!("offset=-"),
        },
        GroupCommand::Fetch {
            group,
            topic,
            member,
            generation,
            partition,
            offset,
            max_bytes,
            wait_ms,
        } => {
            let response = client
                .group_fetch(GroupFetchRequest {
                    group,
                    topic,
                    member,
                    generation,
                    partition,
                    offset,
                    max_bytes,
                    max_wait_ms: wait_ms,
                })
                .await?;
            super::fetch::print_records(&response);
        }
    }
    Ok(())
}

async fn join(
    client: &mut Client,
    group: String,
    topic: String,
    member: String,
) -> Result<(), CliError> {
    let response = client.join_group(group, topic, member).await?;
    let partitions = response
        .partitions()
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "generation={} partitions={partitions}",
        response.generation()
    );
    Ok(())
}
