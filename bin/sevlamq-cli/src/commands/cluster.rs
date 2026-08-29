use sevlamq_client::Client;

use crate::{cli::ClusterCommand, error::CliError};

pub(super) async fn execute(client: &mut Client, command: ClusterCommand) -> Result<(), CliError> {
    match command {
        ClusterCommand::Status => {
            let metadata = client.cluster_metadata().await?;
            println!("responding_broker={}", metadata.responding_broker_id);
            for node in metadata.nodes {
                let current = if node.id == metadata.responding_broker_id {
                    " current"
                } else {
                    ""
                };
                println!(
                    "broker={} address={}:{} admin={}:{} replication={}:{}{}",
                    node.id,
                    node.host,
                    node.port,
                    node.host,
                    node.admin_port,
                    node.host,
                    node.replication_port,
                    current
                );
            }
        }
        ClusterCommand::Promote {
            topic,
            partition,
            broker_id,
        } => {
            let leadership = client.promote_leader(topic, partition, broker_id).await?;
            println!(
                "partition={} leader={} epoch={}",
                leadership.partition, leadership.leader_id, leadership.leader_epoch
            );
        }
    }
    Ok(())
}
