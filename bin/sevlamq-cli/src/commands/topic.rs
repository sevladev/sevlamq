use sevlamq_client::Client;

use crate::{cli::TopicCommand, error::CliError};

pub(super) async fn execute(client: &mut Client, command: TopicCommand) -> Result<(), CliError> {
    match command {
        TopicCommand::Create { topic, partitions } => {
            print_topic(&client.create_topic(topic, partitions).await?);
        }
        TopicCommand::List => {
            for topic in client.list_topics().await? {
                print_topic(&topic);
            }
        }
        TopicCommand::Describe { topic } => {
            print_topic(&client.describe_topic(topic).await?);
        }
    }
    Ok(())
}

fn print_topic(topic: &sevlamq_protocol::TopicMetadata) {
    let leaders = topic
        .leaders
        .iter()
        .enumerate()
        .map(|(partition, leader)| format!("{partition}:{leader}"))
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "topic={} partitions={} leaders=[{}]",
        topic.topic, topic.partitions, leaders
    );
}
