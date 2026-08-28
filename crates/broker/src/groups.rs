use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use sevlamq_protocol::JoinGroupResponse;
use thiserror::Error;

pub struct GroupCoordinator {
    offsets_dir: PathBuf,
    partition_count: u32,
    session_timeout: Duration,
    groups: HashMap<(String, String), ConsumerGroup>,
}

struct ConsumerGroup {
    generation: u64,
    members: BTreeMap<String, Instant>,
    assignments: HashMap<String, Vec<u32>>,
}

impl GroupCoordinator {
    pub fn open(
        data_dir: &Path,
        partition_count: u32,
        session_timeout: Duration,
    ) -> Result<Self, GroupError> {
        let offsets_dir = data_dir.join("__consumer_offsets");
        fs::create_dir_all(&offsets_dir)?;
        Ok(Self {
            offsets_dir,
            partition_count,
            session_timeout,
            groups: HashMap::new(),
        })
    }

    pub fn join(
        &mut self,
        group: &str,
        topic: &str,
        member: &str,
    ) -> Result<JoinGroupResponse, GroupError> {
        validate_id(group)?;
        validate_id(topic)?;
        validate_id(member)?;
        let key = (group.to_owned(), topic.to_owned());
        let consumer_group = self.groups.entry(key).or_insert_with(|| ConsumerGroup {
            generation: 0,
            members: BTreeMap::new(),
            assignments: HashMap::new(),
        });
        let members_expired = expire_members(consumer_group, self.session_timeout);
        let is_new = consumer_group
            .members
            .insert(member.to_owned(), Instant::now())
            .is_none();
        if is_new || members_expired || consumer_group.generation == 0 {
            rebalance(consumer_group, self.partition_count)?;
        }
        Ok(JoinGroupResponse::new(
            consumer_group.generation,
            consumer_group
                .assignments
                .get(member)
                .cloned()
                .unwrap_or_default(),
        ))
    }

    pub fn heartbeat(
        &mut self,
        group: &str,
        topic: &str,
        member: &str,
        generation: u64,
    ) -> Result<(), GroupError> {
        let consumer_group = self.active_group(group, topic)?;
        validate_member(consumer_group, member, generation)?;
        consumer_group
            .members
            .insert(member.to_owned(), Instant::now());
        Ok(())
    }

    pub fn leave(
        &mut self,
        group: &str,
        topic: &str,
        member: &str,
        generation: u64,
    ) -> Result<(), GroupError> {
        let partition_count = self.partition_count;
        let consumer_group = self.active_group(group, topic)?;
        validate_member(consumer_group, member, generation)?;
        consumer_group.members.remove(member);
        rebalance(consumer_group, partition_count)
    }

    pub fn commit(
        &mut self,
        identity: &GroupIdentity<'_>,
        partition: u32,
        offset: u64,
    ) -> Result<(), GroupError> {
        let consumer_group = self.active_group(identity.group, identity.topic)?;
        validate_member(consumer_group, identity.member, identity.generation)?;
        if !consumer_group
            .assignments
            .get(identity.member)
            .is_some_and(|partitions| partitions.contains(&partition))
        {
            return Err(GroupError::PartitionNotAssigned(partition));
        }
        persist_offset(
            &self.offsets_dir,
            identity.group,
            identity.topic,
            partition,
            offset,
        )
    }

    pub fn authorize_fetch(
        &mut self,
        identity: &GroupIdentity<'_>,
        partition: u32,
    ) -> Result<(), GroupError> {
        let consumer_group = self.active_group(identity.group, identity.topic)?;
        validate_member(consumer_group, identity.member, identity.generation)?;
        if !consumer_group
            .assignments
            .get(identity.member)
            .is_some_and(|partitions| partitions.contains(&partition))
        {
            return Err(GroupError::PartitionNotAssigned(partition));
        }
        Ok(())
    }

    pub fn committed_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>, GroupError> {
        validate_id(group)?;
        validate_id(topic)?;
        let path = offset_path(&self.offsets_dir, group, topic, partition);
        match fs::read_to_string(path) {
            Ok(contents) => contents
                .trim()
                .parse()
                .map(Some)
                .map_err(|_| GroupError::InvalidOffsetFile),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn expire_sessions(&mut self) -> Result<(), GroupError> {
        for group in self.groups.values_mut() {
            if expire_members(group, self.session_timeout) {
                rebalance(group, self.partition_count)?;
            }
        }
        Ok(())
    }

    fn active_group(&mut self, group: &str, topic: &str) -> Result<&mut ConsumerGroup, GroupError> {
        let consumer_group = self
            .groups
            .get_mut(&(group.to_owned(), topic.to_owned()))
            .ok_or(GroupError::UnknownGroup)?;
        if expire_members(consumer_group, self.session_timeout) {
            rebalance(consumer_group, self.partition_count)?;
        }
        Ok(consumer_group)
    }
}

pub struct GroupIdentity<'a> {
    pub group: &'a str,
    pub topic: &'a str,
    pub member: &'a str,
    pub generation: u64,
}

fn expire_members(group: &mut ConsumerGroup, timeout: Duration) -> bool {
    let before = group.members.len();
    group
        .members
        .retain(|_, heartbeat| heartbeat.elapsed() <= timeout);
    before != group.members.len()
}

fn rebalance(group: &mut ConsumerGroup, partition_count: u32) -> Result<(), GroupError> {
    group.generation = group
        .generation
        .checked_add(1)
        .ok_or(GroupError::GenerationOverflow)?;
    group.assignments.clear();
    if group.members.is_empty() {
        return Ok(());
    }
    let members: Vec<&String> = group.members.keys().collect();
    for partition in 0..partition_count {
        let index =
            usize::try_from(partition).map_err(|_| GroupError::GenerationOverflow)? % members.len();
        group
            .assignments
            .entry(members[index].clone())
            .or_default()
            .push(partition);
    }
    Ok(())
}

fn validate_member(group: &ConsumerGroup, member: &str, generation: u64) -> Result<(), GroupError> {
    if generation != group.generation {
        return Err(GroupError::StaleGeneration {
            expected: group.generation,
            actual: generation,
        });
    }
    if !group.members.contains_key(member) {
        return Err(GroupError::UnknownMember);
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), GroupError> {
    if value.is_empty()
        || value.len() > 249
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(GroupError::InvalidId);
    }
    Ok(())
}

fn persist_offset(
    root: &Path,
    group: &str,
    topic: &str,
    partition: u32,
    offset: u64,
) -> Result<(), GroupError> {
    let directory = root.join(group).join(topic);
    fs::create_dir_all(&directory)?;
    let path = offset_path(root, group, topic, partition);
    let temporary = path.with_extension("offset.tmp");
    let mut file = File::create(&temporary)?;
    writeln!(file, "{offset}")?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn offset_path(root: &Path, group: &str, topic: &str, partition: u32) -> PathBuf {
    root.join(group)
        .join(topic)
        .join(format!("{partition}.offset"))
}

#[derive(Debug, Error)]
pub enum GroupError {
    #[error("group, topic, or member id is invalid")]
    InvalidId,
    #[error("consumer group does not exist")]
    UnknownGroup,
    #[error("consumer member does not exist")]
    UnknownMember,
    #[error("stale generation: expected {expected}, received {actual}")]
    StaleGeneration { expected: u64, actual: u64 },
    #[error("partition {0} is not assigned to this member")]
    PartitionNotAssigned(u32),
    #[error("consumer group generation space is exhausted")]
    GenerationOverflow,
    #[error("committed offset file is invalid")]
    InvalidOffsetFile,
    #[error("consumer group coordinator lock is poisoned")]
    CoordinatorPoisoned,
    #[error("consumer group I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{GroupCoordinator, GroupError, GroupIdentity};

    #[test]
    fn rebalances_members_rejects_stale_generations_and_recovers_offsets() {
        let data_dir = tempdir().expect("temporary directory should be created");
        let mut coordinator = GroupCoordinator::open(data_dir.path(), 3, Duration::from_secs(10))
            .expect("coordinator should open");

        let worker_a = coordinator
            .join("workers", "payments", "worker-a")
            .expect("first member should join");
        assert_eq!(worker_a.generation(), 1);
        assert_eq!(worker_a.partitions(), [0, 1, 2]);

        let worker_b = coordinator
            .join("workers", "payments", "worker-b")
            .expect("second member should join");
        assert_eq!(worker_b.generation(), 2);
        assert_eq!(worker_b.partitions(), [1]);
        assert!(matches!(
            coordinator.heartbeat("workers", "payments", "worker-a", 1),
            Err(GroupError::StaleGeneration { .. })
        ));

        let worker_a = coordinator
            .join("workers", "payments", "worker-a")
            .expect("first member should rejoin after rebalance");
        assert_eq!(worker_a.generation(), 2);
        assert_eq!(worker_a.partitions(), [0, 2]);
        coordinator
            .commit(
                &GroupIdentity {
                    group: "workers",
                    topic: "payments",
                    member: "worker-a",
                    generation: 2,
                },
                0,
                42,
            )
            .expect("assigned offset should commit");
        drop(coordinator);

        let recovered = GroupCoordinator::open(data_dir.path(), 3, Duration::from_secs(10))
            .expect("coordinator should reopen");
        assert_eq!(
            recovered
                .committed_offset("workers", "payments", 0)
                .expect("offset should load"),
            Some(42)
        );
    }
}
