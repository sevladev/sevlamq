use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use sevlamq_protocol::{ClusterMetadata, PartitionLeadership};
use thiserror::Error;

#[derive(Clone)]
pub struct LeadershipRegistry {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    cluster: Arc<ClusterMetadata>,
    entries: RwLock<HashMap<(String, u32), PartitionLeadership>>,
}

impl LeadershipRegistry {
    pub fn open(data_dir: &Path, cluster: Arc<ClusterMetadata>) -> Result<Self, LeadershipError> {
        let directory = data_dir.join("__cluster");
        fs::create_dir_all(&directory)?;
        let path = directory.join("leadership.meta");
        let entries = if path.exists() {
            parse_entries(&fs::read_to_string(&path)?)?
        } else {
            HashMap::new()
        };
        Ok(Self {
            inner: Arc::new(Inner {
                path,
                cluster,
                entries: RwLock::new(entries),
            }),
        })
    }

    pub fn ensure_topic(&self, topic: &str, partitions: u32) -> Result<(), LeadershipError> {
        let mut entries = self
            .inner
            .entries
            .write()
            .map_err(|_| LeadershipError::Poisoned)?;
        let mut changed = false;
        for partition in 0..partitions {
            let key = (topic.to_owned(), partition);
            if let std::collections::hash_map::Entry::Vacant(entry) = entries.entry(key) {
                entry.insert(PartitionLeadership {
                    partition,
                    leader_id: deterministic_leader(&self.inner.cluster, partition),
                    leader_epoch: 0,
                    high_watermark: 0,
                });
                changed = true;
            }
        }
        if changed {
            persist_entries(&self.inner.path, &entries)?;
        }
        Ok(())
    }

    pub fn topic(
        &self,
        topic: &str,
        partitions: u32,
    ) -> Result<Vec<PartitionLeadership>, LeadershipError> {
        self.ensure_topic(topic, partitions)?;
        let entries = self
            .inner
            .entries
            .read()
            .map_err(|_| LeadershipError::Poisoned)?;
        (0..partitions)
            .map(|partition| {
                entries
                    .get(&(topic.to_owned(), partition))
                    .copied()
                    .ok_or(LeadershipError::MissingPartition(partition))
            })
            .collect()
    }

    pub fn partition(
        &self,
        topic: &str,
        partition: u32,
    ) -> Result<PartitionLeadership, LeadershipError> {
        self.inner
            .entries
            .read()
            .map_err(|_| LeadershipError::Poisoned)?
            .get(&(topic.to_owned(), partition))
            .copied()
            .ok_or(LeadershipError::MissingPartition(partition))
    }

    pub fn apply(
        &self,
        topic: &str,
        leadership: PartitionLeadership,
    ) -> Result<(), LeadershipError> {
        if !self
            .inner
            .cluster
            .nodes
            .iter()
            .any(|node| node.id == leadership.leader_id)
        {
            return Err(LeadershipError::UnknownBroker(leadership.leader_id));
        }
        let mut entries = self
            .inner
            .entries
            .write()
            .map_err(|_| LeadershipError::Poisoned)?;
        let key = (topic.to_owned(), leadership.partition);
        if let Some(current) = entries.get(&key) {
            if leadership.leader_epoch < current.leader_epoch
                || (leadership.leader_epoch == current.leader_epoch
                    && (leadership.leader_id != current.leader_id
                        || leadership.high_watermark < current.high_watermark))
            {
                return Err(LeadershipError::StaleEpoch);
            }
            if leadership == *current {
                return Ok(());
            }
        }
        entries.insert(key, leadership);
        let result = persist_entries(&self.inner.path, &entries);
        drop(entries);
        result
    }

    pub fn migrate_high_watermark(
        &self,
        topic: &str,
        partition: u32,
        high_watermark: u64,
    ) -> Result<(), LeadershipError> {
        let mut entries = self
            .inner
            .entries
            .write()
            .map_err(|_| LeadershipError::Poisoned)?;
        let entry = entries
            .get_mut(&(topic.to_owned(), partition))
            .ok_or(LeadershipError::MissingPartition(partition))?;
        if entry.high_watermark != u64::MAX {
            return Ok(());
        }
        entry.high_watermark = high_watermark;
        let result = persist_entries(&self.inner.path, &entries);
        drop(entries);
        result
    }
}

fn deterministic_leader(cluster: &ClusterMetadata, partition: u32) -> u32 {
    let index = usize::try_from(partition).map_or(0, |value| value % cluster.nodes.len());
    cluster.nodes[index].id
}

fn parse_entries(
    contents: &str,
) -> Result<HashMap<(String, u32), PartitionLeadership>, LeadershipError> {
    let mut entries = HashMap::new();
    for line in contents.lines().filter(|line| !line.is_empty()) {
        let mut fields = line.split('\t');
        let topic = fields.next().ok_or(LeadershipError::InvalidMetadata)?;
        let partition = fields
            .next()
            .ok_or(LeadershipError::InvalidMetadata)?
            .parse()
            .map_err(|_| LeadershipError::InvalidMetadata)?;
        let leader_id = fields
            .next()
            .ok_or(LeadershipError::InvalidMetadata)?
            .parse()
            .map_err(|_| LeadershipError::InvalidMetadata)?;
        let leader_epoch = fields
            .next()
            .ok_or(LeadershipError::InvalidMetadata)?
            .parse()
            .map_err(|_| LeadershipError::InvalidMetadata)?;
        let high_watermark = fields
            .next()
            .map_or(Ok(u64::MAX), str::parse)
            .map_err(|_| LeadershipError::InvalidMetadata)?;
        if fields.next().is_some() {
            return Err(LeadershipError::InvalidMetadata);
        }
        entries.insert(
            (topic.to_owned(), partition),
            PartitionLeadership {
                partition,
                leader_id,
                leader_epoch,
                high_watermark,
            },
        );
    }
    Ok(entries)
}

fn persist_entries(
    path: &Path,
    entries: &HashMap<(String, u32), PartitionLeadership>,
) -> Result<(), LeadershipError> {
    let temporary = path.with_extension("meta.tmp");
    let mut ordered: Vec<_> = entries.iter().collect();
    ordered.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut file = File::create(&temporary)?;
    for ((topic, _), leadership) in ordered {
        writeln!(
            file,
            "{topic}\t{}\t{}\t{}\t{}",
            leadership.partition,
            leadership.leader_id,
            leadership.leader_epoch,
            leadership.high_watermark
        )?;
    }
    file.sync_all()?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum LeadershipError {
    #[error("leadership metadata I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("leadership metadata is malformed")]
    InvalidMetadata,
    #[error("leadership metadata lock is poisoned")]
    Poisoned,
    #[error("partition {0} has no leadership metadata")]
    MissingPartition(u32),
    #[error("broker {0} is not in cluster membership")]
    UnknownBroker(u32),
    #[error("leadership epoch is not newer than the current epoch")]
    StaleEpoch,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use sevlamq_protocol::{ClusterMetadata, ClusterNode, PartitionLeadership};
    use tempfile::tempdir;

    use super::{LeadershipError, LeadershipRegistry};

    fn cluster() -> Arc<ClusterMetadata> {
        Arc::new(ClusterMetadata {
            responding_broker_id: 1,
            nodes: (1_u16..=3)
                .map(|id| ClusterNode {
                    id: u32::from(id),
                    host: "127.0.0.1".to_owned(),
                    port: 7390 + id * 10,
                    admin_port: 7391 + id * 10,
                    replication_port: 7392 + id * 10,
                })
                .collect(),
        })
    }

    #[test]
    fn persists_promoted_leadership_and_rejects_stale_epochs() {
        let directory = tempdir().expect("temporary directory should be created");
        let registry =
            LeadershipRegistry::open(directory.path(), cluster()).expect("registry should open");
        registry
            .ensure_topic("payments", 3)
            .expect("topic should be initialized");
        assert_eq!(
            registry
                .partition("payments", 1)
                .expect("partition should exist")
                .leader_id,
            2
        );
        let promoted = PartitionLeadership {
            partition: 1,
            leader_id: 3,
            leader_epoch: 1,
            high_watermark: 7,
        };
        registry
            .apply("payments", promoted)
            .expect("new epoch should be accepted");
        registry
            .apply("payments", promoted)
            .expect("reapplying identical metadata should be idempotent");
        let stale = PartitionLeadership {
            high_watermark: 6,
            ..promoted
        };
        assert!(matches!(
            registry.apply("payments", stale),
            Err(LeadershipError::StaleEpoch)
        ));
        let committed = PartitionLeadership {
            high_watermark: 8,
            ..promoted
        };
        registry
            .apply("payments", committed)
            .expect("high watermark should advance within an epoch");

        let reopened =
            LeadershipRegistry::open(directory.path(), cluster()).expect("registry should reopen");
        assert_eq!(
            reopened
                .partition("payments", 1)
                .expect("promoted partition should persist"),
            committed
        );
    }
}
