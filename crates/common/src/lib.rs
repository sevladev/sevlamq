use std::{fs, net::SocketAddr, path::Path};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub broker: BrokerConfig,
    #[serde(default)]
    pub admin: AdminConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    pub broker_id: u32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: usize,
    #[serde(default = "default_min_in_sync_replicas")]
    pub min_in_sync_replicas: usize,
    #[serde(default)]
    pub nodes: Vec<ClusterNodeConfig>,
}

const fn default_replication_factor() -> usize {
    1
}

const fn default_min_in_sync_replicas() -> usize {
    1
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            broker_id: 1,
            replication_factor: default_replication_factor(),
            min_in_sync_replicas: default_min_in_sync_replicas(),
            nodes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ClusterNodeConfig {
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub admin_port: u16,
    pub replication_port: u16,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct LoggingConfig {
    pub json: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AdminConfig {
    pub host: String,
    pub port: u16,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 7401,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub data_dir: String,
    pub max_segment_bytes: u64,
    pub index_interval_bytes: u64,
    pub default_partition_count: u32,
    pub group_session_timeout_ms: u64,
    pub retention_bytes: u64,
    pub retention_ms: u64,
    pub storage_queue_capacity: usize,
    pub storage_enqueue_timeout_ms: u64,
    pub auto_create_topics: bool,
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(ConfigError::Read)?;
        toml::from_str(&contents).map_err(ConfigError::Parse)
    }

    pub fn cluster_nodes(&self) -> Result<Vec<ClusterNodeConfig>, ConfigError> {
        let mut nodes = if self.cluster.nodes.is_empty() {
            vec![ClusterNodeConfig {
                id: self.cluster.broker_id,
                host: self.broker.host.clone(),
                port: self.broker.port,
                admin_port: self.admin.port,
                replication_port: self
                    .broker
                    .port
                    .checked_add(100)
                    .ok_or(ConfigError::InvalidCluster)?,
            }]
        } else {
            self.cluster.nodes.clone()
        };
        let mut ids = std::collections::HashSet::new();
        let mut endpoints = std::collections::HashSet::new();
        for node in &nodes {
            if node.id == 0
                || !ids.insert(node.id)
                || !endpoints.insert((node.host.clone(), node.port))
                || !endpoints.insert((node.host.clone(), node.admin_port))
                || !endpoints.insert((node.host.clone(), node.replication_port))
                || [node.port, node.admin_port, node.replication_port]
                    .into_iter()
                    .any(|port| {
                        format!("{}:{port}", node.host)
                            .parse::<SocketAddr>()
                            .is_err()
                    })
            {
                return Err(ConfigError::InvalidCluster);
            }
        }
        if !ids.contains(&self.cluster.broker_id) {
            return Err(ConfigError::LocalBrokerMissing);
        }
        if self.cluster.replication_factor == 0
            || self.cluster.replication_factor > nodes.len()
            || self.cluster.min_in_sync_replicas == 0
            || self.cluster.min_in_sync_replicas > self.cluster.replication_factor
        {
            return Err(ConfigError::InvalidReplicationPolicy);
        }
        nodes.sort_unstable_by_key(|node| node.id);
        Ok(nodes)
    }
}

impl BrokerConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(ConfigError::Address)
    }
}

impl AdminConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(ConfigError::AdminAddress)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration: {0}")]
    Read(std::io::Error),
    #[error("failed to parse configuration: {0}")]
    Parse(toml::de::Error),
    #[error("invalid broker address: {0}")]
    Address(std::net::AddrParseError),
    #[error("invalid administrative address: {0}")]
    AdminAddress(std::net::AddrParseError),
    #[error("cluster nodes contain an invalid or duplicate id/address")]
    InvalidCluster,
    #[error("local broker_id is not present in cluster nodes")]
    LocalBrokerMissing,
    #[error("replication_factor and min_in_sync_replicas do not fit cluster membership")]
    InvalidReplicationPolicy,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::{ClusterConfig, ClusterNodeConfig, Config, ConfigError};

    #[test]
    fn parses_broker_configuration() {
        let config: Config = toml::from_str(
            r#"
                [broker]
                host = "127.0.0.1"
                port = 7400
                data_dir = "./data"
                max_segment_bytes = 268435456
                index_interval_bytes = 4096
                default_partition_count = 3
                group_session_timeout_ms = 10000
                retention_bytes = 0
                retention_ms = 0
                storage_queue_capacity = 1024
                storage_enqueue_timeout_ms = 100
                auto_create_topics = true
            "#,
        )
        .expect("configuration should be valid");

        assert_eq!(config.broker.host, "127.0.0.1");
        assert_eq!(config.broker.port, 7400);
        assert_eq!(config.broker.data_dir, "./data");
        assert_eq!(config.broker.max_segment_bytes, 268_435_456);
        assert_eq!(config.broker.index_interval_bytes, 4096);
        assert_eq!(config.broker.default_partition_count, 3);
        assert_eq!(config.broker.group_session_timeout_ms, 10_000);
        assert_eq!(config.broker.retention_bytes, 0);
        assert_eq!(config.broker.retention_ms, 0);
        assert_eq!(config.broker.storage_queue_capacity, 1024);
        assert_eq!(config.broker.storage_enqueue_timeout_ms, 100);
        assert!(config.broker.auto_create_topics);
        assert_eq!(config.admin, super::AdminConfig::default());
        assert_eq!(config.logging, super::LoggingConfig::default());
        assert_eq!(config.cluster, super::ClusterConfig::default());
        assert_eq!(
            config
                .cluster_nodes()
                .expect("cluster should validate")
                .len(),
            1
        );
        assert_eq!(
            config.broker.socket_addr().expect("address should parse"),
            "127.0.0.1:7400".parse().expect("test address should parse")
        );
    }

    #[test]
    fn validates_static_cluster_membership() {
        let mut config: Config = toml::from_str(
            r#"
                [broker]
                host = "127.0.0.1"
                port = 7400
                data_dir = "./data"
                max_segment_bytes = 1024
                index_interval_bytes = 64
                default_partition_count = 3
                group_session_timeout_ms = 10000
                retention_bytes = 0
                retention_ms = 0
                storage_queue_capacity = 16
                storage_enqueue_timeout_ms = 100
                auto_create_topics = false
            "#,
        )
        .expect("configuration should parse");
        config.cluster.replication_factor = 2;
        assert!(matches!(
            config.cluster_nodes(),
            Err(ConfigError::InvalidReplicationPolicy)
        ));
        config.cluster = ClusterConfig {
            broker_id: 2,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            nodes: vec![ClusterNodeConfig {
                id: 1,
                host: "127.0.0.1".to_owned(),
                port: 7400,
                admin_port: 7401,
                replication_port: 7402,
            }],
        };

        assert!(matches!(
            config.cluster_nodes(),
            Err(ConfigError::LocalBrokerMissing)
        ));
    }
}
