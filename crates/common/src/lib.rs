use std::{fs, net::SocketAddr, path::Path};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub broker: BrokerConfig,
    #[serde(default)]
    pub admin: AdminConfig,
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
}

impl Config {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path).map_err(ConfigError::Read)?;
        toml::from_str(&contents).map_err(ConfigError::Parse)
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
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::Config;

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
        assert_eq!(config.admin, super::AdminConfig::default());
        assert_eq!(
            config.broker.socket_addr().expect("address should parse"),
            "127.0.0.1:7400".parse().expect("test address should parse")
        );
    }
}
