// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::fs;
use std::path::PathBuf;

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};

/// TODO(TaoWu): The configs here may be preferable to be managed under corresponding module
/// separately.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ComputeNodeConfig {
    // For connection
    #[serde(default)]
    pub server: ServerConfig,

    // Below for batch query.
    #[serde(default)]
    pub batch: BatchConfig,

    // Below for streaming.
    #[serde(default)]
    pub streaming: StreamingConfig,

    // Below for Hummock.
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct FrontendConfig {
    // For connection
    #[serde(default)]
    pub server: ServerConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default::heartbeat_interval")]
    pub heartbeat_interval: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default::chunk_size")]
    pub chunk_size: u32,
}

impl Default for BatchConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamingConfig {
    #[serde(default = "default::chunk_size")]
    pub chunk_size: u32,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// Currently all configurations are server before they can be specified with DDL syntaxes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Target size of the SSTable.
    #[serde(default = "default::sst_size")]
    pub sstable_size: u32,

    /// Size of each block in bytes in SST.
    #[serde(default = "default::block_size")]
    pub block_size: u32,

    /// False positive probability of bloom filter.
    #[serde(default = "default::bloom_false_positive")]
    pub bloom_false_positive: f64,

    /// Remote directory for storing data and metadata objects.
    #[serde(default = "default::data_directory")]
    pub data_directory: String,

    /// Checksum algorithm (Crc32c, XxHash64).
    #[serde(
        default = "default::checksum_algorithm",
        deserialize_with = "ser_parse_checksum_algo"
    )]
    pub checksum_algo: risingwave_pb::hummock::checksum::Algorithm,

    /// Whether to enable async checkpoint
    #[serde(default = "default::async_checkpoint_enabled")]
    pub async_checkpoint_enabled: bool,

    /// Whether to enable write conflict detection
    #[serde(default = "default::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,
}

fn ser_parse_checksum_algo<'de, D>(
    deserializer: D,
) -> core::result::Result<risingwave_pb::hummock::checksum::Algorithm, D::Error>
where
    D: Deserializer<'de>,
{
    let checksum_algo = String::deserialize(deserializer)?;
    match parse_checksum_algo(checksum_algo.as_str()) {
        Some(algo) => Ok(algo),
        _ => Err(D::Error::custom(format!(
            "{} is not supported for Hummock",
            checksum_algo
        ))),
    }
}

pub fn parse_checksum_algo(
    checksum_algo: &str,
) -> Option<risingwave_pb::hummock::checksum::Algorithm> {
    match checksum_algo {
        "crc32c" => Some(risingwave_pb::hummock::checksum::Algorithm::Crc32c),
        "xxhash64" => Some(risingwave_pb::hummock::checksum::Algorithm::XxHash64),
        _ => None,
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

impl ComputeNodeConfig {
    pub fn init(path: PathBuf) -> Result<ComputeNodeConfig> {
        let config_str = fs::read_to_string(path.clone()).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to open config file '{}': {}",
                path.to_string_lossy(),
                e
            )))
        })?;
        let config: ComputeNodeConfig = toml::from_str(config_str.as_str())
            .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
        Ok(config)
    }
}

impl FrontendConfig {
    pub fn init(path: PathBuf) -> Result<Self> {
        let config_str = fs::read_to_string(path.clone()).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to open config file '{}': {}",
                path.to_string_lossy(),
                e
            )))
        })?;
        let config: FrontendConfig = toml::from_str(config_str.as_str())
            .map_err(|e| RwError::from(InternalError(format!("parse error {}", e))))?;
        Ok(config)
    }
}

mod default {
    pub fn heartbeat_interval() -> u32 {
        1000
    }

    pub fn chunk_size() -> u32 {
        1024
    }

    pub fn sst_size() -> u32 {
        // 256MB
        268435456
    }

    pub fn block_size() -> u32 {
        65536
    }

    pub fn bloom_false_positive() -> f64 {
        0.1
    }

    pub fn data_directory() -> String {
        "hummock_001".to_string()
    }

    pub fn checksum_algorithm() -> risingwave_pb::hummock::checksum::Algorithm {
        risingwave_pb::hummock::checksum::Algorithm::XxHash64
    }

    pub fn async_checkpoint_enabled() -> bool {
        true
    }

    pub fn write_conflict_detection_enabled() -> bool {
        cfg!(debug_assertions)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_default() {
        use super::*;

        let cfg = ComputeNodeConfig::default();
        assert_eq!(cfg.server.heartbeat_interval, default::heartbeat_interval());

        let cfg: ComputeNodeConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.storage.block_size, default::block_size());

        let partial_toml_str = r#"
        [server]
        heartbeat_interval = 10
        
        [batch]
        chunk_size = 256
        
        [streaming]
        
        [storage]
        sstable_size = 1024
        data_directory = "test"
        async_checkpoint_enabled = false
    "#;
        let cfg: ComputeNodeConfig = toml::from_str(partial_toml_str).unwrap();
        assert_eq!(cfg.server.heartbeat_interval, 10);
        assert_eq!(cfg.batch.chunk_size, 256);
        assert_eq!(cfg.storage.sstable_size, 1024);
        assert_eq!(cfg.storage.block_size, default::block_size());
        assert_eq!(
            cfg.storage.bloom_false_positive,
            default::bloom_false_positive()
        );
        assert_eq!(cfg.storage.data_directory, "test");
        assert_eq!(cfg.storage.checksum_algo, default::checksum_algorithm());
        assert!(!cfg.storage.async_checkpoint_enabled);
    }
}
