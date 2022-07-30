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

use risingwave_common::config::constant::hummock::CompactionFilterFlag;
use risingwave_pb::hummock::compaction_config::CompactionMode;
use risingwave_pb::hummock::CompactionConfig;

const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB
const DEFAULT_MIN_COMPACTION_BYTES: u64 = 256 * 1024 * 1024; // 256MB
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * 1024 * 1024; // 512MB

// decrease this configure when the generation of checkpoint barrier is not frequent.
const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 8;
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 32 * 1024 * 1024; // 32MB
const DEFAULT_MAX_SUB_COMPACTION: u32 = 4;
const MAX_LEVEL: u64 = 6;

pub struct CompactionConfigBuilder {
    config: CompactionConfig,
}

impl CompactionConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: CompactionConfig {
                max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
                max_bytes_for_level_multiplier: 10,
                max_level: MAX_LEVEL,
                max_compaction_bytes: DEFAULT_MAX_COMPACTION_BYTES,
                sub_level_max_compaction_bytes: DEFAULT_MIN_COMPACTION_BYTES,
                level0_tigger_file_numer: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER * 2,
                level0_tier_compact_file_number: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER,
                target_file_size_base: DEFAULT_TARGET_FILE_SIZE_BASE,
                compaction_mode: CompactionMode::Range as i32,
                // support compression setting per level
                // L0 and L1 do not use compression algorithms
                // L2 - L4 use Lz4, else use Zstd
                compression_algorithm: vec![
                    "None".to_string(),
                    "None".to_string(),
                    "None".to_string(),
                    "Lz4".to_string(),
                    "Lz4".to_string(),
                    "Zstd".to_string(),
                    "Zstd".to_string(),
                ],
                compaction_filter_mask: (CompactionFilterFlag::STATE_CLEAN
                    | CompactionFilterFlag::TTL)
                    .into(),
                max_sub_compaction: DEFAULT_MAX_SUB_COMPACTION,
            },
        }
    }

    pub fn new_with(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn build(self) -> CompactionConfig {
        self.config
    }
}

impl Default for CompactionConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! builder_field {
    ($( $name:ident: $type:ty ),* ,) => {
        impl CompactionConfigBuilder {
            $(
                pub fn $name(mut self, v:$type) -> Self {
                    self.config.$name = v;
                    self
                }
            )*
        }
    }
}

builder_field! {
    max_bytes_for_level_base: u64,
    max_bytes_for_level_multiplier: u64,
    max_level: u64,
    max_compaction_bytes: u64,
    sub_level_max_compaction_bytes: u64,
    level0_tigger_file_numer: u64,
    level0_tier_compact_file_number: u64,
    compaction_mode: i32,
    compression_algorithm: Vec<String>,
    compaction_filter_mask: u32,
    max_sub_compaction: u32,
}
