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

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::enumerate;
use num_traits::FromPrimitive;
use prost::Message;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::HummockVersion;

use crate::hummock::compaction::CompactStatus;
use crate::rpc::metrics::MetaMetrics;

pub fn trigger_commit_stat(metrics: &MetaMetrics, current_version: &HummockVersion) {
    metrics
        .max_committed_epoch
        .set(current_version.max_committed_epoch as i64);
    metrics
        .version_size
        .set(current_version.encoded_len() as i64);
}

pub fn trigger_sst_stat(
    metrics: &MetaMetrics,
    compact_status: &CompactStatus,
    current_version: &HummockVersion,
    compaction_group_id: CompactionGroupId,
) {
    // TODO #2065: fix grafana
    let level_sst_cnt = |level_idx: usize| {
        let mut sst_num = 0;
        current_version.map_level(compaction_group_id, level_idx, |level| {
            sst_num += level.table_infos.len();
        });
        sst_num
    };
    let level_sst_size = |level_idx: usize| {
        let mut level_sst_size = 0;
        current_version.map_level(compaction_group_id, level_idx, |level| {
            level_sst_size += level.total_file_size;
        });
        level_sst_size / 1024
    };
    for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
        let sst_num = level_sst_cnt(idx);
        let compact_cnt = level_handler.get_pending_file_count();
        let level_label = format!(
            "{}_{}",
            idx,
            StaticCompactionGroupId::from_u64(compaction_group_id).unwrap(),
        );
        metrics
            .level_sst_num
            .with_label_values(&[&level_label])
            .set(sst_num as i64);
        metrics
            .level_compact_cnt
            .with_label_values(&[&level_label])
            .set(compact_cnt as i64);
        metrics
            .level_file_size
            .with_label_values(&[&level_label])
            .set(level_sst_size(idx) as i64);
    }
    let level_label = format!("cg{}_l0_sub", compaction_group_id);
    let sst_num = current_version
        .get_compaction_group_levels(compaction_group_id)
        .l0
        .as_ref()
        .map(|l0| l0.sub_levels.len())
        .unwrap_or(0);
    metrics
        .level_sst_num
        .with_label_values(&[&level_label])
        .set(sst_num as i64);

    use std::sync::atomic::AtomicU64;

    static TIME_AFTER_LAST_OBSERVATION: AtomicU64 = AtomicU64::new(0);
    let previous_time = TIME_AFTER_LAST_OBSERVATION.load(Ordering::Relaxed);
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if current_time - previous_time > 600
        && TIME_AFTER_LAST_OBSERVATION
            .compare_exchange(
                previous_time,
                current_time,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    {
        for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
            let sst_num = level_sst_cnt(idx);
            let sst_size = level_sst_size(idx);
            let compact_cnt = level_handler.get_pending_file_count();
            tracing::info!(
                "Level {} has {} SSTs, the total size of which is {}KB, while {} of those are being compacted to bottom levels",
                idx,
                sst_num,
                sst_size,
                compact_cnt,
            );
        }
    }
}
