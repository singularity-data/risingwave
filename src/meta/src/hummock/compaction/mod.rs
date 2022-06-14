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

pub mod compaction_config;
mod compaction_picker;
pub mod level_selector;
mod overlap_strategy;
mod prost_type;
mod tier_compaction_picker;

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId, HummockEpoch};
use risingwave_pb::hummock::compaction_config::CompactionMode;
use risingwave_pb::hummock::{CompactTask, CompactionConfig, HummockVersion, KeyRange, Level};

use crate::hummock::compaction::level_selector::{
    DynamicLevelSelector, HashMappingSelector, LevelSelector,
};
use crate::hummock::compaction::overlap_strategy::{
    HashStrategy, OverlapStrategy, RangeOverlapStrategy,
};
use crate::hummock::level_handler::LevelHandler;
use crate::manager::HashMappingManagerRef;

pub struct CompactStatus {
    compaction_group_id: CompactionGroupId,
    pub(crate) level_handlers: Vec<LevelHandler>,
    // TODO: remove this `CompactionConfig`, which is a duplicate of that in `CompactionGroup`.
    compaction_config: CompactionConfig,
    pub(crate) compaction_selectors: Vec<Arc<dyn LevelSelector>>,
}

impl Debug for CompactStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut nameset = String::new();
        for selector in &self.compaction_selectors {
            nameset += selector.name();
            nameset += " | ";
        }
        f.debug_struct("CompactStatus")
            .field("level_handlers", &self.level_handlers)
            .field("compaction_selector", &nameset)
            .finish()
    }
}

impl PartialEq for CompactStatus {
    fn eq(&self, other: &Self) -> bool {
        self.level_handlers.eq(&other.level_handlers)
            && self.compaction_selectors.len() == other.compaction_selectors.len()
            && {
                for i in 0..self.compaction_selectors.len() {
                    if self.compaction_selectors[i].name() != other.compaction_selectors[i].name() {
                        return false;
                    }
                }
                true
            }
            && self.compaction_config == other.compaction_config
    }
}

impl Clone for CompactStatus {
    fn clone(&self) -> Self {
        Self {
            compaction_group_id: self.compaction_group_id,
            level_handlers: self.level_handlers.clone(),
            compaction_config: self.compaction_config.clone(),
            compaction_selectors: self.compaction_selectors.clone(),
        }
    }
}

pub struct SearchResult {
    select_level: Level,
    target_level: Level,
    split_ranges: Vec<KeyRange>,
}

pub fn create_overlap_strategy(compaction_mode: CompactionMode) -> Arc<dyn OverlapStrategy> {
    match compaction_mode {
        CompactionMode::Range => Arc::new(RangeOverlapStrategy::default()),
        CompactionMode::ConsistentHash => Arc::new(HashStrategy::default()),
    }
}

impl CompactStatus {
    pub fn new(
        compaction_group_id: CompactionGroupId,
        config: Arc<CompactionConfig>,
        hash_mapping_manager: HashMappingManagerRef,
    ) -> CompactStatus {
        let mut level_handlers = vec![];
        for level in 0..=config.max_level {
            level_handlers.push(LevelHandler::new(level as u32));
        }
        let overlap_strategy = create_overlap_strategy(config.compaction_mode());
        CompactStatus {
            compaction_group_id,
            level_handlers,
            compaction_config: (*config).clone(),
            compaction_selectors: vec![
                Arc::new(DynamicLevelSelector::new(config, overlap_strategy)),
                Arc::new(HashMappingSelector {
                    hash_mapping_manager,
                }),
            ],
        }
    }

    pub fn get_compact_task(
        &mut self,
        levels: &[Level],
        task_id: HummockCompactionTaskId,
        compaction_group_id: CompactionGroupId,
    ) -> Option<CompactTask> {
        // When we compact the files, we must make the result of compaction meet the following
        // conditions, for any user key, the epoch of it in the file existing in the lower
        // layer must be larger.

        let ret = match self.pick_compaction(levels, task_id) {
            Some(ret) => ret,
            None => return None,
        };

        let select_level_id = ret.select_level.level_idx;
        let target_level_id = ret.target_level.level_idx;

        let splits = if ret.split_ranges.is_empty() {
            vec![KeyRange::inf()]
        } else {
            ret.split_ranges
        };

        let compact_task = CompactTask {
            input_ssts: vec![ret.select_level, ret.target_level],
            splits,
            watermark: HummockEpoch::MAX,
            sorted_output_ssts: vec![],
            task_id,
            target_level: target_level_id,
            is_target_ultimate_and_leveling: target_level_id as usize
                == self.level_handlers.len() - 1
                && select_level_id > 0,
            task_status: false,
            vnode_mappings: vec![],
            compaction_group_id,
            existing_table_ids: vec![],
        };
        Some(compact_task)
    }

    fn pick_compaction(
        &mut self,
        levels: &[Level],
        task_id: HummockCompactionTaskId,
    ) -> Option<SearchResult> {
        for selector in &self.compaction_selectors {
            let result = selector.pick_compaction(task_id, levels, &mut self.level_handlers);
            if result.is_some() {
                return result;
            }
        }
        None
    }

    /// Declares a task is either finished or canceled.
    pub fn report_compact_task(&mut self, compact_task: &CompactTask) {
        for level in &compact_task.input_ssts {
            self.level_handlers[level.level_idx as usize].remove_task(compact_task.task_id);
        }
    }

    pub fn cancel_compaction_tasks_if<F: Fn(u64) -> bool>(&mut self, should_cancel: F) -> u32 {
        let mut count: u32 = 0;
        for level in &mut self.level_handlers {
            for pending_task_id in level.pending_tasks_ids() {
                if should_cancel(pending_task_id) {
                    level.remove_task(pending_task_id);
                    count += 1;
                }
            }
        }
        count
    }

    /// Applies the compact task result and get a new hummock version.
    pub fn apply_compact_result(
        compact_task: &CompactTask,
        based_hummock_version: HummockVersion,
    ) -> HummockVersion {
        let mut new_version = based_hummock_version;
        new_version.safe_epoch = std::cmp::max(new_version.safe_epoch, compact_task.watermark);
        let mut removed_table: HashSet<u64> = HashSet::default();
        for input_level in &compact_task.input_ssts {
            for table in &input_level.table_infos {
                removed_table.insert(table.id);
            }
        }
        if compact_task.target_level == 0 {
            assert_eq!(compact_task.input_ssts[0].level_idx, 0);
            let mut new_table_infos = vec![];
            let mut find_remove_position = false;
            let mut new_total_file_size = 0;
            for (idx, table) in new_version.levels[0].table_infos.iter().enumerate() {
                if !removed_table.contains(&table.id) {
                    new_table_infos.push(new_version.levels[0].table_infos[idx].clone());
                    new_total_file_size += table.file_size;
                } else if !find_remove_position {
                    new_total_file_size += compact_task
                        .sorted_output_ssts
                        .iter()
                        .map(|sst| sst.file_size)
                        .sum::<u64>();
                    new_table_infos.extend(compact_task.sorted_output_ssts.clone());
                    find_remove_position = true;
                }
            }
            new_version.levels[compact_task.target_level as usize].table_infos = new_table_infos;
            new_version.levels[compact_task.target_level as usize].total_file_size =
                new_total_file_size;
        } else {
            for input_level in &compact_task.input_ssts {
                new_version.levels[input_level.level_idx as usize].total_file_size -= input_level
                    .table_infos
                    .iter()
                    .map(|sst| sst.file_size)
                    .sum::<u64>();
                new_version.levels[input_level.level_idx as usize]
                    .table_infos
                    .retain(|sst| !removed_table.contains(&sst.id));
            }
            new_version.levels[compact_task.target_level as usize].total_file_size += compact_task
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.file_size)
                .sum::<u64>();
            new_version.levels[compact_task.target_level as usize]
                .table_infos
                .extend(compact_task.sorted_output_ssts.clone());
            new_version.levels[compact_task.target_level as usize]
                .table_infos
                .sort_by(|sst1, sst2| {
                    let a = sst1.key_range.as_ref().unwrap();
                    let b = sst2.key_range.as_ref().unwrap();
                    a.compare(b)
                });
        }
        new_version
    }

    pub fn compaction_group_id(&self) -> CompactionGroupId {
        self.compaction_group_id
    }
}
