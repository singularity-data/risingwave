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
mod level_selector;
mod manual_compaction_picker;
mod min_overlap_compaction_picker;
mod overlap_strategy;
mod prost_type;
mod tier_compaction_picker;
pub use tier_compaction_picker::TierCompactionPicker;
mod base_level_compaction_picker;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub use base_level_compaction_picker::LevelCompactionPicker;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::{CompactionGroupId, HummockCompactionTaskId, HummockEpoch};
use risingwave_pb::hummock::compaction_config::CompactionMode;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactTask, CompactionConfig, HummockVersion, InputLevel, KeyRange, LevelType,
};

use crate::hummock::compaction::level_selector::{DynamicLevelSelector, LevelSelector};
use crate::hummock::compaction::overlap_strategy::{
    HashStrategy, OverlapStrategy, RangeOverlapStrategy,
};
use crate::hummock::level_handler::LevelHandler;

pub struct CompactStatus {
    compaction_group_id: CompactionGroupId,
    pub(crate) level_handlers: Vec<LevelHandler>,
    // TODO: remove this `CompactionConfig`, which is a duplicate of that in `CompactionGroup`.
    pub compaction_config: CompactionConfig,
    compaction_selector: Arc<dyn LevelSelector>,
}

impl Debug for CompactStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactStatus")
            .field("level_handlers", &self.level_handlers)
            .field("compaction_selector", &self.compaction_selector.name())
            .finish()
    }
}

impl PartialEq for CompactStatus {
    fn eq(&self, other: &Self) -> bool {
        self.level_handlers.eq(&other.level_handlers)
            && self.compaction_selector.name() == other.compaction_selector.name()
            && self.compaction_config == other.compaction_config
    }
}

impl Clone for CompactStatus {
    fn clone(&self) -> Self {
        Self {
            compaction_group_id: self.compaction_group_id,
            level_handlers: self.level_handlers.clone(),
            compaction_config: self.compaction_config.clone(),
            compaction_selector: self.compaction_selector.clone(),
        }
    }
}

pub struct CompactionInput {
    pub input_levels: Vec<InputLevel>,
    pub target_level: usize,
    pub target_sub_level_id: u64,
}

pub struct CompactionTask {
    pub input: CompactionInput,
    pub compression_algorithm: String,
    pub target_file_size: u64,
    pub splits: Vec<KeyRange>,
}

pub fn create_overlap_strategy(compaction_mode: CompactionMode) -> Arc<dyn OverlapStrategy> {
    match compaction_mode {
        CompactionMode::Range => Arc::new(RangeOverlapStrategy::default()),
        CompactionMode::ConsistentHash => Arc::new(HashStrategy::default()),
        CompactionMode::Unspecified => unreachable!(),
    }
}

impl CompactStatus {
    pub fn new(
        compaction_group_id: CompactionGroupId,
        config: Arc<CompactionConfig>,
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
            compaction_selector: Arc::new(DynamicLevelSelector::new(config, overlap_strategy)),
        }
    }

    pub fn get_compact_task(
        &mut self,
        levels: &Levels,
        task_id: HummockCompactionTaskId,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: Option<ManualCompactionOption>,
    ) -> Option<CompactTask> {
        // When we compact the files, we must make the result of compaction meet the following
        // conditions, for any user key, the epoch of it in the file existing in the lower
        // layer must be larger.

        let ret = if let Some(manual_compaction_option) = manual_compaction_option {
            self.manual_pick_compaction(levels, task_id, manual_compaction_option)?
        } else {
            self.pick_compaction(levels, task_id)?
        };

        let select_level_id = ret.input.input_levels[0].level_idx;
        let target_level_id = ret.input.target_level;

        let splits = if ret.splits.is_empty() {
            vec![KeyRange::inf()]
        } else {
            ret.splits
        };

        let compression_algorithm = match ret.compression_algorithm.as_str() {
            "Lz4" => 1,
            "Zstd" => 2,
            _ => 0,
        };

        let compact_task = CompactTask {
            input_ssts: ret.input.input_levels,
            splits,
            watermark: HummockEpoch::MAX,
            sorted_output_ssts: vec![],
            task_id,
            target_level: target_level_id as u32,
            // only gc delete keys in last level because there may be older version in more bottom
            // level.
            gc_delete_keys: target_level_id == self.level_handlers.len() - 1 && select_level_id > 0,
            task_status: false,
            compaction_group_id,
            existing_table_ids: vec![],
            compression_algorithm,
            target_file_size: ret.target_file_size,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
            target_sub_level_id: ret.input.target_sub_level_id,
        };
        Some(compact_task)
    }

    pub fn is_trivial_move_task(task: &CompactTask) -> bool {
        if task.input_ssts.len() != 2
            || task.input_ssts[0].level_type != LevelType::Nonoverlapping as i32
        {
            return false;
        }

        // it may be a manual compaction task
        if task.input_ssts[0].level_idx == task.input_ssts[1].level_idx
            && task.input_ssts[0].level_idx > 0
        {
            return false;
        }

        if task.input_ssts[1].level_idx == task.target_level
            && task.input_ssts[1].table_infos.is_empty()
        {
            return true;
        }

        false
    }

    fn pick_compaction(
        &mut self,
        levels: &Levels,
        task_id: HummockCompactionTaskId,
    ) -> Option<CompactionTask> {
        self.compaction_selector
            .pick_compaction(task_id, levels, &mut self.level_handlers)
    }

    fn manual_pick_compaction(
        &mut self,
        levels: &Levels,
        task_id: HummockCompactionTaskId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Option<CompactionTask> {
        // manual_compaction no need to select level
        // level determined by option
        self.compaction_selector.manual_pick_compaction(
            task_id,
            levels,
            &mut self.level_handlers,
            manual_compaction_option,
        )
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
        let mut removed_levels = vec![];
        for input_level in &compact_task.input_ssts {
            for table in &input_level.table_infos {
                removed_table.insert(table.id);
            }
            removed_levels.push(input_level.level_idx);
        }

        removed_levels.sort();
        removed_levels.dedup();
        new_version.apply_compact_ssts(
            compact_task.compaction_group_id,
            &removed_levels,
            &removed_table,
            compact_task.target_level,
            compact_task.target_sub_level_id,
            compact_task.sorted_output_ssts.clone(),
        );

        new_version
    }

    pub fn compaction_group_id(&self) -> CompactionGroupId {
        self.compaction_group_id
    }

    pub fn get_config(&self) -> &CompactionConfig {
        &self.compaction_config
    }
}

#[derive(Clone, Debug)]
pub struct ManualCompactionOption {
    pub key_range: KeyRange,
    pub internal_table_id: HashSet<u32>,
    pub level: usize,
}

impl Default for ManualCompactionOption {
    fn default() -> Self {
        Self {
            key_range: KeyRange {
                left: vec![],
                right: vec![],
                inf: true,
            },
            internal_table_id: HashSet::default(),
            level: 1,
        }
    }
}

pub trait CompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput>;
}
