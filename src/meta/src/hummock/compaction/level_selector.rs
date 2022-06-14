//  Copyright 2022 Singularity Data
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use std::sync::Arc;

use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::{CompactionConfig, Level};

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::compaction_picker::{CompactionPicker, MinOverlappingPicker};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::tier_compaction_picker::{
    LevelCompactionPicker, TierCompactionPicker,
};
use crate::hummock::compaction::{create_overlap_strategy, SearchResult};
use crate::hummock::level_handler::LevelHandler;
use crate::manager::HashMappingManagerRef;

const SCORE_BASE: u64 = 100;

pub trait LevelSelector: Sync + Send {
    fn need_compaction(&self, levels: &[Level], level_handlers: &mut [LevelHandler]) -> bool;

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult>;

    fn name(&self) -> &'static str;
}

#[derive(Default)]
pub struct SelectContext {
    level_max_bytes: Vec<u64>,

    // All data will be placed in the last level. When the cluster is empty, the files in L0 will
    // be compact to `max_level`, and the `max_level` would be `base_level`. When the total
    // size of the files in  `base_level` reaches its capacity, we will place data in a higher
    // level, which equals to `base_level -= 1;`.
    base_level: usize,
    score_levels: Vec<(u64, usize, usize)>,
}

// TODO: Set these configurations by meta rpc
pub struct DynamicLevelSelector {
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl Default for DynamicLevelSelector {
    fn default() -> Self {
        let config = Arc::new(CompactionConfigBuilder::new().build());
        let overlap_strategy = create_overlap_strategy(config.compaction_mode());
        DynamicLevelSelector::new(config, overlap_strategy)
    }
}

impl DynamicLevelSelector {
    pub fn new(config: Arc<CompactionConfig>, overlap_strategy: Arc<dyn OverlapStrategy>) -> Self {
        DynamicLevelSelector {
            config,
            overlap_strategy,
        }
    }

    fn create_compaction_picker(
        &self,
        select_level: usize,
        target_level: usize,
        task_id: HummockCompactionTaskId,
    ) -> Box<dyn CompactionPicker> {
        if select_level == 0 {
            if target_level == 0 {
                Box::new(TierCompactionPicker::new(task_id, self.config.clone()))
            } else {
                Box::new(LevelCompactionPicker::new(
                    task_id,
                    target_level,
                    self.config.clone(),
                    self.overlap_strategy.clone(),
                ))
            }
        } else {
            Box::new(MinOverlappingPicker::new(
                task_id,
                select_level,
                self.overlap_strategy.clone(),
            ))
        }
    }

    // TODO: calculate this scores in apply compact result.
    /// `calculate_level_base_size` calculate base level and the base size of LSM tree build for
    /// current dataset. In other words,  `level_max_bytes` is our compaction goal which shall
    /// reach. This algorithm refers to the implementation in  `</>https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706</>`
    fn calculate_level_base_size(&self, levels: &[Level]) -> SelectContext {
        let mut first_non_empty_level = 0;
        let mut max_level_size = 0;
        let mut ctx = SelectContext::default();

        let mut l0_size = 0;
        for level in levels.iter() {
            if level.level_idx > 0 {
                if level.total_file_size > 0 && first_non_empty_level == 0 {
                    first_non_empty_level = level.level_idx as usize;
                }
                max_level_size = std::cmp::max(max_level_size, level.total_file_size);
            } else {
                l0_size = level.total_file_size;
            }
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        if max_level_size == 0 {
            // Use the bottommost level.
            ctx.base_level = self.config.max_level as usize;
            return ctx;
        }

        let base_bytes_max = std::cmp::max(self.config.max_bytes_for_level_base, l0_size);
        let base_bytes_min = base_bytes_max / self.config.max_bytes_for_level_multiplier;

        let mut cur_level_size = max_level_size;
        for _ in first_non_empty_level..self.config.max_level as usize {
            cur_level_size /= self.config.max_bytes_for_level_multiplier;
        }

        let base_level_size = if cur_level_size <= base_bytes_min {
            // Case 1. If we make target size of last level to be max_level_size,
            // target size of the first non-empty level would be smaller than
            // base_bytes_min. We set it be base_bytes_min.
            ctx.base_level = first_non_empty_level;
            base_bytes_min + 1
        } else {
            ctx.base_level = first_non_empty_level;
            while ctx.base_level > 1 && cur_level_size > base_bytes_max {
                ctx.base_level -= 1;
                cur_level_size /= self.config.max_bytes_for_level_multiplier;
            }
            std::cmp::min(base_bytes_max, cur_level_size)
        };

        let level_multiplier = self.config.max_bytes_for_level_multiplier as f64;
        let mut level_size = base_level_size;
        for i in ctx.base_level..=self.config.max_level as usize {
            // Don't set any level below base_bytes_max. Otherwise, the LSM can
            // assume an hourglass shape where L1+ sizes are smaller than L0. This
            // causes compaction scoring, which depends on level sizes, to favor L1+
            // at the expense of L0, which may fill up and stall.
            ctx.level_max_bytes[i] = std::cmp::max(level_size, base_bytes_max);
            level_size = (level_size as f64 * level_multiplier) as u64;
        }
        ctx
    }

    fn get_priority_levels(
        &self,
        levels: &[Level],
        handlers: &mut [LevelHandler],
    ) -> SelectContext {
        let mut ctx = self.calculate_level_base_size(levels);

        // The bottommost level can not be input level.
        for level in &levels[..self.config.max_level as usize] {
            let level_idx = level.level_idx as usize;
            let idle_file_count =
                (level.table_infos.len() - handlers[level_idx].get_pending_file_count()) as u64;
            let total_size = level.total_file_size - handlers[level_idx].get_pending_file_size();
            if total_size == 0 {
                continue;
            }
            if level_idx == 0 {
                // trigger intra-l0 compaction at first when the number of files is too large.
                let score = idle_file_count * SCORE_BASE
                    / self.config.level0_tier_compact_file_number as u64;
                ctx.score_levels.push((score, 0, 0));
                let score = 2 * total_size * SCORE_BASE / self.config.max_bytes_for_level_base
                    + idle_file_count * SCORE_BASE / self.config.level0_tigger_file_numer as u64;
                ctx.score_levels.push((score, 0, ctx.base_level));
            } else {
                ctx.score_levels.push((
                    total_size * SCORE_BASE / ctx.level_max_bytes[level_idx],
                    level_idx,
                    level_idx + 1,
                ));
            }
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| b.0.cmp(&a.0));
        ctx
    }
}

impl LevelSelector for DynamicLevelSelector {
    fn need_compaction(&self, levels: &[Level], level_handlers: &mut [LevelHandler]) -> bool {
        let ctx = self.get_priority_levels(levels, level_handlers);
        ctx.score_levels
            .first()
            .map(|(score, _, _)| *score > SCORE_BASE)
            .unwrap_or(false)
    }

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        for (score, select_level, target_level) in ctx.score_levels {
            if score <= SCORE_BASE {
                return None;
            }
            let picker = self.create_compaction_picker(select_level, target_level, task_id);
            if let Some(ret) = picker.pick_compaction(levels, level_handlers) {
                return Some(ret);
            }
        }
        None
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }
}

pub struct HashMappingSelector {
    pub hash_mapping_manager: HashMappingManagerRef,
}

impl LevelSelector for HashMappingSelector {
    fn need_compaction(&self, _: &[Level], _: &mut [LevelHandler]) -> bool {
        true
    }

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Option<SearchResult> {
        for level in levels.iter().rev() {
            let level_idx = level.level_idx as usize;
            for table in &level.table_infos {
                if !level_handlers[level_idx].is_pending_compact(&table.id)
                    && self
                        .hash_mapping_manager
                        .check_sst_deprecated(table.get_vnode_bitmaps())
                {
                    let select_input_ssts = vec![table.clone()];
                    level_handlers[level_idx].add_pending_task(task_id, &select_input_ssts);
                    return Some(SearchResult {
                        select_level: Level {
                            level_idx: level_idx as u32,
                            level_type: levels[level_idx].level_type,
                            table_infos: select_input_ssts,
                            total_file_size: 0,
                        },
                        target_level: Level {
                            level_idx: level_idx as u32,
                            level_type: levels[level_idx].level_type,
                            table_infos: vec![],
                            total_file_size: 0,
                        },
                        split_ranges: vec![],
                    });
                }
            }
        }
        None
    }

    fn name(&self) -> &'static str {
        "HashMappingSelector"
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::Range;

    use itertools::Itertools;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::{LevelType, SstableInfo};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::tier_compaction_picker::tests::generate_table;

    pub fn generate_tables(
        ids: Range<u64>,
        keys: Range<usize>,
        epoch: u64,
        file_size: u64,
    ) -> Vec<SstableInfo> {
        let step = (keys.end - keys.start) / (ids.end - ids.start) as usize;
        let mut start = keys.start;
        let mut tables = vec![];
        for id in ids {
            let mut table = generate_table(id, 1, start, start + step - 1, epoch);
            table.file_size = file_size;
            tables.push(table);
            start += step;
        }
        tables
    }

    fn generate_level(level_idx: u32, table_infos: Vec<SstableInfo>) -> Level {
        let total_file_size = table_infos.iter().map(|sst| sst.file_size).sum();
        Level {
            level_idx,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos,
            total_file_size,
        }
    }

    #[test]
    fn test_dynamic_level() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(100)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .max_compaction_bytes(1)
            .min_compaction_bytes(0)
            .level0_tigger_file_numer(1)
            .level0_tier_compact_file_number(2)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let selector =
            DynamicLevelSelector::new(Arc::new(config), Arc::new(RangeOverlapStrategy::default()));
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
            },
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 2);
        assert_eq!(ctx.level_max_bytes[2], 100);
        assert_eq!(ctx.level_max_bytes[3], 200);
        assert_eq!(ctx.level_max_bytes[4], 1000);

        levels[4]
            .table_infos
            .append(&mut generate_tables(15..20, 2000..3000, 1, 400));
        levels[4].total_file_size = levels[4]
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum::<u64>();

        let ctx = selector.calculate_level_base_size(&levels);
        // data size increase, so we need increase one level to place more data.
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);

        // append a large data to L0 but it does not change the base size of LSM tree.
        levels[0]
            .table_infos
            .append(&mut generate_tables(20..26, 0..1000, 1, 100));
        levels[0].total_file_size = levels[0]
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum::<u64>();

        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 2);
        assert_eq!(ctx.level_max_bytes[2], 600);
        assert_eq!(ctx.level_max_bytes[3], 605);
        assert_eq!(ctx.level_max_bytes[4], 3025);

        levels[0].table_infos.clear();
        levels[0].total_file_size = 0;
        levels[1].table_infos = generate_tables(26..32, 0..1000, 1, 100);
        levels[1].total_file_size = levels[1]
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum::<u64>();

        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);
    }

    #[test]
    fn test_pick_compaction() {
        let config = CompactionConfigBuilder::new()
            .max_bytes_for_level_base(200)
            .max_level(4)
            .max_bytes_for_level_multiplier(5)
            .max_compaction_bytes(10000)
            .min_compaction_bytes(200)
            .level0_tigger_file_numer(8)
            .level0_tier_compact_file_number(4)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let mut levels = vec![
            generate_level(0, generate_tables(15..25, 0..600, 3, 10)),
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        levels[0].level_type = LevelType::Overlapping as i32;

        let selector = DynamicLevelSelector::new(
            Arc::new(config.clone()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        assert_eq!(compaction.select_level.level_idx, 0);
        assert_eq!(compaction.target_level.level_idx, 0);
        assert_eq!(compaction.select_level.table_infos.len(), 10);
        assert_eq!(compaction.target_level.table_infos.len(), 0);

        let config = CompactionConfigBuilder::new_with(config)
            .min_compaction_bytes(1)
            .max_bytes_for_level_base(100)
            .level0_tigger_file_numer(8)
            .build();
        let selector =
            DynamicLevelSelector::new(Arc::new(config), Arc::new(RangeOverlapStrategy::default()));
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        assert_eq!(compaction.select_level.level_idx, 0);
        assert_eq!(compaction.target_level.level_idx, 2);
        assert_eq!(compaction.select_level.table_infos.len(), 10);
        assert_eq!(compaction.target_level.table_infos.len(), 3);

        levels_handlers[0].remove_task(1);
        levels_handlers[2].remove_task(1);
        levels[0].table_infos.clear();
        levels[2].table_infos = generate_tables(20..30, 0..1000, 3, 10);
        let compaction = selector
            .pick_compaction(2, &levels, &mut levels_handlers)
            .unwrap();
        assert_eq!(compaction.select_level.level_idx, 3);
        assert_eq!(compaction.target_level.level_idx, 4);
        assert_eq!(compaction.select_level.table_infos.len(), 1);
        assert_eq!(compaction.target_level.table_infos.len(), 1);

        // no compaction need to be scheduled because we do not calculate the size of pending files
        // to score.
        let compaction = selector.pick_compaction(2, &levels, &mut levels_handlers);
        assert!(compaction.is_none());
    }
}
