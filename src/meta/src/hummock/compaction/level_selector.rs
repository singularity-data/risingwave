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

use itertools::Itertools;
use risingwave_hummock_sdk::key::{user_key, FullKey};
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockEpoch, VersionedComparator};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, KeyRange};

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::manual_compaction_picker::ManualCompactionPicker;
use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{
    create_overlap_strategy, CompactionInput, CompactionPicker, CompactionTask,
    LevelCompactionPicker, ManualCompactionOption, TierCompactionPicker,
};
use crate::hummock::level_handler::LevelHandler;

const SCORE_BASE: u64 = 100;

pub trait LevelSelector: Sync + Send {
    fn need_compaction(&self, levels: &Levels, level_handlers: &mut [LevelHandler]) -> bool;

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionTask>;

    fn manual_pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        option: ManualCompactionOption,
    ) -> Option<CompactionTask>;

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
                Box::new(TierCompactionPicker::new(
                    task_id,
                    self.config.clone(),
                    self.overlap_strategy.clone(),
                ))
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
                select_level + 1,
                self.overlap_strategy.clone(),
            ))
        }
    }

    // TODO: calculate this scores in apply compact result.
    /// `calculate_level_base_size` calculate base level and the base size of LSM tree build for
    /// current dataset. In other words,  `level_max_bytes` is our compaction goal which shall
    /// reach. This algorithm refers to the implementation in  `</>https://github.com/facebook/rocksdb/blob/v7.2.2/db/version_set.cc#L3706</>`
    fn calculate_level_base_size(&self, levels: &Levels) -> SelectContext {
        let mut first_non_empty_level = 0;
        let mut max_level_size = 0;
        let mut ctx = SelectContext::default();

        for level in &levels.levels {
            if level.total_file_size > 0 && first_non_empty_level == 0 {
                first_non_empty_level = level.level_idx as usize;
            }
            max_level_size = std::cmp::max(max_level_size, level.total_file_size);
        }

        ctx.level_max_bytes
            .resize(self.config.max_level as usize + 1, u64::MAX);

        if max_level_size == 0 {
            // Use the bottommost level.
            ctx.base_level = self.config.max_level as usize;
            return ctx;
        }

        let base_bytes_max = self.config.max_bytes_for_level_base;
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

    fn get_priority_levels(&self, levels: &Levels, handlers: &mut [LevelHandler]) -> SelectContext {
        let mut ctx = self.calculate_level_base_size(levels);

        let idle_file_count = levels
            .l0
            .as_ref()
            .unwrap()
            .sub_levels
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>()
            - handlers[0].get_pending_file_count();
        let max_l0_score = std::cmp::max(
            SCORE_BASE * 2,
            levels.l0.as_ref().unwrap().sub_levels.len() as u64 * SCORE_BASE
                / self.config.level0_tier_compact_file_number,
        );

        let total_size =
            levels.l0.as_ref().unwrap().total_file_size - handlers[0].get_pending_file_size();
        if idle_file_count > 0 {
            // trigger intra-l0 compaction at first when the number of files is too large.
            let l0_score =
                idle_file_count as u64 * SCORE_BASE / self.config.level0_tier_compact_file_number;
            ctx.score_levels
                .push((std::cmp::min(l0_score, max_l0_score), 0, 0));
            let score = total_size * SCORE_BASE / self.config.max_bytes_for_level_base;
            ctx.score_levels.push((score, 0, ctx.base_level));
        }

        // The bottommost level can not be input level.
        for level in &levels.levels {
            let level_idx = level.level_idx as usize;
            if level_idx < ctx.base_level || level_idx >= self.config.max_level as usize {
                continue;
            }
            let upper_level = if level_idx == ctx.base_level {
                0
            } else {
                level_idx - 1
            };
            let total_size = level.total_file_size
                + handlers[upper_level].get_pending_output_file_size(level.level_idx)
                - handlers[level_idx].get_pending_output_file_size(level.level_idx + 1);
            if total_size == 0 {
                continue;
            }
            ctx.score_levels.push((
                total_size * SCORE_BASE / ctx.level_max_bytes[level_idx],
                level_idx,
                level_idx + 1,
            ));
        }

        // sort reverse to pick the largest one.
        ctx.score_levels.sort_by(|a, b| b.0.cmp(&a.0));
        ctx
    }

    fn generate_task_by_compaction_config(
        &self,
        input: CompactionInput,
        base_level: usize,
    ) -> CompactionTask {
        let target_file_size = if input.target_level == 0 {
            self.config.target_file_size_base
        } else {
            assert!(input.target_level >= base_level);
            self.config.target_file_size_base << (input.target_level - base_level)
        };
        let compression_algorithm = if input.target_level == 0 {
            self.config.compression_algorithm[0].clone()
        } else {
            let idx = input.target_level - base_level + 1;
            self.config.compression_algorithm[idx].clone()
        };
        let mut splits = vec![];
        const SPLIT_RANGE_STEP: usize = 8;
        let mut keys = input
            .input_levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| {
                FullKey::from_user_key_slice(
                    user_key(&table.key_range.as_ref().unwrap().left),
                    HummockEpoch::MAX,
                )
                .into_inner()
            })
            .collect_vec();
        let total_file_size = input
            .input_levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|table| table.file_size)
            .sum::<u64>();

        if (total_file_size > self.config.sub_level_max_compaction_bytes
            || total_file_size > self.config.max_bytes_for_level_base)
            && keys.len() >= SPLIT_RANGE_STEP * 2
        {
            splits.push(KeyRange::new(vec![], vec![]));
            keys.sort_by(|a, b| VersionedComparator::compare_key(a.as_ref(), b.as_ref()));
            let concurrency = std::cmp::min(
                keys.len() / SPLIT_RANGE_STEP,
                self.config.max_sub_compaction as usize,
            );
            let step = (keys.len() + concurrency - 1) / concurrency;
            assert!(step > 1);
            for (idx, key) in keys.into_iter().enumerate() {
                if idx > 0 && idx % step == 0 {
                    splits.last_mut().unwrap().right = key.clone();
                    splits.push(KeyRange::new(key, vec![]));
                }
            }
        }
        CompactionTask {
            input,
            compression_algorithm,
            target_file_size,
            splits,
        }
    }
}

impl LevelSelector for DynamicLevelSelector {
    fn need_compaction(&self, levels: &Levels, level_handlers: &mut [LevelHandler]) -> bool {
        let ctx = self.get_priority_levels(levels, level_handlers);
        ctx.score_levels
            .first()
            .map(|(score, _, _)| *score > SCORE_BASE)
            .unwrap_or(false)
    }

    fn pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionTask> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        for (score, select_level, target_level) in ctx.score_levels {
            if score <= SCORE_BASE {
                return None;
            }
            let picker = self.create_compaction_picker(select_level, target_level, task_id);
            if let Some(ret) = picker.pick_compaction(levels, level_handlers) {
                return Some(self.generate_task_by_compaction_config(ret, ctx.base_level));
            }
        }
        None
    }

    fn manual_pick_compaction(
        &self,
        task_id: HummockCompactionTaskId,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
        option: ManualCompactionOption,
    ) -> Option<CompactionTask> {
        let ctx = self.get_priority_levels(levels, level_handlers);
        let target_level = if option.level == 0 {
            ctx.base_level
        } else if option.level == self.config.max_level as usize {
            option.level
        } else {
            option.level + 1
        };

        let picker = ManualCompactionPicker::new(
            task_id,
            self.overlap_strategy.clone(),
            option,
            target_level,
        );

        let ret = picker.pick_compaction(levels, level_handlers)?;
        Some(self.generate_task_by_compaction_config(ret, ctx.base_level))
    }

    fn name(&self) -> &'static str {
        "DynamicLevelSelector"
    }
}

#[cfg(test)]
pub mod tests {
    use std::ops::Range;

    use itertools::Itertools;
    use risingwave_common::config::constant::hummock::CompactionFilterFlag;
    use risingwave_pb::hummock::compaction_config::CompactionMode;
    use risingwave_pb::hummock::{Level, LevelType, OverlappingLevel, SstableInfo};

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn push_table_level0(levels: &mut Levels, sst: SstableInfo) {
        levels.l0.as_mut().unwrap().total_file_size += sst.file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping as i32,
            total_file_size: sst.file_size,
            sub_level_id: sst.id,
            table_infos: vec![sst],
        });
    }

    pub fn push_tables_level0(levels: &mut Levels, table_infos: Vec<SstableInfo>) {
        let total_file_size = table_infos.iter().map(|table| table.file_size).sum::<u64>();
        let sub_level_id = table_infos[0].id;
        levels.l0.as_mut().unwrap().total_file_size += total_file_size;
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Nonoverlapping as i32,
            total_file_size,
            sub_level_id,
            table_infos,
        });
    }

    pub fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                inf: false,
            }),
            file_size: (right - left + 1) as u64,
            table_ids: vec![],
        }
    }

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

    pub fn generate_level(level_idx: u32, table_infos: Vec<SstableInfo>) -> Level {
        let total_file_size = table_infos.iter().map(|sst| sst.file_size).sum();
        Level {
            level_idx,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos,
            total_file_size,
            sub_level_id: 0,
        }
    }

    pub fn generate_l0_with_overlap(table_infos: Vec<SstableInfo>) -> OverlappingLevel {
        let total_file_size = table_infos.iter().map(|table| table.file_size).sum::<u64>();
        OverlappingLevel {
            sub_levels: table_infos
                .into_iter()
                .enumerate()
                .map(|(idx, table)| Level {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    total_file_size: table.file_size,
                    sub_level_id: idx as u64,
                    table_infos: vec![table],
                })
                .collect_vec(),
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
            .level0_tigger_file_numer(1)
            .level0_tier_compact_file_number(2)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let selector =
            DynamicLevelSelector::new(Arc::new(config), Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 2);
        assert_eq!(ctx.level_max_bytes[2], 100);
        assert_eq!(ctx.level_max_bytes[3], 200);
        assert_eq!(ctx.level_max_bytes[4], 1000);

        levels.levels[3]
            .table_infos
            .append(&mut generate_tables(15..20, 2000..3000, 1, 400));
        levels.levels[3].total_file_size = levels.levels[3]
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
        push_tables_level0(&mut levels, generate_tables(20..26, 0..1000, 1, 100));

        let ctx = selector.calculate_level_base_size(&levels);
        assert_eq!(ctx.base_level, 1);
        assert_eq!(ctx.level_max_bytes[1], 100);
        assert_eq!(ctx.level_max_bytes[2], 120);
        assert_eq!(ctx.level_max_bytes[3], 600);
        assert_eq!(ctx.level_max_bytes[4], 3000);

        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.l0.as_mut().unwrap().total_file_size = 0;
        levels.levels[0].table_infos = generate_tables(26..32, 0..1000, 1, 100);
        levels.levels[0].total_file_size = levels.levels[0]
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
            .level0_tigger_file_numer(8)
            .level0_tier_compact_file_number(4)
            .compaction_mode(CompactionMode::Range as i32)
            .build();
        let levels = vec![
            generate_level(1, vec![]),
            generate_level(2, generate_tables(0..5, 0..1000, 3, 10)),
            generate_level(3, generate_tables(5..10, 0..1000, 2, 50)),
            generate_level(4, generate_tables(10..15, 0..1000, 1, 200)),
        ];
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_with_overlap(generate_tables(
                15..25,
                0..600,
                3,
                10,
            ))),
        };

        let selector = DynamicLevelSelector::new(
            Arc::new(config.clone()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        // trival move.
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert!(compaction.input.input_levels[1].table_infos.is_empty());
        assert_eq!(compaction.input.target_level, 0);

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        let config = CompactionConfigBuilder::new_with(config)
            .max_bytes_for_level_base(100)
            .level0_tigger_file_numer(8)
            .compaction_filter_mask(compaction_filter_flag.into())
            .build();
        let selector = DynamicLevelSelector::new(
            Arc::new(config.clone()),
            Arc::new(RangeOverlapStrategy::default()),
        );

        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.l0.as_mut().unwrap().total_file_size = 0;
        push_tables_level0(&mut levels, generate_tables(15..25, 0..600, 3, 20));
        let mut levels_handlers = (0..5).into_iter().map(LevelHandler::new).collect_vec();
        let compaction = selector
            .pick_compaction(1, &levels, &mut levels_handlers)
            .unwrap();
        assert_eq!(compaction.input.input_levels[0].level_idx, 0);
        assert_eq!(compaction.input.target_level, 2);
        assert_eq!(compaction.target_file_size, config.target_file_size_base);

        levels_handlers[0].remove_task(1);
        levels_handlers[2].remove_task(1);
        levels.l0.as_mut().unwrap().sub_levels.clear();
        levels.levels[1].table_infos = generate_tables(20..30, 0..1000, 3, 10);
        let compaction = selector
            .pick_compaction(2, &levels, &mut levels_handlers)
            .unwrap();
        assert_eq!(compaction.input.input_levels[0].level_idx, 3);
        assert_eq!(compaction.input.target_level, 4);
        assert_eq!(compaction.input.input_levels[0].table_infos.len(), 1);
        assert_eq!(compaction.input.input_levels[1].table_infos.len(), 1);
        assert_eq!(
            compaction.target_file_size,
            config.target_file_size_base * 4
        );
        assert_eq!(compaction.compression_algorithm.as_str(), "Lz4",);
        // no compaction need to be scheduled because we do not calculate the size of pending files
        // to score.
        let compaction = selector.pick_compaction(2, &levels, &mut levels_handlers);
        assert!(compaction.is_none());
    }
}
