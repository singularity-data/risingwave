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

use std::sync::Arc;

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel, LevelType, OverlappingLevel};

use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker};
use crate::hummock::level_handler::LevelHandler;

pub struct TierCompactionPicker {
    compact_task_id: u64,
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl TierCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> TierCompactionPicker {
        TierCompactionPicker {
            compact_task_id,
            config,
            overlap_strategy,
        }
    }
}
impl TierCompactionPicker {
    fn pick_overlapping_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &mut LevelHandler,
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Nonoverlapping as i32 {
                continue;
            }

            if level_handler.is_level_pending_compact(level) {
                continue;
            }

            let mut compaction_bytes = level.total_file_size;
            let mut select_level_inputs = vec![InputLevel {
                table_infos: level.table_infos.clone(),
                level_idx: 0,
                level_type: level.level_type,
            }];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.level_type == LevelType::Nonoverlapping as i32 {
                    break;
                }

                if other
                    .table_infos
                    .iter()
                    .any(|table| level_handler.is_pending_compact(&table.id))
                {
                    break;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(InputLevel {
                    table_infos: other.table_infos.clone(),
                    level_idx: 0,
                    level_type: other.level_type,
                });
            }

            if select_level_inputs
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
                < self.config.level0_tier_compact_file_number as usize
            {
                continue;
            }

            for input_level in &mut select_level_inputs {
                level_handler.add_pending_task(self.compact_task_id, 0, &input_level.table_infos);
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }
        None
    }

    fn pick_sharding_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &mut LevelHandler,
    ) -> Option<CompactionInput> {
        // do not pick the first sub-level because we do not want to block the level compaction.
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32
                || level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
                continue;
            }

            if level
                .table_infos
                .iter()
                .any(|table| level_handler.is_pending_compact(&table.id))
            {
                continue;
            }

            let mut compaction_bytes = level.total_file_size;
            let mut select_level_inputs = vec![InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            }];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.max_bytes_for_level_base,
            );

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.total_file_size > self.config.sub_level_max_compaction_bytes {
                    break;
                }

                if other
                    .table_infos
                    .iter()
                    .any(|table| level_handler.is_pending_compact(&table.id))
                {
                    break;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: other.level_type,
                    table_infos: other.table_infos.clone(),
                });
            }

            if select_level_inputs
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
                < self.config.level0_tier_compact_file_number as usize
            {
                continue;
            }

            if select_level_inputs.len() <= self.config.level0_tier_compact_file_number as usize / 2
            {
                continue;
            }

            for input_level in &select_level_inputs {
                level_handler.add_pending_task(self.compact_task_id, 0, &input_level.table_infos);
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }
        None
    }

    fn pick_trivial_move_file(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32 || idx + 1 >= l0.sub_levels.len() {
                continue;
            }

            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping as i32 {
                continue;
            }

            let min_overlap_picker = MinOverlappingPicker::new(
                self.compact_task_id,
                0,
                0,
                self.overlap_strategy.clone(),
            );

            let (select_tables, target_tables) = min_overlap_picker.pick_tables(
                &l0.sub_levels[idx + 1].table_infos,
                &level.table_infos,
                level_handlers,
            );

            // only pick tables for trivial move
            if select_tables.is_empty() || !target_tables.is_empty() {
                continue;
            }

            level_handlers[0].add_pending_task(self.compact_task_id, 0, &select_tables);
            let input_levels = vec![
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: select_tables,
                },
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: target_tables,
                },
            ];
            return Some(CompactionInput {
                input_levels,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }
        None
    }
}

impl CompactionPicker for TierCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty() {
            return None;
        }

        if let Some(input) = self.pick_trivial_move_file(l0, level_handlers) {
            return Some(input);
        }

        if let Some(input) = self.pick_overlapping_level(l0, &mut level_handlers[0]) {
            return Some(input);
        }

        self.pick_sharding_level(l0, &mut level_handlers[0])
    }
}
