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

use std::collections::HashSet;
use std::sync::Arc;

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, LevelType, SstableInfo};

use super::CompactionPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct MinOverlappingPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    level: usize,
    target_level: usize,
}

impl MinOverlappingPicker {
    pub fn new(
        compact_task_id: u64,
        level: usize,
        target_level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> MinOverlappingPicker {
        MinOverlappingPicker {
            compact_task_id,
            overlap_strategy,
            level,
            target_level,
        }
    }

    // For example:
    //  L1:      [(k0, k4),  (k5, k7), (k8, k9)]
    //  L2:      [(k0, k1), (k2, k6)]
    // If we only choose file (k0, k4), (k0, k1), (k2, k6), the file (k5, k7) will compact with the
    // result of their compaction task again. So a better strategy is choosing (k0, k4), (k0, k1),
    // (k2, k6), (k5, k7) all in one task.
    pub fn try_expand_input(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        select_level_handler: &LevelHandler,
        select_input_ssts: Vec<SstableInfo>,
        target_input_ssts: &[SstableInfo],
    ) -> Vec<SstableInfo> {
        assert_eq!(select_input_ssts.len(), 1);
        let select_overlap_files = self
            .overlap_strategy
            .check_base_level_overlap(target_input_ssts, select_tables);
        if select_overlap_files
            .iter()
            .any(|table| select_level_handler.is_pending_compact(&table.id))
        {
            return select_input_ssts;
        }
        if select_overlap_files.len() == select_input_ssts.len() {
            return select_input_ssts;
        }
        let mut target_input_ids = HashSet::with_capacity(target_input_ssts.len());
        for table in target_input_ssts {
            target_input_ids.insert(table.id);
        }
        let select_table_id = select_input_ssts[0].id;
        let mut select_overlap_results = vec![];
        for table in select_overlap_files {
            if table.id == select_table_id {
                select_overlap_results.push(table);
                continue;
            }

            let mut info = self.overlap_strategy.create_overlap_info();
            info.update(&table);
            let target_overlap_files = info.check_multiple_overlap(target_tables);
            if target_overlap_files
                .iter()
                .any(|other| !target_input_ids.contains(&other.id))
            {
                continue;
            }
            select_overlap_results.push(table);
        }

        // Check again because there may be a file does not overlap with any other files in target
        // level but the range between it and another file would overlap more files in
        // target level.
        if select_overlap_results.len() > select_input_ssts.len() {
            let target_overlap_files = self
                .overlap_strategy
                .check_base_level_overlap(&select_overlap_results, target_tables);
            if target_overlap_files
                .iter()
                .all(|table| target_input_ids.contains(&table.id))
            {
                return select_overlap_results;
            }
        }
        select_input_ssts
    }

    pub fn pick_tables(
        &self,
        select_tables: &[SstableInfo],
        target_tables: &[SstableInfo],
        level_handlers: &[LevelHandler],
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let mut scores = vec![];
        for table in select_tables {
            if level_handlers[self.level].is_pending_compact(&table.id) {
                continue;
            }
            let mut total_file_size = 0;
            let mut pending_campct = false;
            let overlap_files = self
                .overlap_strategy
                .check_base_level_overlap(&[table.clone()], target_tables);
            for other in overlap_files {
                if level_handlers[self.target_level].is_pending_compact(&other.id) {
                    pending_campct = true;
                    break;
                }
                total_file_size += other.file_size;
            }
            if pending_campct {
                continue;
            }
            scores.push((total_file_size * 100 / (table.file_size + 1), table.clone()));
        }
        if scores.is_empty() {
            return (vec![], vec![]);
        }
        let (_, table) = scores.iter().min_by(|x, y| x.0.cmp(&y.0)).unwrap();
        let mut select_input_ssts = vec![table.clone()];
        let target_input_ssts = self
            .overlap_strategy
            .check_base_level_overlap(&select_input_ssts, target_tables);
        if !target_input_ssts.is_empty() {
            let expand_select_ssts = self.try_expand_input(
                select_tables,
                target_tables,
                &level_handlers[self.level],
                select_input_ssts,
                &target_input_ssts,
            );
            select_input_ssts = expand_select_ssts;
        }
        (select_input_ssts, target_input_ssts)
    }
}

impl CompactionPicker for MinOverlappingPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        assert!(self.level > 0);
        let (select_input_ssts, target_input_ssts) = self.pick_tables(
            &levels.get_level(self.level).table_infos,
            &levels.get_level(self.target_level).table_infos,
            level_handlers,
        );
        if select_input_ssts.is_empty() {
            return None;
        }
        level_handlers[self.level].add_pending_task(
            self.compact_task_id,
            self.target_level,
            &select_input_ssts,
        );
        if !target_input_ssts.is_empty() {
            level_handlers[self.target_level].add_pending_task(
                self.compact_task_id,
                self.target_level,
                &target_input_ssts,
            );
        }
        Some(CompactionInput {
            input_levels: vec![
                InputLevel {
                    level_idx: self.level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: target_input_ssts,
                },
            ],
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }
}

#[cfg(test)]
pub mod tests {
    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_with_overlap, generate_table,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;

    #[test]
    fn test_compact_l1() {
        let picker = MinOverlappingPicker::new(0, 1, 2, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 0, 100, 1),
                    generate_table(1, 1, 101, 200, 1),
                    generate_table(2, 1, 222, 300, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 0, 100, 1),
                    generate_table(5, 1, 101, 150, 1),
                    generate_table(6, 1, 151, 201, 1),
                    generate_table(7, 1, 501, 800, 1),
                    generate_table(8, 2, 301, 400, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trival move to next level.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 2);
        assert_eq!(ret.input_levels[1].table_infos.len(), 0);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 0);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 4);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.target_level, 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[1].table_infos[1].id, 6);
    }

    #[test]
    fn test_expand_l1_files() {
        let picker = MinOverlappingPicker::new(0, 1, 2, Arc::new(RangeOverlapStrategy::default()));
        let levels = vec![
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(0, 1, 50, 99, 2),
                    generate_table(1, 1, 100, 149, 2),
                    generate_table(2, 1, 150, 249, 2),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
            Level {
                level_idx: 2,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 50, 199, 1),
                    generate_table(5, 1, 200, 399, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            },
        ];
        let levels = Levels {
            levels,
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        // pick a non-overlapping files. It means that this file could be trival move to next level.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].level_idx, 1);
        assert_eq!(ret.input_levels[1].level_idx, 2);

        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 0);
        assert_eq!(ret.input_levels[0].table_infos[1].id, 1);

        assert_eq!(ret.input_levels[1].table_infos.len(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 4);
    }
}
