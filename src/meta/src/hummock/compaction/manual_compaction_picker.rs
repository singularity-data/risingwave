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

use itertools::Itertools;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{InputLevel, LevelType, SstableInfo};

use super::overlap_strategy::OverlapInfo;
use super::CompactionPicker;
use crate::hummock::compaction::overlap_strategy::{OverlapStrategy, RangeOverlapInfo};
use crate::hummock::compaction::{CompactionInput, ManualCompactionOption};
use crate::hummock::level_handler::LevelHandler;

pub struct ManualCompactionPicker {
    compact_task_id: u64,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    option: ManualCompactionOption,
    target_level: usize,
}

impl ManualCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        option: ManualCompactionOption,
        target_level: usize,
    ) -> Self {
        Self {
            compact_task_id,
            overlap_strategy,
            option,
            target_level,
        }
    }

    fn pick_l0_files(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        let mut input_levels = vec![];
        let mut max_sub_level_idx = usize::MAX;
        let mut info = self.overlap_strategy.create_overlap_info();
        let mut tmp_sst_info = SstableInfo::default();
        let mut range_overlap_info = RangeOverlapInfo::default();
        tmp_sst_info.key_range = Some(self.option.key_range.clone());
        range_overlap_info.update(&tmp_sst_info);
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if self
                .overlap_strategy
                .check_overlap_with_tables(&[tmp_sst_info.clone()], &level.table_infos)
                .is_empty()
            {
                continue;
            }
            if self.option.internal_table_id.is_empty() {
                max_sub_level_idx = idx;
                continue;
            }

            // to collect internal_table_id from sst_info
            let table_id_in_sst: Vec<u32> = level
                .table_infos
                .iter()
                .flat_map(|sst| sst.table_ids.clone())
                .collect_vec();

            // to filter sst_file by table_id
            let mut found = false;
            for table_id in &table_id_in_sst {
                if self.option.internal_table_id.contains(table_id) {
                    found = true;
                    break;
                }
            }
            if found {
                max_sub_level_idx = idx;
            }
        }
        if max_sub_level_idx == usize::MAX {
            return None;
        }
        for idx in 0..=max_sub_level_idx {
            if level_handlers[0].is_level_pending_compact(&l0.sub_levels[idx]) {
                return None;
            }
            for table in &l0.sub_levels[idx].table_infos {
                info.update(table);
            }
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: l0.sub_levels[idx].level_type,
                table_infos: l0.sub_levels[idx].table_infos.clone(),
            })
        }
        let target_input_ssts =
            info.check_multiple_overlap(&levels.levels[self.target_level - 1].table_infos);
        if target_input_ssts
            .iter()
            .any(|table| level_handlers[self.target_level].is_pending_compact(&table.id))
        {
            return None;
        }

        input_levels.push(InputLevel {
            level_idx: self.target_level as u32,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: target_input_ssts,
        });
        for level in &input_levels {
            if !level.table_infos.is_empty() {
                level_handlers[level.level_idx as usize].add_pending_task(
                    self.compact_task_id,
                    self.target_level,
                    &level.table_infos,
                );
            }
        }
        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }
}

impl CompactionPicker for ManualCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        if self.option.level == 0 {
            return self.pick_l0_files(levels, level_handlers);
        }

        let level = self.option.level;
        let target_level = self.target_level;

        let mut select_input_ssts = vec![];
        let mut tmp_sst_info = SstableInfo::default();
        let mut range_overlap_info = RangeOverlapInfo::default();
        tmp_sst_info.key_range = Some(self.option.key_range.clone());
        range_overlap_info.update(&tmp_sst_info);

        let level_table_infos: Vec<SstableInfo> = levels.levels[level - 1]
            .table_infos
            .iter()
            .filter(|sst_info| range_overlap_info.check_overlap(sst_info))
            .filter(|sst_info| {
                if self.option.internal_table_id.is_empty() {
                    return true;
                }

                // to collect internal_table_id from sst_info
                let table_id_in_sst: Vec<u32> =
                    sst_info.get_table_ids().iter().cloned().collect_vec();

                // to filter sst_file by table_id
                for table_id in &table_id_in_sst {
                    if self.option.internal_table_id.contains(table_id) {
                        return true;
                    }
                }

                false
            })
            .cloned()
            .collect();

        for table in &level_table_infos {
            if level_handlers[level].is_pending_compact(&table.id) {
                continue;
            }

            if target_level != level {
                let overlap_files = self.overlap_strategy.check_base_level_overlap(
                    &[table.clone()],
                    &levels.levels[target_level - 1].table_infos,
                );

                if overlap_files
                    .iter()
                    .any(|table| level_handlers[target_level].is_pending_compact(&table.id))
                {
                    continue;
                }
            }

            select_input_ssts.push(table.clone());
        }

        if select_input_ssts.is_empty() {
            return None;
        }

        let target_input_ssts = if target_level == level {
            vec![]
        } else {
            self.overlap_strategy.check_base_level_overlap(
                &select_input_ssts,
                &levels.levels[target_level - 1].table_infos,
            )
        };

        if target_input_ssts
            .iter()
            .any(|table| level_handlers[target_level].is_pending_compact(&table.id))
        {
            return None;
        }

        level_handlers[level].add_pending_task(
            self.compact_task_id,
            target_level,
            &select_input_ssts,
        );
        if !target_input_ssts.is_empty() {
            level_handlers[target_level].add_pending_task(
                self.compact_task_id,
                target_level,
                &target_input_ssts,
            );
        }

        Some(CompactionInput {
            input_levels: vec![
                InputLevel {
                    level_idx: level as u32,
                    level_type: levels.levels[level - 1].level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: target_level as u32,
                    level_type: levels.levels[target_level - 1].level_type,
                    table_infos: target_input_ssts,
                },
            ],
            target_level,
            target_sub_level_id: 0,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashSet;

    pub use risingwave_pb::hummock::{KeyRange, Level, LevelType};

    use super::*;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_with_overlap, generate_table,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    #[test]
    fn test_manaul_compaction_picker() {
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
        let mut levels = Levels {
            levels,
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        let mut levels_handler = vec![
            LevelHandler::new(0),
            LevelHandler::new(1),
            LevelHandler::new(2),
        ];

        let clean_task_state = |level_handler: &mut LevelHandler| {
            for pending_task_id in &level_handler.pending_tasks_ids() {
                level_handler.remove_task(*pending_task_id);
            }
        };

        {
            // test key_range option
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 0, 1),
                    right: iterator_test_key_of_epoch(1, 201, 1),
                    inf: false,
                },
                ..Default::default()
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                0,
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(2, result.input_levels[0].table_infos.len());
            assert_eq!(3, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // test all key range
            let option = ManualCompactionOption::default();
            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                0,
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );
            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(3, result.input_levels[0].table_infos.len());
            assert_eq!(3, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            let level_table_info = &mut levels.levels[0].table_infos;
            let table_info_1 = &mut level_table_info[1];
            table_info_1.table_ids.resize(2, 0);
            table_info_1.table_ids[0] = 1;
            table_info_1.table_ids[1] = 2;

            // test internal_table_id
            let option = ManualCompactionOption {
                level: 1,
                internal_table_id: HashSet::from([2]),
                ..Default::default()
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                0,
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(1, result.input_levels[0].table_infos.len());
            assert_eq!(2, result.input_levels[1].table_infos.len());
        }

        {
            clean_task_state(&mut levels_handler[1]);
            clean_task_state(&mut levels_handler[2]);

            // include all table_info
            let level_table_info = &mut levels.levels[0].table_infos;
            for table_info in level_table_info {
                table_info.table_ids.resize(2, 0);
                table_info.table_ids[0] = 1;
                table_info.table_ids[1] = 2;
            }

            // test key range filter first
            let option = ManualCompactionOption {
                level: 1,
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 101, 1),
                    right: iterator_test_key_of_epoch(1, 199, 1),
                    inf: false,
                },
                internal_table_id: HashSet::from([2]),
            };

            let target_level = option.level + 1;
            let picker = ManualCompactionPicker::new(
                0,
                Arc::new(RangeOverlapStrategy::default()),
                option,
                target_level,
            );

            let result = picker
                .pick_compaction(&levels, &mut levels_handler)
                .unwrap();

            assert_eq!(1, result.input_levels[0].table_infos.len());
            assert_eq!(2, result.input_levels[1].table_infos.len());
        }
    }
}
