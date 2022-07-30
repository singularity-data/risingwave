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

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactionConfig, InputLevel, Level, LevelType, OverlappingLevel, SstableInfo,
};

use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker};
use crate::hummock::level_handler::LevelHandler;

fn cal_file_size(table_infos: &[SstableInfo]) -> u64 {
    table_infos.iter().map(|table| table.file_size).sum::<u64>()
}

pub struct LevelCompactionPicker {
    compact_task_id: u64,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    config: Arc<CompactionConfig>,
}

impl CompactionPicker for LevelCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        let select_level = 0;
        let target_level = self.target_level as u32;
        let next_task_id = self.compact_task_id;

        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty()
            || l0.sub_levels[0].level_type != LevelType::Nonoverlapping as i32
        {
            return None;
        }

        let is_l0_pending_compact = level_handlers[0].is_level_pending_compact(&l0.sub_levels[0]);

        // move the whole level to target level.
        if !is_l0_pending_compact && levels.get_level(self.target_level).table_infos.is_empty() {
            level_handlers[select_level].add_pending_task(
                next_task_id,
                self.target_level,
                &l0.sub_levels[0].table_infos,
            );
            return Some(CompactionInput {
                input_levels: vec![
                    InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: l0.sub_levels[0].table_infos.clone(),
                    },
                    InputLevel {
                        level_idx: target_level as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                    },
                ],
                target_level: self.target_level,
                target_sub_level_id: 0,
            });
        }

        // Pick one table which overlap with smallest data. There may be no file in target level
        //  which overlap with select files. That would be a trivial move.
        let mut input_levels =
            self.pick_min_overlap_tables(l0, levels.get_level(self.target_level), level_handlers);
        if input_levels.is_empty() {
            return None;
        }

        const MAX_WRITE_AMPLIFICATION: u64 = 150;

        let write_amplification = cal_file_size(&input_levels[1].table_infos) * 100
            / cal_file_size(&input_levels[0].table_infos);

        // Pick the whole level to reduce write amplification.
        if write_amplification >= MAX_WRITE_AMPLIFICATION {
            // If there is any pending compact file in sub-level 0 or target level,
            //  we can not pick the whole level to compact.
            if is_l0_pending_compact
                || level_handlers[self.target_level].get_pending_file_count() > 0
            {
                return None;
            }
            input_levels.clear();
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: l0.sub_levels[0].table_infos.clone(),
            });

            let mut l0_total_file_size = l0.sub_levels[0].total_file_size;
            for level in l0.sub_levels[1..].iter() {
                if l0_total_file_size >= self.config.max_compaction_bytes {
                    break;
                }
                if level_handlers[0].is_level_pending_compact(level) {
                    break;
                }
                l0_total_file_size += level.total_file_size;
                input_levels.push(InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: level.table_infos.clone(),
                });
            }

            let all_level_amplification =
                cal_file_size(&levels.get_level(self.target_level).table_infos) * 100
                    / l0_total_file_size;
            if write_amplification < all_level_amplification {
                return None;
            }
            input_levels.push(InputLevel {
                level_idx: target_level as u32,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: levels.get_level(self.target_level).table_infos.clone(),
            });
        }

        for input_level in &input_levels {
            level_handlers[input_level.level_idx as usize].add_pending_task(
                self.compact_task_id,
                self.target_level,
                &input_level.table_infos,
            );
        }

        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        target_level: usize,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
            compact_task_id,
            target_level,
            overlap_strategy,
            config,
        }
    }

    fn pick_min_overlap_tables(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        level_handlers: &mut [LevelHandler],
    ) -> Vec<InputLevel> {
        let min_overlap_picker = MinOverlappingPicker::new(
            self.compact_task_id,
            0,
            self.target_level,
            self.overlap_strategy.clone(),
        );

        // Do not use `pick_compaction` because it can not select a sub-level.
        let (select_tables, target_tables) = min_overlap_picker.pick_tables(
            &l0.sub_levels[0].table_infos,
            &target_level.table_infos,
            level_handlers,
        );
        if select_tables.is_empty() {
            return vec![];
        }
        vec![
            InputLevel {
                level_idx: 0,
                level_type: l0.sub_levels[0].level_type,
                table_infos: select_tables,
            },
            InputLevel {
                level_idx: self.target_level as u32,
                level_type: target_level.level_type,
                table_infos: target_tables,
            },
        ]
    }
}
#[cfg(test)]
pub mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_with_overlap, generate_level, generate_table, push_table_level0,
        push_tables_level0,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::{CompactionMode, TierCompactionPicker};

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .build(),
        );
        LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()))
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let picker = create_compaction_picker_for_test();
        let l0 = generate_level(
            0,
            vec![
                generate_table(5, 1, 100, 200, 2),
                generate_table(4, 1, 201, 300, 2),
            ],
        );
        let mut levels = Levels {
            l0: Some(OverlappingLevel {
                total_file_size: l0.total_file_size,
                sub_levels: vec![l0],
            }),
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(2, 1, 111, 200, 1),
                    generate_table(1, 1, 222, 300, 1),
                    generate_table(0, 1, 301, 400, 1),
                ],
            )],
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 4);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 1);

        // no conflict with the last job but we do not allow compact higher level to l1 when there
        // is a pending task.
        push_table_level0(&mut levels, generate_table(6, 1, 100, 200, 2));
        push_table_level0(&mut levels, generate_table(7, 1, 301, 333, 4));
        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels.l0.as_mut().unwrap().sub_levels[0]
            .table_infos
            .retain(|table| table.id != 4);
        levels.l0.as_mut().unwrap().total_file_size -= ret.input_levels[0].table_infos[0].file_size;

        levels_handler[0].remove_task(0);
        levels_handler[1].remove_task(0);

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 6);
        assert_eq!(ret.input_levels[2].table_infos[0].id, 7);
        assert_eq!(ret.input_levels[3].table_infos.len(), 4);

        // the first idle table in L0 is table 6 and its confict with the last job so we can not
        // pick table 7.
        let picker = LevelCompactionPicker::new(
            1,
            1,
            Arc::new(CompactionConfigBuilder::new().build()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        push_table_level0(&mut levels, generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &mut levels_handler);
        assert!(ret.is_none());

        // compact L0 to L0
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .build();
        let picker = TierCompactionPicker::new(
            2,
            Arc::new(config),
            Arc::new(RangeOverlapStrategy::default()),
        );
        push_table_level0(&mut levels, generate_table(9, 1, 100, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 8);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 9);

        levels_handler[0].remove_task(1);
        levels
            .l0
            .as_mut()
            .unwrap()
            .sub_levels
            .retain(|level| level.table_infos[0].id < 7);
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(10, 1, 100, 200, 3),
                generate_table(11, 1, 201, 300, 3),
                generate_table(12, 1, 301, 400, 3),
            ],
        );
        push_table_level0(&mut levels, generate_table(12, 1, 100, 400, 4));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 3);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .compaction_mode(CompactionMode::Range as i32)
                .build(),
        );
        let picker =
            LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()));

        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![
                generate_table(3, 1, 0, 50, 1),
                generate_table(4, 1, 150, 200, 1),
                generate_table(5, 1, 250, 300, 1),
            ],
            total_file_size: 0,
            sub_level_id: 0,
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            }),
        };
        push_tables_level0(&mut levels, vec![generate_table(1, 1, 50, 60, 2)]);
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(7, 1, 200, 250, 2),
                generate_table(8, 1, 400, 500, 2),
            ],
        );

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 3);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);

        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(
            ret.input_levels[0]
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![1]
        );

        assert_eq!(
            ret.input_levels[1]
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![7, 8]
        );
    }

    #[test]
    fn test_l0_to_l1_compact_conflict() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let picker = create_compaction_picker_for_test();
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            }),
        };
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(1, 1, 100, 300, 2),
                generate_table(2, 1, 350, 500, 2),
            ],
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 0);

        push_tables_level0(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let picker =
            TierCompactionPicker::new(1, picker.config.clone(), picker.overlap_strategy.clone());
        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }

    #[test]
    fn test_compact_to_l1_concurrently() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with any L1 files
        // under compaction.
        let picker = create_compaction_picker_for_test();

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(2, 1, 150, 300, 2)],
                total_file_size: 0,
                sub_level_id: 0,
            }],
            l0: Some(generate_l0_with_overlap(vec![generate_table(
                1, 1, 200, 250, 2,
            )])),
        };

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels.l0.as_mut().unwrap().sub_levels[0].table_infos = vec![
            generate_table(3, 1, 100, 140, 3),
            generate_table(1, 1, 200, 250, 2),
            generate_table(4, 1, 400, 500, 3),
        ];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        // Will be trivial move. The second file can not be picked up because the range of files
        // [3,4] would be overlap with file [0]
        assert!(ret.input_levels[1].table_infos.is_empty());
        assert_eq!(ret.target_level, 1);
        assert_eq!(
            ret.input_levels[0]
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![3]
        );
    }

    #[test]
    fn test_compacting_key_range_overlap_intra_l0() {
        // When picking L0->L0, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let picker = create_compaction_picker_for_test();

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
                total_file_size: 0,
                sub_level_id: 0,
            }],
            l0: Some(generate_l0_with_overlap(vec![
                generate_table(1, 1, 100, 210, 2),
                generate_table(2, 1, 200, 250, 2),
            ])),
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        push_table_level0(&mut levels, generate_table(4, 1, 170, 180, 3));
        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }

    // compact the whole level and upper sub-level when the write-amplification is more than 1.5.
    #[test]
    fn test_compact_whole_level_write_amplification_limit() {
        let picker = create_compaction_picker_for_test();
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 199, 2),
                    generate_table(2, 1, 200, 260, 2),
                    generate_table(3, 1, 300, 600, 2),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            }],
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(4, 1, 130, 180, 2),
                generate_table(5, 1, 190, 250, 2),
                generate_table(6, 1, 200, 300, 2),
            ],
        );
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(7, 1, 130, 180, 2),
                generate_table(8, 1, 190, 250, 2),
                generate_table(9, 1, 200, 300, 2),
            ],
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 6);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[2].table_infos[0].id, 1);
        assert_eq!(ret.input_levels[2].table_infos[1].id, 2);
        assert_eq!(ret.input_levels[2].table_infos[2].id, 3);
    }

    #[test]
    fn test_skip_compact_write_amplification_limit() {
        let picker = create_compaction_picker_for_test();
        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 399, 2),
                    generate_table(2, 1, 400, 699, 2),
                    generate_table(3, 1, 700, 999, 2),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            }],
            l0: Some(generate_l0_with_overlap(vec![])),
        };
        push_tables_level0(
            &mut levels,
            vec![
                generate_table(4, 1, 100, 180, 2),
                generate_table(5, 1, 400, 450, 2),
                generate_table(6, 1, 600, 700, 2),
            ],
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker.pick_compaction(&levels, &mut levels_handler);
        // Skip this compaction because the write amplification is too large.
        assert!(ret.is_none());
    }
}
