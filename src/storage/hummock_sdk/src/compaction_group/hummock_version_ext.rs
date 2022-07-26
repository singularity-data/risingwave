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

use itertools::Itertools;
use risingwave_pb::hummock::{HummockVersion, HummockVersionDelta, Level, SstableId, SstableInfo};

use crate::prost_key_range::KeyRangeExt;
use crate::{CompactionGroupId, HummockSstableId};

pub trait HummockVersionExt {
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level>;
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level>;
    /// Gets all levels.
    ///
    /// Levels belonging to the same compaction group retain their relative order.
    fn get_combined_levels(&self) -> Vec<&Level>;
    fn get_sst_ids(&self) -> Vec<HummockSstableId>;
    fn apply_compact_ssts(
        levels: &mut Vec<Level>,
        delete_sst_levels: &[u32],
        delete_sst_ids_set: &HashSet<HummockSstableId>,
        insert_sst_level: u32,
        insert_table_infos: Vec<SstableInfo>,
    );
    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta);
}

impl HummockVersionExt for HummockVersion {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Vec<Level> {
        &self
            .levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Vec<Level> {
        &mut self
            .levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
            .levels
    }

    fn get_combined_levels(&self) -> Vec<&Level> {
        let mut combined_levels = vec![];
        for level in self.levels.values() {
            combined_levels.extend(level.levels.iter());
        }
        combined_levels
    }

    fn get_sst_ids(&self) -> Vec<HummockSstableId> {
        self.levels
            .iter()
            .flat_map(|(_, l)| &l.levels)
            .flat_map(|level| {
                level
                    .table_infos
                    .iter()
                    .map(|table_info| table_info.id_as_int())
            })
            .collect_vec()
    }

    fn apply_compact_ssts(
        levels: &mut Vec<Level>,
        delete_sst_levels: &[u32],
        delete_sst_ids_set: &HashSet<HummockSstableId>,
        insert_sst_level: u32,
        insert_table_infos: Vec<SstableInfo>,
    ) {
        let mut l0_remove_position = None;
        for level_idx in delete_sst_levels {
            level_delete_ssts(
                &mut levels[*level_idx as usize],
                delete_sst_ids_set,
                &mut l0_remove_position,
            );
        }
        if !insert_table_infos.is_empty() {
            level_insert_ssts(
                &mut levels[insert_sst_level as usize],
                insert_table_infos,
                &l0_remove_position,
            );
        }
    }

    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, level_deltas) in &version_delta.level_deltas {
            let mut delete_sst_levels = Vec::with_capacity(level_deltas.level_deltas.len());
            let mut delete_sst_ids_set = HashSet::new();
            let mut insert_sst_level = u32::MAX;
            let mut insert_table_infos = vec![];
            for level_delta in &level_deltas.level_deltas {
                if !level_delta.removed_table_ids.is_empty() {
                    delete_sst_levels.push(level_delta.level_idx);
                    delete_sst_ids_set.extend(
                        level_delta
                            .removed_table_ids
                            .iter()
                            .map(|s| s.as_int())
                            .clone(),
                    );
                }
                if !level_delta.inserted_table_infos.is_empty() {
                    insert_sst_level = level_delta.level_idx;
                    insert_table_infos.extend(level_delta.inserted_table_infos.iter().cloned());
                }
            }
            let operand = &mut self
                .get_compaction_group_levels_mut(*compaction_group_id as CompactionGroupId);
            HummockVersion::apply_compact_ssts(
                operand,
                &delete_sst_levels,
                &delete_sst_ids_set,
                insert_sst_level,
                insert_table_infos,
            );
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
    }
}

fn level_delete_ssts(
    operand: &mut Level,
    delete_sst_ids_superset: &HashSet<HummockSstableId>,
    l0_remove_position: &mut Option<usize>,
) {
    let mut new_table_infos = Vec::with_capacity(operand.table_infos.len());
    let mut new_total_file_size = 0;
    for table_info in &operand.table_infos {
        if delete_sst_ids_superset.contains(&table_info.id_as_int()) {
            if operand.level_idx == 0 && l0_remove_position.is_none() {
                *l0_remove_position = Some(new_table_infos.len());
            }
        } else {
            new_total_file_size += table_info.file_size;
            new_table_infos.push(table_info.clone());
        }
    }
    operand.table_infos = new_table_infos;
    operand.total_file_size = new_total_file_size;
}

fn level_insert_ssts(
    operand: &mut Level,
    insert_table_infos: Vec<SstableInfo>,
    l0_remove_position: &Option<usize>,
) {
    operand.total_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.file_size)
        .sum::<u64>();
    let mut l0_remove_position = *l0_remove_position;
    if operand.level_idx != 0 {
        l0_remove_position = None;
    }
    if let Some(l0_remove_pos) = l0_remove_position {
        let (l, r) = operand.table_infos.split_at_mut(l0_remove_pos);
        let mut new_table_infos = l.to_vec();
        new_table_infos.extend(insert_table_infos);
        new_table_infos.extend_from_slice(r);
        operand.table_infos = new_table_infos;
    } else {
        operand.table_infos.extend(insert_table_infos);
        if operand.level_idx != 0 {
            operand.table_infos.sort_by(|sst1, sst2| {
                let a = sst1.key_range.as_ref().unwrap();
                let b = sst2.key_range.as_ref().unwrap();
                a.compare(b)
            });
        }
    }
}

pub trait SstableIdExt {
    fn as_int(&self) -> HummockSstableId;
    fn as_string(&self) -> String;
    fn from_int(id: HummockSstableId) -> Self;
}

const NODE_ID_MASK: u128 = (u64::MAX as u128) << 64;
const SEQ_ID_MASK: u128 = u64::MAX as u128;

impl SstableIdExt for SstableId {
    fn as_int(&self) -> HummockSstableId {
        debug_assert_eq!(HummockSstableId::BITS, 128);
        ((self.node_id as u128) << 64) | (self.seq_id as u128 & SEQ_ID_MASK)
    }

    fn as_string(&self) -> String {
        format!("{:x}_{:x}", self.node_id, self.seq_id)
    }

    fn from_int(id: HummockSstableId) -> Self {
        Self {
            node_id: ((id & NODE_ID_MASK) >> 64) as u64,
            seq_id: (id & SEQ_ID_MASK) as u64,
        }
    }
}

pub trait SstableInfoExt {
    /// Returns SST id as int
    fn id_as_int(&self) -> HummockSstableId;
    /// Returns node id in SST id
    fn node_id(&self) -> u64;
    /// Returns seq id in SST id
    fn seq_id(&self) -> u64;
}

impl SstableInfoExt for SstableInfo {
    fn id_as_int(&self) -> HummockSstableId {
        self.id.as_ref().unwrap().as_int()
    }

    fn node_id(&self) -> u64 {
        self.id.as_ref().unwrap().node_id
    }

    fn seq_id(&self) -> u64 {
        self.id.as_ref().unwrap().seq_id
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::SstableId;

    use crate::SstableIdExt;

    #[test]
    fn test_sstable_id_ext() {
        let id = SstableId {
            node_id: 1,
            seq_id: 2,
        };
        assert_eq!(id.as_int(), 18446744073709551618u128);
        assert_eq!(id.as_string(), "1_2");

        let id = SstableId {
            node_id: u64::MAX - 1,
            seq_id: u64::MAX - 2,
        };

        assert_eq!(id.as_int(), 340282366920938463444927863358058659837);
        assert_eq!(id.as_string(), "fffffffffffffffe_fffffffffffffffd");
    }
}
