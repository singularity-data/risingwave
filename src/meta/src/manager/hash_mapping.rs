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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::consistent_hash::{VirtualNode, VIRTUAL_NODE_COUNT};
use risingwave_pb::common::{ParallelUnit, VNodeBitmap};

use super::TableId;
use crate::cluster::ParallelUnitId;
use crate::model::FragmentId;

pub type HashMappingManagerRef = Arc<HashMappingManager>;

/// `HashMappingManager` maintains vnode hash mappings based on consistent hash.
pub struct HashMappingManager {
    core: Mutex<HashMappingManagerCore>,
}

impl Default for HashMappingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl HashMappingManager {
    pub fn new() -> Self {
        Self {
            core: Mutex::new(HashMappingManagerCore::new()),
        }
    }

    pub fn build_fragment_hash_mapping(
        &self,
        fragment_id: FragmentId,
        parallel_units: &[ParallelUnit],
    ) -> Vec<ParallelUnitId> {
        let mut core = self.core.lock();
        core.build_fragment_hash_mapping(fragment_id, parallel_units)
    }

    pub fn set_fragment_hash_mapping(
        &self,
        fragment_id: FragmentId,
        hash_mapping: Vec<ParallelUnitId>,
    ) {
        let mut core = self.core.lock();
        core.set_fragment_hash_mapping(fragment_id, hash_mapping);
    }

    pub fn set_fragment_state_table(&self, fragment_id: FragmentId, state_table_id: TableId) {
        let mut core = self.core.lock();
        core.state_table_fragment_mapping
            .insert(state_table_id, fragment_id);
    }

    pub fn get_table_hash_mapping(&self, table_id: &TableId) -> Option<Vec<ParallelUnitId>> {
        let core = self.core.lock();
        let fragment_id = core.state_table_fragment_mapping.get(table_id);
        if let Some(fragment_id) = fragment_id {
            core.hash_mapping_infos
                .get(fragment_id)
                .map(|info| info.vnode_mapping.clone())
        } else {
            None
        }
    }

    pub fn get_fragment_hash_mapping(
        &self,
        fragment_id: &FragmentId,
    ) -> Option<Vec<ParallelUnitId>> {
        let core = self.core.lock();
        core.hash_mapping_infos
            .get(fragment_id)
            .map(|info| info.vnode_mapping.clone())
    }

    pub fn set_need_consolidation(&self, newflag: bool) {
        let mut core = self.core.lock();
        core.need_sst_consolidation = newflag;
    }

    pub fn get_need_consolidation(&self) -> bool {
        let core = self.core.lock();
        core.need_sst_consolidation
    }

    pub fn check_sst_deprecated(&self, vnode_bitmaps: &[VNodeBitmap]) -> bool {
        let core = self.core.lock();
        let mut unit: Option<u64> = None;
        for vnode_bitmap in vnode_bitmaps {
            let mut is_dummy = false;
            if let Some(fragment_id) = core
                .state_table_fragment_mapping
                .get(&vnode_bitmap.get_table_id())
            {
                if let Some(HashMappingInfo { vnode_mapping, .. }) =
                    core.hash_mapping_infos.get(fragment_id)
                {
                    let len = vnode_bitmap.get_bitmap().len();
                    for i in 0..len {
                        for j in 0..u8::BITS {
                            if vnode_bitmap.get_bitmap()[i] & (1 << j) != 0 {
                                let cur = vnode_mapping[i * u8::BITS as usize + j as usize] as u64;
                                match unit {
                                    Some(pre) => {
                                        if cur != pre {
                                            return true;
                                        }
                                    }
                                    None => {
                                        unit = Some(cur);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    is_dummy = true;
                }
            } else {
                is_dummy = true;
            }
            if is_dummy {
                let cur = u64::MAX;
                match unit {
                    Some(pre) => {
                        if cur != pre {
                            return true;
                        }
                    }
                    None => {
                        unit = Some(cur);
                    }
                }
            }
        }
        false
    }

    /// For test.
    #[cfg(test)]
    fn get_fragment_mapping_info(&self, fragment_id: &FragmentId) -> Option<HashMappingInfo> {
        let core = self.core.lock();
        core.hash_mapping_infos.get(fragment_id).cloned()
    }
}

/// `HashMappingInfo` stores the vnode mapping and some other helpers for maintaining a
/// load-balanced vnode mapping.
#[derive(Clone)]
struct HashMappingInfo {
    /// Hash mapping from virtual node to parallel unit.
    vnode_mapping: Vec<ParallelUnitId>,
    /// Mapping from parallel unit to virtual node.
    #[cfg_attr(not(test), expect(dead_code))]
    owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>>,
    /// Mapping from vnode count to parallel unit, aiming to maintain load balance.
    #[cfg_attr(not(test), expect(dead_code))]
    load_balancer: BTreeMap<usize, Vec<ParallelUnitId>>,
}

struct HashMappingManagerCore {
    need_sst_consolidation: bool,
    /// Mapping from fragment to hash mapping information. One fragment will have exactly one vnode
    /// mapping, which describes the data distribution of the fragment.
    hash_mapping_infos: HashMap<FragmentId, HashMappingInfo>,
    /// Mapping from state table to fragment. Used for providing vnode information for compactor.
    state_table_fragment_mapping: HashMap<TableId, FragmentId>,
}

impl HashMappingManagerCore {
    fn new() -> Self {
        Self {
            need_sst_consolidation: true,
            hash_mapping_infos: HashMap::new(),
            state_table_fragment_mapping: HashMap::new(),
        }
    }

    fn build_fragment_hash_mapping(
        &mut self,
        fragment_id: FragmentId,
        parallel_units: &[ParallelUnit],
    ) -> Vec<ParallelUnitId> {
        let mut vnode_mapping = Vec::with_capacity(VIRTUAL_NODE_COUNT);
        let mut owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();
        let mut load_balancer: BTreeMap<usize, Vec<ParallelUnitId>> = BTreeMap::new();
        let hash_shard_size = VIRTUAL_NODE_COUNT / parallel_units.len();
        let mut init_bound = hash_shard_size;

        parallel_units.iter().for_each(|parallel_unit| {
            let parallel_unit_id = parallel_unit.id;
            vnode_mapping.resize(init_bound, parallel_unit_id);
            let vnodes = (init_bound - hash_shard_size..init_bound)
                .map(|id| id as VirtualNode)
                .collect();
            owner_mapping.insert(parallel_unit_id, vnodes);
            init_bound += hash_shard_size;
        });

        let mut parallel_unit_iter = parallel_units.iter().cycle();
        for vnode in init_bound - hash_shard_size..VIRTUAL_NODE_COUNT {
            let id = parallel_unit_iter.next().unwrap().id;
            vnode_mapping.push(id);
            owner_mapping
                .entry(id)
                .or_default()
                .push(vnode as VirtualNode);
        }

        owner_mapping.iter().for_each(|(parallel_unit_id, vnodes)| {
            let vnode_count = vnodes.len();
            load_balancer
                .entry(vnode_count)
                .or_default()
                .push(*parallel_unit_id);
        });

        let mapping_info = HashMappingInfo {
            vnode_mapping: vnode_mapping.clone(),
            owner_mapping,
            load_balancer,
        };
        self.hash_mapping_infos.insert(fragment_id, mapping_info);

        vnode_mapping
    }

    fn set_fragment_hash_mapping(
        &mut self,
        fragment_id: FragmentId,
        vnode_mapping: Vec<ParallelUnitId>,
    ) {
        let mut owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();
        let mut load_balancer: BTreeMap<usize, Vec<ParallelUnitId>> = BTreeMap::new();

        vnode_mapping
            .iter()
            .enumerate()
            .for_each(|(vnode, parallel_unit)| {
                owner_mapping
                    .entry(*parallel_unit)
                    .or_default()
                    .push(vnode as VirtualNode);
            });
        owner_mapping.iter().for_each(|(parallel_unit, vnodes)| {
            load_balancer
                .entry(vnodes.len())
                .or_default()
                .push(*parallel_unit);
        });

        let mapping_info = HashMappingInfo {
            vnode_mapping,
            owner_mapping,
            load_balancer,
        };

        self.hash_mapping_infos.insert(fragment_id, mapping_info);
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::consistent_hash::VIRTUAL_NODE_COUNT;
    use risingwave_pb::common::{ParallelUnit, ParallelUnitType, VNodeBitmap};

    use super::{HashMappingInfo, HashMappingManager};

    #[test]
    fn test_build_hash_mapping() {
        let parallel_unit_count = 6usize;
        let parallel_units = (1..parallel_unit_count + 1)
            .map(|id| ParallelUnit {
                id: id as u32,
                r#type: ParallelUnitType::Hash as i32,
                worker_node_id: 1,
            })
            .collect_vec();
        let hash_mapping_manager = HashMappingManager::new();

        let fragment_id = 1u32;
        hash_mapping_manager.build_fragment_hash_mapping(fragment_id, &parallel_units);
        let vnode_mapping = hash_mapping_manager
            .get_fragment_hash_mapping(&fragment_id)
            .unwrap();
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 1)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count + 1
        );
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 2)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count + 1
        );
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 3)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 4)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 5)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );
        assert_eq!(
            vnode_mapping
                .iter()
                .filter(|&parallel_unit_id| *parallel_unit_id == 6)
                .count(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );

        let table_id = 2u32;
        hash_mapping_manager.set_fragment_state_table(fragment_id, table_id);
        assert_eq!(
            hash_mapping_manager
                .get_table_hash_mapping(&table_id)
                .unwrap(),
            vnode_mapping
        );
    }

    #[test]
    fn test_restore_hash_mapping() {
        let fragment_id = 3u32;
        let mut old_vnode_mapping = Vec::new();
        let parallel_unit_count = 5usize;
        for i in 1..parallel_unit_count + 1 {
            old_vnode_mapping.resize(VIRTUAL_NODE_COUNT / parallel_unit_count * i, i as u32);
        }
        old_vnode_mapping.push(1);
        old_vnode_mapping.push(2);
        old_vnode_mapping.push(3);

        let hash_mapping_manager = HashMappingManager::new();
        hash_mapping_manager.set_fragment_hash_mapping(fragment_id, old_vnode_mapping.clone());
        let HashMappingInfo {
            vnode_mapping,
            owner_mapping,
            load_balancer,
        } = hash_mapping_manager
            .get_fragment_mapping_info(&fragment_id)
            .unwrap();
        assert_eq!(vnode_mapping, old_vnode_mapping);
        assert_eq!(
            owner_mapping.get(&1).unwrap().len(),
            VIRTUAL_NODE_COUNT / parallel_unit_count + 1
        );
        assert_eq!(
            owner_mapping.get(&2).unwrap().len(),
            VIRTUAL_NODE_COUNT / parallel_unit_count + 1
        );
        assert_eq!(
            owner_mapping.get(&3).unwrap().len(),
            VIRTUAL_NODE_COUNT / parallel_unit_count + 1
        );
        assert_eq!(
            owner_mapping.get(&4).unwrap().len(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );
        assert_eq!(
            owner_mapping.get(&5).unwrap().len(),
            VIRTUAL_NODE_COUNT / parallel_unit_count
        );

        let mut more_counts = load_balancer
            .get(&(VIRTUAL_NODE_COUNT / parallel_unit_count + 1))
            .cloned()
            .unwrap();
        more_counts.sort();
        assert_eq!(*more_counts, vec![1u32, 2, 3]);
        let mut less_counts = load_balancer
            .get(&(VIRTUAL_NODE_COUNT / parallel_unit_count))
            .cloned()
            .unwrap();
        less_counts.sort();
        assert_eq!(less_counts, vec![4u32, 5]);
    }

    #[test]
    fn test_check_sst_deprecated() {
        let parallel_unit_count = 6usize;
        let parallel_units = (1..parallel_unit_count + 1)
            .map(|id| ParallelUnit {
                id: id as u32,
                r#type: ParallelUnitType::Hash as i32,
                worker_node_id: 1,
            })
            .collect_vec();
        let hash_mapping_manager = HashMappingManager::new();

        let fragment_id = 1u32;
        hash_mapping_manager.build_fragment_hash_mapping(fragment_id, &parallel_units);
        let vnode_mapping = hash_mapping_manager
            .get_fragment_hash_mapping(&fragment_id)
            .unwrap();

        let table_id = 2u32;
        hash_mapping_manager.set_fragment_state_table(fragment_id, table_id);
        assert_eq!(
            hash_mapping_manager
                .get_table_hash_mapping(&table_id)
                .unwrap(),
            vnode_mapping
        );

        let full_bitmap = VNodeBitmap {
            table_id,
            bitmap: [u8::MAX; VIRTUAL_NODE_COUNT / u8::BITS as usize].to_vec(),
        };
        assert!(hash_mapping_manager.check_sst_deprecated(&[full_bitmap]));

        let mut bitmap = [0; VIRTUAL_NODE_COUNT / u8::BITS as usize].to_vec();
        for i in 0..VIRTUAL_NODE_COUNT / parallel_unit_count {
            bitmap[i / u8::BITS as usize] |= 1 << (i % u8::BITS as usize);
        }
        let fresh_bitmap = VNodeBitmap { table_id, bitmap };
        assert!(!hash_mapping_manager.check_sst_deprecated(&[fresh_bitmap]));

        bitmap = [0; VIRTUAL_NODE_COUNT / u8::BITS as usize].to_vec();
        for i in 0..VIRTUAL_NODE_COUNT / parallel_unit_count + 1 {
            bitmap[i / u8::BITS as usize] |= 1 << (i % u8::BITS as usize);
        }
        let stale_bitmap = VNodeBitmap { table_id, bitmap };
        assert!(hash_mapping_manager.check_sst_deprecated(&[stale_bitmap]));
    }
}
