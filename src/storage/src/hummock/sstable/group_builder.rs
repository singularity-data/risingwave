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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{get_table_id, FullKey};
use risingwave_hummock_sdk::CompactionGroupId;
use tokio::sync::mpsc;

use crate::hummock::multi_builder::{
    CapacitySplitTableBuilder, SealedSstableBuilder, UploadRequest,
};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{CachePolicy, HummockResult, SSTableBuilder};

pub type KeyValueGroupId = u64;

/// [`KeyValueGroupingImpl`] defines strategies to group key values
#[derive(Clone)]
pub enum KeyValueGroupingImpl {
    VirtualNode(VirtualNodeGrouping),
    CompactionGroup(CompactionGroupGrouping),
}

pub trait KeyValueGrouping {
    fn group(&self, full_key: &FullKey<&[u8]>, value: &HummockValue<&[u8]>) -> KeyValueGroupId;
}

impl KeyValueGrouping for KeyValueGroupingImpl {
    fn group(&self, full_key: &FullKey<&[u8]>, value: &HummockValue<&[u8]>) -> KeyValueGroupId {
        match self {
            KeyValueGroupingImpl::VirtualNode(grouping) => grouping.group(full_key, value),
            KeyValueGroupingImpl::CompactionGroup(grouping) => grouping.group(full_key, value),
        }
    }
}

/// Groups key value by compaction group
#[derive(Clone)]
pub struct CompactionGroupGrouping {
    mapping: HashMap<StateTableId, CompactionGroupId>,
}

impl CompactionGroupGrouping {
    pub fn new(mapping: HashMap<StateTableId, CompactionGroupId>) -> Self {
        Self { mapping }
    }
}

impl KeyValueGrouping for CompactionGroupGrouping {
    fn group(&self, full_key: &FullKey<&[u8]>, _value: &HummockValue<&[u8]>) -> KeyValueGroupId {
        let table_id = get_table_id(full_key.inner()).unwrap();
        let group_key = self
            .mapping
            .get(&table_id)
            .cloned()
            .unwrap_or(CompactionGroupId::MAX);
        group_key
    }
}

/// Groups key value by virtual node
#[derive(Clone)]
pub struct VirtualNodeGrouping {
    vnode2unit: Arc<HashMap<u32, Vec<u32>>>,
}

impl VirtualNodeGrouping {
    pub fn new(vnode2unit: Arc<HashMap<u32, Vec<u32>>>) -> Self {
        VirtualNodeGrouping { vnode2unit }
    }
}

impl KeyValueGrouping for VirtualNodeGrouping {
    fn group(&self, full_key: &FullKey<&[u8]>, value: &HummockValue<&[u8]>) -> KeyValueGroupId {
        let group = if let Some(table_id) = get_table_id(full_key.inner()) {
            self.vnode2unit.get(&table_id).map(|mapping| {
                let idx = match value {
                    HummockValue::Put(meta, _) => meta.vnode,
                    HummockValue::Delete(meta) => meta.vnode,
                };
                mapping[idx as usize] as u64
            })
        } else {
            None
        };
        group.unwrap_or(u64::MAX)
    }
}

/// A wrapper for [`CapacitySplitTableBuilder`] which automatically split key-value pairs into
/// multiple `SSTables`, based on [`KeyValueGroupingImpl`].
pub struct GroupedSstableBuilder<B, G: KeyValueGrouping> {
    /// See [`CapacitySplitTableBuilder`]
    get_id_and_builder: B,
    grouping: G,
    builders: HashMap<KeyValueGroupId, CapacitySplitTableBuilder<B>>,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    uploading_sender: Option<mpsc::UnboundedSender<UploadRequest>>,
}

impl<B, G, F> GroupedSstableBuilder<B, G>
where
    B: Clone + Fn() -> F,
    G: KeyValueGrouping,
    F: Future<Output = HummockResult<SSTableBuilder>>,
{
    pub fn new(
        get_id_and_builder: B,
        grouping: G,
        policy: CachePolicy,
        sstable_store: SstableStoreRef,
        uploading_sender: Option<mpsc::UnboundedSender<UploadRequest>>,
    ) -> Self {
        Self {
            get_id_and_builder,
            grouping,
            builders: Default::default(),
            sstable_store,
            policy,
            uploading_sender,
        }
    }

    pub fn len(&self) -> usize {
        self.builders.iter().map(|(_k, v)| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.builders.iter().all(|(_k, v)| v.is_empty())
    }

    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        let group_id = self.grouping.group(&full_key, &value);
        let entry = self.builders.entry(group_id).or_insert_with(|| {
            CapacitySplitTableBuilder::new(
                self.get_id_and_builder.clone(),
                self.policy,
                self.sstable_store.clone(),
                self.uploading_sender.clone(),
            )
        });
        entry.add_full_key(full_key, value, allow_split).await
    }

    pub fn seal_current(&mut self) {
        self.builders
            .iter_mut()
            .for_each(|(_k, v)| v.seal_current());
    }

    pub fn finish(mut self) -> Vec<SealedSstableBuilder> {
        self.seal_current();
        self.builders
            .into_iter()
            .flat_map(|(k, v)| {
                v.finish().into_iter().map(move |mut builder| {
                    builder.unit_id = k;
                    builder
                })
            })
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use risingwave_common::types::VirtualNode;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::{SSTableBuilderOptions, DEFAULT_RESTART_INTERVAL};
    use crate::storage_value::ValueMeta;

    #[tokio::test]
    async fn test_compaction_group_grouping() {
        let next_id = AtomicU64::new(1001);
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = || async {
            Ok(SSTableBuilder::new(
                next_id.fetch_add(1, SeqCst),
                SSTableBuilderOptions {
                    capacity: table_capacity,
                    block_capacity: block_size,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                    bloom_false_positive: 0.1,
                    compression_algorithm: CompressionAlgorithm::None,
                },
            ))
        };
        let table_id = 1u32;
        // one compaction group defined
        let grouping = KeyValueGroupingImpl::CompactionGroup(CompactionGroupGrouping::new(
            HashMap::from([(table_id, 1 as CompactionGroupId)]),
        ));
        let mut builder = GroupedSstableBuilder::new(
            get_id_and_builder,
            grouping,
            CachePolicy::Disable,
            mock_sstable_store(),
            None,
        );
        for i in 0..10 {
            // key value belongs to no compaction group
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        b"\x74\x00\x00\x00\x00".to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::put(b"value"),
                    false,
                )
                .await
                .unwrap();
            // key value belongs to compaction group `prefix`
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        [b"\x74", table_id.to_be_bytes().as_slice()]
                            .concat()
                            .to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::put(b"value"),
                    false,
                )
                .await
                .unwrap();
        }
        builder.seal_current();
        let results = builder.finish();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_virtual_node_grouping() {
        let next_id = AtomicU64::new(1001);
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = || async {
            Ok(SSTableBuilder::new(
                next_id.fetch_add(1, SeqCst),
                SSTableBuilderOptions {
                    capacity: table_capacity,
                    block_capacity: block_size,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                    bloom_false_positive: 0.1,
                    compression_algorithm: CompressionAlgorithm::None,
                },
            ))
        };
        let table_id = 1u32;
        let vnode_number = 10u32;
        let parallel_unit_number = 3u32;
        // See Keyspace::table_root
        let magic_prefix = b't';
        let vnode2unit = Arc::new(HashMap::from([(
            table_id,
            { 0..vnode_number }
                .into_iter()
                .map(|i| i % parallel_unit_number)
                .collect_vec(),
        )]));

        // Test < parallel_unit_number
        let grouping = KeyValueGroupingImpl::VirtualNode(VirtualNodeGrouping::new(vnode2unit));
        let mut builder = GroupedSstableBuilder::new(
            get_id_and_builder,
            grouping.clone(),
            CachePolicy::Disable,
            mock_sstable_store(),
            None,
        );
        for i in 0..2 {
            // key value matches table and mapping
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        [&[magic_prefix], table_id.to_be_bytes().as_slice()]
                            .concat()
                            .to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::Put(ValueMeta::with_vnode(i as VirtualNode), b"value"),
                    false,
                )
                .await
                .unwrap();
        }
        builder.seal_current();
        let results = builder.finish();
        assert_eq!(results.len(), 2);
        assert!(results.len() < parallel_unit_number as usize);

        // Test > parallel_unit_number
        let mut builder = GroupedSstableBuilder::new(
            get_id_and_builder,
            grouping.clone(),
            CachePolicy::Disable,
            mock_sstable_store(),
            None,
        );
        for i in 0..10 {
            // key value matches table and mapping
            builder
                .add_full_key(
                    FullKey::from_user_key(
                        [&[magic_prefix], table_id.to_be_bytes().as_slice()]
                            .concat()
                            .to_vec(),
                        (table_capacity - i) as u64,
                    )
                    .as_slice(),
                    HummockValue::Put(ValueMeta::with_vnode(i as VirtualNode), b"value"),
                    false,
                )
                .await
                .unwrap();
        }
        builder.seal_current();
        let results = builder.finish();
        assert_eq!(results.len(), parallel_unit_number as usize);

        // Test no matching table id
        let mut builder = GroupedSstableBuilder::new(
            get_id_and_builder,
            grouping.clone(),
            CachePolicy::Disable,
            mock_sstable_store(),
            None,
        );
        builder
            .add_full_key(
                FullKey::from_user_key(
                    [&[magic_prefix], (2u32).to_be_bytes().as_slice()]
                        .concat()
                        .to_vec(),
                    table_capacity as u64,
                )
                .as_slice(),
                HummockValue::Put(ValueMeta::with_vnode(0 as VirtualNode), b"value"),
                false,
            )
            .await
            .unwrap();
        builder.seal_current();
        let results = builder.finish();
        assert_eq!(results.len(), 1usize);
    }
}
