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

use bytes::Bytes;
use madsim::collections::{btree_map, BTreeMap};
use risingwave_common::array::data_chunk_iter::RowDeserializer;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use super::super::flush_status::BtreeMapFlushStatus as FlushStatus;
use super::*;

type JoinEntryStateIter<'a> = btree_map::Iter<'a, PkType, StateValueType>;

type JoinEntryStateValues<'a> = btree_map::Values<'a, PkType, StateValueType>;

type JoinEntryStateValuesMut<'a> = btree_map::ValuesMut<'a, PkType, StateValueType>;

/// We manages a `BTreeMap` in memory for all entries belonging to a join key,
/// since each `WriteBatch` is an ordered list of key-value pairs.
/// When evicted, `BTreeMap` does not hold any entries.
pub struct JoinEntryState<S: StateStore> {
    /// The full copy of the state. If evicted, it will be `None`.
    cached: Option<BTreeMap<PkType, StateValueType>>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<PkType, FlushStatus<StateValueType>>,

    /// Data types of the sort column
    data_types: Arc<[DataType]>,

    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore> JoinEntryState<S> {
#[allow(unused)]
    pub fn new(
        keyspace: Keyspace<S>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Self {
        Self {
            cached: None,
            flush_buffer: BTreeMap::new(),
            data_types,
            pk_data_types,
            keyspace,
        }
    }

    pub fn new_with_empty_cache(
        keyspace: Keyspace<S>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Self {
        Self {
            cached: Some(BTreeMap::new()),
            flush_buffer: BTreeMap::new(),
            data_types,
            pk_data_types,
            keyspace,
        }
    }

    pub async fn with_cached_state(
        keyspace: Keyspace<S>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
        epoch: u64,
    ) -> Result<Option<Self>> {
        let all_data = keyspace.scan(None, epoch).await?;
        if !all_data.is_empty() {
            // Insert cached states.
            let cached = Self::fill_cached(all_data, data_types.clone(), pk_data_types.clone())?;
            Ok(Some(Self {
                cached: Some(cached),
                flush_buffer: BTreeMap::new(),
                data_types,
                pk_data_types,
                keyspace,
            }))
        } else {
            Ok(None)
        }
    }

    fn fill_cached(
        data: Vec<(Bytes, Bytes)>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Result<BTreeMap<PkType, StateValueType>> {
        let mut cached = BTreeMap::new();
        for (raw_key, raw_value) in data {
            let pk_deserializer = RowDeserializer::new(pk_data_types.to_vec());
            let key = pk_deserializer.value_decode(raw_key)?;
            let deserializer = JoinRowDeserializer::new(data_types.to_vec());
            let value = deserializer.deserialize(raw_value)?;
            cached.insert(key, value);
        }
        Ok(cached)
    }

    /// The state is dirty means there are unflush
    #[allow(dead_code)]
    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    // Insert into the cache and flush buffer.
    pub fn insert(&mut self, key: PkType, value: StateValueType) {
        if let Some(cached) = self.cached.as_mut() {
            cached.insert(key.clone(), value.clone());
        }
        // If no cache maintained, only update the flush buffer.
        FlushStatus::do_insert(self.flush_buffer.entry(key), value);
    }

    pub fn remove(&mut self, pk: PkType) {
        if let Some(cached) = self.cached.as_mut() {
            cached.remove(&pk);
        }
        // If no cache maintained, only update the flush buffer.
        FlushStatus::do_delete(self.flush_buffer.entry(pk));
    }

    // Flush data to the state store
    pub fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
        let mut local = write_batch.prefixify(&self.keyspace);

        for (pk, v) in std::mem::take(&mut self.flush_buffer) {
            // pk here does not to be memcomparable.
            let key_encoded = pk.value_encode()?;

            match v.into_option() {
                Some(v) => {
                    let value = v.serialize()?;
                    // TODO(Yuanxin): Implement value meta
                    local.put(key_encoded, StorageValue::new_default_put(value));
                }
                None => {
                    local.delete(key_encoded);
                }
            }
        }
        Ok(())
    }

    // Fetch cache from the state store.
    pub(crate) async fn populate_cache(&mut self, epoch: u64) -> Result<()> {
        assert!(self.cached.is_none());

        let all_data = self.keyspace.scan(None, epoch).await?;

        // Insert cached states.
        let mut cached = Self::fill_cached(
            all_data,
            self.data_types.clone(),
            self.pk_data_types.clone(),
        )?;

        // Apply current flush buffer to cached states.
        for (pk, row) in &self.flush_buffer {
            match row.as_option() {
                Some(row) => {
                    cached.insert(pk.clone(), row.clone());
                }
                None => {
                    cached.remove(pk);
                }
            }
        }

        self.cached = Some(cached);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while all or none state is dirty"
        );
        self.cached = None;
    }

    #[allow(dead_code)]
    pub async fn iter(&mut self, epoch: u64) -> JoinEntryStateIter<'_> {
        if self.cached.is_none() {
            self.populate_cache(epoch).await.unwrap();
        }
        self.cached.as_ref().unwrap().iter()
    }

    #[allow(dead_code)]
    pub async fn values(&mut self, epoch: u64) -> JoinEntryStateValues<'_> {
        if self.cached.is_none() {
            self.populate_cache(epoch).await.unwrap();
        }
        self.cached.as_ref().unwrap().values()
    }

    // TODO: The right semantics here should be to mark the modified join entries as dirty etc...
    pub async fn values_mut(&mut self, epoch: u64) -> JoinEntryStateValuesMut<'_> {
        if self.cached.is_none() {
            self.populate_cache(epoch).await.unwrap();
        }
        self.cached.as_mut().unwrap().values_mut()
    }

    pub(crate) fn has_cached(&self) -> bool {
        self.cached.is_some()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::types::ScalarImpl;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::executor_root(store.clone(), 0x2333);
        let mut managed_state = JoinEntryState::new(
            keyspace,
            vec![DataType::Int64, DataType::Int64].into(),
            vec![DataType::Int64].into(),
        );
        assert!(!managed_state.is_dirty());
        let pk_indices = [0];
        let col1 = [1, 2, 3];
        let col2 = [6, 5, 4];
        let data_chunk = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 6",
        );

        for row_ref in data_chunk.rows() {
            let row: Row = row_ref.into();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            let pk = Row(pk);
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, join_row);
        }

        let epoch = 0;
        for state in managed_state
            .iter(epoch)
            .await
            .zip_eq(col1.iter().zip_eq(col2.iter()))
        {
            let ((key, value), (d1, d2)) = state;
            assert_eq!(key.0[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(value.degree, 0);
        }

        // flush to write batch and write to state store
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();

        assert!(!managed_state.is_dirty());
    }
}
