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

mod join_entry_state;
use std::alloc::Global;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut, Index};
use std::sync::Arc;

use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
pub use join_entry_state::JoinEntryState;
use risingwave_common::array::Row;
use risingwave_common::bail;
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::StateStore;
use stats_alloc::{SharedStatsAlloc, StatsAlloc};

use crate::executor::error::StreamExecutorResult;
use crate::executor::monitor::StreamingMetrics;

type DegreeType = u64;
/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow {
    pub row: Row,
    degree: DegreeType,
}

impl Index<usize> for JoinRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row[index]
    }
}

impl JoinRow {
    pub fn new(row: Row, degree: DegreeType) -> Self {
        Self { row, degree }
    }

    #[expect(dead_code)]
    pub fn size(&self) -> usize {
        self.row.size()
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    pub fn inc_degree(&mut self) -> DegreeType {
        self.degree += 1;
        self.degree
    }

    pub fn dec_degree(&mut self) -> StreamExecutorResult<DegreeType> {
        if self.degree == 0 {
            bail!("Tried to decrement zero join row degree");
        }
        self.degree -= 1;
        Ok(self.degree)
    }

    pub fn row_by_indices(&self, indices: &[usize]) -> Row {
        Row(indices
            .iter()
            .map(|&idx| self.row.index(idx).to_owned())
            .collect_vec())
    }

    /// Make degree as the last datum of row
    pub fn into_row(mut self) -> Row {
        self.row.0.push(Some(ScalarImpl::Int64(self.degree as i64)));
        self.row
    }

    /// Convert [`Row`] with last datum as degree to [`JoinRow`]
    pub fn from_row(row: Row) -> Self {
        let mut datums = row.0;
        let degree_datum = datums
            .pop()
            .expect("missing degree in JoinRow")
            .expect("degree should not be null");
        let degree = degree_datum.into_int64() as u64;
        JoinRow {
            row: Row(datums),
            degree,
        }
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;
pub type HashValueType = JoinEntryState;

type JoinHashMapInner<K> =
    EvictableHashMap<K, HashValueType, PrecomputedBuildHasher, SharedStatsAlloc<Global>>;

// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/95174) get completed.
pub type JoinCachePolicyPrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinCachePolicy {
    use risingwave_pb::stream_plan::hash_join_node::CachePolicy as CachePolicyProst;

    use super::JoinCachePolicyPrimitive;

    /// Populate the cache when read a join key
    pub const OnRead: JoinCachePolicyPrimitive = 0;
    /// Populate the cache when read or write a join key
    pub const OnReadWrite: JoinCachePolicyPrimitive = 1;

    pub fn from_prost(policy_prost: &CachePolicyProst) -> JoinCachePolicyPrimitive {
        match policy_prost {
            CachePolicyProst::Onread => OnRead,
            CachePolicyProst::Onreadwrite => OnReadWrite,
        }
    }
}

pub struct JoinHashMapMetrics {
    /// Metrics used by join executor
    metrics: Arc<StreamingMetrics>,
    /// Basic information
    actor_id: String,
    side: &'static str,
    /// How many times have we hit the cache of join executor
    lookup_miss_count: usize,
    total_lookup_count: usize,
}

impl JoinHashMapMetrics {
    pub fn new(metrics: Arc<StreamingMetrics>, actor_id: u64, side: &'static str) -> Self {
        Self {
            metrics,
            actor_id: actor_id.to_string(),
            side,
            lookup_miss_count: 0,
            total_lookup_count: 0,
        }
    }

    pub fn flush(&mut self) {
        self.metrics
            .join_lookup_miss_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.lookup_miss_count as u64);
        self.metrics
            .join_total_lookup_count
            .with_label_values(&[&self.actor_id, self.side])
            .inc_by(self.total_lookup_count as u64);
        self.total_lookup_count = 0;
        self.lookup_miss_count = 0;
    }
}

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Allocator
    #[expect(dead_code)]
    alloc: SharedStatsAlloc<Global>,
    /// Store the join states.
    // SAFETY: This is a self-referential data structure and the allocator is owned by the struct
    // itself. Use the field is safe iff the struct is constructed with [`moveit`](https://crates.io/crates/moveit)'s way.
    inner: JoinHashMapInner<K>,
    /// Data types of the columns
    join_key_data_types: Vec<DataType>,
    /// Indices of the primary keys
    pk_indices: Vec<usize>,
    /// Current epoch
    current_epoch: u64,
    /// State table
    state_table: StateTable<S>,
    /// Cache policy
    /// TODO(yuhao): make it const generic if is bottleneck
    cache_policy: JoinCachePolicyPrimitive,
    /// Metrics of the hash map
    metrics: JoinHashMapMetrics,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target_cap: usize,
        pk_indices: Vec<usize>,
        join_key_indices: Vec<usize>,
        mut data_types: Vec<DataType>,
        state_table: StateTable<S>,
        metrics: Arc<StreamingMetrics>,
        actor_id: u64,
        side: &'static str,
        cache_policy: JoinCachePolicyPrimitive,
    ) -> Self {
        let join_key_data_types = join_key_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();

        // Put the degree to the last column of the table.
        data_types.push(DataType::Int64);

        let alloc = StatsAlloc::new(Global).shared();
        Self {
            inner: EvictableHashMap::with_hasher_in(
                target_cap,
                PrecomputedBuildHasher,
                alloc.clone(),
            ),
            join_key_data_types,
            pk_indices,
            current_epoch: 0,
            state_table,
            alloc,
            cache_policy,
            metrics: JoinHashMapMetrics::new(metrics, actor_id, side),
        }
    }

    #[expect(dead_code)]
    /// Report the bytes used by the join map.
    // FIXME: Currently, only memory used in the hash map itself is counted.
    pub fn bytes_in_use(&self) -> usize {
        self.alloc.bytes_in_use()
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    #[expect(dead_code)]
    #[cfg(any())]
    pub async fn get<'a>(&'a mut self, key: &K) -> Option<&'a HashValueType> {
        // TODO: add metrics for get
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => self.inner.get(key),
            None => {
                let remote_state = self.fetch_cached_state(key).await.unwrap();
                self.inner.put(key.clone(), remote_state);
                self.inner.get(key)
            }
        }
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    #[expect(dead_code)]
    #[cfg(any())]
    pub async fn get_mut<'a>(&'a mut self, key: &'a K) -> Option<&'a mut HashValueType> {
        // TODO: add metrics for get_mut
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => self.inner.get_mut(key),
            None => {
                let remote_state = self.fetch_cached_state(key).await.unwrap();
                self.inner.put(key.clone(), remote_state);
                self.inner.get_mut(key)
            }
        }
    }

    /// Remove the key in the memory, returning the value at the key if the
    /// key was previously in the map. If does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    /// WARNING: This will NOT remove anything from remote storage.
    pub async fn remove_state<'a>(
        &mut self,
        key: &K,
    ) -> StreamExecutorResult<Option<HashValueType>> {
        let state = self.inner.pop(key);
        self.metrics.total_lookup_count += 1;
        Ok(match state {
            Some(_) => state,
            None => {
                self.metrics.lookup_miss_count += 1;
                Some(self.fetch_cached_state(key).await?)
            }
        })
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    /// Will return a empty `JoinEntryState` even when state does not exist in remote.
    async fn fetch_cached_state(&self, key: &K) -> StreamExecutorResult<JoinEntryState> {
        let key = key.clone().deserialize(self.join_key_data_types.iter())?;

        let table_iter = self
            .state_table
            .iter_with_pk_prefix(&key, self.current_epoch)
            .await?;
        pin_mut!(table_iter);

        let mut cached = BTreeMap::new();
        #[for_await]
        for row in table_iter {
            let row = row?.into_owned();
            let pk = row.by_indices(&self.pk_indices);

            cached.insert(pk, JoinRow::from_row(row));
        }
        Ok(JoinEntryState::with_cached(cached))
    }

    pub async fn flush(&mut self) -> StreamExecutorResult<()> {
        self.metrics.flush();
        self.state_table.commit(self.current_epoch).await?;
        Ok(())
    }

    /// Insert a key
    pub async fn insert(
        &mut self,
        join_key: &K,
        pk: Row,
        value: JoinRow,
    ) -> StreamExecutorResult<()> {
        if let Some(entry) = self.inner.get_mut(join_key) {
            entry.insert(pk, value.clone());
        } else {
            match self.cache_policy {
                JoinCachePolicy::OnRead => {
                    // do nothing
                }
                JoinCachePolicy::OnReadWrite => {
                    // populate the cache
                    let mut state = self.fetch_cached_state(join_key).await?;
                    state.insert(pk, value.clone());
                    self.inner.put(join_key.clone(), state);
                }
                _ => bail!("Hash join cache policy not implemented"),
            };
        }

        // If no cache maintained, only update the flush buffer.
        self.state_table.insert(value.into_row())?;
        Ok(())
    }

    /// Delete a key
    pub fn delete(&mut self, join_key: &K, pk: Row, value: JoinRow) -> StreamExecutorResult<()> {
        if let Some(entry) = self.inner.get_mut(join_key) {
            entry.remove(pk);
        }

        // If no cache maintained, only update the flush buffer.
        self.state_table.delete(value.into_row())?;
        Ok(())
    }

    /// Insert a [`JoinEntryState`]
    pub fn insert_state(&mut self, key: &K, state: JoinEntryState) {
        self.inner.put(key.clone(), state);
    }

    pub fn inc_degree(&mut self, join_row: &mut JoinRow) -> StreamExecutorResult<()> {
        let old_row = join_row.clone().into_row();
        join_row.inc_degree();
        let new_row = join_row.clone().into_row();

        self.state_table.update(old_row, new_row)?;
        Ok(())
    }

    pub fn dec_degree(&mut self, join_row: &mut JoinRow) -> StreamExecutorResult<()> {
        let old_row = join_row.clone().into_row();
        join_row.dec_degree()?;
        let new_row = join_row.clone().into_row();

        self.state_table.update(old_row, new_row)?;
        Ok(())
    }
}

impl<K: HashKey, S: StateStore> Deref for JoinHashMap<K, S> {
    type Target = JoinHashMapInner<K>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K: HashKey, S: StateStore> DerefMut for JoinHashMap<K, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
