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
use std::ops::{Deref, DerefMut, Index};
use std::sync::Arc;

use bytes::Buf;
use itertools::Itertools;
pub use join_entry_state::JoinEntryState;
use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::{ErrorCode, Result as RwResult};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::types::{DataType, Datum};
use risingwave_storage::{Keyspace, StateStore};

/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow {
    pub row: Row,
    degree: u64,
}

impl Index<usize> for JoinRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row[index]
    }
}

impl JoinRow {
    pub fn new(row: Row, degree: u64) -> Self {
        Self { row, degree }
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.row.size()
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    pub fn inc_degree(&mut self) -> u64 {
        self.degree += 1;
        self.degree
    }

    pub fn dec_degree(&mut self) -> RwResult<u64> {
        if self.degree == 0 {
            return Err(
                ErrorCode::InternalError("Tried to decrement zero join row degree".into()).into(),
            );
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

    /// Serialize the `JoinRow` into a value encoding bytes.
    pub fn serialize(&self) -> RwResult<Vec<u8>> {
        let mut vec = Vec::with_capacity(10);

        // Serialize row.
        vec.extend(self.row.value_encode()?);

        // Serialize degree.
        vec.extend(self.degree.to_le_bytes());

        Ok(vec)
    }
}

/// Deserializer of the `JoinRow`.
pub struct JoinRowDeserializer {
    data_types: Vec<DataType>,
}

impl JoinRowDeserializer {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(schema: Vec<DataType>) -> Self {
        JoinRowDeserializer { data_types: schema }
    }

    /// Deserialize the [`JoinRow`] from a value encoding bytes.
    pub fn deserialize(&self, mut data: impl Buf) -> RwResult<JoinRow> {
        let deserializer = RowDeserializer::new(self.data_types.clone());
        let row = deserializer.value_decode(&mut data)?;
        let degree = data.get_u64_le();
        Ok(JoinRow { row, degree })
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;

#[derive(Clone)]
pub(crate) struct TableInfo<S: StateStore> {
    /// Data types of the columns
    data_types: Arc<[DataType]>,
    /// Data types of the columns
    join_key_data_types: Arc<[DataType]>,
    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// Current epoch
    current_epoch: u64,
}

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Store the join states.
    inner: EvictableHashMap<K, JoinEntryState<S>, PrecomputedBuildHasher>,
    /// Info about the table data stored
    table_info: TableInfo<S>,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    pub fn new(
        target_cap: usize,
        pk_indices: Vec<usize>,
        join_key_indices: Vec<usize>,
        data_types: Vec<DataType>,
        keyspace: Keyspace<S>,
    ) -> Self {
        let pk_data_types = pk_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();
        let join_key_data_types = join_key_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();

        Self {
            inner: EvictableHashMap::with_hasher(target_cap, PrecomputedBuildHasher),
            table_info: TableInfo {
                data_types: data_types.into(),
                join_key_data_types: join_key_data_types.into(),
                pk_data_types: pk_data_types.into(),
                keyspace,
                current_epoch: 0,
            },
        }
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.table_info.current_epoch = epoch;
    }

    fn get_state_keyspace(key: &K, table_info: &TableInfo<S>) -> RwResult<Keyspace<S>> {
        // TODO: in pure in-memory engine, we should not do this serialization.
        let key = key
            .clone()
            .deserialize(table_info.join_key_data_types.iter())?;
        let key_encoded = key.serialize().unwrap();
        Ok(table_info.keyspace.append(key_encoded))
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    async fn fetch_cached_state(
        key: &K,
        table_info: &TableInfo<S>,
    ) -> RwResult<Option<JoinEntryState<S>>> {
        let keyspace = Self::get_state_keyspace(key, table_info)?;
        JoinEntryState::with_cached_state(
            keyspace,
            table_info.data_types.clone(),
            table_info.pk_data_types.clone(),
            table_info.current_epoch,
        )
        .await
    }

    /// Create a [`JoinEntryState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    #[allow(unused)]
    fn init_without_cache(key: &K, table_info: &TableInfo<S>) -> RwResult<JoinEntryState<S>> {
        let keyspace = Self::get_state_keyspace(key, table_info)?;
        let state = JoinEntryState::new(
            keyspace,
            table_info.data_types.clone(),
            table_info.pk_data_types.clone(),
        );
        Ok(state)
    }

    /// Create a [`JoinEntryState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    fn init_with_empty_cache(key: &K, table_info: &TableInfo<S>) -> RwResult<JoinEntryState<S>> {
        let keyspace = Self::get_state_keyspace(key, table_info)?;
        let state = JoinEntryState::new_with_empty_cache(
            keyspace,
            table_info.data_types.clone(),
            table_info.pk_data_types.clone(),
        );
        Ok(state)
    }

    pub fn pop_cached(&mut self, key: &K) -> Option<JoinEntryState<S>> {
        self.inner.pop(key)
    }

    pub fn push(&mut self, key: K, state: JoinEntryState<S>) {
        self.inner.push(key, state);
    }

    // Iter keys on the
    pub(crate) fn iter_keys<'a>(
        &'a mut self,
        side_match: &'a mut Self,
        keys: impl Iterator<Item = &'a K>,
    ) -> IterJoinEntriesMut<'a, K, S> {
        IterJoinEntriesMut::<'a>::new(self.table_info.current_epoch, side_match, self, keys)
    }
}

type IterType<K, S> = (K, Option<JoinEntryState<S>>, JoinEntryState<S>);

/// Iterates
pub(crate) struct IterJoinEntriesMut<'a, K: HashKey, S: StateStore> {
    side_match: &'a mut JoinHashMap<K, S>,
    side_update: &'a mut JoinHashMap<K, S>,
    last_item: Option<IterType<K, S>>,
    queue: tokio::sync::mpsc::UnboundedReceiver<IterType<K, S>>, /* TODO: change this to a
                                                                  * reasonably
                                                                  * sized bounded queue... */
}

impl<'a, K: HashKey, S: StateStore> IterJoinEntriesMut<'a, K, S> {
    pub(crate) fn new(
        epoch: u64,
        side_match: &'a mut JoinHashMap<K, S>,
        side_update: &'a mut JoinHashMap<K, S>,
        keys: impl Iterator<Item = &'a K>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<IterType<K, S>>();
        let new_iter = Self {
            side_match,
            side_update,
            last_item: None,
            queue: rx,
        };

        let start = std::time::Instant::now();
        let mut cache_miss_counter = 0;
        let mut total_counter = 0;

        for key in keys {
            total_counter += 1;
            let update_entry = new_iter.side_update.pop_cached(key);
            let match_entry = if key.has_null() {
                None
            } else {
                new_iter.side_match.pop_cached(key)
            };
            let key = key.clone();
            let update_table_info = &new_iter.side_update.table_info;

            let update_entry = update_entry.unwrap_or_else(|| {
                JoinHashMap::<K, S>::init_with_empty_cache(&key, update_table_info)
                    .ok()
                    .unwrap()
            });

            if key.has_null() {
                tx.send((key, None, update_entry)).ok().unwrap()
            } else if let Some(mut maybe_cached) = match_entry {
                if maybe_cached.has_cached() {
                    tx.send((key, Some(maybe_cached), update_entry))
                        .ok()
                        .unwrap()
                } else {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        maybe_cached.populate_cache(epoch).await.ok().unwrap();
                        tx.send((key, Some(maybe_cached), update_entry))
                            .ok()
                            .unwrap()
                    });
                }
            } else {
                cache_miss_counter += 1;
                // tx.send((key, None, update_entry))
                //     .ok()
                //     .expect("Failed to send join entries for join key");
                let match_table_info = new_iter.side_match.table_info.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let match_entry =
                        JoinHashMap::<K, S>::fetch_cached_state(&key, &match_table_info)
                            .await
                            .ok()
                            .unwrap();
                    tx.send((key, match_entry, update_entry)).ok().unwrap()
                });
            }
        }

        println!(
            "Elapsed: {}us. Cache miss/total: {}/{}",
            start.elapsed().as_micros(),
            cache_miss_counter,
            total_counter
        );

        new_iter
    }

    pub(crate) async fn next(&mut self) -> Option<&mut IterType<K, S>> {
        if let Some(last) = self.last_item.take() {
            if let Some(matches) = last.1 {
                self.side_match.push(last.0.clone(), matches);
            }
            self.side_update.push(last.0, last.2);
        }
        self.last_item = self.queue.recv().await;
        self.last_item.as_mut()
    }
}

impl<'a, K: HashKey, S: StateStore> Drop for IterJoinEntriesMut<'a, K, S> {
    fn drop(&mut self) {
        if let Some(last) = self.last_item.take() {
            if let Some(matches) = last.1 {
                self.side_match.push(last.0.clone(), matches);
            }
            self.side_update.push(last.0, last.2);
        }
    }
}

impl<K: HashKey, S: StateStore> Deref for JoinHashMap<K, S> {
    type Target = EvictableHashMap<K, JoinEntryState<S>, PrecomputedBuildHasher>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K: HashKey, S: StateStore> DerefMut for JoinHashMap<K, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
