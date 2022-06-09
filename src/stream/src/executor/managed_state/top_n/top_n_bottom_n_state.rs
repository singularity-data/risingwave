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

#![allow(clippy::mutable_key_type)]
#![allow(dead_code)]

use std::vec::Drain;

use futures::pin_mut;
use futures::stream::StreamExt;
use madsim::collections::BTreeMap;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::*;
use risingwave_storage::cell_based_row_deserializer::CellBasedRowDeserializer;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::{Keyspace, StateStore};

use super::super::flush_status::BtreeMapFlushStatus as FlushStatus;
use super::variants::TOP_N_MIN;
use super::PkAndRowIterator;
use crate::executor::managed_state::top_n::deserialize_pk;
use crate::executor::PkIndices;

/// This state is used for `[offset, offset+limit)` part in the `TopNExecutor`.
///
/// Since the elements in this range may be moved to `[0, offset)` or `[offset+limit, +inf)`,
/// we would like to cache the two ends of the range. Since this would call for a `reverse iterator`
/// from `Hummock`, we temporarily adopt a all-or-nothing cache policy instead of a top-n and a
/// bottom-n policy.
pub struct ManagedTopNBottomNState<S: StateStore> {
    /// Top-N Cache.
    top_n: BTreeMap<OrderedRow, Row>,
    /// Bottom-N Cache. We always try to first fill into the bottom-n cache.
    bottom_n: BTreeMap<OrderedRow, Row>,
    /// Relational table.
    state_table: StateTable<S>,
    /// The number of elements in both cache and storage.
    total_count: usize,
    /// Number of entries to retain in top-n cache after each flush.
    top_n_count: Option<usize>,
    /// Number of entries to retain in bottom-n cache after each flush.
    bottom_n_count: Option<usize>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// `DataType`s use for deserializing `Row`.
    data_types: Vec<DataType>,
    /// For deserializing `OrderedRow`.
    ordered_row_deserializer: OrderedRowDeserializer,
    /// For deserializing `Row`.
    cell_based_row_deserializer: CellBasedRowDeserializer,
}

impl<S: StateStore> ManagedTopNBottomNState<S> {
    pub fn new(
        cache_size: Option<usize>,
        total_count: usize,
        keyspace: Keyspace<S>,
        data_types: Vec<DataType>,
        ordered_row_deserializer: OrderedRowDeserializer,
        cell_based_row_deserializer: CellBasedRowDeserializer,
        pk_indices: PkIndices,
    ) -> Self {
        let order_type = ordered_row_deserializer.clone().order_types;
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let state_table =
            StateTable::new(keyspace.clone(), column_descs, order_type, None, pk_indices);
        Self {
            top_n: BTreeMap::new(),
            bottom_n: BTreeMap::new(),
            state_table,
            total_count,
            top_n_count: cache_size,
            bottom_n_count: cache_size,
            keyspace,
            data_types,
            ordered_row_deserializer,
            cell_based_row_deserializer,
        }
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }

    pub fn is_dirty(&self) -> bool {
        !self.state_table.mem_table.buffer.is_empty()
    }

    // May have weird cache policy in the future, reserve an `n`.
    pub fn retain_top_n(&mut self, n: usize) {
        while self.top_n.len() > n {
            self.top_n.pop_first();
        }
    }

    // May have weird cache policy in the future, reserve an `n`.
    pub fn retain_bottom_n(&mut self, n: usize) {
        while self.bottom_n.len() > n {
            self.bottom_n.pop_last();
        }
    }

    pub fn retain_both_n(&mut self) {
        if let Some(n) = self.top_n_count {
            self.retain_top_n(n);
        }
        if let Some(n) = self.bottom_n_count {
            self.retain_bottom_n(n);
        }
    }

    pub async fn pop_top_element(&mut self, epoch: u64) -> Result<Option<(OrderedRow, Row)>> {
        if self.total_count == 0 {
            Ok(None)
        } else {
            let cache_to_pop = if self.top_n.is_empty() {
                &self.bottom_n
            } else {
                &self.top_n
            };
            let key = cache_to_pop.last_key_value().unwrap().0.clone();
            let old_value = cache_to_pop.last_key_value().unwrap().1.clone();
            let value = self.delete(&key, old_value, epoch).await?;
            Ok(Some((key, value.unwrap())))
        }
    }

    pub async fn pop_bottom_element(&mut self, epoch: u64) -> Result<Option<(OrderedRow, Row)>> {
        if self.total_count == 0 {
            Ok(None)
        } else {
            let cache_to_pop = if self.bottom_n.is_empty() {
                &self.top_n
            } else {
                &self.bottom_n
            };
            let key = cache_to_pop.first_key_value().unwrap().0.clone();
            let old_value = cache_to_pop.last_key_value().unwrap().1.clone();
            let value = self.delete(&key, old_value, epoch).await?;
            Ok(Some((key, value.unwrap())))
        }
    }

    pub fn top_element(&mut self) -> Option<(&OrderedRow, &Row)> {
        if self.total_count == 0 {
            None
        } else if self.top_n.is_empty() {
            self.bottom_n.last_key_value()
        } else {
            self.top_n.last_key_value()
        }
    }

    pub fn bottom_element(&mut self) -> Option<(&OrderedRow, &Row)> {
        if self.total_count == 0 {
            None
        } else if self.bottom_n.is_empty() {
            self.top_n.first_key_value()
        } else {
            self.bottom_n.first_key_value()
        }
    }

    pub async fn insert(&mut self, key: OrderedRow, value: Row) {
        // We can have different strategy of which cache we should insert the element into.
        // Right now, we keep it simple and insert the element into the cache with smaller size,
        // without violating the constraint that these two caches' current range must NOT overlap.
        let top_n_size = self.top_n.len();
        let bottom_n_size = self.bottom_n.len();
        let insert_to_cache = if top_n_size > bottom_n_size {
            // top_n_size must > 0, directly `unwrap`
            if self.top_n.first_key_value().unwrap().0 < &key {
                &mut self.top_n
            } else {
                &mut self.bottom_n
            }
        } else if self.bottom_n.is_empty() || self.bottom_n.last_key_value().unwrap().0 <= &key {
            &mut self.top_n
        } else {
            &mut self.bottom_n
        };
        insert_to_cache.insert(key.clone(), value.clone());
        self.state_table
            .insert::<false>(&key.clone().into_row(), value.clone())
            .unwrap();
        self.total_count += 1;
    }

    pub async fn delete(
        &mut self,
        key: &OrderedRow,
        value: Row,
        epoch: u64,
    ) -> Result<Option<Row>> {
        let prev_top_n_entry = self.top_n.remove(key);
        let prev_bottom_n_entry = self.bottom_n.remove(key);
        self.state_table
            .delete::<false>(&key.clone().into_row(), value)?;
        self.total_count -= 1;
        // If we have nothing in both caches, we have to scan from the storage.
        if self.top_n.is_empty() && self.bottom_n.is_empty() && self.total_count > 0 {
            self.scan_and_merge(epoch).await?;
        }
        let value = match (prev_top_n_entry, prev_bottom_n_entry) {
            (None, None) => None,
            (Some(row), None) | (None, Some(row)) => Some(row),
            (Some(_), Some(_)) => unreachable!(),
        };
        Ok(value)
    }

    /// The same as the one in `ManagedTopNState`.
    pub async fn scan_and_merge(&mut self, epoch: u64) -> Result<()> {
        let mut kv_pairs = vec![];
        let state_table_iter = self.state_table.iter(epoch).await?;
        pin_mut!(state_table_iter);
        loop {
            match state_table_iter.next().await {
                Some(next_res) => {
                    let row = next_res.unwrap().into_owned();
                    let mut pk_bytes = vec![];
                    for pk_indice in &self.state_table.pk_indices {
                        pk_bytes.append(&mut row.serialize_datum(*pk_indice).unwrap());
                    }
                    let pk = deserialize_pk::<TOP_N_MIN>(
                        &mut pk_bytes.clone(),
                        &mut self.ordered_row_deserializer,
                    )?;
                    kv_pairs.push((pk, row));
                }
                None => {
                    break;
                }
            }
        }

        // The reason we can split the `kv_pairs` without caring whether the key to be inserted is
        // already in the top_n or bottom_n is that we would only trigger `scan_and_merge` when both
        // caches are empty.

        {
            let part1 = kv_pairs.drain(0..kv_pairs.len() / 2);
            for (key, value) in part1 {
                self.bottom_n.insert(key, value);
            }
        }
        {
            let part2 = kv_pairs.drain(..);
            for (key, value) in part2 {
                self.top_n.insert(key, value);
            }
        }
        Ok(())
    }

    /// We can fill in the cache from storage only when state is not dirty, i.e. right after
    /// `flush`.
    pub async fn fill_in_cache(&mut self, epoch: u64) -> Result<()> {
        debug_assert!(!self.is_dirty());
        let mut pk_and_row_iter = PkAndRowIterator::<_, TOP_N_MIN>::new(
            self.keyspace.iter(epoch).await?,
            &mut self.ordered_row_deserializer,
            &mut self.cell_based_row_deserializer,
        );
        while let Some((pk, row)) = pk_and_row_iter.next().await? {
            self.bottom_n.insert(pk, row);
        }
        // We don't retain `n` elements as we have a all-or-nothing policy for now.
        Ok(())
    }

    /// `Flush` can be called by the executor when it receives a barrier and thus needs to
    /// checkpoint.
    pub async fn flush(&mut self, epoch: u64) -> Result<()> {
        if !self.is_dirty() {
            // We don't retain `n` elements as we have a all-or-nothing policy for now.
            return Ok(());
        }

        self.state_table.commit(epoch).await?;

        // We don't retain `n` elements as we have a all-or-nothing policy for now.
        Ok(())
    }

    pub fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while top n bottom n state is dirty"
        );

        self.top_n.clear();
        self.bottom_n.clear();
    }
}

/// Test-related methods
impl<S: StateStore> ManagedTopNBottomNState<S> {
    #[cfg(test)]
    fn get_cache_len(&self) -> usize {
        self.top_n.len() + self.bottom_n.len()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{ColumnDesc, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::{Keyspace, StateStore};

    use super::*;
    use crate::row_nonnull;

    fn create_managed_top_n_bottom_n_state<S: StateStore>(
        store: &S,
        row_count: usize,
        data_types: Vec<DataType>,
        order_types: Vec<OrderType>,
    ) -> ManagedTopNBottomNState<S> {
        let ordered_row_deserializer = OrderedRowDeserializer::new(data_types.clone(), order_types);
        let table_column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| {
                ColumnDesc::unnamed(ColumnId::from(id as i32), data_type.clone())
            })
            .collect::<Vec<_>>();
        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_column_descs);

        ManagedTopNBottomNState::new(
            Some(1),
            row_count,
            Keyspace::table_root(store.clone(), &TableId::from(0x2333)),
            data_types,
            ordered_row_deserializer,
            cell_based_row_deserializer,
            vec![0_usize, 1_usize],
        )
    }

    #[tokio::test]
    async fn test_managed_top_n_bottom_n_state() {
        let data_types = vec![DataType::Varchar, DataType::Int64];
        let order_types = vec![OrderType::Descending, OrderType::Ascending];
        let store = MemoryStateStore::new();
        let mut managed_state =
            create_managed_top_n_bottom_n_state(&store, 0, data_types.clone(), order_types.clone());
        let row1 = row_nonnull!["abc".to_string(), 2i64];
        let row2 = row_nonnull!["abc".to_string(), 3i64];
        let row3 = row_nonnull!["abd".to_string(), 3i64];
        let row4 = row_nonnull!["ab".to_string(), 4i64];
        let rows = vec![row1, row2, row3, row4];
        let ordered_rows = rows
            .clone()
            .into_iter()
            .map(|row| OrderedRow::new(row, &order_types))
            .collect::<Vec<_>>();

        managed_state
            .insert(ordered_rows[3].clone(), rows[3].clone())
            .await;
        // now ("ab", 4)

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 1);

        managed_state
            .insert(ordered_rows[2].clone(), rows[2].clone())
            .await;
        // now ("abd", 3) -> ("ab", 4)

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 2);

        managed_state
            .insert(ordered_rows[1].clone(), rows[1].clone())
            .await;
        // now ("abd", 3) -> ("abc", 3) -> ("ab", 4)
        let epoch: u64 = 0;

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        assert_eq!(managed_state.get_cache_len(), 3);
        managed_state.flush(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());
        let row_count = managed_state.total_count;
        assert_eq!(row_count, 3);
        // After flush, all elements should be kept in the cache.
        assert_eq!(managed_state.get_cache_len(), 3);

        drop(managed_state);
        let mut managed_state = create_managed_top_n_bottom_n_state(
            &store,
            row_count,
            data_types.clone(),
            order_types.clone(),
        );
        assert_eq!(managed_state.top_element(), None);
        managed_state.fill_in_cache(epoch).await.unwrap();
        // now ("abd", 3) -> ("abc", 3) -> ("ab", 4)
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[3], &rows[3]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        // Right after recovery.
        assert!(!managed_state.is_dirty());
        assert_eq!(managed_state.get_cache_len(), 3);

        assert_eq!(
            managed_state.pop_top_element(epoch).await.unwrap(),
            Some((ordered_rows[3].clone(), rows[3].clone()))
        );
        // now ("abd", 3) -> ("abc", 3)
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[1], &rows[1]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 2);
        assert_eq!(managed_state.get_cache_len(), 2);
        assert_eq!(
            managed_state.pop_top_element(epoch).await.unwrap(),
            Some((ordered_rows[1].clone(), rows[1].clone()))
        );
        // now ("abd", 3)
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.total_count, 1);
        assert_eq!(managed_state.get_cache_len(), 1);

        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        managed_state.flush(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());

        managed_state
            .insert(ordered_rows[0].clone(), rows[0].clone())
            .await;
        // now ("abd", 3) -> ("abc", 2)
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[0], &rows[0]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );

        // Exclude the last `insert` as the state crashes before recovery.
        let row_count = managed_state.total_count - 1;
        drop(managed_state);
        let mut managed_state = create_managed_top_n_bottom_n_state(
            &store,
            row_count,
            data_types.clone(),
            order_types.clone(),
        );
        managed_state.fill_in_cache(epoch).await.unwrap();
        assert_eq!(
            managed_state.top_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
        assert_eq!(
            managed_state.bottom_element(),
            Some((&ordered_rows[2], &rows[2]))
        );
    }
}
