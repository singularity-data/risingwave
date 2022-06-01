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

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::error::RwError;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::next_key;

use super::mem_table::RowOp;
use super::TableIter;
use crate::cell_based_row_deserializer::CellBasedRowDeserializer;
use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::monitor::StateStoreMetrics;
use crate::storage_value::{StorageValue, ValueMeta};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
#[derive(Clone)]
pub struct CellBasedTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// The schema of this table viewed by some source executor, e.g. RowSeqScanExecutor.
    schema: Schema,

    /// `ColumnDesc` contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Mapping from column id to column index
    pk_serializer: Option<OrderedRowSerializer>,

    cell_based_row_serializer: CellBasedRowSerializer,

    column_ids: Vec<ColumnId>,

    /// Statistics.
    stats: Arc<StateStoreMetrics>,

    /// Indices of distribution keys in pk for computing value meta. None if value meta is not
    /// required.
    dist_key_indices: Option<Vec<usize>>,
}

impl<S: StateStore> std::fmt::Debug for CellBasedTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellBasedTable")
            .field("column_descs", &self.column_descs)
            .finish()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::CellBasedTable(rw.into())
}

impl<S: StateStore> CellBasedTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        ordered_row_serializer: Option<OrderedRowSerializer>,
        stats: Arc<StateStoreMetrics>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let schema = Schema::new(
            column_descs
                .iter()
                .map(|cd| Field::with_name(cd.data_type.clone(), cd.name.clone()))
                .collect_vec(),
        );
        let column_ids = generate_column_id(&column_descs);

        Self {
            keyspace,
            schema,
            column_descs,

            pk_serializer: ordered_row_serializer,
            cell_based_row_serializer: CellBasedRowSerializer::new(),
            column_ids,
            stats,
            dist_key_indices,
        }
    }

    pub fn new_for_test(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        Self::new(
            keyspace,
            column_descs,
            Some(OrderedRowSerializer::new(order_types)),
            Arc::new(StateStoreMetrics::unused()),
            None,
        )
    }

    /// Creates an "adhoc" [`CellBasedTable`] with specified columns.
    pub fn new_adhoc(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        stats: Arc<StateStoreMetrics>,
    ) -> Self {
        Self::new(keyspace, column_descs, None, stats, None)
    }

    // cell-based interface
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // get row by state_store get
        // TODO: use multi-get for cell_based get_row
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
        let serialized_pk = &serialize_pk(pk, pk_serializer).map_err(err)?[..];
        let sentinel_key = [
            serialized_pk,
            &serialize_column_id(&SENTINEL_CELL_ID).map_err(err)?,
        ]
        .concat();
        let mut get_res = Vec::new();
        let sentinel_cell = self.keyspace.get(&sentinel_key, epoch).await?;

        if sentinel_cell.is_none() {
            // if sentinel cell is none, this row doesn't exist
            return Ok(None);
        } else {
            get_res.push((sentinel_key, sentinel_cell.unwrap()));
        }
        for column_id in &self.column_ids {
            let key = [serialized_pk, &serialize_column_id(column_id).map_err(err)?].concat();
            let state_store_get_res = self.keyspace.get(&key, epoch).await?;
            if let Some(state_store_get_res) = state_store_get_res {
                get_res.push((key, state_store_get_res));
            }
        }
        let mut cell_based_row_deserializer =
            CellBasedRowDeserializer::new(self.column_descs.clone());
        for (key, value) in get_res {
            let deserialize_res = cell_based_row_deserializer
                .deserialize(&Bytes::from(key), &value)
                .map_err(err)?;
            assert!(deserialize_res.is_none());
        }
        let pk_and_row = cell_based_row_deserializer.take();
        Ok(pk_and_row.map(|(_pk, row)| row))
    }

    pub async fn get_row_by_scan(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // get row by state_store scan
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
        let start_key = self
            .keyspace
            .prefixed_key(&serialize_pk(pk, pk_serializer).map_err(err)?);
        let end_key = next_key(&start_key);

        let state_store_range_scan_res = self
            .keyspace
            .state_store()
            .scan(start_key..end_key, None, epoch)
            .await?;
        let mut cell_based_row_deserializer =
            CellBasedRowDeserializer::new(self.column_descs.clone());
        for (key, value) in state_store_range_scan_res {
            cell_based_row_deserializer
                .deserialize(&key, &value)
                .map_err(err)?;
        }
        let pk_and_row = cell_based_row_deserializer.take();
        match pk_and_row {
            Some(_) => Ok(pk_and_row.map(|(_pk, row)| row)),
            None => Ok(None),
        }
    }

    async fn batch_write_rows_inner<const WITH_VALUE_META: bool>(
        &mut self,
        buffer: BTreeMap<Row, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        // stateful executors need to compute vnode.
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let ordered_row_serializer = self.pk_serializer.as_ref().unwrap();
        let hash_builder = CRC32FastBuilder {};
        for (pk, row_op) in buffer {
            let arrange_key_buf = serialize_pk(&pk, ordered_row_serializer).map_err(err)?;

            let value_meta = if WITH_VALUE_META {
                // If value meta is computed here, then the cell based table is guaranteed to have
                // distribution keys. Also, it is guaranteed that distribution key indices will
                // not exceed the length of pk. So we simply do unwrap here.
                let vnode = pk
                    .hash_by_indices(self.dist_key_indices.as_ref().unwrap(), &hash_builder)
                    .unwrap()
                    .to_vnode();
                ValueMeta::with_vnode(vnode)
            } else {
                ValueMeta::default()
            };

            match row_op {
                RowOp::Insert(row) => {
                    let bytes = self
                        .cell_based_row_serializer
                        .serialize(&arrange_key_buf, row, &self.column_ids)
                        .map_err(err)?;
                    for (key, value) in bytes {
                        local.put(key, StorageValue::new_put(value_meta, value))
                    }
                }
                RowOp::Delete(old_row) => {
                    // TODO(wcy-fdu): only serialize key on deletion
                    let bytes = self
                        .cell_based_row_serializer
                        .serialize(&arrange_key_buf, old_row, &self.column_ids)
                        .map_err(err)?;
                    for (key, _) in bytes {
                        local.delete_with_value_meta(key, value_meta);
                    }
                }
                RowOp::Update((old_row, new_row)) => {
                    let delete_bytes = self
                        .cell_based_row_serializer
                        .serialize_without_filter(&arrange_key_buf, old_row, &self.column_ids)
                        .map_err(err)?;
                    let insert_bytes = self
                        .cell_based_row_serializer
                        .serialize_without_filter(&arrange_key_buf, new_row, &self.column_ids)
                        .map_err(err)?;
                    for (delete, insert) in
                        delete_bytes.into_iter().zip_eq(insert_bytes.into_iter())
                    {
                        match (delete, insert) {
                            (Some((delete_pk, _)), None) => {
                                local.delete_with_value_meta(delete_pk, value_meta);
                            }
                            (None, Some((insert_pk, insert_row))) => {
                                local.put(insert_pk, StorageValue::new_put(value_meta, insert_row));
                            }
                            (None, None) => {}
                            (Some((delete_pk, _)), Some((insert_pk, insert_row))) => {
                                debug_assert_eq!(delete_pk, insert_pk);
                                local.put(insert_pk, StorageValue::new_put(value_meta, insert_row));
                            }
                        }
                    }
                }
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    pub async fn batch_write_rows_with_value_meta(
        &mut self,
        buffer: BTreeMap<Row, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<true>(buffer, epoch).await
    }

    pub async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Row, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<false>(buffer, epoch).await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> StorageResult<CellBasedTableRowIter<S>> {
        CellBasedTableRowIter::new(
            self.keyspace.clone(),
            self.column_descs.clone(),
            epoch,
            self.stats.clone(),
        )
        .await
    }

    // streaming_iter is uesed for streaming executors, which is regarded as a short-term iterator
    // and will not wait for epoch.
    pub async fn streaming_iter(
        &self,
        epoch: u64,
    ) -> StorageResult<CellBasedTableStreamingIter<S>> {
        CellBasedTableStreamingIter::new(&self.keyspace, self.column_descs.clone(), epoch).await
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn generate_column_id(column_descs: &[ColumnDesc]) -> Vec<ColumnId> {
    column_descs.iter().map(|d| d.column_id).collect()
}

// (st1page): Maybe we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct CellBasedTableRowIter<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,
    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer,
    /// Statistics
    _stats: Arc<StateStoreMetrics>,
}

impl<S: StateStore> CellBasedTableRowIter<S> {
    pub async fn new(
        keyspace: Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        epoch: u64,
        _stats: Arc<StateStoreMetrics>,
    ) -> StorageResult<Self> {
        keyspace.state_store().wait_epoch(epoch).await?;

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = keyspace.iter(epoch).await?;

        let iter = Self {
            iter,
            cell_based_row_deserializer,
            _stats,
        };
        Ok(iter)
    }

    pub async fn collect_data_chunk(
        &mut self,
        schema: &Schema,
        chunk_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        let mut builders = schema
            .create_array_builders(chunk_size.unwrap_or(0))
            .map_err(err)?;

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(row) => {
                    for (datum, builder) in row.0.into_iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&datum).map_err(err)?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(|a| Column::new(Arc::new(a))))
                .try_collect()
                .map_err(err)?;
            DataChunk::new(columns, row_count)
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableIter for CellBasedTableRowIter<S> {
    async fn next(&mut self) -> StorageResult<Option<Row>> {
        loop {
            match self.iter.next().await? {
                None => {
                    let pk_and_row = self.cell_based_row_deserializer.take();
                    return Ok(pk_and_row.map(|(_pk, row)| row));
                }
                Some((key, value)) => {
                    tracing::trace!(
                        target: "events::storage::CellBasedTable::scan",
                        "CellBasedTable scanned key = {:?}, value = {:?}",
                        key,
                        value
                    );
                    let pk_and_row = self
                        .cell_based_row_deserializer
                        .deserialize(&key, &value)
                        .map_err(err)?;
                    match pk_and_row {
                        Some(_) => return Ok(pk_and_row.map(|(_pk, row)| row)),
                        None => {}
                    }
                }
            }
        }
    }
}

/// [`CellBasedTableStreamingIter`] is used for streaming executor, and will not wait for epoch.
pub struct CellBasedTableStreamingIter<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,
    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer,
}

impl<S: StateStore> CellBasedTableStreamingIter<S> {
    pub async fn new(
        keyspace: &Keyspace<S>,
        table_descs: Vec<ColumnDesc>,
        epoch: u64,
    ) -> StorageResult<Self> {
        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);
        let iter = keyspace.iter(epoch).await?;
        let iter = Self {
            iter,
            cell_based_row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    pub async fn into_stream(mut self) {
        while let Some((key, value)) = self.iter.next().await? {
            tracing::trace!(
                target: "events::storage::CellBasedTable::scan",
                "CellBasedTable scanned key = {:?}, value = {:?}",
                key,
                value
            );

            if let Some(pk_and_row) = self
                .cell_based_row_deserializer
                .deserialize(&key, &value)
                .map_err(err)?
            {
                yield pk_and_row;
            }
        }

        if let Some(pk_and_row) = self.cell_based_row_deserializer.take() {
            yield pk_and_row;
        }
    }
}
