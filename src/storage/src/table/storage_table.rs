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
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use auto_enums::auto_enum;
use bytes::BufMut;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use log::trace;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{
    ColumnDesc, ColumnId, OrderedColumnDesc, Schema, TableId, TableOption,
};
use risingwave_common::error::RwError;
use risingwave_common::types::{Datum, VirtualNode};
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{end_bound_of_prefix, next_key, prefixed_range, range_of_prefix};
use risingwave_pb::catalog::Table;

use super::mem_table::RowOp;
use super::{Distribution, TableIter};
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::row_serde::cell_based_encoding_util::serialize_pk_and_column_id;
use crate::row_serde::{
    serialize_pk, CellBasedRowSerde, ColumnDescMapping, RowDeserialize, RowSerde, RowSerialize,
};
use crate::storage_value::StorageValue;
use crate::store::{ReadOptions, WriteOptions};
use crate::{Keyspace, StateStore, StateStoreIter};

mod iter_utils;

pub type AccessType = bool;
/// Table with `READ_ONLY` is used for batch scan or point lookup.
pub const READ_ONLY: AccessType = false;
/// Table with `READ_WRITE` is used for streaming executors through `StateTable`.
pub const READ_WRITE: AccessType = true;

/// For tables without distribution (singleton), the `DEFAULT_VNODE` is encoded.
pub const DEFAULT_VNODE: VirtualNode = 0;

/// [`StorageTable`] is the interface accessing relational data in KV(`StateStore`) with cell-based
/// encoding format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
pub type StorageTable<S, const T: AccessType> = StorageTableBase<S, CellBasedRowSerde, T>;
/// [`StorageTableBase`] is the interface accessing relational data in KV(`StateStore`) with
/// encoding format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation.
/// It is parameterized by its encoding, by specifying cell serializer and deserializers.
/// TODO: Parameterize on `CellDeserializer`.
#[derive(Clone)]
pub struct StorageTableBase<S: StateStore, RS: RowSerde, const T: AccessType> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// All columns of this table. Note that this is different from the output columns in
    /// `mapping.output_columns`.
    table_columns: Vec<ColumnDesc>,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    schema: Schema,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Used for serializing the row.
    row_serializer: RS::Serializer,

    /// Mapping from column id to column index. Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. For READ_WRITE instances, the table will also check whether the writed rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,

    /// If true, sanity check is disabled on this table.
    disable_sanity_check: bool,

    /// Used for catalog table_properties
    table_option: TableOption,

    // TODO: check and build bloom_filter_key by read_pattern_prefix_column
    _read_pattern_prefix_column: u32,
}

impl<S: StateStore, RS: RowSerde, const T: AccessType> std::fmt::Debug
    for StorageTableBase<S, RS, T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTable").finish_non_exhaustive()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StorageTable(rw.into())
}

impl<S: StateStore, RS: RowSerde> StorageTableBase<S, RS, READ_ONLY> {
    /// Create a read-only [`StorageTableBase`] given a complete set of `columns` and a partial
    /// set of `column_ids`. The output will only contains columns with the given ids in the same
    /// order.
    /// This is parameterized on cell based row serializer.
    pub fn new_partial(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: Distribution,
    ) -> Self {
        Self::new_inner(
            store,
            table_id,
            table_columns,
            column_ids,
            order_types,
            pk_indices,
            distribution,
            Default::default(),
            0,
        )
    }
}

impl<S: StateStore, RS: RowSerde> StorageTableBase<S, RS, READ_WRITE> {
    /// Create a read-write [`StorageTableBase`] given a complete set of `columns`.
    /// This is parameterized on cell based row serializer.
    pub fn new(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: Distribution,
    ) -> Self {
        let column_ids = columns.iter().map(|c| c.column_id).collect();

        Self::new_inner(
            store,
            table_id,
            columns,
            column_ids,
            order_types,
            pk_indices,
            distribution,
            Default::default(),
            0,
        )
    }

    pub fn new_for_test(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self::new(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
        )
    }
}

/// Allow transforming a `READ_WRITE` instance to a `READ_ONLY` one.
impl<S: StateStore, RS: RowSerde> From<StorageTableBase<S, RS, READ_WRITE>>
    for StorageTableBase<S, RS, READ_ONLY>
{
    fn from(rw: StorageTableBase<S, RS, READ_WRITE>) -> Self {
        Self { ..rw }
    }
}

impl<S: StateStore, RS: RowSerde, const T: AccessType> StorageTableBase<S, RS, T> {
    /// Create storage table from table catalog and store.
    pub fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let order_types = table_catalog
            .order_key
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();
        let dist_key_indices = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();
        let pk_indices = table_catalog
            .order_key
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect();
        let distribution = match vnodes {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes,
            },
            None => Distribution::fallback(),
        };
        Self::new_inner(
            store,
            TableId::new(table_catalog.id),
            table_columns.clone(),
            table_columns
                .iter()
                .map(|table_column| table_column.column_id)
                .collect(),
            order_types,
            pk_indices,
            distribution,
            TableOption::build_table_option(table_catalog.get_properties()),
            table_catalog.read_pattern_prefix_column,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        Distribution {
            dist_key_indices,
            vnodes,
        }: Distribution,
        table_option: TableOption,
        read_pattern_prefix_column: u32,
    ) -> Self {
        let row_serializer = RS::create_serializer(&pk_indices, &table_columns, &column_ids);

        assert_eq!(order_types.len(), pk_indices.len());
        let mapping = ColumnDescMapping::new_partial(&table_columns, &column_ids);
        let schema = Schema::new(mapping.output_columns.iter().map(Into::into).collect());
        let pk_serializer = OrderedRowSerializer::new(order_types);

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        let keyspace = Keyspace::table_root(store, &table_id);
        Self {
            keyspace,
            table_columns,
            schema,
            pk_serializer,
            row_serializer,
            mapping,
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            disable_sanity_check: false,
            table_option,
            _read_pattern_prefix_column: read_pattern_prefix_column,
        }
    }

    /// Disable sanity check on this storage table.
    pub fn disable_sanity_check(&mut self) {
        self.disable_sanity_check = true;
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(super) fn pk_serializer(&self) -> &OrderedRowSerializer {
        &self.pk_serializer
    }

    pub(super) fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub(super) fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.table_columns.iter().map(|t| t.column_id)
    }
}

/// Get
impl<S: StateStore, RS: RowSerde, const T: AccessType> StorageTableBase<S, RS, T> {
    /// Check whether the given `vnode` is set in the `vnodes` of this table.
    fn check_vnode_is_set(&self, vnode: VirtualNode) {
        let is_set = self.vnodes.is_set(vnode as usize).unwrap();
        assert!(
            is_set,
            "vnode {} should not be accessed by this table: {:#?}, dist key {:?}",
            vnode, self.table_columns, self.dist_key_indices
        );
    }

    /// Get vnode value with `indices` on the given `row`. Should not be used directly.
    fn compute_vnode(&self, row: &Row, indices: &[usize]) -> VirtualNode {
        let vnode = if indices.is_empty() {
            DEFAULT_VNODE
        } else {
            row.hash_by_indices(indices, &CRC32FastBuilder {})
                .to_vnode()
        };

        tracing::trace!(target: "events::storage::storage_table", "compute vnode: {:?} key {:?} => {}", row, indices, vnode);

        // TODO(rc): tmp
        // self.check_vnode_is_set(vnode);
        vnode
    }

    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: &Row) -> VirtualNode {
        self.compute_vnode(pk, &self.dist_key_in_pk_indices)
    }

    /// Try getting vnode value with given primary key prefix, used for `vnode_hint` in iterators.
    /// Return `None` if the provided columns are not enough.
    fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: &Row) -> Option<VirtualNode> {
        self.dist_key_in_pk_indices
            .iter()
            .all(|&d| d < pk_prefix.0.len())
            .then(|| self.compute_vnode(pk_prefix, &self.dist_key_in_pk_indices))
    }

    /// `vnode | pk`
    fn serialize_pk_with_vnode(&self, pk: &Row) -> Vec<u8> {
        let mut output = Vec::new();
        output.put_slice(&self.compute_vnode_by_pk(pk).to_be_bytes());
        self.pk_serializer.serialize(pk, &mut output);
        output
    }

    /// Get a single row by point get
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // TODO: use multi-get for storage get_row
        let serialized_pk = self.serialize_pk_with_vnode(pk);
        let mut deserializer = RS::create_deserializer(self.mapping.clone());
        let sentinel_key = <RS as RowSerde>::Serializer::serialize_sentinel_cell(
            &serialized_pk,
            &SENTINEL_CELL_ID,
        )
        .map_err(err)?;

        let read_options = self.get_read_option(epoch);
        match sentinel_key {
            Some(sentinel_key) => {
                if self
                    .keyspace
                    .get(&sentinel_key, read_options.clone())
                    .await?
                    .is_none()
                {
                    // if sentinel cell is none, this row doesn't exist
                    return Ok(None);
                };
            }
            // if sentinel cell does not exist, the encoding format is row-based.
            None => {
                if let Some(value) = self
                    .keyspace
                    .get(&serialized_pk, read_options.clone())
                    .await?
                {
                    let deserialize_res = deserializer
                        .deserialize(&serialized_pk, &value)
                        .map_err(err)?;
                    match deserialize_res {
                        Some(deserialize_res) => return Ok(Some(deserialize_res.2)),
                        None => return Ok(None),
                    }
                }
            }
        }

        for column_id in self.column_ids() {
            let key = serialize_pk_and_column_id(&serialized_pk, &column_id).map_err(err)?;
            if let Some(value) = self.keyspace.get(&key, read_options.clone()).await? {
                let deserialize_res = deserializer.deserialize(&key, &value).map_err(err)?;
                assert!(deserialize_res.is_none());
            }
        }

        let result = deserializer.take();
        Ok(result.map(|(vnode, _pk, row)| {
            self.check_vnode_is_set(vnode);
            row
        }))
    }

    /// Get a single row by range scan
    pub async fn get_row_by_scan(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let serialized_pk = self.serialize_pk_with_vnode(pk);
        let key_range = range_of_prefix(&serialized_pk);

        let read_options = self.get_read_option(epoch);
        let kv_pairs = self
            .keyspace
            .scan_with_range(key_range, None, read_options)
            .await?;

        let mut deserializer = RS::create_deserializer(self.mapping.clone());
        for (key, value) in kv_pairs {
            deserializer.deserialize(&key, &value).map_err(err)?;
        }

        let result = deserializer.take();
        Ok(result.map(|(vnode, _pk, row)| {
            self.check_vnode_is_set(vnode);
            row
        }))
    }

    fn get_read_option(&self, epoch: u64) -> ReadOptions {
        ReadOptions {
            epoch,
            table_id: Some(self.keyspace.table_id()),
            ttl: self.table_option.ttl,
        }
    }
}

const ENABLE_STATE_TABLE_SANITY_CHECK: bool = cfg!(debug_assertions);

/// Write with different encoding format, depending on the specific implementation of RS.
impl<S: StateStore, RS: RowSerde> StorageTableBase<S, RS, READ_WRITE> {
    /// Get vnode value with full row.
    fn compute_vnode_by_row(&self, row: &Row) -> VirtualNode {
        // With `READ_WRITE`, the output columns should be exactly same with the table columns, so
        // we can directly index into the row with indices to the table columns.
        self.compute_vnode(row, &self.dist_key_indices)
    }

    /// Write to state store.
    pub async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        let mut batch = self.keyspace.state_store().start_write_batch(WriteOptions {
            epoch,
            table_id: self.keyspace.table_id(),
        });
        let mut local = batch.prefixify(&self.keyspace);

        for (pk, row_op) in buffer {
            match row_op {
                RowOp::Insert(row) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to insert a row, it should not exist in storage.
                        let storage_row = self
                            .get_row(&row.by_indices(&self.pk_indices), epoch)
                            .await?;

                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(
                            storage_row.is_none(),
                            "overwriting an existing row:\nin-storage: {:?}\nto-be-written: {:?}",
                            storage_row.unwrap(),
                            row
                        );
                    }

                    let vnode = self.compute_vnode_by_row(&row);
                    let bytes = self
                        .row_serializer
                        .serialize(vnode, &pk, row)
                        .map_err(err)?;
                    for (key, value) in bytes {
                        local.put(key, StorageValue::new_default_put(value));
                    }
                }
                RowOp::Delete(old_row) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to delete a row, it should exist in storage, and should
                        // have the same old_value as recorded.
                        let storage_row = self
                            .get_row(&old_row.by_indices(&self.pk_indices), epoch)
                            .await?;

                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(storage_row.is_some(), "deleting an non-existing row");
                        assert!(
                            storage_row.as_ref().unwrap() == &old_row,
                            "inconsistent deletion:\nin-storage: {:?}\nold-value: {:?}",
                            storage_row.as_ref().unwrap(),
                            old_row
                        );
                    }

                    let vnode = self.compute_vnode_by_row(&old_row);
                    let bytes = self
                        .row_serializer
                        .serialize(vnode, &pk, old_row)
                        .map_err(err)?;
                    for (key, _) in bytes {
                        local.delete(key);
                    }
                }
                RowOp::Update((old_row, new_row)) => {
                    if ENABLE_STATE_TABLE_SANITY_CHECK && !self.disable_sanity_check {
                        // If we want to update a row, it should exist in storage, and should
                        // have the same old_value as recorded.
                        let storage_row = self
                            .get_row(&old_row.by_indices(&self.pk_indices), epoch)
                            .await?;

                        // It's normal for some executors to fail this assert, you can use
                        // `.disable_sanity_check()` on state table to disable this check.
                        assert!(
                            storage_row.is_some(),
                            "update a non-existing row: {:?}",
                            old_row
                        );
                        assert!(
                            storage_row.as_ref().unwrap() == &old_row,
                            "value mismatch when updating row: {:?} != {:?}",
                            storage_row,
                            old_row
                        );
                    }

                    // The row to update should keep the same primary key, so distribution key as
                    // well.
                    let vnode = self.compute_vnode_by_row(&new_row);
                    debug_assert_eq!(self.compute_vnode_by_row(&old_row), vnode);

                    // TODO: Row-based encoding does not need to serializer old_row, while a little
                    // overhead can be allowed here. Refactor this part after cell-based encoding is
                    // removed.
                    let delete_bytes = self
                        .row_serializer
                        .serialize_for_update(vnode, &pk, old_row)
                        .map_err(err)?;
                    let insert_bytes = self
                        .row_serializer
                        .serialize_for_update(vnode, &pk, new_row)
                        .map_err(err)?;
                    for (delete, insert) in
                        delete_bytes.into_iter().zip_eq(insert_bytes.into_iter())
                    {
                        match (delete, insert) {
                            (Some((delete_pk, _)), None) => {
                                local.delete(delete_pk);
                            }
                            (None, Some((insert_pk, insert_row))) => {
                                local.put(insert_pk, StorageValue::new_default_put(insert_row));
                            }
                            (None, None) => {}
                            (Some((delete_pk, _)), Some((insert_pk, insert_row))) => {
                                debug_assert_eq!(delete_pk, insert_pk);
                                local.put(insert_pk, StorageValue::new_default_put(insert_row));
                            }
                        }
                    }
                }
            }
        }
        batch.ingest().await?;
        Ok(())
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The row iterator of the storage table.
/// The wrapper of [`StorageTableIter`] if pk is not persisted.
pub type StorageTableIter<S: StateStore, RS: RowSerde> = impl PkAndRowStream;

pub type BatchDedupPkIter<S: StateStore, RS: RowSerde> = impl PkAndRowStream;

#[async_trait::async_trait]
impl<S: PkAndRowStream + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<Row>> {
        self.next()
            .await
            .transpose()
            .map(|r| r.map(|(_pk, row)| row))
    }
}

/// Iterators
impl<S: StateStore, RS: RowSerde, const T: AccessType> StorageTableBase<S, RS, T> {
    /// Get multiple [`StorageTableIter`] based on the specified vnodes of this table with
    /// `vnode_hint`, and merge or concat them by given `ordered`.
    async fn iter_with_encoded_key_range<R, B>(
        &self,
        encoded_key_range: R,
        epoch: u64,
        vnode_hint: Option<VirtualNode>,
        wait_epoch: bool,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S, RS>>
    where
        R: RangeBounds<B> + Send + Clone,
        B: AsRef<[u8]> + Send,
    {
        // Vnodes that are set and should be accessed.
        #[auto_enum(Iterator)]
        let vnodes = match vnode_hint {
            // If `vnode_hint` is set, we can only access this single vnode.
            Some(vnode) => std::iter::once(vnode),
            // Otherwise, we need to access all vnodes of this table.
            None => self
                .vnodes
                .iter()
                .enumerate()
                .filter(|&(_, set)| set)
                .map(|(i, _)| i as VirtualNode),
        };

        // For each vnode, construct an iterator.
        // TODO: if there're some vnodes continuously in the range and we don't care about order, we
        // can use a single iterator.
        let iterators: Vec<_> = try_join_all(vnodes.map(|vnode| {
            let raw_key_range = prefixed_range(encoded_key_range.clone(), &vnode.to_be_bytes());

            async move {
                let iter = StorageTableIterInner::<S, RS>::new(
                    &self.keyspace,
                    self.mapping.clone(),
                    raw_key_range,
                    wait_epoch,
                    self.get_read_option(epoch),
                )
                .await?
                .into_stream();
                Ok::<_, StorageError>(iter)
            }
        }))
        .await?;

        #[auto_enum(futures::Stream)]
        let iter = match iterators.len() {
            0 => unreachable!(),
            1 => iterators.into_iter().next().unwrap(),
            // Concat all iterators if not to preserve order.
            _ if !ordered => futures::stream::iter(iterators).flatten(),
            // Merge all iterators if to preserve order.
            _ => iter_utils::merge_sort(iterators.into_iter().map(Box::pin).collect()),
        };

        Ok(iter)
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds of
    /// the next primary key column in `next_col_bounds`.
    // TODO: support multiple datums or `Row` for `next_col_bounds`.
    async fn iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: &Row,
        next_col_bounds: impl RangeBounds<Datum>,
        wait_epoch: bool,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S, RS>> {
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerializer,
            pk_prefix: &Row,
            next_col_bound: Bound<&Datum>,
            is_start_bound: bool,
        ) -> Bound<Vec<u8>> {
            match next_col_bound {
                Included(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        Included(serialized_key)
                    } else {
                        // Should use excluded next key for end bound.
                        // Otherwise keys starting with the bound is not included.
                        end_bound_of_prefix(&serialized_key)
                    }
                }
                Excluded(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        // storage doesn't support excluded begin key yet, so transform it to
                        // included
                        // FIXME: What if `serialized_key` is `\xff\xff..`? Should the frontend
                        // reject this?
                        Included(next_key(&serialized_key))
                    } else {
                        Excluded(serialized_key)
                    }
                }
                Unbounded => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size());
                    let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
                    if pk_prefix.size() == 0 {
                        Unbounded
                    } else if is_start_bound {
                        Included(serialized_pk_prefix)
                    } else {
                        end_bound_of_prefix(&serialized_pk_prefix)
                    }
                }
            }
        }

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            pk_prefix,
            next_col_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            pk_prefix,
            next_col_bounds.end_bound(),
            false,
        );

        trace!(
            "iter_with_pk_bounds: start_key: {:?}, end_key: {:?}",
            start_key,
            end_key
        );

        self.iter_with_encoded_key_range(
            (start_key, end_key),
            epoch,
            self.try_compute_vnode_by_pk_prefix(pk_prefix),
            wait_epoch,
            ordered,
        )
        .await
    }

    /// Construct a [`StorageTableIter`] for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration, and
    /// the order of the rows among different virtual nodes is not guaranteed.
    // TODO: introduce ordered batch iterator.
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: &Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<StorageTableIter<S, RS>> {
        self.iter_with_pk_bounds(epoch, pk_prefix, next_col_bounds, true, false)
            .await
    }

    /// Construct a [`StorageTableIter`] for streaming executors.
    pub async fn streaming_iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: &Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<StorageTableIter<S, RS>> {
        self.iter_with_pk_bounds(epoch, pk_prefix, next_col_bounds, false, true)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(&self, epoch: u64) -> StorageResult<StorageTableIter<S, RS>> {
        self.batch_iter_with_pk_bounds(epoch, Row::empty(), ..)
            .await
    }

    /// `dedup_pk_iter` should be used when pk is not persisted as value in storage.
    /// It will attempt to decode pk from key instead of cell value.
    /// Tracking issue: <https://github.com/singularity-data/risingwave/issues/588>
    pub async fn batch_dedup_pk_iter(
        &self,
        epoch: u64,
        // TODO: remove this parameter: https://github.com/singularity-data/risingwave/issues/3203
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<BatchDedupPkIter<S, RS>> {
        Ok(DedupPkStorageTableIter::new(
            self.batch_iter(epoch).await?,
            self.mapping.clone(),
            pk_descs,
        )?
        .into_stream())
    }
}

/// [`StorageTableIterInner`] iterates on the storage table.
struct StorageTableIterInner<S: StateStore, RS: RowSerde> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,

    /// Cell-based row deserializer
    row_deserializer: RS::Deserializer, // CellBasedRowDeserializer<Arc<ColumnDescMapping>>,
}

impl<S: StateStore, RS: RowSerde> StorageTableIterInner<S, RS> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        raw_key_range: R,
        wait_epoch: bool,
        read_options: ReadOptions,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        if wait_epoch {
            keyspace
                .state_store()
                .wait_epoch(read_options.epoch)
                .await?;
        }

        let row_deserializer = RS::create_deserializer(table_descs);

        let iter = keyspace
            .iter_with_range(raw_key_range, read_options)
            .await?;
        let iter = Self {
            iter,
            row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((key, value)) = self.iter.next().await? {
            if let Some((_vnode, pk, row)) = self
                .row_deserializer
                .deserialize(&key, &value)
                .map_err(err)?
            {
                yield (pk, row)
            }
        }

        if let Some((_vnode, pk, row)) = self.row_deserializer.take() {
            yield (pk, row);
        }
    }
}

/// Provides a layer on top of [`StorageTableIter`]
/// for decoding pk into its constituent datums in a row.
///
/// Given the following row: `| user_id | age | name |`,
/// if pk was derived from `user_id, name`
/// we can decode pk -> `user_id, name`,
/// and retrieve the row: `|_| age |_|`,
/// then fill in empty spots with datum decoded from pk: `| user_id | age | name |`
struct DedupPkStorageTableIter<I> {
    inner: I,
    pk_decoder: OrderedRowDeserializer,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions. e.g. _row_id is unlikely to be part of selected row.
    pk_to_row_mapping: Vec<Option<usize>>,
}

impl<I> DedupPkStorageTableIter<I> {
    fn new(
        inner: I,
        mapping: Arc<ColumnDescMapping>,
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<Self> {
        let (data_types, order_types) = pk_descs
            .iter()
            .map(|ordered_desc| {
                (
                    ordered_desc.column_desc.data_type.clone(),
                    ordered_desc.order,
                )
            })
            .unzip();
        let pk_decoder = OrderedRowDeserializer::new(data_types, order_types);

        let pk_to_row_mapping = pk_descs
            .iter()
            .map(|d| {
                let column_desc = &d.column_desc;
                if column_desc.data_type.mem_cmp_eq_value_enc() {
                    mapping.get(column_desc.column_id).map(|(_, index)| index)
                } else {
                    None
                }
            })
            .collect();

        Ok(Self {
            inner,
            pk_decoder,
            pk_to_row_mapping,
        })
    }
}

impl<I: PkAndRowStream> DedupPkStorageTableIter<I> {
    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(self) {
        #[for_await]
        for r in self.inner {
            let (pk_vec, Row(mut row_inner)) = r?;
            let pk_decoded = self.pk_decoder.deserialize(&pk_vec).map_err(err)?;
            for (pk_idx, datum) in pk_decoded.into_vec().into_iter().enumerate() {
                if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                    row_inner[row_idx] = datum;
                }
            }
            yield (pk_vec, Row(row_inner));
        }
    }
}
