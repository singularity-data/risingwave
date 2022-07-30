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
use std::iter::empty;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_async_stream::try_stream;
use itertools::{repeat_n, Itertools};
use risingwave_common::array::column::Column;
use risingwave_common::array::{Array, DataChunk, RowRef};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{
    calc_hash_key_kind, HashKey, HashKeyDispatcher, PrecomputedBuildHasher,
};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_expr::expr::{build_from_prost, BoxedExpression, Expression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{ChunkedData, JoinType, RowId};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// Hash Join Executor
///
/// High-level idea:
/// 1. Iterate over the build side (i.e. right table) and build a hash map.
/// 2. Iterate over the probe side (i.e. left table) and compute the hash value of each row.
///    Then find the matched build side row for each probe side row in the hash map.
/// 3. Concatenate the matched pair of probe side row and build side row into a single row and push
/// it into the data chunk builder. 4. Yield chunks from the builder.
pub struct HashJoinExecutor<K> {
    /// Join type e.g. inner, left outer, ...
    join_type: JoinType,
    /// Output schema without applying `output_indices`
    original_schema: Schema,
    /// Output schema after applying `output_indices`
    schema: Schema,
    /// output_indices are the indices of the columns that we needed.
    output_indices: Vec<usize>,
    /// Left child executor
    probe_side_source: BoxedExecutor,
    /// Right child executor
    build_side_source: BoxedExecutor,
    /// Column indices of left keys in equi join
    probe_key_idxs: Vec<usize>,
    /// Column indices of right keys in equi join
    build_key_idxs: Vec<usize>,
    /// Non-equi join condition (optional)
    cond: Option<BoxedExpression>,
    /// Whether or not to find matched build rows for probe rows with NULL keys
    match_null: bool,
    identity: String,
    _phantom: PhantomData<K>,
}

impl<K: HashKey> Executor for HashJoinExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

/// In `JoinHashMap`, we only save the row id of the first build row that has the hash key.
/// In fact, in the build side there may be multiple rows with the same hash key. To handle this
/// case, we use `ChunkedData` to link them together. For example:
///
/// | id | key | row |
/// | --- | --- | --- |
/// | 0 | 1 | (1, 2, 3) |
/// | 1 | 4 | (4, 5, 6) |
/// | 2 | 1 | (1, 3, 7) |
/// | 3 | 1 | (1, 3, 2) |
/// | 4 | 3 | (3, 2, 1) |
///
/// The corresponding join hash map is:
///
/// | key | value |
/// | --- | --- |
/// | 1 | 0 |
/// | 4 | 1 |
/// | 3 | 4 |
///
/// And we save build rows with the same key like this:
///
/// | id | value |
/// | --- | --- |
/// | 0 | 2 |
/// | 1 | None |
/// | 2 | 3 |
/// | 3 | None |
/// | 4 | None |
///
/// This can be seen as an implicit linked list. For convenience, we use `RowIdIter` to iterate all
/// build side row ids with the given key.
type JoinHashMap<K> = HashMap<K, RowId, PrecomputedBuildHasher>;

struct RowIdIter<'a> {
    current_row_id: Option<RowId>,
    next_row_id: &'a ChunkedData<Option<RowId>>,
}

impl ChunkedData<Option<RowId>> {
    fn row_id_iter(&self, begin: Option<RowId>) -> RowIdIter {
        RowIdIter {
            current_row_id: begin,
            next_row_id: self,
        }
    }
}

impl<'a> Iterator for RowIdIter<'a> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_row_id.map(|row_id| {
            self.current_row_id = self.next_row_id[row_id];
            row_id
        })
    }
}

struct EquiJoinParams<K> {
    probe_side: BoxedExecutor,
    probe_data_types: Vec<DataType>,
    probe_key_idxs: Vec<usize>,
    build_side: Vec<DataChunk>,
    build_data_types: Vec<DataType>,
    full_data_types: Vec<DataType>,
    hash_map: JoinHashMap<K>,
    next_build_row_with_same_key: ChunkedData<Option<RowId>>,
}

/// State variables used in left outer/semi/anti join and full outer join.
#[derive(Default)]
struct LeftNonEquiJoinState {
    /// The number of columns in probe side.
    probe_column_count: usize,
    /// The offset of the first output row in **current** chunk for each probe side row that has
    /// been processed.
    first_output_row_id: Vec<usize>,
    /// Whether the probe row being processed currently has output rows in **next** output chunk.
    has_more_output_rows: bool,
    /// Whether the probe row being processed currently has matched non-NULL build rows in **last**
    /// output chunk.
    found_matched: bool,
}

/// State variables used in right outer/semi/anti join and full outer join.
#[derive(Default)]
struct RightNonEquiJoinState {
    /// Corresponding build row id for each row in **current** output chunk.
    build_row_ids: Vec<RowId>,
    /// Whether a build row has been matched.
    build_row_matched: ChunkedData<bool>,
}

impl<K: HashKey> HashJoinExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let probe_data_types = self.probe_side_source.schema().data_types();
        let build_data_types = self.build_side_source.schema().data_types();
        let full_data_types = [probe_data_types.clone(), build_data_types.clone()].concat();

        let mut build_side = Vec::new();
        let mut build_row_count = 0;
        #[for_await]
        for build_chunk in self.build_side_source.execute() {
            let build_chunk = build_chunk?;
            if build_chunk.cardinality() > 0 {
                build_row_count += build_chunk.cardinality();
                build_side.push(build_chunk.compact()?)
            }
        }
        let mut hash_map =
            JoinHashMap::with_capacity_and_hasher(build_row_count, PrecomputedBuildHasher);
        let mut next_build_row_with_same_key =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        // Build hash map
        for (build_chunk_id, build_chunk) in build_side.iter().enumerate() {
            let build_keys = K::build(&self.build_key_idxs, build_chunk)?;
            for (build_row_id, build_key) in build_keys
                .into_iter()
                .enumerate()
                .filter(|(_, key)| self.match_null || !key.has_null())
            {
                let row_id = RowId::new(build_chunk_id, build_row_id);
                next_build_row_with_same_key[row_id] = hash_map.insert(build_key, row_id);
            }
        }

        let params = EquiJoinParams {
            probe_side: self.probe_side_source,
            probe_data_types,
            probe_key_idxs: self.probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
        };

        if let Some(cond) = self.cond {
            let stream = match self.join_type {
                JoinType::Inner => Self::do_inner_join_with_non_equi_condition(params, cond),
                JoinType::LeftOuter => {
                    Self::do_left_outer_join_with_non_equi_condition(params, cond)
                }
                JoinType::LeftSemi => Self::do_left_semi_join_with_non_equi_condition(params, cond),
                JoinType::LeftAnti => Self::do_left_anti_join_with_non_equi_condition(params, cond),
                JoinType::RightOuter => {
                    Self::do_right_outer_join_with_non_equi_condition(params, cond)
                }
                JoinType::RightSemi => {
                    Self::do_right_semi_anti_join_with_non_equi_condition::<false>(params, cond)
                }
                JoinType::RightAnti => {
                    Self::do_right_semi_anti_join_with_non_equi_condition::<true>(params, cond)
                }
                JoinType::FullOuter => {
                    Self::do_full_outer_join_with_non_equi_condition(params, cond)
                }
            };
            // For non-equi join, we need an output chunk builder to align the output chunks.
            let mut output_chunk_builder =
                DataChunkBuilder::with_default_size(self.schema.data_types());
            #[for_await]
            for chunk in stream {
                #[for_await]
                for output_chunk in Self::append_chunk(
                    &mut output_chunk_builder,
                    chunk?.reorder_columns(&self.output_indices),
                ) {
                    yield output_chunk?
                }
            }
            if let Some(output_chunk) = output_chunk_builder.consume_all()? {
                yield output_chunk
            }
        } else {
            let stream = match self.join_type {
                JoinType::Inner => Self::do_inner_join(params),
                JoinType::LeftOuter => Self::do_left_outer_join(params),
                JoinType::LeftSemi => Self::do_left_semi_anti_join::<false>(params),
                JoinType::LeftAnti => Self::do_left_semi_anti_join::<true>(params),
                JoinType::RightOuter => Self::do_right_outer_join(params),
                JoinType::RightSemi => Self::do_right_semi_anti_join::<false>(params),
                JoinType::RightAnti => Self::do_right_semi_anti_join::<true>(params),
                JoinType::FullOuter => Self::do_full_outer_join(params),
            };
            #[for_await]
            for chunk in stream {
                yield chunk?.reorder_columns(&self.output_indices)
            }
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_inner_join(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_inner_join_with_non_equi_condition(
        params: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        #[for_await]
        for chunk in Self::do_inner_join(params) {
            let mut chunk = chunk?;
            chunk.set_visibility(cond.eval(&chunk)?.as_bool().iter().collect());
            yield chunk
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_outer_join(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    for build_row_id in
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id))
                    {
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            yield spilled
                        }
                    }
                } else {
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_outer_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut non_equi_state = LeftNonEquiJoinState {
            probe_column_count: probe_data_types.len(),
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                non_equi_state.found_matched = false;
                non_equi_state
                    .first_output_row_id
                    .push(chunk_builder.buffered_count());
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_left_outer_join_non_equi_condition(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )?
                        }
                    }
                } else {
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield Self::process_left_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_semi_anti_join<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            hash_map,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(probe_data_types);
        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                if !ANTI_JOIN {
                    if hash_map.get(probe_key).is_some() {
                        if let Some(spilled) = Self::append_one_probe_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                        )? {
                            yield spilled
                        }
                    }
                } else if hash_map.get(probe_key).is_none() {
                    if let Some(spilled) =
                        Self::append_one_probe_row(&mut chunk_builder, &probe_chunk, probe_row_id)?
                    {
                        yield spilled
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_semi_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut non_equi_state = LeftNonEquiJoinState::default();

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                non_equi_state
                    .first_output_row_id
                    .push(chunk_builder.buffered_count());
                non_equi_state.found_matched = false;
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    for build_row_id in
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id))
                    {
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            yield Self::process_left_semi_anti_join_non_equi_condition::<false>(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )?
                        }
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield Self::process_left_semi_anti_join_non_equi_condition::<false>(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_left_anti_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut remaining_chunk_builder = DataChunkBuilder::with_default_size(probe_data_types);
        let mut non_equi_state = LeftNonEquiJoinState::default();

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                non_equi_state.found_matched = false;
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());
                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_left_semi_anti_join_non_equi_condition::<true>(
                                spilled,
                                cond.as_ref(),
                                &mut non_equi_state,
                            )?
                        }
                    }
                } else if let Some(spilled) = Self::append_one_probe_row(
                    &mut remaining_chunk_builder,
                    &probe_chunk,
                    probe_row_id,
                )? {
                    yield spilled
                }
            }
        }
        non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield Self::process_left_semi_anti_join_non_equi_condition::<true>(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )?
        }
        if let Some(spilled) = remaining_chunk_builder.consume_all()? {
            yield spilled
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_outer_join(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    build_row_matched[build_row_id] = true;
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_outer_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    non_equi_state.build_row_ids.push(build_row_id);
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    )? {
                        yield Self::process_right_outer_join_non_equi_condition(
                            spilled,
                            cond.as_ref(),
                            &mut non_equi_state,
                        )?
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield Self::process_right_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &non_equi_state.build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_semi_anti_join<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(build_data_types);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for probe_key in &probe_keys {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    build_row_matched[build_row_id] = true;
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_semi_anti_join::<ANTI_JOIN>(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_right_semi_anti_join_with_non_equi_condition<const ANTI_JOIN: bool>(
        EquiJoinParams {
            probe_side,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut remaining_chunk_builder = DataChunkBuilder::with_default_size(build_data_types);
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                for build_row_id in
                    next_build_row_with_same_key.row_id_iter(hash_map.get(probe_key).copied())
                {
                    non_equi_state.build_row_ids.push(build_row_id);
                    let build_chunk = &build_side[build_row_id.chunk_id()];
                    if let Some(spilled) = Self::append_one_row(
                        &mut chunk_builder,
                        &probe_chunk,
                        probe_row_id,
                        build_chunk,
                        build_row_id.row_id(),
                    )? {
                        Self::process_right_semi_anti_join_non_equi_condition(
                            spilled,
                            cond.as_ref(),
                            &mut non_equi_state,
                        )?
                    }
                }
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            Self::process_right_semi_anti_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut non_equi_state,
            )?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_semi_anti_join::<ANTI_JOIN>(
            &mut remaining_chunk_builder,
            &build_side,
            &non_equi_state.build_row_matched,
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_full_outer_join(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    for build_row_id in
                        next_build_row_with_same_key.row_id_iter(Some(*first_matched_build_row_id))
                    {
                        build_row_matched[build_row_id] = true;
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            yield spilled
                        }
                    }
                } else {
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut chunk_builder,
            &build_side,
            &build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_full_outer_join_with_non_equi_condition(
        EquiJoinParams {
            probe_side,
            probe_data_types,
            probe_key_idxs,
            build_side,
            build_data_types,
            full_data_types,
            hash_map,
            next_build_row_with_same_key,
            ..
        }: EquiJoinParams<K>,
        cond: BoxedExpression,
    ) {
        let mut chunk_builder = DataChunkBuilder::with_default_size(full_data_types.clone());
        let mut remaining_chunk_builder = DataChunkBuilder::with_default_size(full_data_types);
        let mut left_non_equi_state = LeftNonEquiJoinState {
            probe_column_count: probe_data_types.len(),
            ..Default::default()
        };
        let build_row_matched =
            ChunkedData::with_chunk_sizes(build_side.iter().map(|c| c.capacity()))?;
        let mut right_non_equi_state = RightNonEquiJoinState {
            build_row_matched,
            ..Default::default()
        };

        #[for_await]
        for probe_chunk in probe_side.execute() {
            let probe_chunk = probe_chunk?;
            let probe_keys = K::build(&probe_key_idxs, &probe_chunk)?;
            for (probe_row_id, probe_key) in probe_keys.iter().enumerate() {
                left_non_equi_state.found_matched = false;
                if let Some(first_matched_build_row_id) = hash_map.get(probe_key) {
                    left_non_equi_state
                        .first_output_row_id
                        .push(chunk_builder.buffered_count());
                    let mut build_row_id_iter = next_build_row_with_same_key
                        .row_id_iter(Some(*first_matched_build_row_id))
                        .peekable();
                    while let Some(build_row_id) = build_row_id_iter.next() {
                        right_non_equi_state.build_row_ids.push(build_row_id);
                        let build_chunk = &build_side[build_row_id.chunk_id()];
                        if let Some(spilled) = Self::append_one_row(
                            &mut chunk_builder,
                            &probe_chunk,
                            probe_row_id,
                            build_chunk,
                            build_row_id.row_id(),
                        )? {
                            left_non_equi_state.has_more_output_rows =
                                build_row_id_iter.peek().is_some();
                            yield Self::process_full_outer_join_non_equi_condition(
                                spilled,
                                cond.as_ref(),
                                &mut left_non_equi_state,
                                &mut right_non_equi_state,
                            )?
                        }
                    }
                } else {
                    let probe_row = probe_chunk.row_at_unchecked_vis(probe_row_id);
                    if let Some(spilled) = Self::append_one_row_with_null_build_side(
                        &mut remaining_chunk_builder,
                        probe_row,
                        build_data_types.len(),
                    )? {
                        yield spilled
                    }
                }
            }
        }
        left_non_equi_state.has_more_output_rows = false;
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield Self::process_full_outer_join_non_equi_condition(
                spilled,
                cond.as_ref(),
                &mut left_non_equi_state,
                &mut right_non_equi_state,
            )?
        }
        #[for_await]
        for spilled in Self::handle_remaining_build_rows_for_right_outer_join(
            &mut remaining_chunk_builder,
            &build_side,
            &right_non_equi_state.build_row_matched,
            probe_data_types.len(),
        ) {
            yield spilled?
        }
    }

    /// Process output chunk for left outer join when non-equi condition is presented.
    ///
    /// # Arguments
    /// * `chunk` - Output chunk from `do_left_outer_join_with_non_equi_condition`, containing:
    ///     - Concatenation of probe row and its corresponding build row according to the hash map.
    ///     - Concatenation of probe row and `NULL` build row, if there is no matched build row
    ///       found for the probe row.
    /// * `cond` - Non-equi join condition.
    /// * `probe_column_count` - The number of columns in the probe side.
    /// * `first_output_row_id` - The offset of the first output row in `chunk` for each probe side
    ///   row that has been processed.
    /// * `has_more_output_rows` - Whether the probe row being processed currently has output rows
    ///   in next output chunk.
    /// * `found_matched` - Whether the probe row being processed currently has matched non-NULL
    ///   build rows in last output chunk.
    ///
    /// # Examples
    /// Assume we have two tables `t1` and `t2` as probe side and build side, respectively.
    /// ```sql
    /// CREATE TABLE t1 (v1 int, v2 int);
    /// CREATE TABLE t2 (v3 int);
    /// ```
    ///
    /// Now we de left outer join on `t1` and `t2`, as the following query shows:
    /// ```sql
    /// SELECT * FROM t1 LEFT JOIN t2 ON t1.v1 = t2.v3 AND t1.v2 <> t2.v3;
    /// ```
    ///
    /// Assume the chunk builder in `do_left_outer_join_with_non_equi_condition` has buffer size 5,
    /// and we have the following chunk as the first output ('-' represents NULL).
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | 1 |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | 3 |
    /// | 4 | 3 | 3 | 3 |
    ///
    /// We have the following precondition:
    /// ```ignore
    /// assert_eq!(probe_column_count, 2);
    /// assert_eq!(first_out_row_id, vec![0, 1, 2, 3]);
    /// assert_eq!(has_more_output_rows);
    /// assert_eq!(!found_matched);
    /// ```
    ///
    /// In `process_left_outer_join_non_equi_condition`, we transform the chunk in following steps.
    ///
    /// 1. Evaluate the non-equi condition on the chunk. Here the condition is `t1.v2 <> t2.v3`.
    ///
    /// We get the result array:
    ///
    /// | offset | value |
    /// | --- | --- |
    /// | 0 | true |
    /// | 1 | false |
    /// | 2 | false |
    /// | 3 | false |
    /// | 4 | false |
    ///
    /// 2. Set the build side columns to NULL if the corresponding result value is false.
    ///
    /// The chunk is changed to:
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | - |
    /// | 4 | 3 | 3 | - |
    ///
    /// 3. Remove duplicate rows with NULL build side. This is done by setting the visibility bitmap
    /// of the chunk.
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | ~~3~~ | ~~3~~ | ~~-~~ |
    /// | 4 | ~~3~~ | ~~3~~ | ~~-~~ |
    ///
    /// For the probe row being processed currently (`(3, 3)` here), we don't have output rows with
    /// non-NULL build side, so we set `found_matched` to false.
    ///
    /// In `do_left_outer_join_with_non_equi_condition`, we have next output chunk as follows:
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 3 | 3 | 3 |
    /// | 1 | 3 | 3 | 3 |
    /// | 2 | 5 | 5 | - |
    /// | 3 | 5 | 3 | - |
    /// | 4 | 5 | 3 | - |
    ///
    /// This time We have the following precondition:
    /// ```ignore
    /// assert_eq!(probe_column_count, 2);
    /// assert_eq!(first_out_row_id, vec![2, 3]);
    /// assert_eq!(!has_more_output_rows);
    /// assert_eq!(!found_matched);
    /// ```
    ///
    /// The transformed chunk is as follows after the same steps.
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | ~~3~~ | ~~3~~ | ~~3~~ |
    /// | 1 | 3 | 3 | - |
    /// | 2 | 5 | 5 | - |
    /// | 3 | 5 | 3 | - |
    /// | 4 | ~~5~~ | ~~3~~ | ~~-~~ |
    ///
    /// After we add these chunks to output chunk builder in `do_execute`, we get the final output:
    ///
    /// Chunk 1
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 1 | 2 | 1 |
    /// | 1 | 1 | 1 | - |
    /// | 2 | 2 | 3 | - |
    /// | 3 | 3 | 3 | - |
    /// | 4 | 5 | 5 | - |
    ///
    /// Chunk 2
    ///
    /// | offset | v1 | v2 | v3 |
    /// |---|---|---|---|
    /// | 0 | 5 | 3 | - |
    ///
    ///
    /// For more information about how `process_*_join_non_equi_condition` work, see their unit
    /// tests.
    fn process_left_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        LeftNonEquiJoinState {
            probe_column_count,
            first_output_row_id,
            has_more_output_rows,
            found_matched,
        }: &mut LeftNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk)?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .nullify_build_side_for_non_equi_condition(&filter, *probe_column_count)
            .remove_duplicate_rows_for_left_outer_join(
                &filter,
                first_output_row_id,
                *has_more_output_rows,
                found_matched,
            )
            .take())
    }

    fn process_left_semi_anti_join_non_equi_condition<const ANTI_JOIN: bool>(
        chunk: DataChunk,
        cond: &dyn Expression,
        LeftNonEquiJoinState {
            first_output_row_id,
            found_matched,
            has_more_output_rows,
            ..
        }: &mut LeftNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk)?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .remove_duplicate_rows_for_left_semi_anti_join::<ANTI_JOIN>(
                &filter,
                first_output_row_id,
                *has_more_output_rows,
                found_matched,
            )
            .take())
    }

    fn process_right_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk)?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .remove_duplicate_rows_for_right_outer_join(&filter, build_row_ids, build_row_matched)
            .take())
    }

    fn process_right_semi_anti_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Result<()> {
        let filter = cond.eval(&chunk)?.as_bool().iter().collect();
        DataChunkMutator(chunk).remove_duplicate_rows_for_right_semi_anti_join(
            &filter,
            build_row_ids,
            build_row_matched,
        );
        Ok(())
    }

    fn process_full_outer_join_non_equi_condition(
        chunk: DataChunk,
        cond: &dyn Expression,
        left_non_equi_state: &mut LeftNonEquiJoinState,
        right_non_equi_state: &mut RightNonEquiJoinState,
    ) -> Result<DataChunk> {
        let filter = cond.eval(&chunk)?.as_bool().iter().collect();
        Ok(DataChunkMutator(chunk)
            .nullify_build_side_for_non_equi_condition(
                &filter,
                left_non_equi_state.probe_column_count,
            )
            .remove_duplicate_rows_for_full_outer_join(
                &filter,
                left_non_equi_state,
                right_non_equi_state,
            )
            .take())
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn handle_remaining_build_rows_for_right_outer_join<'a>(
        chunk_builder: &'a mut DataChunkBuilder,
        build_side: &'a [DataChunk],
        build_row_matched: &'a ChunkedData<bool>,
        probe_column_count: usize,
    ) {
        for build_row_id in build_row_matched
            .all_row_ids()
            .filter(|build_row_id| !build_row_matched[*build_row_id])
        {
            let build_row =
                build_side[build_row_id.chunk_id()].row_at_unchecked_vis(build_row_id.row_id());
            if let Some(spilled) = Self::append_one_row_with_null_probe_side(
                chunk_builder,
                build_row,
                probe_column_count,
            )? {
                yield spilled
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn handle_remaining_build_rows_for_right_semi_anti_join<'a, const ANTI_JOIN: bool>(
        chunk_builder: &'a mut DataChunkBuilder,
        build_side: &'a [DataChunk],
        build_row_matched: &'a ChunkedData<bool>,
    ) {
        for build_row_id in build_row_matched.all_row_ids().filter(|build_row_id| {
            if !ANTI_JOIN {
                build_row_matched[*build_row_id]
            } else {
                !build_row_matched[*build_row_id]
            }
        }) {
            if let Some(spilled) = Self::append_one_build_row(
                chunk_builder,
                &build_side[build_row_id.chunk_id()],
                build_row_id.row_id(),
            )? {
                yield spilled
            }
        }
        if let Some(spilled) = chunk_builder.consume_all()? {
            yield spilled
        }
    }

    // Disable clippy's annoying lifetime warning
    #[allow(clippy::needless_lifetimes)]
    #[try_stream(ok = DataChunk, error = RwError)]
    async fn append_chunk(chunk_builder: &mut DataChunkBuilder, chunk: DataChunk) {
        let (mut remain, mut output) =
            chunk_builder.append_chunk(SlicedDataChunk::new_checked(chunk)?)?;
        if let Some(output_chunk) = output {
            yield output_chunk
        }
        while let Some(remain_chunk) = remain {
            (remain, output) = chunk_builder.append_chunk(remain_chunk)?;
            if let Some(output_chunk) = output {
                yield output_chunk
            }
        }
    }

    fn append_one_row(
        chunk_builder: &mut DataChunkBuilder,
        probe_chunk: &DataChunk,
        probe_row_id: usize,
        build_chunk: &DataChunk,
        build_row_id: usize,
    ) -> Result<Option<DataChunk>> {
        Ok(chunk_builder.append_one_row_from_array_elements(
            probe_chunk.columns().iter().map(|c| c.array_ref()),
            probe_row_id,
            build_chunk.columns().iter().map(|c| c.array_ref()),
            build_row_id,
        )?)
    }

    fn append_one_probe_row(
        chunk_builder: &mut DataChunkBuilder,
        probe_chunk: &DataChunk,
        probe_row_id: usize,
    ) -> Result<Option<DataChunk>> {
        Ok(chunk_builder.append_one_row_from_array_elements(
            probe_chunk.columns().iter().map(|c| c.array_ref()),
            probe_row_id,
            empty(),
            0,
        )?)
    }

    fn append_one_build_row(
        chunk_builder: &mut DataChunkBuilder,
        build_chunk: &DataChunk,
        build_row_id: usize,
    ) -> Result<Option<DataChunk>> {
        Ok(chunk_builder.append_one_row_from_array_elements(
            empty(),
            0,
            build_chunk.columns().iter().map(|c| c.array_ref()),
            build_row_id,
        )?)
    }

    fn append_one_row_with_null_build_side(
        chunk_builder: &mut DataChunkBuilder,
        probe_row_ref: RowRef,
        build_column_count: usize,
    ) -> Result<Option<DataChunk>> {
        Ok(chunk_builder.append_one_row_from_datum_refs(
            probe_row_ref
                .values()
                .chain(repeat_n(None, build_column_count)),
        )?)
    }

    fn append_one_row_with_null_probe_side(
        chunk_builder: &mut DataChunkBuilder,
        build_row_ref: RowRef,
        probe_column_count: usize,
    ) -> Result<Option<DataChunk>> {
        Ok(chunk_builder.append_one_row_from_datum_refs(
            repeat_n(None, probe_column_count).chain(build_row_ref.values()),
        )?)
    }
}

/// `DataChunkMutator` transforms the given data chunk for non-equi join.
#[repr(transparent)]
struct DataChunkMutator(DataChunk);

impl DataChunkMutator {
    fn nullify_build_side_for_non_equi_condition(
        self,
        filter: &Bitmap,
        probe_column_count: usize,
    ) -> Self {
        let (mut columns, vis) = self.0.into_parts();

        for build_column in columns.split_off(probe_column_count) {
            // Is it really safe to use Arc::try_unwrap here?
            let mut array = Arc::try_unwrap(build_column.into_inner()).unwrap();
            array.set_bitmap(filter.clone());
            columns.push(Column::new(Arc::new(array)));
        }

        Self(DataChunk::new(columns, vis))
    }

    fn remove_duplicate_rows_for_left_outer_join(
        mut self,
        filter: &Bitmap,
        first_output_row_ids: &mut Vec<usize>,
        has_more_output_rows: bool,
        found_non_null: &mut bool,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in repeat_n(&0, 1)
            .chain(first_output_row_ids.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id).unwrap() {
                    *found_non_null = true;
                    new_visibility.set(row_id, true);
                }
            }
            if !*found_non_null {
                new_visibility.set(start_row_id, true);
            }
            *found_non_null = false;
        }

        let start_row_id = first_output_row_ids.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id).unwrap() {
                *found_non_null = true;
                new_visibility.set(row_id, true);
            }
        }
        if !has_more_output_rows && !*found_non_null {
            new_visibility.set(start_row_id, true);
        }

        first_output_row_ids.clear();

        self.0.set_visibility(new_visibility.finish());
        self
    }

    fn remove_duplicate_rows_for_left_semi_anti_join<const ANTI_JOIN: bool>(
        mut self,
        filter: &Bitmap,
        first_output_row_ids: &mut Vec<usize>,
        has_more_output_rows: bool,
        found_matched: &mut bool,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in repeat_n(&0, 1)
            .chain(first_output_row_ids.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id).unwrap() {
                    if !ANTI_JOIN && !*found_matched {
                        new_visibility.set(row_id, true);
                    }
                    *found_matched = true;
                    break;
                }
            }
            if ANTI_JOIN && !*found_matched {
                new_visibility.set(start_row_id, true);
            }
            *found_matched = false;
        }

        let start_row_id = first_output_row_ids.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id).unwrap() {
                if !ANTI_JOIN && !*found_matched {
                    new_visibility.set(row_id, true);
                }
                *found_matched = true;
                break;
            }
        }
        if ANTI_JOIN && !has_more_output_rows && !*found_matched {
            new_visibility.set(start_row_id, true);
        }

        first_output_row_ids.clear();

        self.0.set_visibility(new_visibility.finish());
        self
    }

    fn remove_duplicate_rows_for_right_outer_join(
        mut self,
        filter: &Bitmap,
        build_row_ids: &mut Vec<RowId>,
        build_row_matched: &mut ChunkedData<bool>,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());
        for (output_row_id, (output_row_non_null, &build_row_id)) in
            filter.iter().zip_eq(build_row_ids.iter()).enumerate()
        {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
                new_visibility.set(output_row_id, true);
            }
        }

        build_row_ids.clear();

        self.0.set_visibility(new_visibility.finish());
        self
    }

    fn remove_duplicate_rows_for_right_semi_anti_join(
        self,
        filter: &Bitmap,
        build_row_ids: &mut Vec<RowId>,
        build_row_matched: &mut ChunkedData<bool>,
    ) {
        for (output_row_non_null, &build_row_id) in filter.iter().zip_eq(build_row_ids.iter()) {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
            }
        }

        build_row_ids.clear();
    }

    fn remove_duplicate_rows_for_full_outer_join(
        mut self,
        filter: &Bitmap,
        LeftNonEquiJoinState {
            first_output_row_id,
            has_more_output_rows,
            found_matched,
            ..
        }: &mut LeftNonEquiJoinState,
        RightNonEquiJoinState {
            build_row_ids,
            build_row_matched,
        }: &mut RightNonEquiJoinState,
    ) -> Self {
        let mut new_visibility = BitmapBuilder::zeroed(self.0.capacity());

        for (&start_row_id, &end_row_id) in repeat_n(&0, 1)
            .chain(first_output_row_id.iter())
            .tuple_windows()
            .filter(|(start_row_id, end_row_id)| start_row_id < end_row_id)
        {
            for row_id in start_row_id..end_row_id {
                if filter.is_set(row_id).unwrap() {
                    *found_matched = true;
                    new_visibility.set(row_id, true);
                }
            }
            if !*found_matched {
                new_visibility.set(start_row_id, true);
            }
            *found_matched = false;
        }

        let start_row_id = first_output_row_id.last().copied().unwrap_or_default();
        for row_id in start_row_id..filter.len() {
            if filter.is_set(row_id).unwrap() {
                *found_matched = true;
                new_visibility.set(row_id, true);
            }
        }
        if !*has_more_output_rows && !*found_matched {
            new_visibility.set(start_row_id, true);
        }

        first_output_row_id.clear();

        for (output_row_id, (output_row_non_null, &build_row_id)) in
            filter.iter().zip_eq(build_row_ids.iter()).enumerate()
        {
            if output_row_non_null {
                build_row_matched[build_row_id] = true;
                new_visibility.set(output_row_id, true);
            }
        }

        build_row_ids.clear();

        self.0.set_visibility(new_visibility.finish());
        self
    }

    fn take(self) -> DataChunk {
        self.0
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HashJoinExecutor<()> {
    async fn new_boxed_executor<C: BatchTaskContext>(
        context: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.len() == 2,
            "HashJoinExecutor should have 2 children!"
        );

        let left_child = inputs.remove(0);
        let right_child = inputs.remove(0);

        let hash_join_node = try_match_expand!(
            context.plan_node().get_node_body().unwrap(),
            NodeBody::HashJoin
        )?;

        let join_type = JoinType::from_prost(hash_join_node.get_join_type()?);

        let cond = match hash_join_node.get_condition() {
            Ok(cond_prost) => Some(build_from_prost(cond_prost)?),
            Err(_) => None,
        };

        let left_key_idxs = hash_join_node
            .get_left_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect_vec();
        let right_key_idxs = hash_join_node
            .get_right_key()
            .iter()
            .map(|&idx| idx as usize)
            .collect_vec();

        ensure!(left_key_idxs.len() == right_key_idxs.len());

        let right_data_types = right_child.schema().data_types();
        let right_key_types = right_key_idxs
            .iter()
            .map(|&idx| right_data_types[idx].clone())
            .collect_vec();

        let hash_key_kind = calc_hash_key_kind(&right_key_types);

        let output_indices: Vec<usize> = hash_join_node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect();

        Ok(HashJoinExecutor::dispatch_by_kind(
            hash_key_kind,
            HashJoinExecutor::new(
                join_type,
                output_indices,
                left_child,
                right_child,
                left_key_idxs,
                right_key_idxs,
                // TODO: frontend should pass match_null in plan node
                false,
                cond,
                context.plan_node().get_identity().clone(),
            ),
        ))
    }
}

impl HashKeyDispatcher for HashJoinExecutor<()> {
    type Input = Self;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: Self::Input) -> Self::Output {
        Box::new(HashJoinExecutor::<K>::new(
            input.join_type,
            input.output_indices,
            input.probe_side_source,
            input.build_side_source,
            input.probe_key_idxs,
            input.build_key_idxs,
            input.match_null,
            input.cond,
            input.identity,
        ))
    }
}

impl<K> HashJoinExecutor<K> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        join_type: JoinType,
        output_indices: Vec<usize>,
        probe_side_source: BoxedExecutor,
        build_side_source: BoxedExecutor,
        probe_key_idxs: Vec<usize>,
        build_key_idxs: Vec<usize>,
        match_null: bool,
        cond: Option<BoxedExpression>,
        identity: String,
    ) -> Self {
        let original_schema = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => probe_side_source.schema().clone(),
            JoinType::RightSemi | JoinType::RightAnti => build_side_source.schema().clone(),
            _ => Schema::from_iter(
                probe_side_source
                    .schema()
                    .fields()
                    .iter()
                    .chain(build_side_source.schema().fields().iter())
                    .cloned(),
            ),
        };
        let schema = Schema::from_iter(
            output_indices
                .iter()
                .map(|&idx| original_schema[idx].clone()),
        );
        Self {
            join_type,
            original_schema,
            schema,
            output_indices,
            probe_side_source,
            build_side_source,
            probe_key_idxs,
            build_key_idxs,
            match_null,
            cond,
            identity,
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::error::Result;
    use risingwave_common::hash::Key32;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_expr::expr::{BoxedExpression, InputRefExpression};
    use risingwave_pb::expr::expr_node::Type;

    use super::{
        ChunkedData, HashJoinExecutor, JoinType, LeftNonEquiJoinState, RightNonEquiJoinState, RowId,
    };
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::BoxedExecutor;
    struct DataChunkMerger {
        data_types: Vec<DataType>,
        array_builders: Vec<ArrayBuilderImpl>,
        array_len: usize,
    }

    impl DataChunkMerger {
        fn new(data_types: Vec<DataType>) -> Result<Self> {
            let array_builders = data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(1024))
                .collect();

            Ok(Self {
                data_types,
                array_builders,
                array_len: 0,
            })
        }

        fn append(&mut self, data_chunk: &DataChunk) -> Result<()> {
            ensure!(self.array_builders.len() == data_chunk.dimension());
            for idx in 0..self.array_builders.len() {
                self.array_builders[idx].append_array(data_chunk.column_at(idx).array_ref())?;
            }
            self.array_len += data_chunk.capacity();

            Ok(())
        }

        fn finish(self) -> Result<DataChunk> {
            let columns = self
                .array_builders
                .into_iter()
                .map(|array_builder| array_builder.finish().map(|arr| Column::new(Arc::new(arr))))
                .try_collect()?;

            Ok(DataChunk::new(columns, self.array_len))
        }
    }

    fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) -> bool {
        assert!(left.visibility().is_none());
        assert!(right.visibility().is_none());

        if left.cardinality() != right.cardinality() {
            return false;
        }

        left.rows()
            .zip_eq(right.rows())
            .all(|(row1, row2)| row1 == row2)
    }

    struct TestFixture {
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, null), (null, 8.4::FLOAT), (3, 3.9::FLOAT), (null, null),
    /// (4, 6.6::FLOAT), (3, null), (null, 0.7::FLOAT), (5, null), (null, 5.5::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (8, 6.1::REAL), (2, null), (null, 8.9::REAL), (3, null), (null, 3.5::REAL),
    /// (6, null), (4, 7.5::REAL), (6, null), (null, 8::REAL), (7, null),
    /// (null, 9.1::REAL), (9, null), (3, 3.7::REAL), (9, null), (null, 9.6::REAL),
    /// (100, null), (null, 8.18::REAL), (200, null);
    /// ```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![DataType::Int32, DataType::Float32],
                right_types: vec![DataType::Int32, DataType::Float64],
                join_type,
            }
        }

        fn create_left_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float32),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i f
                 1 6.1
                 2 .
                 . 8.4
                 3 3.9
                 . .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i f
                 4 6.6
                 3 .
                 . 0.7
                 5 .
                 . 5.5",
            ));

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float64),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            executor.add(DataChunk::from_pretty(
                "i F
                 8 6.1
                 2 .
                 . 8.9
                 3 .
                 . 3.5
                 6 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "i F
                 4 7.5
                 6 .
                 . 8
                 7 .
                 . 9.1
                 9 .  ",
            ));

            executor.add(DataChunk::from_pretty(
                "  i F
                   3 3.7
                   9 .
                   . 9.6
                 100 .
                   . 8.18
                 200 .   ",
            ));

            Box::new(executor)
        }

        fn full_data_types(&self) -> Vec<DataType> {
            [self.left_types.clone(), self.right_types.clone()].concat()
        }

        fn output_data_types(&self) -> Vec<DataType> {
            let join_type = self.join_type;
            if join_type.keep_all() {
                [self.left_types.clone(), self.right_types.clone()].concat()
            } else if join_type.keep_left() {
                self.left_types.clone()
            } else if join_type.keep_right() {
                self.right_types.clone()
            } else {
                unreachable!()
            }
        }

        fn create_cond() -> BoxedExpression {
            let left_expr = InputRefExpression::new(DataType::Float32, 1);
            let right_expr = InputRefExpression::new(DataType::Float64, 3);
            new_binary_expr(
                Type::LessThan,
                DataType::Boolean,
                Box::new(left_expr),
                Box::new(right_expr),
            )
        }

        fn create_join_executor(&self, has_non_equi_cond: bool) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let output_indices = (0..match join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => left_child.schema().fields().len(),
                JoinType::RightSemi | JoinType::RightAnti => right_child.schema().fields().len(),
                _ => left_child.schema().fields().len() + right_child.schema().fields().len(),
            })
                .collect();

            let cond = if has_non_equi_cond {
                Some(Self::create_cond())
            } else {
                None
            };

            Box::new(HashJoinExecutor::<Key32>::new(
                join_type,
                output_indices,
                left_child,
                right_child,
                vec![0],
                vec![0],
                false,
                cond,
                "HashJoinExecutor".to_string(),
            ))
        }

        async fn do_test(&self, expected: DataChunk, has_non_equi_cond: bool) {
            let join_executor = self.create_join_executor(has_non_equi_cond);

            let mut data_chunk_merger = DataChunkMerger::new(self.output_data_types()).unwrap();

            let fields = &join_executor.schema().fields;

            if self.join_type.keep_all() {
                assert_eq!(fields[1].data_type, DataType::Float32);
                assert_eq!(fields[3].data_type, DataType::Float64);
            } else if self.join_type.keep_left() {
                assert_eq!(fields[1].data_type, DataType::Float32);
            } else if self.join_type.keep_right() {
                assert_eq!(fields[1].data_type, DataType::Float64)
            } else {
                unreachable!()
            }

            let mut stream = join_executor.execute();

            while let Some(data_chunk) = stream.next().await {
                let data_chunk = data_chunk.unwrap();
                let data_chunk = data_chunk.compact().unwrap();
                data_chunk_merger.append(&data_chunk).unwrap();
            }

            let result_chunk = data_chunk_merger.finish().unwrap();

            // TODO: Replace this with unsorted comparison
            // assert_eq!(expected, result_chunk);
            assert!(is_data_chunk_eq(&expected, &result_chunk));
        }
    }

    /// Sql:
    /// ```sql
    /// select * from t1 join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   2   .
             3   3.9 3   3.7
             3   3.9 3   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_inner_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   6.6 4   7.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   6.1 .   .
             2   .   2   .
             .   8.4 .   .
             3   3.9 3   3.7
             3   3.9 3   .
             .   .   .   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   6.1 .   .
             2   .   .   .
             .   8.4 .   .
             3   3.9 .   .
             .   .   .   .
             4   6.6 4   7.5
             3   .   .   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 right outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   2   .
             3   3.9 3   3.7
             3   3.9 3   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   .   8   6.1
             .   .   .   8.9
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// Sql:
    /// ```sql
    /// select * from t1 left outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   6.6 4   7.5
             .   .   8   6.1
             .   .   2   .
             .   .   .   8.9
             .   .   3   .
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   3   3.7
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    /// ```sql
    /// select * from t1 full outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   6.1 .   .
             2   .   2   .
             .   8.4 .   .
             3   3.9 3   3.7
             3   3.9 3   .
             .   .   .   .
             4   6.6 4   7.5
             3   .   3   3.7
             3   .   3   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .
             .   .   8   6.1
             .   .   .   8.9
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    /// ```sql
    /// select * from t1 full outer join t2 on t1.v1 = t2.v1 and t1.v2 < t2.v2;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let expected_chunk = DataChunk::from_pretty(
            "i   f   i   F
             2   .   .   .
             3   3.9 .   .
             4   6.6 4   7.5
             3   .   .   .
             1   6.1 .   .
             .   8.4 .   .
             .   .   .   .
             .   0.7 .   .
             5   .   .   .
             .   5.5 .   .
             .   .   8   6.1
             .   .   2   .
             .   .   .   8.9
             .   .   3   .
             .   .   .   3.5
             .   .   6   .
             .   .   6   .
             .   .   .   8
             .   .   7   .
             .   .   .   9.1
             .   .   9   .
             .   .   3   3.7
             .   .   9   .
             .   .   .   9.6
             .   .   100 .
             .   .   .   8.18
             .   .   200 .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             1   6.1
             .   8.4
             .   .
             .   0.7
             5   .
             .   5.5",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_left_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             2   .
             3   3.9
             3   .
             1   6.1
             .   8.4
             .   .
             .   0.7
             5   .
             .   5.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             2   .
             3   3.9
             4   6.6
             3   .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_left_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   f
             4   6.6",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             8   6.1
             .   8.9
             .   3.5
             6   .
             6   .
             .   8.0
             7   .
             .   9.1
             9   .
             9   .
             .   9.6
             100 .
             .   8.18
             200 .",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_right_anti_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             8   6.1
             2   .
             .   8.9
             3   .
             .   3.5
             6   .
             6   .
             .   8
             7   .
             .   9.1
             9   .
             3   3.7
             9   .
             .   9.6
             100 .
             .   8.18
             200 .",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             2   .
             3   .
             4   7.5
             3   3.7",
        );

        test_fixture.do_test(expected_chunk, false).await;
    }

    #[tokio::test]
    async fn test_right_semi_join_with_non_equi_condition() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let expected_chunk = DataChunk::from_pretty(
            "i   F
             4   7.5",
        );

        test_fixture.do_test(expected_chunk, true).await;
    }

    #[tokio::test]
    async fn test_process_left_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             2   4.0 .   .
             3   5.0 .   .
             3   5.0 .   .",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            has_more_output_rows: true,
            found_matched: false,
        };
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0
             5   4.0 .   .
             6   7.0 .   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 .   .
             5   4.0 .   .
             6   7.0 6   8.0",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(state.found_matched);
    }

    #[tokio::test]
    async fn test_process_left_semi_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            found_matched: false,
            ..Default::default()
        };
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0",
        );
        state.first_output_row_id = vec![2, 3];
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             6   7.0 6   8.0",
        );
        state.first_output_row_id = vec![2, 3];
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<false>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(state.found_matched);
    }

    #[tokio::test]
    async fn test_process_left_anti_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             2   4.0 .   .
             3   5.0 3   4.0
             3   5.0 3   4.0",
        );
        let cond = TestFixture::create_cond();
        let mut state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 3, 5, 7],
            has_more_output_rows: true,
            found_matched: false,
        };
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             5   4.0 5   .
             6   7.0 6   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(!state.found_matched);

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   1.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             5   4.0 5   .",
        );
        state.first_output_row_id = vec![2, 3];
        state.has_more_output_rows = false;
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_left_semi_anti_join_non_equi_condition::<true>(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.first_output_row_id, Vec::<usize>::new());
        assert!(state.found_matched);
    }

    #[tokio::test]
    async fn test_process_right_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5",
        );
        let cond = TestFixture::create_cond();
        // For simplicity, all rows are in one chunk.
        // Build side table
        // 0  - (1, 5.5)
        // 1  - (1, 2.5)
        // 2  - ?
        // 3  - (3, 4.0)
        // 4  - (3, 3.0)
        // 5  - (4, 0)
        // 6  - ?
        // 7  - (4, 0.5)
        // 8  - (4, 0.6)
        // 9  - (4, 2.0)
        // 10 - (5, .)
        // 11 - ?
        // 12 - (6, .)
        // 13 - (6, 5.0)
        // Rows with '?' are never matched here.
        let build_row_matched = ChunkedData::with_chunk_sizes([14].into_iter()).unwrap();
        let mut state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched,
        };
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_right_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0",
        );
        state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_right_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v
            }])
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_process_right_semi_anti_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let cond = TestFixture::create_cond();
        let build_row_matched = ChunkedData::with_chunk_sizes([14].into_iter()).unwrap();
        let mut state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched,
        };

        assert!(
            HashJoinExecutor::<Key32>::process_right_semi_anti_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .is_ok()
        );
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   5.0",
        );
        state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(
            HashJoinExecutor::<Key32>::process_right_semi_anti_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut state
            )
            .is_ok()
        );
        assert_eq!(state.build_row_ids, Vec::new());
        assert_eq!(
            state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v
            }])
            .unwrap()
        );
    }

    #[tokio::test]
    async fn test_process_full_outer_join_non_equi_condition() {
        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             1   3.5 1   2.5
             3   5.0 3   4.0
             3   5.0 3   3.0
             3   5.0 3   4.0
             3   5.0 3   3.0
             4   1.0 4   0
             4   1.0 4   0.5",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             1   3.5 1   5.5
             3   5.0 .   .
             3   5.0 .   .",
        );
        let cond = TestFixture::create_cond();
        let mut left_state = LeftNonEquiJoinState {
            probe_column_count: 2,
            first_output_row_id: vec![0, 2, 4, 6],
            has_more_output_rows: true,
            found_matched: false,
        };
        let mut right_state = RightNonEquiJoinState {
            build_row_ids: vec![
                RowId::new(0, 0),
                RowId::new(0, 1),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 3),
                RowId::new(0, 4),
                RowId::new(0, 5),
                RowId::new(0, 7),
            ],
            build_row_matched: ChunkedData::with_chunk_sizes([14].into_iter()).unwrap(),
        };
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_full_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut left_state,
                &mut right_state,
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(left_state.first_output_row_id, Vec::<usize>::new());
        assert!(!left_state.found_matched);
        assert_eq!(right_state.build_row_ids, Vec::new());
        assert_eq!(
            right_state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v
            }])
            .unwrap()
        );

        let chunk = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   0.6
             4   1.0 4   2.0
             5   4.0 5   .
             6   7.0 6   .
             6   7.0 6   8.0",
        );
        let expect = DataChunk::from_pretty(
            "i   f   i   F
             4   1.0 4   2.0
             5   4.0 .   .
             6   7.0 6   8.0",
        );
        left_state.first_output_row_id = vec![2, 3];
        left_state.has_more_output_rows = false;
        right_state.build_row_ids = vec![
            RowId::new(0, 8),
            RowId::new(0, 9),
            RowId::new(0, 10),
            RowId::new(0, 12),
            RowId::new(0, 13),
        ];
        assert!(is_data_chunk_eq(
            &HashJoinExecutor::<Key32>::process_full_outer_join_non_equi_condition(
                chunk,
                cond.as_ref(),
                &mut left_state,
                &mut right_state,
            )
            .unwrap()
            .compact()
            .unwrap(),
            &expect
        ));
        assert_eq!(left_state.first_output_row_id, Vec::<usize>::new());
        assert!(left_state.found_matched);
        assert_eq!(right_state.build_row_ids, Vec::new());
        assert_eq!(
            right_state.build_row_matched,
            ChunkedData::try_from(vec![{
                let mut v = vec![false; 14];
                v[0] = true;
                v[9] = true;
                v[13] = true;
                v
            }])
            .unwrap()
        );
    }
}
