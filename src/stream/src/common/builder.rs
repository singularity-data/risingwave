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

use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilderImpl, ArrayImpl, ArrayResult, Op, Row, RowRef, StreamChunk,
};
use risingwave_common::types::DataType;

/// Build a array and it's corresponding operations.
pub struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,

    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,

    /// Data types of columns
    data_types: Vec<DataType>,
    /// map indices in matched columns to output columns
    matched_indices_mapping: Vec<(usize, usize)>,
    /// map indices in update columns to output columns
    update_indices_mapping: Vec<(usize, usize)>,

    /// Maximum capacity of column builder
    capacity: usize,

    /// Size of column builder
    size: usize,
}

impl Drop for StreamChunkBuilder {
    fn drop(&mut self) {
        assert_eq!(self.size, 0, "dropping non-empty stream chunk builder");
    }
}

impl StreamChunkBuilder {
    pub fn new(
        capacity: usize,
        original_data_types: &[DataType],
        output_indices: &[usize],
        update_range: Range<usize>,
        matched_range: Range<usize>,
    ) -> ArrayResult<Self> {
        // Leave room for paired `UpdateDelete` and `UpdateInsert`. When there are `capacity - 1`
        // ops in current builder and the last op is `UpdateDelete`, we delay the chunk generation
        // until `UpdateInsert` comes. This means that the effective output message size will indeed
        // be at most the original `capacity`
        let reduced_capacity = capacity - 1;
        assert!(reduced_capacity > 0);

        let ops = Vec::with_capacity(reduced_capacity);
        let data_types_after_mapping = output_indices
            .iter()
            .map(|&idx| original_data_types[idx].clone())
            .collect_vec();
        let column_builders = data_types_after_mapping
            .iter()
            .map(|datatype| datatype.create_array_builder(reduced_capacity))
            .collect();
        let (matched_indices_mapping, update_indices_mapping) = {
            let mut matched_indices_mapping = Vec::new();
            let mut update_indices_mapping = Vec::new();
            for (i, &output_idx) in output_indices.iter().enumerate() {
                if matched_range.contains(&output_idx) {
                    matched_indices_mapping.push((output_idx - matched_range.start, i));
                }
                if update_range.contains(&output_idx) {
                    update_indices_mapping.push((output_idx - update_range.start, i));
                }
            }
            (matched_indices_mapping, update_indices_mapping)
        };
        Ok(Self {
            ops,
            column_builders,
            data_types: data_types_after_mapping,
            capacity: reduced_capacity,
            size: 0,
            matched_indices_mapping,
            update_indices_mapping,
        })
    }

    /// Increase chunk size
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    fn inc_size(&mut self) -> ArrayResult<Option<StreamChunk>> {
        self.size += 1;

        // Take a chunk when capacity is exceeded, but splitting `UpdateDelete` and `UpdateInsert`
        // should be avoided
        if self.size >= self.capacity && self.ops[self.ops.len() - 1] != Op::UpdateDelete {
            self.take()
        } else {
            Ok(None)
        }
    }

    /// Append a row with coming update value and matched value
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row(
        &mut self,
        op: Op,
        row_update: &RowRef<'_>,
        row_matched: &Row,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for &(update_idx, output_idx) in &self.update_indices_mapping {
            self.column_builders[output_idx].append_datum_ref(row_update.value_at(update_idx))?;
        }
        for &(matched_idx, output_idx) in &self.matched_indices_mapping {
            self.column_builders[output_idx].append_datum(&row_matched[matched_idx])?;
        }
        self.inc_size()
    }

    /// Append a row with coming update value and fill the other side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row_update(
        &mut self,
        op: Op,
        row_update: &RowRef<'_>,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for &(update_idx, output_idx) in &self.update_indices_mapping {
            self.column_builders[output_idx].append_datum_ref(row_update.value_at(update_idx))?;
        }
        for &(_matched_idx, output_idx) in &self.matched_indices_mapping {
            self.column_builders[output_idx].append_datum(&None)?;
        }
        self.inc_size()
    }

    /// append a row with matched value and fill the coming side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row_matched(
        &mut self,
        op: Op,
        row_matched: &Row,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for &(_update_idx, output_idx) in &self.update_indices_mapping {
            self.column_builders[output_idx].append_datum_ref(None)?;
        }
        for &(matched_idx, output_idx) in &self.matched_indices_mapping {
            self.column_builders[output_idx].append_datum(&row_matched[matched_idx])?;
        }
        self.inc_size()
    }

    pub fn take(&mut self) -> ArrayResult<Option<StreamChunk>> {
        self.size = 0;
        let new_arrays: Vec<ArrayImpl> = self
            .column_builders
            .iter_mut()
            .zip_eq(&self.data_types)
            .map(|(builder, datatype)| {
                std::mem::replace(builder, datatype.create_array_builder(self.capacity)).finish()
            })
            .try_collect()?;
        let new_columns = new_arrays
            .into_iter()
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();
        Ok(Some(StreamChunk::new(
            std::mem::take(&mut self.ops),
            new_columns,
            None,
        )))
    }
}
