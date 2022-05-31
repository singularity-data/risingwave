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
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use itertools::Itertools;
use prost::DecodeError;
use risingwave_pb::data::{Op as ProstOp, StreamChunk as ProstStreamChunk};

use crate::array::column::Column;
use crate::array::{ArrayBuilderImpl, DataChunk, Row};
use crate::buffer::Bitmap;
use crate::error::{ErrorCode, Result, RwError};
use crate::types::{DataType, NaiveDateTimeWrapper};
use crate::util::hash_util::finalize_hashers;

/// `Op` represents three operations in `StreamChunk`.
///
/// `UpdateDelete` and `UpdateInsert` are semantically equivalent to `Delete` and `Insert`
/// but always appear in pairs to represent an update operation.
/// For example, table source, aggregation and outer join can generate updates by themselves,
/// while most of the other operators only pass through updates with best effort.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum Op {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

impl Op {
    pub fn to_protobuf(self) -> ProstOp {
        match self {
            Op::Insert => ProstOp::Insert,
            Op::Delete => ProstOp::Delete,
            Op::UpdateInsert => ProstOp::UpdateInsert,
            Op::UpdateDelete => ProstOp::UpdateDelete,
        }
    }

    pub fn from_protobuf(prost: &i32) -> Result<Op> {
        let op = match ProstOp::from_i32(*prost) {
            Some(ProstOp::Insert) => Op::Insert,
            Some(ProstOp::Delete) => Op::Delete,
            Some(ProstOp::UpdateInsert) => Op::UpdateInsert,
            Some(ProstOp::UpdateDelete) => Op::UpdateDelete,
            None => {
                return Err(RwError::from(ErrorCode::ProstError(DecodeError::new(
                    "No such op type",
                ))))
            }
        };
        Ok(op)
    }
}

pub type Ops<'a> = &'a [Op];

/// `StreamChunk` is used to pass data over the streaming pathway.
#[derive(Clone, PartialEq)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    ops: Vec<Op>,

    pub(super) data: DataChunk,
}

impl Default for StreamChunk {
    /// Create a 0-row-0-col `StreamChunk`. Only used in some existing tests.
    /// This is NOT the same as an **empty** chunk, which has 0 rows but with
    /// columns aligned with executor schema.
    fn default() -> Self {
        Self {
            ops: Default::default(),
            data: DataChunk::new(vec![], None),
        }
    }
}

impl StreamChunk {
    pub fn new(ops: Vec<Op>, columns: Vec<Column>, visibility: Option<Bitmap>) -> Self {
        for col in &columns {
            assert_eq!(col.array_ref().len(), ops.len());
        }

        let data = DataChunk::new(columns, visibility);
        StreamChunk { ops, data }
    }

    /// Build a `StreamChunk` from rows.
    // TODO: introducing something like `StreamChunkBuilder` maybe better.
    pub fn from_rows(rows: &[(Op, Row)], data_types: &[DataType]) -> Result<Self> {
        let mut array_builders = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(rows.len()))
            .collect::<Result<Vec<_>>>()?;
        let mut ops = vec![];

        for (op, row) in rows {
            ops.push(*op);
            for (datum, builder) in row.0.iter().zip_eq(array_builders.iter_mut()) {
                builder.append_datum(datum)?;
            }
        }

        let new_arrays = array_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();
        Ok(StreamChunk::new(ops, new_columns, None))
    }

    /// `cardinality` return the number of visible tuples
    pub fn cardinality(&self) -> usize {
        self.data.cardinality()
    }

    /// `capacity` return physical length of internals ops & columns
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn columns(&self) -> &[Column] {
        self.data.columns()
    }

    pub fn column_at(&self, index: usize) -> &Column {
        self.data.column_at(index)
    }

    /// compact the `StreamChunk` with its visibility map
    pub fn compact(self) -> Result<Self> {
        if self.visibility().is_none() {
            return Ok(self);
        }

        let (ops, columns, visibility) = self.into_inner();
        let visibility = visibility.unwrap();

        let cardinality = visibility
            .iter()
            .fold(0, |vis_cnt, vis| vis_cnt + vis as usize);
        let columns = columns
            .into_iter()
            .map(|col| {
                let array = col.array();
                array
                    .compact(&visibility, cardinality)
                    .map(|array| Column::new(Arc::new(array)))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut new_ops = Vec::with_capacity(cardinality);
        for (op, visible) in ops.into_iter().zip_eq(visibility.iter()) {
            if visible {
                new_ops.push(op);
            }
        }
        Ok(StreamChunk::new(new_ops, columns, None))
    }

    pub fn into_parts(self) -> (DataChunk, Vec<Op>) {
        (self.data, self.ops)
    }

    pub fn from_parts(ops: Vec<Op>, data_chunk: DataChunk) -> Self {
        let (columns, visibility) = data_chunk.into_parts();
        Self::new(ops, columns, visibility)
    }

    pub fn into_inner(self) -> (Vec<Op>, Vec<Column>, Option<Bitmap>) {
        let (columns, visibility) = self.data.into_parts();
        (self.ops, columns, visibility)
    }

    pub fn to_protobuf(&self) -> ProstStreamChunk {
        ProstStreamChunk {
            cardinality: self.cardinality() as u32,
            ops: self.ops.iter().map(|op| op.to_protobuf() as i32).collect(),
            columns: self.columns().iter().map(|col| col.to_protobuf()).collect(),
        }
    }

    pub fn from_protobuf(prost: &ProstStreamChunk) -> Result<Self> {
        let cardinality = prost.get_cardinality() as usize;
        let mut ops = Vec::with_capacity(cardinality);
        for op in prost.get_ops() {
            ops.push(Op::from_protobuf(op)?);
        }
        let mut columns = vec![];
        for column in prost.get_columns() {
            columns.push(Column::from_protobuf(column, cardinality)?);
        }
        Ok(StreamChunk::new(ops, columns, None))
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn visibility(&self) -> Option<&Bitmap> {
        self.data.visibility()
    }

    pub fn get_hash_values<H: BuildHasher>(
        &self,
        keys: &[usize],
        hasher_builder: H,
    ) -> Result<Vec<u64>> {
        let mut states = vec![];
        states.resize_with(self.capacity(), || hasher_builder.build_hasher());
        for key in keys {
            let array = self.columns()[*key].array();
            array.hash_vec(&mut states[..]);
        }
        Ok(finalize_hashers(&mut states[..]))
    }

    /// `to_pretty_string` returns a table-like text representation of the `StreamChunk`.
    pub fn to_pretty_string(&self) -> String {
        use comfy_table::{Cell, CellAlignment, Table};

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        for (op, row_ref) in self.rows() {
            let mut cells = Vec::with_capacity(row_ref.size() + 1);
            cells.push(
                Cell::new(match op {
                    Op::Insert => "+",
                    Op::Delete => "-",
                    Op::UpdateDelete => "U-",
                    Op::UpdateInsert => "U+",
                })
                .set_alignment(CellAlignment::Right),
            );
            for datum in row_ref.values() {
                let str = match datum {
                    None => "".to_owned(), // NULL
                    Some(scalar) => scalar.to_string(),
                };
                cells.push(Cell::new(&str));
            }
            table.add_row(cells);
        }
        table.to_string()
    }

    /// Reorder columns. e.g. if `column_mapping` is `[2, 1, 0]`, and
    /// the chunk contains column `[a, b, c]`, then the output will be
    /// `[c, b, a]`.
    pub fn reorder_columns(self, column_mapping: &[usize]) -> Self {
        Self {
            ops: self.ops,
            data: self.data.reorder_columns(column_mapping),
        }
    }

    /// Two `StreamChunk`s are semantically equivalent if every row
    /// has the same order of updates.
    pub fn semantically_equivalent_to(&self, other: &Self) -> bool {
        let mut self_row_updates = HashMap::<Row, Vec<Op>>::new();
        let mut other_row_updates = HashMap::<Row, Vec<Op>>::new();
        for (op, row) in self.rows() {
            let entry = self_row_updates
                .entry(row.to_owned_row())
                .or_insert_with(Vec::new);
            entry.push(op);
        }
        for (op, row) in other.rows() {
            let entry = other_row_updates
                .entry(row.to_owned_row())
                .or_insert_with(Vec::new);
            entry.push(op);
        }
        self_row_updates == other_row_updates
    }
}

impl fmt::Debug for StreamChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamChunk {{ cardinality = {}, capacity = {}, data = \n{} }}",
            self.cardinality(),
            self.capacity(),
            self.to_pretty_string()
        )
    }
}

/// Test utilities for [`StreamChunk`].
pub trait StreamChunkTestExt {
    fn from_pretty(s: &str) -> Self;
}

impl StreamChunkTestExt for StreamChunk {
    /// Parse a chunk from string.
    ///
    /// # Format
    ///
    /// The first line is a header indicating the column types.
    /// The following lines indicate rows within the chunk.
    /// Each line starts with an operation followed by values.
    /// NULL values are represented as `.`.
    ///
    /// # Example
    /// ```
    /// use risingwave_common::array::stream_chunk::StreamChunkTestExt as _;
    /// use risingwave_common::array::StreamChunk;
    /// let chunk = StreamChunk::from_pretty(
    ///     "  I I I I      // type chars
    ///     U- 2 5 . .      // '.' means NULL
    ///     U+ 2 5 2 6 D    // 'D' means deleted in visibility
    ///     +  . . 4 8      // ^ comments are ignored
    ///     -  . . 3 4",
    /// );
    /// //  ^ operations:
    /// //     +: Insert
    /// //     -: Delete
    /// //    U+: UpdateInsert
    /// //    U-: UpdateDelete
    ///
    /// // type chars:
    /// //     I: i64
    /// //     i: i32
    /// //     F: f64
    /// //     f: f32
    /// //     T: str
    /// //    TS: Timestamp
    /// ```
    fn from_pretty(s: &str) -> Self {
        use crate::types::ScalarImpl;

        let mut lines = s.split('\n').filter(|l| !l.trim().is_empty());
        let mut ops = vec![];
        // initialize array builders from the first line
        let header = lines.next().unwrap().trim();
        let mut array_builders = header
            .split_ascii_whitespace()
            .take_while(|c| *c != "//")
            .map(|c| match c {
                "I" => DataType::Int64,
                "i" => DataType::Int32,
                "F" => DataType::Float64,
                "f" => DataType::Float32,
                "TS" => DataType::Timestamp,
                "T" => DataType::Varchar,
                _ => todo!("unsupported type: {c:?}"),
            })
            .map(|ty| ty.create_array_builder(1))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut visibility = vec![];
        for mut line in lines {
            line = line.trim();
            let mut token = line.split_ascii_whitespace();
            let op = match token.next().expect("missing operation") {
                "+" => Op::Insert,
                "-" => Op::Delete,
                "U+" => Op::UpdateInsert,
                "U-" => Op::UpdateDelete,
                t => panic!("invalid op: {t:?}"),
            };
            ops.push(op);
            // allow `zip` since `token` may longer than `array_builders`
            #[allow(clippy::disallowed_methods)]
            for (builder, val_str) in array_builders.iter_mut().zip(&mut token) {
                let datum = match val_str {
                    "." => None,
                    s if matches!(builder, ArrayBuilderImpl::Int32(_)) => Some(ScalarImpl::Int32(
                        s.parse()
                            .map_err(|_| panic!("invalid int32: {s:?}"))
                            .unwrap(),
                    )),
                    s if matches!(builder, ArrayBuilderImpl::Int64(_)) => Some(ScalarImpl::Int64(
                        s.parse()
                            .map_err(|_| panic!("invalid int64: {s:?}"))
                            .unwrap(),
                    )),
                    s if matches!(builder, ArrayBuilderImpl::Float64(_)) => {
                        Some(ScalarImpl::Float64(
                            s.parse()
                                .map_err(|_| panic!("invalid float64: {s:?}"))
                                .unwrap(),
                        ))
                    }
                    s if matches!(builder, ArrayBuilderImpl::NaiveDateTime(_)) => {
                        Some(ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper(
                            s.parse()
                                .map_err(|_| panic!("invalid datetime: {s:?}"))
                                .unwrap(),
                        )))
                    }
                    s if matches!(builder, ArrayBuilderImpl::Utf8(_)) => {
                        Some(ScalarImpl::Utf8(s.into()))
                    }
                    _ => panic!("invalid data type"),
                };
                builder
                    .append_datum(&datum)
                    .expect("failed to append datum");
            }
            let visible = match token.next() {
                None | Some("//") => true,
                Some("D") => false,
                Some(t) => panic!("invalid token: {t:?}"),
            };
            visibility.push(visible);
        }
        let columns = array_builders
            .into_iter()
            .map(|builder| Column::new(Arc::new(builder.finish().unwrap())))
            .collect();
        let visibility = if visibility.iter().all(|b| *b) {
            None
        } else {
            Some(Bitmap::try_from(visibility).unwrap())
        };
        StreamChunk::new(ops, columns, visibility)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::I64Array;
    use crate::{column, column_nonnull};

    #[test]
    fn test_to_pretty_string() {
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Delete, Op::UpdateDelete, Op::UpdateInsert],
            vec![
                column_nonnull!(I64Array, [1, 2, 3, 4]),
                column!(I64Array, [Some(6), None, Some(7), None]),
            ],
            None,
        );
        assert_eq!(
            chunk.to_pretty_string(),
            "\
+----+---+---+
|  + | 1 | 6 |
|  - | 2 |   |
| U- | 3 | 7 |
| U+ | 4 |   |
+----+---+---+"
        );
    }
}
