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

mod chunked_data;
pub mod hash_join;
pub mod lookup_join;
pub mod nested_loop_join;
mod sort_merge_join;

use std::sync::Arc;

pub use chunked_data::*;
pub use hash_join::*;
use itertools::Itertools;
pub use lookup_join::*;
pub use nested_loop_join::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, RowRef, Vis};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, DatumRef};
use risingwave_pb::plan_common::JoinType as JoinTypeProst;
pub use sort_merge_join::*;

use crate::error::BatchError;
use crate::executor::join::JoinType::Inner;
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    /// Semi join when probe side should output when matched
    LeftSemi,
    /// Anti join when probe side should not output when matched
    LeftAnti,
    RightOuter,
    /// Semi join when build side should output when matched
    RightSemi,
    /// Anti join when build side should output when matched
    RightAnti,
    FullOuter,
}

impl JoinType {
    #[inline(always)]
    pub(super) fn need_join_remaining(self) -> bool {
        matches!(
            self,
            JoinType::RightOuter | JoinType::RightAnti | JoinType::FullOuter
        )
    }

    pub fn from_prost(prost: JoinTypeProst) -> Self {
        match prost {
            JoinTypeProst::Inner => JoinType::Inner,
            JoinTypeProst::LeftOuter => JoinType::LeftOuter,
            JoinTypeProst::LeftSemi => JoinType::LeftSemi,
            JoinTypeProst::LeftAnti => JoinType::LeftAnti,
            JoinTypeProst::RightOuter => JoinType::RightOuter,
            JoinTypeProst::RightSemi => JoinType::RightSemi,
            JoinTypeProst::RightAnti => JoinType::RightAnti,
            JoinTypeProst::FullOuter => JoinType::FullOuter,
            JoinTypeProst::Unspecified => unreachable!(),
        }
    }

    fn need_build(self) -> bool {
        match self {
            JoinType::RightSemi => true,
            other => other.need_join_remaining(),
        }
    }

    fn need_probe(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::LeftAnti | JoinType::LeftSemi
        )
    }

    fn keep_all(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::RightOuter | JoinType::Inner
        )
    }

    fn keep_left(self) -> bool {
        matches!(self, JoinType::LeftAnti | JoinType::LeftSemi)
    }

    fn keep_right(self) -> bool {
        matches!(self, JoinType::RightAnti | JoinType::RightSemi)
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}

/// The layout be like:
///
/// [ `left` chunk     |  `right` chunk     ]
///
/// # Arguments
///
/// * `left` Data chunk padded to the left half of result data chunk..
/// * `right` Data chunk padded to the right half of result data chunk.
///
/// Note: Use this function with careful: It is not designed to be a general concatenate of two
/// chunk: Usually one side should be const row chunk and the other side is normal chunk.
/// Currently only feasible to use in join executor.
/// If two normal chunk, the result is undefined.
fn concatenate(left: &DataChunk, right: &DataChunk) -> Result<DataChunk> {
    assert_eq!(left.capacity(), right.capacity());
    let mut concated_columns = Vec::with_capacity(left.columns().len() + right.columns().len());
    concated_columns.extend_from_slice(left.columns());
    concated_columns.extend_from_slice(right.columns());
    // Only handle one side is constant row chunk: One of visibility must be None.
    let vis = match (left.vis(), right.vis()) {
        (Vis::Compact(_), _) => right.vis().clone(),
        (_, Vis::Compact(_)) => left.vis().clone(),
        (Vis::Bitmap(_), Vis::Bitmap(_)) => {
            return Err(BatchError::UnsupportedFunction(
                "The concatenate behaviour of two chunk with visibility is undefined".to_string(),
            )
            .into())
        }
    };
    let data_chunk = DataChunk::new(concated_columns, vis);
    Ok(data_chunk)
}

/// Create constant data chunk (one tuple repeat `num_tuples` times).
fn convert_datum_refs_to_chunk(
    datum_refs: &[DatumRef<'_>],
    num_tuples: usize,
    data_types: &[DataType],
) -> Result<DataChunk> {
    let mut output_array_builders: Vec<_> = data_types
        .iter()
        .map(|data_type| data_type.create_array_builder(num_tuples))
        .collect();
    for _i in 0..num_tuples {
        for (builder, datum_ref) in output_array_builders.iter_mut().zip_eq(datum_refs) {
            builder.append_datum_ref(*datum_ref)?;
        }
    }

    // Finish each array builder and get Column.
    let result_columns = output_array_builders
        .into_iter()
        .map(|builder| builder.finish().map(|arr| Column::new(Arc::new(arr))))
        .try_collect()?;

    Ok(DataChunk::new(result_columns, num_tuples))
}

/// Create constant data chunk (one tuple repeat `num_tuples` times).
fn convert_row_to_chunk(
    row_ref: &RowRef<'_>,
    num_tuples: usize,
    data_types: &[DataType],
) -> Result<DataChunk> {
    let datum_refs = row_ref.values().collect_vec();
    convert_datum_refs_to_chunk(&datum_refs, num_tuples, data_types)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayBuilder, DataChunk, PrimitiveArrayBuilder, Vis};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarRefImpl};

    use crate::executor::join::{concatenate, convert_datum_refs_to_chunk};

    #[test]
    fn test_concatenate() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length);
            for _ in 0..length {
                builder.append(Some(i as i32)).unwrap();
            }
            let arr = builder.finish().unwrap();
            columns.push(Column::new(Arc::new(arr.into())))
        }
        let chunk1: DataChunk = DataChunk::new(columns.clone(), length);
        let bool_vec = vec![true, false, true, false, false];
        let chunk2: DataChunk = DataChunk::new(
            columns.clone(),
            Vis::Bitmap((bool_vec.clone()).into_iter().collect()),
        );
        let chunk = concatenate(&chunk1, &chunk2).unwrap();
        assert_eq!(chunk.capacity(), chunk1.capacity());
        assert_eq!(chunk.capacity(), chunk2.capacity());
        assert_eq!(chunk.columns().len(), chunk1.columns().len() * 2);
        assert_eq!(
            chunk.visibility().cloned().unwrap(),
            (bool_vec).into_iter().collect()
        );
    }

    /// Test the function of convert row into constant row chunk (one row repeat multiple times).
    #[test]
    fn test_convert_row_to_chunk() {
        let row = vec![Some(ScalarRefImpl::Int32(3))];
        let probe_side_schema = Schema {
            fields: vec![Field::unnamed(DataType::Int32)],
        };
        let const_row_chunk =
            convert_datum_refs_to_chunk(&row, 5, &probe_side_schema.data_types()).unwrap();
        assert_eq!(const_row_chunk.capacity(), 5);
        assert_eq!(
            const_row_chunk.row_at(2).unwrap().0.value_at(0),
            Some(ScalarRefImpl::Int32(3))
        );
    }
}
