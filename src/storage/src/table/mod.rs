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

pub mod cell_based_table;
pub mod mem_table;
pub mod state_table;

#[cfg(test)]
pub mod test_relational_table;

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::types::VIRTUAL_NODE_COUNT;

use crate::error::StorageResult;

/// Represents the distribution for a specific table instance.
pub struct Distribution {
    /// Indices of distribution keys for computing vnode, based on the all columns of the table.
    pub dist_key_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    pub vnodes: Arc<Bitmap>,
}

impl Distribution {
    /// Fallback distribution for singleton or tests.
    pub fn fallback() -> Self {
        lazy_static::lazy_static! {
            /// A bitmap that only the vnode `0x0000` is set. Used for fallback or no distribution.
            static ref FALLBACK_VNODES: Arc<Bitmap> = {
                let mut vnodes = BitmapBuilder::zeroed(VIRTUAL_NODE_COUNT);
                vnodes.set(0, true);
                vnodes.finish().into()
            };
        }
        Self {
            dist_key_indices: vec![],
            vnodes: FALLBACK_VNODES.clone(),
        }
    }
}

// TODO: GAT-ify this trait or remove this trait
#[async_trait::async_trait]
pub trait TableIter: Send {
    async fn next_row(&mut self) -> StorageResult<Option<Row>>;

    async fn collect_data_chunk(
        &mut self,
        schema: &Schema,
        chunk_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        let mut builders = schema.create_array_builders(chunk_size.unwrap_or(0));

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match self.next_row().await? {
                Some(row) => {
                    for (datum, builder) in row.0.into_iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&datum)?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(Into::into))
                .try_collect()?;
            DataChunk::new(columns, row_count)
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}
