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

use std::ops::{Index, IndexMut};

use itertools::repeat_n;
use risingwave_common::error::{Result, RwError};

/// Id of one row in chunked data.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub struct RowId {
    chunk_id: u32,
    row_id: u32,
}

/// [`ChunkedData`] is in fact a list of list.
///
/// We use this data structure instead of [`Vec<Vec<V>>`] to save allocation call.
#[derive(Debug, Default, PartialEq)]
pub(super) struct ChunkedData<V> {
    data: Vec<V>,
    chunk_offsets: Vec<usize>,
}

pub(super) struct AllRowIdIter<'a> {
    cur: RowId,
    chunk_offsets: &'a [usize],
}

impl<'a> Iterator for AllRowIdIter<'a> {
    type Item = RowId;

    fn next(&mut self) -> Option<Self::Item> {
        if (self.cur.chunk_id() + 1) >= self.chunk_offsets.len() {
            None
        } else {
            let ret = Some(self.cur);
            let current_chunk_row_count = self.chunk_offsets[self.cur.chunk_id() + 1]
                - self.chunk_offsets[self.cur.chunk_id()];
            self.cur = self.cur.next_row(current_chunk_row_count);
            ret
        }
    }
}

impl RowId {
    pub(super) fn new(chunk_id: usize, row_id: usize) -> Self {
        Self {
            chunk_id: chunk_id as u32,
            row_id: row_id as u32,
        }
    }

    #[inline(always)]
    pub(super) fn chunk_id(&self) -> usize {
        self.chunk_id as usize
    }

    #[inline(always)]
    pub(super) fn row_id(&self) -> usize {
        self.row_id as usize
    }

    #[inline(always)]
    pub(super) fn next_row(self, cur_chunk_row_count: usize) -> RowId {
        if (self.row_id + 1) >= (cur_chunk_row_count as u32) {
            RowId {
                chunk_id: self.chunk_id + 1,
                row_id: 0,
            }
        } else {
            RowId {
                chunk_id: self.chunk_id,
                row_id: self.row_id + 1,
            }
        }
    }
}

impl<V> ChunkedData<V> {
    pub(super) fn with_chunk_sizes<C>(chunk_sizes: C) -> Result<Self>
    where
        C: IntoIterator<Item = usize>,
        V: Default,
    {
        let chunk_sizes = chunk_sizes.into_iter();
        let mut chunk_offsets = Vec::with_capacity(chunk_sizes.size_hint().0 + 1);
        let mut cur = 0usize;
        chunk_offsets.push(0);
        for chunk_size in chunk_sizes {
            ensure!(chunk_size > 0, "Chunk size can't be zero!");
            cur += chunk_size;
            chunk_offsets.push(cur);
        }

        let mut data = Vec::with_capacity(cur);
        data.resize_with(cur, V::default);

        Ok(Self {
            data,
            chunk_offsets,
        })
    }

    fn index_in_data(&self, index: RowId) -> usize {
        self.chunk_offsets[index.chunk_id()] + index.row_id()
    }

    pub(super) fn all_row_ids(&self) -> impl Iterator<Item = RowId> + '_ {
        AllRowIdIter {
            cur: RowId::default(),
            chunk_offsets: &self.chunk_offsets,
        }
    }

    pub(super) fn next_row_id(&self, cur: RowId) -> Option<RowId> {
        let current_chunk_row_count =
            self.chunk_offsets[cur.chunk_id() + 1] - self.chunk_offsets[cur.chunk_id()];
        let next = cur.next_row(current_chunk_row_count);
        if (next.chunk_id() + 1) >= self.chunk_offsets.len() {
            None
        } else {
            Some(next)
        }
    }
}

impl<V> Index<RowId> for ChunkedData<V> {
    type Output = V;

    fn index(&self, index: RowId) -> &V {
        &self.data[self.index_in_data(index)]
    }
}

impl<V> IndexMut<RowId> for ChunkedData<V> {
    fn index_mut(&mut self, index: RowId) -> &mut V {
        let index_in_data = self.index_in_data(index);
        &mut self.data[index_in_data]
    }
}

impl<V> TryFrom<Vec<Vec<V>>> for ChunkedData<V> {
    type Error = RwError;

    fn try_from(value: Vec<Vec<V>>) -> Result<Self> {
        let chunk_offsets = repeat_n(Ok(0), 1)
            .chain(value.iter().map(|chunk| -> Result<usize> {
                ensure!(!chunk.is_empty(), "Chunk size can't be zero!");
                Ok(chunk.len())
            }))
            .try_collect()?;
        let data = value.into_iter().flatten().collect();
        Ok(Self {
            data,
            chunk_offsets,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_row_ids() {
        let chunk_sizes = vec![4, 3, 1, 2usize];

        let chunked_data =
            ChunkedData::<()>::with_chunk_sizes(chunk_sizes).expect("Build chunked data.");
        let expected_all_row_ids = vec![
            RowId::new(0, 0),
            RowId::new(0, 1),
            RowId::new(0, 2),
            RowId::new(0, 3),
            RowId::new(1, 0),
            RowId::new(1, 1),
            RowId::new(1, 2),
            RowId::new(2, 0),
            RowId::new(3, 0),
            RowId::new(3, 1),
        ];

        assert_eq!(
            expected_all_row_ids,
            chunked_data.all_row_ids().collect::<Vec<RowId>>()
        );
    }

    #[test]
    fn test_indexes() {
        let chunk_sizes = vec![4, 3, 1, 2usize];

        let mut chunked_data =
            ChunkedData::<usize>::with_chunk_sizes(chunk_sizes).expect("Build chunked data.");

        let row_ids = vec![
            RowId::new(0, 3),
            RowId::new(1, 1),
            RowId::new(2, 0),
            RowId::new(3, 1),
        ];

        for row_id in &row_ids {
            chunked_data[*row_id] = row_id.chunk_id() + row_id.row_id();
        }

        for row_id in &row_ids {
            let expected = row_id.chunk_id() + row_id.row_id();
            assert_eq!(expected, chunked_data[*row_id]);
        }
    }

    #[test]
    fn test_try_from() {
        assert_eq!(
            ChunkedData {
                data: vec![1, 2, 3, 4, 5, 6, 7, 9, 8, 7, 6, 5, 123],
                chunk_offsets: vec![0, 4, 3, 5, 1],
            },
            ChunkedData::try_from(vec![
                vec![1, 2, 3, 4],
                vec![5, 6, 7],
                vec![9, 8, 7, 6, 5],
                vec![123],
            ])
            .unwrap()
        );
    }

    #[test]
    #[should_panic]
    fn test_zero_chunk_size_should_fail() {
        let chunk_sizes = vec![4, 3, 0, 1, 2usize];
        ChunkedData::<()>::with_chunk_sizes(chunk_sizes).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_try_from_zero_chunk_size_should_fail() {
        let chunks = vec![vec![0; 4], vec![0; 3], vec![], vec![0; 1], vec![0, 2]];
        ChunkedData::try_from(chunks).unwrap();
    }
}
