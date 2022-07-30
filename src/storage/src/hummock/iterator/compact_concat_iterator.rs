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

use std::cmp::Ordering;
use std::future::Future;
use std::sync::Arc;

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SstableIterator};
use crate::monitor::StoreLocalStatistic;

pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<SstableIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    tables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    read_options: Arc<SstableIteratorReadOptions>,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        tables: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            tables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            read_options,
        }
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.tables.len() {
            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }
        } else {
            let table = if self.read_options.prefetch {
                self.sstable_store
                    .load_table(self.tables[idx].id, true, &mut self.stats)
                    .await?
            } else {
                self.sstable_store
                    .sstable(self.tables[idx].id, &mut self.stats)
                    .await?
            };
            let mut sstable_iter =
                SstableIterator::new(table, self.sstable_store.clone(), self.read_options.clone());

            if let Some(key) = seek_key {
                sstable_iter.seek(key).await?;
            } else {
                sstable_iter.rewind().await?;
            }

            if let Some(old_iter) = self.sstable_iter.take() {
                old_iter.collect_local_statistic(&mut self.stats);
            }

            self.sstable_iter = Some(sstable_iter);
            self.cur_idx = idx;
        }
        Ok(())
    }
}

impl HummockIterator for ConcatSstableIterator {
    type Direction = Forward;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");
            sstable_iter.next_inner().await?;

            if sstable_iter.is_valid() {
                Ok(())
            } else {
                // seek to next table
                self.seek_idx(self.cur_idx + 1, None).await
            }
        }
    }

    fn key(&self) -> &[u8] {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async { self.seek_idx(0, None).await }
    }

    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async {
            let table_idx = self
                .tables
                .partition_point(|table| {
                    let ord = VersionedComparator::compare_key(
                        &table.key_range.as_ref().unwrap().left,
                        key,
                    );
                    ord == Ordering::Less || ord == Ordering::Equal
                })
                .saturating_sub(1); // considering the boundary of 0

            self.seek_idx(table_idx, Some(key)).await?;
            if !self.is_valid() {
                // Seek to next table
                self.seek_idx(table_idx + 1, None).await?;
            }
            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }
}
