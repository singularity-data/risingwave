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

use std::future::Future;
use std::ops::Bound::{Excluded, Included};
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::hummock::LevelType;

use super::iterator::{
    BackwardUserIterator, ConcatIteratorInner, DirectedUserIterator, UserIterator,
};
use super::utils::{can_concat, search_sst_idx, validate_epoch};
use super::{BackwardSstableIterator, HummockStorage, SstableIterator, SstableIteratorType};
use crate::error::StorageResult;
use crate::hummock::iterator::{
    Backward, DirectedUserIteratorBuilder, DirectionEnum, Forward, HummockIteratorDirection,
    HummockIteratorUnion,
};
use crate::hummock::local_version::PinnedVersion;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{
    build_ordered_merge_iter, OrderSortedUncommittedData, UncommittedData,
};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::utils::prune_ssts;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

pub(crate) trait HummockIteratorType {
    type Direction: HummockIteratorDirection;
    type SstableIteratorType: SstableIteratorType<Direction = Self::Direction>;
    type UserIteratorBuilder: DirectedUserIteratorBuilder<
        Direction = Self::Direction,
        SstableIteratorType = Self::SstableIteratorType,
    >;

    fn direction() -> DirectionEnum {
        Self::Direction::direction()
    }
}

pub(crate) struct ForwardIter;
pub(crate) struct BackwardIter;

impl HummockIteratorType for ForwardIter {
    type Direction = Forward;
    type SstableIteratorType = SstableIterator;
    type UserIteratorBuilder = UserIterator;
}

impl HummockIteratorType for BackwardIter {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;
    type UserIteratorBuilder = BackwardUserIterator;
}

impl HummockStorage {
    async fn iter_inner<R, B, T, P>(
        &self,
        _prefix_key: P,
        key_range: R,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStateStoreIter>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
        T: HummockIteratorType,
        P: AsRef<[u8]> + Send,
    {
        let epoch = read_options.epoch;
        let compaction_group_id = match read_options.table_id.as_ref() {
            None => None,
            Some(table_id) => Some(self.get_compaction_group_id(*table_id).await?),
        };
        let min_epoch = read_options.min_epoch();
        let iter_read_options = Arc::new(SstableIteratorReadOptions::default());
        let mut overlapped_iters = vec![];

        let (shared_buffer_data, pinned_version) = self.read_filter(&read_options, &key_range)?;

        let mut stats = StoreLocalStatistic::default();

        for (replicated_batches, uncommitted_data) in shared_buffer_data {
            for batch in replicated_batches {
                overlapped_iters.push(HummockIteratorUnion::First(batch.into_directed_iter()));
            }
            overlapped_iters.push(HummockIteratorUnion::Second(
                build_ordered_merge_iter::<T>(
                    &uncommitted_data,
                    self.sstable_store.clone(),
                    self.stats.clone(),
                    &mut stats,
                    iter_read_options.clone(),
                )
                .await?,
            ));
        }
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["memory-iter"])
            .observe(overlapped_iters.len() as f64);

        // Generate iterators for versioned ssts by filter out ssts that do not overlap with given
        // `key_range`

        // The correctness of using compaction_group_id in read path and write path holds only
        // because we are currently:
        //
        // a) adopting static compaction group. It means a table_id->compaction_group mapping would
        // never change since creation until the table is dropped.
        //
        // b) enforcing shared buffer to split output SSTs by compaction group. It means no SSTs
        // would contain tables from different compaction_group, even for those in L0.
        //
        // When adopting dynamic compaction group in the future, be sure to revisit this assumption.
        for level in pinned_version.levels(compaction_group_id) {
            let table_infos = prune_ssts(level.table_infos.iter(), &key_range);
            if table_infos.is_empty() {
                continue;
            }
            if level.level_type == LevelType::Nonoverlapping as i32 {
                debug_assert!(can_concat(&table_infos));
                let start_table_idx = match key_range.start_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => 0,
                };
                let end_table_idx = match key_range.end_bound() {
                    Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                    _ => table_infos.len().saturating_sub(1),
                };
                assert!(start_table_idx < table_infos.len() && end_table_idx < table_infos.len());
                let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                let tables = match T::Direction::direction() {
                    DirectionEnum::Backward => matched_table_infos
                        .iter()
                        .rev()
                        .map(|&info| info.clone())
                        .collect_vec(),
                    DirectionEnum::Forward => matched_table_infos
                        .iter()
                        .map(|&info| info.clone())
                        .collect_vec(),
                };

                overlapped_iters.push(HummockIteratorUnion::Third(ConcatIteratorInner::<
                    T::SstableIteratorType,
                >::new(
                    tables,
                    self.sstable_store(),
                    iter_read_options.clone(),
                )));
            } else {
                for table_info in table_infos.into_iter().rev() {
                    let table = self
                        .sstable_store
                        .sstable(table_info.id, &mut stats)
                        .await?;
                    overlapped_iters.push(HummockIteratorUnion::Fourth(
                        T::SstableIteratorType::create(
                            table,
                            self.sstable_store(),
                            iter_read_options.clone(),
                        ),
                    ));
                }
            }
        }

        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(overlapped_iters.len() as f64);

        let key_range = (
            key_range.start_bound().map(|b| b.as_ref().to_owned()),
            key_range.end_bound().map(|b| b.as_ref().to_owned()),
        );

        // The input of the user iterator is a `HummockIteratorUnion` of 4 different types. We use
        // the union because the underlying merge iterator
        let mut user_iterator = T::UserIteratorBuilder::create(
            overlapped_iters,
            self.stats.clone(),
            key_range,
            epoch,
            min_epoch,
            Some(pinned_version),
        );

        user_iterator.rewind().await?;
        stats.report(self.stats.as_ref());
        Ok(HummockStateStoreIter::new(user_iterator))
    }

    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get<'a>(
        &'a self,
        key: &'a [u8],
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let epoch = read_options.epoch;
        let compaction_group_id = match read_options.table_id.as_ref() {
            None => None,
            Some(table_id) => Some(self.get_compaction_group_id(*table_id).await?),
        };
        let mut stats = StoreLocalStatistic::default();
        let (shared_buffer_data, pinned_version) = self.read_filter(&read_options, &(key..=key))?;

        // Return `Some(None)` means the key is deleted.
        let get_from_batch = |batch: &SharedBufferBatch| -> Option<Option<Bytes>> {
            batch.get(key).map(|v| {
                self.stats.get_shared_buffer_hit_counts.inc();
                v.into_user_value().map(|v| v.into())
            })
        };

        let mut table_counts = 0;
        let internal_key = key_with_epoch(key.to_vec(), epoch);

        // Query shared buffer. Return the value without iterating SSTs if found
        for (replicated_batches, uncommitted_data) in shared_buffer_data {
            for batch in replicated_batches {
                if let Some(v) = get_from_batch(&batch) {
                    return Ok(v);
                }
            }
            // iterate over uncommitted data in order index in descending order
            for data_list in uncommitted_data {
                for data in data_list {
                    match data {
                        UncommittedData::Batch(batch) => {
                            if let Some(v) = get_from_batch(&batch) {
                                return Ok(v);
                            }
                        }
                        UncommittedData::Sst((_, table_info)) => {
                            let table = self
                                .sstable_store
                                .sstable(table_info.id, &mut stats)
                                .await?;
                            table_counts += 1;
                            if let Some(v) = self
                                .get_from_table(
                                    table,
                                    &internal_key,
                                    key,
                                    &read_options,
                                    &mut stats,
                                )
                                .await?
                            {
                                return Ok(v);
                            }
                        }
                    }
                }
            }
        }

        // See comments in HummockStorage::iter_inner for details about using compaction_group_id in
        // read/write path.
        for level in pinned_version.levels(compaction_group_id) {
            if level.table_infos.is_empty() {
                continue;
            }
            {
                let table_infos = prune_ssts(level.table_infos.iter(), &(key..=key));
                for table_info in table_infos.into_iter().rev() {
                    let table = self
                        .sstable_store
                        .sstable(table_info.id, &mut stats)
                        .await?;
                    table_counts += 1;
                    if let Some(v) = self
                        .get_from_table(table, &internal_key, key, &read_options, &mut stats)
                        .await?
                    {
                        return Ok(v);
                    }
                }
            }
        }

        stats.report(self.stats.as_ref());
        self.stats
            .iter_merge_sstable_counts
            .with_label_values(&["sub-iter"])
            .observe(table_counts as f64);
        Ok(None)
    }

    #[expect(clippy::type_complexity)]
    fn read_filter<R, B>(
        &self,
        read_options: &ReadOptions,
        key_range: &R,
    ) -> HummockResult<(
        Vec<(Vec<SharedBufferBatch>, OrderSortedUncommittedData)>,
        Arc<PinnedVersion>,
    )>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let epoch = read_options.epoch;
        let read_version = self.local_version_manager.read_version(epoch);

        // Check epoch validity
        validate_epoch(read_version.pinned_version.safe_epoch(), epoch)?;

        let shared_buffer_data = read_version
            .shared_buffer
            .iter()
            .map(|shared_buffer| shared_buffer.get_overlap_data(key_range))
            .collect();

        Ok((shared_buffer_data, read_version.pinned_version))
    }
}

impl StateStore for HummockStorage {
    type Iter = HummockStateStoreIter;

    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move { self.get(key, read_options).await }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.iter(key_range, read_options)
                .await?
                .collect(limit)
                .await
        }
    }

    fn prefix_scan<R, B, P>(
        &self,
        prefix_key: P,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::PrefixScanFuture<'_, R, B, P>
    where
        R: RangeBounds<B> + Send + 'static,
        B: AsRef<[u8]> + Send + 'static,
        P: AsRef<[u8]> + Send + 'static,
    {
        async move {
            self.prefix_iter(prefix_key, key_range, read_options)
                .await?
                .collect(limit)
                .await
        }
    }

    fn backward_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::BackwardScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            self.backward_iter(key_range, read_options)
                .await?
                .collect(limit)
                .await
        }
    }

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;
            let compaction_group_id = self.get_compaction_group_id(write_options.table_id).await?;
            // See comments in HummockStorage::iter_inner for details about using
            // compaction_group_id in read/write path.
            let size = self
                .local_version_manager
                .write_shared_buffer(
                    epoch,
                    compaction_group_id,
                    kv_pairs,
                    false,
                    write_options.table_id.into(),
                )
                .await?;
            Ok(size)
        }
    }

    /// Replicates a batch to shared buffer, without uploading to the storage backend.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;
            let compaction_group_id = self.get_compaction_group_id(write_options.table_id).await?;
            // See comments in HummockStorage::iter_inner for details about using
            // compaction_group_id in read/write path.
            self.local_version_manager
                .write_shared_buffer(
                    epoch,
                    compaction_group_id,
                    kv_pairs,
                    true,
                    write_options.table_id.into(),
                )
                .await?;

            Ok(())
        }
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn iter<R, B>(&self, key_range: R, read_options: ReadOptions) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        self.iter_inner::<_, _, ForwardIter, _>(b"", key_range, read_options)
    }

    fn prefix_iter<R, B, P>(
        &self,
        prefix_key: P,
        key_range: R,
        read_options: ReadOptions,
    ) -> Self::PrefixIterFuture<'_, R, B, P>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
        P: AsRef<[u8]> + Send,
    {
        self.iter_inner::<R, B, ForwardIter, P>(prefix_key, key_range, read_options)
    }

    /// Returns a backward iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn backward_iter<R, B>(
        &self,
        key_range: R,
        read_options: ReadOptions,
    ) -> Self::BackwardIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let key_range = (
            key_range.end_bound().map(|v| v.as_ref().to_vec()),
            key_range.start_bound().map(|v| v.as_ref().to_vec()),
        );
        self.iter_inner::<_, _, BackwardIter, _>(b"", key_range, read_options)
    }

    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { Ok(self.local_version_manager.wait_epoch(epoch).await?) }
    }

    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            self.local_version_manager()
                .sync_shared_buffer(epoch)
                .await?;
            Ok(())
        }
    }

    fn get_uncommitted_ssts(&self, epoch: u64) -> Vec<LocalSstableInfo> {
        self.local_version_manager.get_uncommitted_ssts(epoch)
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            self.local_version_manager.clear_shared_buffer().await;
            Ok(())
        }
    }
}

pub struct HummockStateStoreIter {
    inner: DirectedUserIterator,
}

impl HummockStateStoreIter {
    fn new(inner: DirectedUserIterator) -> Self {
        Self { inner }
    }

    async fn collect(mut self, limit: Option<usize>) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

        for _ in 0..limit.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(kv) => kvs.push(kv),
                None => break,
            }
        }

        Ok(kvs)
    }
}

impl StateStoreIter for HummockStateStoreIter {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                );
                iter.next().await?;
                Ok(Some(kv))
            } else {
                Ok(None)
            }
        }
    }
}
