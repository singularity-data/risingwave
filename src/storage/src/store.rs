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
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::LocalSstableInfo;

use crate::error::StorageResult;
use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send;
pub trait ScanFutureTrait<'a, R, B> = Future<Output = StorageResult<Vec<(Bytes, Bytes)>>> + Send;
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send;
pub trait SyncFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send;
pub trait IngestBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type ScanFuture<'a, R, B> = impl ScanFutureTrait<'a, R, B> where R: 'static + Send, B: 'static + Send;
        type BackwardScanFuture<'a, R, B> = impl ScanFutureTrait<'a, R, B> where R: 'static + Send, B: 'static + Send;
        type IngestBatchFuture<'a> = impl IngestBatchFutureTrait<'a>;
        type ReplicateBatchFuture<'a> = impl EmptyFutureTrait<'a>;
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a>;
        type SyncFuture<'a> = impl SyncFutureTrait<'a>;
        type IterFuture<'a, R, B> = impl Future<Output = $crate::error::StorageResult<Self::Iter>> + Send where R: 'static + Send, B: 'static + Send;
        type BackwardIterFuture<'a, R, B> = impl Future<Output = $crate::error::StorageResult<Self::Iter>> + Send where R: 'static + Send, B: 'static + Send;
        type ClearSharedBufferFuture<'a> = impl EmptyFutureTrait<'a>;
    }
}

pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type ScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        R: 'static + Send,
        B: 'static + Send;

    type BackwardScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        R: 'static + Send,
        B: 'static + Send;

    type IngestBatchFuture<'a>: IngestBatchFutureTrait<'a>;

    type ReplicateBatchFuture<'a>: EmptyFutureTrait<'a>;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: SyncFutureTrait<'a>;

    type IterFuture<'a, R, B>: Future<Output = StorageResult<Self::Iter>> + Send
    where
        R: 'static + Send,
        B: 'static + Send;

    type BackwardIterFuture<'a, R, B>: Future<Output = StorageResult<Self::Iter>> + Send
    where
        R: 'static + Send,
        B: 'static + Send;

    type ClearSharedBufferFuture<'a>: EmptyFutureTrait<'a>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(&'a self, key: &'a [u8], read_options: ReadOptions) -> Self::GetFuture<'_>;

    /// Scans `limit` number of keys from a key range. If `limit` is `None`, scans all elements.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    fn backward_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::BackwardScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Ingests a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// An epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rolled back if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_>;

    /// Functions the same as `ingest_batch`, except that data won't be persisted.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::ReplicateBatchFuture<'_>;

    /// Opens and returns an iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`.
    fn iter<R, B>(&self, key_range: R, read_options: ReadOptions) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Opens and returns a backward iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`
    fn backward_iter<R, B>(
        &self,
        key_range: R,
        read_options: ReadOptions,
    ) -> Self::BackwardIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Creates a `WriteBatch` associated with this state store.
    fn start_write_batch(&self, write_options: WriteOptions) -> WriteBatch<Self> {
        WriteBatch::new(self.clone(), write_options)
    }

    /// Waits until the epoch is committed and its data is ready to read.
    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_>;

    /// Syncs buffered data to S3.
    /// If the epoch is None, all buffered data will be synced.
    /// Otherwise, only data of the provided epoch will be synced.
    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_>;

    /// Creates a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }

    /// Gets `epoch`'s uncommitted `Sstables`.
    fn get_uncommitted_ssts(&self, _epoch: u64) -> Vec<LocalSstableInfo> {
        todo!()
    }

    /// Clears contents in shared buffer.
    /// This method should only be called when dropping all actors in the local compute node.
    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        todo!()
    }
}

pub trait StateStoreIter: Send + 'static {
    type Item;
    type NextFuture<'a>: Future<Output = StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_>;
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    pub epoch: u64,
    pub table_id: Option<TableId>,
    pub ttl: Option<u32>, // second
}

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}

impl ReadOptions {
    pub fn min_epoch(&self) -> u64 {
        let epoch = Epoch(self.epoch);
        match self.ttl {
            Some(ttl_second_u32) => epoch.subtract_ms((ttl_second_u32 * 1000) as u64).0,
            None => 0,
        }
    }
}
