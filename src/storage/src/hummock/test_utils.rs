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

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::hummock::{KeyRange, SstableInfo};

use super::{CompressionAlgorithm, SstableMeta, DEFAULT_RESTART_INTERVAL};
use crate::hummock::iterator::test_utils::iterator_test_key_of_epoch;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    CachePolicy, HummockStateStoreIter, LruCache, Sstable, SstableBuilder, SstableBuilderOptions,
    SstableStoreRef,
};
use crate::storage_value::StorageValue;
use crate::store::StateStoreIter;

pub fn default_config_for_test() -> StorageConfig {
    StorageConfig {
        sstable_size_mb: 256,
        block_size_kb: 64,
        bloom_false_positive: 0.1,
        share_buffers_sync_parallelism: 2,
        share_buffer_compaction_worker_threads_number: 1,
        shared_buffer_capacity_mb: 64,
        data_directory: "hummock_001".to_string(),
        write_conflict_detection_enabled: true,
        block_cache_capacity_mb: 64,
        meta_cache_capacity_mb: 64,
        disable_remote_compactor: false,
        enable_local_spill: false,
        local_object_store: "memory".to_string(),
        share_buffer_upload_concurrency: 1,
        compactor_memory_limit_mb: 64,
        sstable_id_remote_fetch_number: 1,
    }
}

pub fn gen_dummy_batch(epoch: u64) -> Vec<(Bytes, StorageValue)> {
    vec![(
        iterator_test_key_of_epoch(0, epoch).into(),
        StorageValue::new_put(b"value1".to_vec()),
    )]
}

pub fn gen_dummy_sst_info(id: HummockSstableId, batches: Vec<SharedBufferBatch>) -> SstableInfo {
    let mut min_key: Vec<u8> = batches[0].start_key().to_vec();
    let mut max_key: Vec<u8> = batches[0].end_key().to_vec();
    for batch in batches.iter().skip(1) {
        if min_key.as_slice() > batch.start_key() {
            min_key = batch.start_key().to_vec();
        }
        if max_key.as_slice() < batch.end_key() {
            max_key = batch.end_key().to_vec();
        }
    }
    SstableInfo {
        id,
        key_range: Some(KeyRange {
            left: min_key,
            right: max_key,
            inf: false,
        }),
        file_size: batches.len() as u64,
        table_ids: vec![],
    }
}

/// Number of keys in table generated in `generate_table`.
pub const TEST_KEYS_COUNT: usize = 10000;

pub fn default_builder_opt_for_test() -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: 256 * (1 << 20), // 256MB
        block_capacity: 4096,      // 4KB
        restart_interval: DEFAULT_RESTART_INTERVAL,
        bloom_false_positive: 0.1,
        compression_algorithm: CompressionAlgorithm::None,
    }
}

/// Generates sstable data and metadata from given `kv_iter`
pub fn gen_test_sstable_data(
    opts: SstableBuilderOptions,
    kv_iter: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
) -> (Bytes, SstableMeta, Vec<u32>) {
    let mut b = SstableBuilder::new(0, opts);
    for (key, value) in kv_iter {
        b.add(&key, value.as_slice())
    }
    let (_, data, meta, table_ids) = b.finish();
    (data, meta, table_ids)
}

/// Generates a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable_inner(
    opts: SstableBuilderOptions,
    sst_id: HummockSstableId,
    kv_iter: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
) -> Sstable {
    let (data, meta, _) = gen_test_sstable_data(opts, kv_iter);
    let sst = Sstable::new(sst_id, meta.clone());
    sstable_store
        .put(Sstable::new(sst_id, meta.clone()), data, policy)
        .await
        .unwrap();
    sst
}

/// Generate a test table from the given `kv_iter` and put the kv value to `sstable_store`
pub async fn gen_test_sstable(
    opts: SstableBuilderOptions,
    sst_id: HummockSstableId,
    kv_iter: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable_inner(opts, sst_id, kv_iter, sstable_store, CachePolicy::Fill).await
}

/// The key (with epoch 0) of an index in the test table
pub fn test_key_of(idx: usize) -> Vec<u8> {
    let user_key = format!("key_test_{:05}", idx * 2).as_bytes().to_vec();
    key_with_epoch(user_key, 233)
}

/// The value of an index in the test table
pub fn test_value_of(idx: usize) -> Vec<u8> {
    "23332333"
        .as_bytes()
        .iter()
        .cycle()
        .cloned()
        .take(idx % 100 + 1) // so that the table is not too big
        .collect_vec()
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_default_test_sstable(
    opts: SstableBuilderOptions,
    sst_id: HummockSstableId,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable(
        opts,
        sst_id,
        (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
        sstable_store,
    )
    .await
}

pub async fn count_iter(iter: &mut HummockStateStoreIter) -> usize {
    let mut c: usize = 0;
    while iter.next().await.unwrap().is_some() {
        c += 1
    }
    c
}

pub fn create_small_table_cache() -> Arc<LruCache<HummockSstableId, Box<Sstable>>> {
    Arc::new(LruCache::new(1, 4))
}
