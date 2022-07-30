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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};
use risingwave_pb::hummock::SstableInfo;
use risingwave_storage::hummock::compactor::{Compactor, DummyCompactionFilter};
use risingwave_storage::hummock::iterator::{
    ConcatIterator, ConcatSstableIterator, Forward, HummockIterator, HummockIteratorUnion,
    MultiSstIterator, UnorderedMergeIteratorInner,
};
use risingwave_storage::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use risingwave_storage::hummock::sstable::SstableIteratorReadOptions;
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CachePolicy, CompressionAlgorithm, HummockResult, MemoryLimiter, Sstable, SstableBuilder,
    SstableBuilderOptions, SstableIterator, SstableMeta, SstableStore,
};
use risingwave_storage::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub fn mock_sstable_store() -> SstableStoreRef {
    let store = InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused()));
    let store = Arc::new(ObjectStoreImpl::InMem(store));
    let path = "test".to_string();
    Arc::new(SstableStore::new(store, path, 64 << 20, 128 << 20))
}

pub fn test_key_of(idx: usize, epoch: u64) -> Vec<u8> {
    let user_key = format!("key_test_{:08}", idx * 2).as_bytes().to_vec();
    key_with_epoch(user_key, epoch)
}

const MAX_KEY_COUNT: usize = 128 * 1024;

fn build_table(sstable_id: u64, range: Range<u64>, epoch: u64) -> (Bytes, SstableMeta) {
    let mut builder = SstableBuilder::new(
        sstable_id,
        SstableBuilderOptions {
            capacity: 32 * 1024 * 1024,
            block_capacity: 16 * 1024,
            restart_interval: 16,
            bloom_false_positive: 0.01,
            compression_algorithm: CompressionAlgorithm::None,
        },
    );
    let value = b"1234567890123456789";
    let mut full_key = test_key_of(0, epoch);
    let user_len = full_key.len() - 8;
    for i in range {
        let start = (i % 8) as usize;
        let end = (start + 8) as usize;
        full_key[(user_len - 8)..user_len].copy_from_slice(&i.to_be_bytes());
        builder.add(&full_key, HummockValue::put(&value[start..end]));
    }
    let (_, data, meta, _) = builder.finish();
    (data, meta)
}

async fn scan_all_table(sstable_store: SstableStoreRef) {
    let mut stats = StoreLocalStatistic::default();
    let table = sstable_store.sstable(1, &mut stats).await.unwrap();
    let default_read_options = Arc::new(SstableIteratorReadOptions::default());
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let mut iter = SstableIterator::new(table, sstable_store.clone(), default_read_options);
    iter.rewind().await.unwrap();
    while iter.is_valid() {
        iter.next().await.unwrap();
    }
}

fn bench_table_build(c: &mut Criterion) {
    c.bench_function("bench_table_build", |b| {
        b.iter(|| {
            let _ = build_table(0, 0..(MAX_KEY_COUNT as u64), 1);
        });
    });
}

fn bench_table_scan(c: &mut Criterion) {
    let (data, meta) = build_table(0, 0..(MAX_KEY_COUNT as u64), 1);
    let sstable_store = mock_sstable_store();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let sstable_store1 = sstable_store.clone();
    runtime.block_on(async move {
        sstable_store1
            .put(Sstable::new(1, meta.clone()), data, CachePolicy::NotFill)
            .await
            .unwrap();
    });
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch
    // block from meta.
    let sstable_store1 = sstable_store.clone();
    runtime.block_on(async move {
        scan_all_table(sstable_store1).await;
    });

    c.bench_function("bench_table_iterator", |b| {
        b.to_async(FuturesExecutor)
            .iter(|| scan_all_table(sstable_store.clone()));
    });
}

pub struct LocalTableBuilderFactory {
    global_table_id: AtomicU64,
    options: SstableBuilderOptions,
    limiter: MemoryLimiter,
}

#[async_trait::async_trait]
impl TableBuilderFactory for LocalTableBuilderFactory {
    async fn open_builder(
        &self,
    ) -> HummockResult<(
        risingwave_storage::hummock::utils::MemoryTracker,
        SstableBuilder,
    )> {
        let table_id = self.global_table_id.fetch_add(1, Ordering::SeqCst);
        Ok((
            self.limiter.require_memory(1).await.unwrap(),
            SstableBuilder::new(table_id, self.options.clone()),
        ))
    }
}

async fn compact<I: HummockIterator<Direction = Forward>>(iter: I, sstable_store: SstableStoreRef) {
    let global_table_id = AtomicU64::new(32);
    let mut builder = CapacitySplitTableBuilder::new(
        LocalTableBuilderFactory {
            global_table_id,
            options: SstableBuilderOptions {
                capacity: 32 * 1024 * 1024,
                block_capacity: 64 * 1024,
                restart_interval: 16,
                bloom_false_positive: 0.01,
                compression_algorithm: CompressionAlgorithm::None,
            },
            limiter: MemoryLimiter::new(100000),
        },
        CachePolicy::NotFill,
        sstable_store.clone(),
    );

    Compactor::compact_and_build_sst(
        &mut builder,
        KeyRange::inf(),
        iter,
        false,
        0,
        DummyCompactionFilter,
    )
    .await
    .unwrap();
}

pub fn generate_tables(metas: Vec<(u64, SstableMeta)>) -> Vec<SstableInfo> {
    metas
        .into_iter()
        .map(|(id, meta)| SstableInfo {
            id,
            key_range: Some(risingwave_pb::hummock::KeyRange {
                left: meta.smallest_key.clone(),
                right: meta.largest_key.clone(),
                inf: false,
            }),
            file_size: meta.estimated_size as u64,
            table_ids: vec![id as u32],
        })
        .collect_vec()
}

fn bench_merge_iterator_compactor(c: &mut Criterion) {
    let sstable_store = mock_sstable_store();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let sstable_store1 = sstable_store.clone();
    let test_key_size = 256 * 1024;
    let (data1, meta1) = build_table(1, 0..test_key_size, 1);
    let (data2, meta2) = build_table(2, 0..test_key_size, 1);
    let sst1 = Sstable::new_with_data(1, meta1.clone(), data1.clone()).unwrap();
    let sst2 = Sstable::new_with_data(2, meta2.clone(), data2.clone()).unwrap();
    runtime.block_on(async move {
        sstable_store1
            .put(sst1, data1, CachePolicy::Fill)
            .await
            .unwrap();
        sstable_store1
            .put(sst2, data2, CachePolicy::Fill)
            .await
            .unwrap();
    });
    let level1 = generate_tables(vec![(1, meta1), (2, meta2)]);

    let (data1, meta1) = build_table(1, 0..test_key_size, 2);
    let (data2, meta2) = build_table(2, 0..test_key_size, 2);
    let sst3 = Sstable::new_with_data(3, meta1.clone(), data1.clone()).unwrap();
    let sst4 = Sstable::new_with_data(4, meta2.clone(), data2.clone()).unwrap();
    let sstable_store1 = sstable_store.clone();
    runtime.block_on(async move {
        sstable_store1
            .put(sst3, data1, CachePolicy::Fill)
            .await
            .unwrap();
        sstable_store1
            .put(sst4, data2, CachePolicy::Fill)
            .await
            .unwrap();
    });
    let level2 = generate_tables(vec![(1, meta1), (2, meta2)]);
    let read_options = Arc::new(SstableIteratorReadOptions { prefetch: true });
    c.bench_function("bench_union_merge_iterator", |b| {
        let stats = Arc::new(StateStoreMetrics::unused());
        b.to_async(FuturesExecutor).iter(|| {
            let sstable_store1 = sstable_store.clone();
            let sub_iters = vec![
                HummockIteratorUnion::First(ConcatIterator::new(
                    level1.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                )),
                HummockIteratorUnion::First(ConcatIterator::new(
                    level2.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                )),
            ];
            let iter = MultiSstIterator::new(sub_iters, stats.clone());
            async move { compact(iter, sstable_store1).await }
        });
    });
    c.bench_function("bench_merge_iterator", |b| {
        let stats = Arc::new(StateStoreMetrics::unused());
        b.to_async(FuturesExecutor).iter(|| {
            let sstable_store1 = sstable_store.clone();
            let sub_iters = vec![
                ConcatSstableIterator::new(
                    level1.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
                ConcatSstableIterator::new(
                    level2.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                ),
            ];
            let iter = UnorderedMergeIteratorInner::new(sub_iters, stats.clone());
            async move { compact(iter, sstable_store1).await }
        });
    });
}

criterion_group!(
    benches,
    bench_table_build,
    bench_table_scan,
    bench_merge_iterator_compactor
);
criterion_main!(benches);
