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

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::compactor::{Compactor, CompactorContext};
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::WriteOptions;
use risingwave_storage::StateStore;

use super::{Batch, Operations, PerfMetrics};
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

pub struct BatchTaskContext {
    task_count: AtomicUsize,
    epoch: AtomicU64,
    meta_client: Arc<MockHummockMetaClient>,
}

impl BatchTaskContext {
    fn new(meta_client: Arc<MockHummockMetaClient>, origin_task_count: usize) -> Self {
        assert!(origin_task_count < (1 << 8));
        Self {
            meta_client,
            epoch: AtomicU64::new(1),
            task_count: AtomicUsize::new(origin_task_count << 8),
        }
    }

    pub fn epoch_barrier_finish(&self, exit: bool) -> bool {
        loop {
            let task_count = self.task_count.load(Ordering::Acquire);
            let mut origin_task_count = task_count >> 8;
            let mut finish_count = task_count & 255;
            if exit {
                origin_task_count -= 1;
            } else {
                finish_count += 1;
            }
            let finished = finish_count == origin_task_count;
            if finish_count == origin_task_count {
                finish_count = 0;
            }
            if self
                .task_count
                .compare_exchange_weak(
                    task_count,
                    origin_task_count << 8 | finish_count,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return finished;
            }
        }
    }
}

impl Operations {
    pub(crate) async fn write_batch(
        &mut self,
        store: &impl StateStore,
        opts: &Opts,
        context: Option<(Arc<CompactorContext>, Arc<LocalVersionManager>)>,
    ) {
        let (prefixes, keys) = Workload::new_random_keys(opts, opts.writes as u64, &mut self.rng);
        let values = Workload::new_values(opts, opts.writes as u64, &mut self.rng);

        // add new prefixes and keys to global prefixes and keys
        self.track_prefixes(prefixes);
        self.track_keys(keys.clone());

        let batches = Workload::make_batches(opts, keys, values);
        println!("batch size: {}", batches.len());

        let perf = self.run_batches(store, opts, batches).await;
        if opts.compact_level_after_write > 0 {
            if let Some((compact_context, local_version_manager)) = context {
                if let Some(task) = self.meta_client.get_compact_task().await {
                    Compactor::compact(compact_context.clone(), task).await;
                    // Ensure the version after compaction is available locally.
                    let last_pinned_id = local_version_manager.get_pinned_version().id();
                    let version = self.meta_client.pin_version(last_pinned_id).await.unwrap();
                    local_version_manager.try_update_pinned_version(Some(last_pinned_id), version);
                }
            }
        }

        println!(
            "
    writebatch
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    pub(crate) async fn delete_random(&mut self, store: &impl StateStore, opts: &Opts) {
        let delete_keys = match self.keys.is_empty() {
            true => Workload::new_random_keys(opts, opts.deletes as u64, &mut self.rng).1,
            false => {
                let dist = Uniform::from(0..self.keys.len());
                (0..opts.deletes)
                    .into_iter()
                    .map(|_| self.keys[dist.sample(&mut self.rng)].clone())
                    .collect_vec()
            }
        };
        self.untrack_keys(&delete_keys);

        let values = vec![None; opts.deletes as usize];

        let batches = Workload::make_batches(opts, delete_keys, values);

        let perf = self.run_batches(store, opts, batches).await;

        println!(
            "
    deleterandom
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    async fn run_batches(
        &mut self,
        store: &impl StateStore,
        opts: &Opts,
        mut batches: Vec<Batch>,
    ) -> PerfMetrics {
        let batches_len = batches.len();
        // TODO(Ting Sun): use sizes from metrics directly
        let size = batches
            .iter()
            .flat_map(|batch| batch.iter())
            .map(|(key, value)| key.len() + value.as_ref().map(|v| v.len()).unwrap_or(0))
            .sum::<usize>();

        // partitioned these batches for each concurrency
        let mut grouped_batches = vec![vec![]; opts.concurrency_num as usize];
        for (i, batch) in batches.drain(..).enumerate() {
            grouped_batches[i % opts.concurrency_num as usize].push(batch);
        }

        let ctx = Arc::new(BatchTaskContext::new(
            self.meta_client.clone(),
            grouped_batches.len(),
        ));
        let mut args = grouped_batches
            .into_iter()
            .map(|batches| (batches, store.clone(), ctx.clone()))
            .collect_vec();

        let futures = args
            .drain(..)
            .map(|(batches, store, ctx)| async move {
                let mut latencies: Vec<u128> = vec![];
                let l = batches.len();
                for (i, batch) in batches.into_iter().enumerate() {
                    let start = Instant::now();
                    let batch = batch
                        .into_iter()
                        .map(|(k, v)| (k, StorageValue::new(v)))
                        .collect_vec();
                    let epoch = ctx.epoch.load(Ordering::Acquire);
                    store
                        .ingest_batch(
                            batch,
                            WriteOptions {
                                epoch,
                                table_id: Default::default(),
                            },
                        )
                        .await
                        .unwrap();
                    let last_batch = i + 1 == l;
                    if ctx.epoch_barrier_finish(last_batch) {
                        store.sync(Some(epoch)).await.unwrap();
                        let synced_sst = store.get_uncommitted_ssts(epoch);
                        ctx.meta_client
                            .commit_epoch(epoch, synced_sst)
                            .await
                            .unwrap();
                        ctx.epoch.fetch_add(1, Ordering::SeqCst);
                    }
                    store.wait_epoch(epoch).await.unwrap();
                    let time_nano = start.elapsed().as_nanos();
                    latencies.push(time_nano);
                }
                latencies
            })
            .collect_vec();

        let total_start = Instant::now();

        let handles = futures.into_iter().map(tokio::spawn).collect_vec();
        let latencies_list = futures::future::join_all(handles).await;

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let latencies: Vec<u128> = latencies_list
            .into_iter()
            .flat_map(|res| res.unwrap())
            .collect_vec();
        let stat = LatencyStat::new(latencies);
        // calculate operation per second
        let ops = opts.batch_size as u128 * 1_000_000_000 * batches_len as u128 / total_time_nano;
        let bytes_pre_sec = size as u128 * 1_000_000_000 / total_time_nano;

        PerfMetrics {
            stat,
            qps: ops,
            bytes_pre_sec,
        }
    }
}
