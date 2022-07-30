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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut};
use clap::Parser;
use itertools::Itertools;
use rand::{Rng, SeedableRng};
use risingwave_storage::hummock::file_cache::cache::{FileCache, FileCacheOptions};
use risingwave_storage::hummock::TieredCacheKey;
use tokio::sync::oneshot;

use crate::analyze::{analyze, monitor, Hook, Metrics};
use crate::utils::{dev_stat_path, iostat};

#[derive(Parser, Debug, Clone)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(long, default_value = "1073741824")] // 1 GiB
    capacity: usize,
    #[clap(long, default_value = "134217728")] // 2 * 64 MiB
    total_buffer_capacity: usize,
    #[clap(long, default_value = "67108864")] // 64 MiB
    cache_file_fallocate_unit: usize,

    #[clap(long, default_value = "1048576")] // 1 MiB
    bs: usize,
    #[clap(long, default_value = "8")]
    concurrency: usize,
    #[clap(long, default_value = "600")] // 600s
    time: u64,
    #[clap(long, default_value = "1")]
    read: usize,
    #[clap(long, default_value = "1")]
    write: usize,
    #[clap(long, default_value = "10000")]
    look_up_range: u32,
    #[clap(long, default_value = "0")] // 0ms
    loop_min_interval: u64,

    #[clap(long, default_value = "1")] // (s)
    report_interval: u64,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct Index {
    sst: u32,
    idx: u32,
}

impl TieredCacheKey for Index {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u32(self.sst);
        buf.put_u32(self.idx);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst = buf.get_u32();
        let idx = buf.get_u32();
        Self { sst, idx }
    }
}

pub async fn run() {
    let args = Args::parse();

    let metrics = Metrics::default();
    let hook = Arc::new(Hook::new(metrics.clone()));

    let options = FileCacheOptions {
        dir: args.path.clone(),
        capacity: args.capacity,
        total_buffer_capacity: args.total_buffer_capacity,
        cache_file_fallocate_unit: args.cache_file_fallocate_unit,
        flush_buffer_hooks: vec![hook],
    };

    let cache: FileCache<Index> = FileCache::open(options).await.unwrap();

    let iostat_path = dev_stat_path(&args.path);

    let iostat_start = iostat(&iostat_path);
    let metrics_dump_start = metrics.dump();
    let time_start = Instant::now();

    let (txs, rxs): (Vec<_>, Vec<_>) = (0..args.concurrency).map(|_| oneshot::channel()).unzip();

    let futures = rxs
        .into_iter()
        .enumerate()
        .map(|(id, rx)| {
            bench(
                id,
                args.clone(),
                cache.clone(),
                args.time,
                metrics.clone(),
                rx,
            )
        })
        .collect_vec();

    let handles = futures.into_iter().map(tokio::spawn).collect_vec();

    let (tx_monitor, rx_monitor) = oneshot::channel();
    let handle_monitor = tokio::spawn({
        let iostat_path = iostat_path.clone();
        let metrics = metrics.clone();
        async move {
            monitor(
                iostat_path,
                Duration::from_secs(args.report_interval),
                metrics,
                rx_monitor,
            )
            .await;
        }
    });

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        for tx in txs {
            let _ = tx.send(());
        }
        let _ = tx_monitor.send(());
    });

    for handle in handles {
        handle.await.unwrap();
    }
    handle_monitor.abort();

    let iostat_end = iostat(&iostat_path);
    let metrics_dump_end = metrics.dump();
    let analysis = analyze(
        time_start.elapsed(),
        &iostat_start,
        &iostat_end,
        &metrics_dump_start,
        &metrics_dump_end,
    );
    println!("\nTotal:\n{}", analysis);

    // TODO: Remove this after graceful shutdown is done.
    // Waiting for the inflight flush io to pervent files from being closed.
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn bench(
    id: usize,
    args: Args,
    cache: FileCache<Index>,
    time: u64,
    metrics: Metrics,
    mut stop: oneshot::Receiver<()>,
) {
    let start = Instant::now();

    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let sst = id as u32;
    let mut idx = 0;

    loop {
        let loop_start = Instant::now();

        match stop.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            _ => return,
        }
        if start.elapsed().as_secs() >= time {
            return;
        }

        for _ in 0..args.write {
            idx += 1;
            let key = Index { sst, idx };
            let value = vec![b'x'; args.bs];

            let start = Instant::now();
            cache.insert(key, value).unwrap();
            metrics
                .insert_lats
                .write()
                .record(start.elapsed().as_micros() as u64)
                .expect("record out of range");
            metrics.insert_ios.fetch_add(1, Ordering::Relaxed);
            metrics.insert_bytes.fetch_add(args.bs, Ordering::Relaxed);
        }
        for _ in 0..args.read {
            let key = Index {
                sst,
                idx: rng.gen_range(
                    std::cmp::max(idx, args.look_up_range + 1) - args.look_up_range..=idx,
                ),
            };

            let start = Instant::now();
            let hit = cache.get(&key).await.unwrap().is_some();
            let lat = start.elapsed().as_micros() as u64;
            if hit {
                metrics
                    .get_hit_lats
                    .write()
                    .record(lat)
                    .expect("record out of range");
            } else {
                metrics
                    .get_miss_lats
                    .write()
                    .record(lat)
                    .expect("record out of range");
                metrics.get_miss_ios.fetch_add(1, Ordering::Relaxed);
            }
            metrics.get_ios.fetch_add(1, Ordering::Relaxed);
        }

        if args.loop_min_interval == 0 {
            tokio::task::yield_now().await;
        } else {
            let elapsed = loop_start.elapsed().as_millis() as u64;
            if elapsed < args.loop_min_interval {
                tokio::time::sleep(Duration::from_millis(args.loop_min_interval - elapsed)).await;
            }
        }
    }
}
