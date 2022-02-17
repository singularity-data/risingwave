use std::time::Instant;

use async_std::task;
use bytes::Buf;
use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::{Distribution, StdRng};
use rand::SeedableRng;
use risingwave_storage::hummock::key::next_key;
use risingwave_storage::StateStore;

use super::Operations;
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::Workload;
use crate::Opts;

impl Operations {
    pub(crate) async fn prefix_scan_random(&self, store: &impl StateStore, opts: &Opts) {
        // generate queried prefixes
        let mut scan_prefixes = match self.prefixes.is_empty() {
            // if prefixes is empty, use default prefix: ["a"*key_prefix_size]
            true => Workload::new_random_keys(opts, 233).0,
            false => {
                let mut rng = StdRng::seed_from_u64(233);
                let dist = Uniform::from(0..self.prefixes.len());
                (0..opts.reads as usize)
                    .into_iter()
                    .map(|_| self.prefixes[dist.sample(&mut rng)].clone())
                    .collect_vec()
            }
        };

        // partitioned these prefixes for each concurrency
        let mut grouped_prefixes = vec![vec![]; opts.concurrency_num as usize];
        for (i, prefix) in scan_prefixes.drain(..).enumerate() {
            grouped_prefixes[i % opts.concurrency_num as usize].push(prefix);
        }

        let stores = (0..opts.concurrency_num as usize)
            .map(|_| store.clone())
            .collect_vec();

        let mut grouped_prefixes = grouped_prefixes.into_iter().zip_eq(stores).collect_vec();

        let total_start = Instant::now();
        let futures = grouped_prefixes
            .drain(..)
            .map(|(prefixes, store)| async move {
                let mut latencies = vec![];
                // actual prefix scan process
                for prefix in prefixes {
                    let start = Instant::now();
                    store
                        .scan(
                            prefix.chunk().to_vec()..next_key(prefix.chunk()),
                            None,
                            u64::MAX,
                        )
                        .await
                        .unwrap();
                    let time_nano = start.elapsed().as_nanos();
                    latencies.push(time_nano);
                }
                latencies
            })
            .collect_vec();

        let handles = futures.into_iter().map(task::spawn).collect_vec();

        let latencies_list = futures::future::join_all(handles).await;

        // let latencies_list: Vec<Vec<u128>> = future::join_all(futures).await;
        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let latencies = latencies_list.into_iter().flatten().collect();
        let stat = LatencyStat::new(latencies);
        let qps = opts.reads as u128 * 1_000_000_000 / total_time_nano as u128;

        println!(
            "
    prefixscanrandom
      {}
      QPS: {}",
            stat, qps
        );
    }
}
