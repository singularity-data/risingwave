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

use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntGauge, IntGaugeVec, Registry,
};

pub struct MetaMetrics {
    registry: Registry,

    /// gRPC latency of meta services
    pub grpc_latency: HistogramVec,
    /// The duration from barrier injection to commit
    /// It is the sum of inflight-latency , sync-latency and wait-commit-latency
    pub barrier_latency: Histogram,
    /// The duration from barrier complete to commit
    pub barrier_wait_commit_latency: Histogram,

    /// latency between each barrier send
    pub barrier_send_latency: Histogram,
    /// the nums of all barrier. the it is the sum of in-flight and complete but waiting for other
    /// barrier
    pub all_barrier_nums: IntGauge,
    /// the nums of in-flight barrier
    pub in_flight_barrier_nums: IntGauge,

    /// max committed epoch
    pub max_committed_epoch: IntGauge,
    /// num of uncommitted SSTs,
    pub uncommitted_sst_num: IntGauge,
    /// num of SSTs in each level
    pub level_sst_num: IntGaugeVec,
    // /// num of SSTs to be merged to next level in each level
    pub level_compact_cnt: IntGaugeVec,

    pub level_file_size: IntGaugeVec,
    /// hummock version size
    pub version_size: IntGauge,

    /// Latency for hummock manager to acquire lock
    pub hummock_manager_lock_time: HistogramVec,

    /// Latency for hummock manager to really process a request after acquire the lock
    pub hummock_manager_real_process_time: HistogramVec,
}

impl MetaMetrics {
    pub fn new() -> Self {
        let registry = prometheus::Registry::new();
        let opts = histogram_opts!(
            "meta_grpc_duration_seconds",
            "gRPC latency of meta services",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let grpc_latency =
            register_histogram_vec_with_registry!(opts, &["path"], registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_duration_seconds",
            "barrier latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43s
        );
        let barrier_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_wait_commit_duration_seconds",
            "barrier_wait_commit_latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43s
        );
        let barrier_wait_commit_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_send_duration_seconds",
            "barrier send latency",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let barrier_send_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let all_barrier_nums = register_int_gauge_with_registry!(
            "all_barrier_nums",
            "num of of all_barrier",
            registry
        )
        .unwrap();
        let in_flight_barrier_nums = register_int_gauge_with_registry!(
            "in_flight_barrier_nums",
            "num of of in_flight_barrier",
            registry
        )
        .unwrap();

        let max_committed_epoch = register_int_gauge_with_registry!(
            "storage_max_committed_epoch",
            "max committed epoch",
            registry
        )
        .unwrap();

        let uncommitted_sst_num = register_int_gauge_with_registry!(
            "storage_uncommitted_sst_num",
            "num of uncommitted SSTs",
            registry
        )
        .unwrap();

        let level_sst_num = register_int_gauge_vec_with_registry!(
            "storage_level_sst_num",
            "num of SSTs in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_cnt",
            "num of SSTs to be merged to next level in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let version_size =
            register_int_gauge_with_registry!("version_size", "version size", registry).unwrap();
        let level_file_size = register_int_gauge_vec_with_registry!(
            "storage_level_total_file_size",
            "KBs total file bytes in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let hummock_manager_lock_time = register_histogram_vec_with_registry!(
            "hummock_manager_lock_time",
            "latency for hummock manager to acquire the rwlock",
            &["method", "lock_name", "lock_type"],
            registry
        )
        .unwrap();

        let hummock_manager_real_process_time = register_histogram_vec_with_registry!(
            "meta_hummock_manager_real_process_time",
            "latency for hummock manager to really process the request",
            &["method"],
            registry
        )
        .unwrap();

        Self {
            registry,

            grpc_latency,
            barrier_latency,
            barrier_wait_commit_latency,
            barrier_send_latency,
            all_barrier_nums,
            in_flight_barrier_nums,

            max_committed_epoch,
            uncommitted_sst_num,
            level_sst_num,
            level_compact_cnt,
            level_file_size,
            version_size,
            hummock_manager_lock_time,
            hummock_manager_real_process_time,
        }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}
impl Default for MetaMetrics {
    fn default() -> Self {
        Self::new()
    }
}
