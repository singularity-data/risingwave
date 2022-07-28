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

use async_trait::async_trait;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::hummock::{
    CompactTask, CompactionGroup, HummockVersion, HummockVersionDelta,
    SubscribeCompactTasksResponse, VacuumTask,
};
use risingwave_rpc_client::error::Result;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};
use tonic::Streaming;

use crate::hummock::{HummockEpoch, HummockSstableId, HummockVersionId};
use crate::monitor::HummockMetrics;

pub struct MonitoredHummockMetaClient {
    meta_client: MetaClient,

    stats: Arc<HummockMetrics>,
}

impl MonitoredHummockMetaClient {
    pub fn new(meta_client: MetaClient, stats: Arc<HummockMetrics>) -> MonitoredHummockMetaClient {
        MonitoredHummockMetaClient { meta_client, stats }
    }
}

#[async_trait]
impl HummockMetaClient for MonitoredHummockMetaClient {
    async fn pin_version(
        &self,
        last_pinned: HummockVersionId,
    ) -> Result<(bool, Vec<HummockVersionDelta>, Option<HummockVersion>)> {
        self.stats.pin_version_counts.inc();
        let timer = self.stats.pin_version_latency.start_timer();
        let res = self.meta_client.pin_version(last_pinned).await;
        timer.observe_duration();
        res
    }

    async fn unpin_version(&self) -> Result<()> {
        self.stats.unpin_version_counts.inc();
        let timer = self.stats.unpin_version_latency.start_timer();
        let res = self.meta_client.unpin_version().await;
        timer.observe_duration();
        res
    }

    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        self.stats.unpin_version_before_counts.inc();
        let timer = self.stats.unpin_version_before_latency.start_timer();
        let res = self
            .meta_client
            .unpin_version_before(unpin_version_before)
            .await;
        timer.observe_duration();
        res
    }

    async fn pin_snapshot(&self) -> Result<HummockEpoch> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let res = self.meta_client.pin_snapshot().await;
        timer.observe_duration();
        res
    }

    async fn get_epoch(&self) -> Result<HummockEpoch> {
        self.stats.pin_snapshot_counts.inc();
        let timer = self.stats.pin_snapshot_latency.start_timer();
        let res = self.meta_client.get_epoch().await;
        timer.observe_duration();
        res
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        self.stats.unpin_snapshot_counts.inc();
        let timer = self.stats.unpin_snapshot_latency.start_timer();
        let res = self.meta_client.unpin_snapshot().await;
        timer.observe_duration();
        res
    }

    async fn unpin_snapshot_before(&self, _min_epoch: HummockEpoch) -> Result<()> {
        unreachable!("Currently CNs should not call this function")
    }

    async fn get_new_table_id(&self) -> Result<HummockSstableId> {
        self.stats.get_new_table_id_counts.inc();
        let timer = self.stats.get_new_table_id_latency.start_timer();
        let res = self.meta_client.get_new_table_id().await;
        timer.observe_duration();
        res
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        self.stats.report_compaction_task_counts.inc();
        let timer = self.stats.report_compaction_task_latency.start_timer();
        let res = self.meta_client.report_compaction_task(compact_task).await;
        timer.observe_duration();
        res
    }

    async fn commit_epoch(
        &self,
        _epoch: HummockEpoch,
        _sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        panic!("Only meta service can commit_epoch in production.")
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        self.meta_client.subscribe_compact_tasks().await
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        self.meta_client.report_vacuum_task(vacuum_task).await
    }

    async fn get_compaction_groups(&self) -> Result<Vec<CompactionGroup>> {
        self.meta_client.get_compaction_groups().await
    }

    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()> {
        self.meta_client
            .trigger_manual_compaction(compaction_group_id, table_id, level)
            .await
    }

    async fn report_full_scan_task(&self, sst_ids: Vec<HummockSstableId>) -> Result<()> {
        self.meta_client.report_full_scan_task(sst_ids).await
    }
}
