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

use std::ops::Sub;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_object_store::object::{ObjectMetadata, ObjectStore};
use risingwave_pb::hummock::{FullScanTask, VacuumTask};
use risingwave_rpc_client::HummockMetaClient;

use super::{HummockError, HummockResult};
use crate::hummock::SstableStoreRef;

pub struct Vacuum;

impl Vacuum {
    /// Wrapper method that warns on any error and doesn't propagate it.
    /// Returns false if any error.
    pub async fn vacuum(
        vacuum_task: VacuumTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!("Try to vacuum SSTs {:?}", vacuum_task.sstable_ids);
        match Vacuum::vacuum_inner(
            vacuum_task,
            sstable_store.clone(),
            hummock_meta_client.clone(),
        )
        .await
        {
            Ok(_) => {
                tracing::info!("Finished vacuuming SSTs");
            }
            Err(e) => {
                tracing::warn!("Failed to vacuum SSTs.\n{:#?}", e);
                return false;
            }
        }
        true
    }

    pub async fn vacuum_inner(
        vacuum_task: VacuumTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockResult<()> {
        let sst_ids = vacuum_task.sstable_ids;
        for sst_id in &sst_ids {
            sstable_store.delete(*sst_id).await?;
        }

        // TODO: report progress instead of in one go.
        hummock_meta_client
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_ids,
            })
            .await
            .map_err(|e| {
                HummockError::meta_error(format!("Failed to report vacuum task.\n{:#?}", e))
            })?;

        Ok(())
    }

    /// Wrapper method that warns on any error and doesn't propagate it.
    /// Returns false if any error.
    pub async fn full_scan(
        full_scan_task: FullScanTask,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> bool {
        tracing::info!(
            "Try to full scan SSTs with timestamp {}",
            full_scan_task.sst_retention_time_sec
        );

        let object_metadata = match sstable_store.list_ssts_from_object_store().await {
            Ok(object_metadata) => object_metadata,
            Err(e) => {
                tracing::warn!("Failed to list object store.\n{:#?}", e);
                return false;
            }
        };

        let sst_ids =
            Vacuum::full_scan_inner(full_scan_task, object_metadata, sstable_store.clone());
        match hummock_meta_client.report_full_scan_task(sst_ids).await {
            Ok(_) => {
                tracing::info!("Finished full scan SSTs");
            }
            Err(e) => {
                tracing::warn!("Failed to report full scan task.\n{:#?}", e);
                return false;
            }
        }
        true
    }

    pub fn full_scan_inner(
        full_scan_task: FullScanTask,
        object_metadata: Vec<ObjectMetadata>,
        sstable_store: SstableStoreRef,
    ) -> Vec<HummockSstableId> {
        let timestamp_watermark = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .sub(Duration::from_secs(full_scan_task.sst_retention_time_sec))
            .as_secs_f64();
        object_metadata
            .into_iter()
            .filter(|o| o.last_modified < timestamp_watermark)
            .map(|o| sstable_store.get_sst_id_from_path(&o.key))
            .dedup()
            .collect_vec()
    }
}
