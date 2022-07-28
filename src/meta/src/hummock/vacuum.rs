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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{FullScanTask, VacuumTask};

use crate::hummock::error::{Error, Result};
use crate::hummock::{CompactorManager, HummockManagerRef};
use crate::storage::MetaStore;

pub struct VacuumTrigger<S: MetaStore> {
    hummock_manager: HummockManagerRef<S>,
    /// Use the CompactorManager to dispatch VacuumTask.
    compactor_manager: Arc<CompactorManager>,
    /// SST ids which have been dispatched to vacuum nodes but are not replied yet.
    pending_sst_ids: parking_lot::RwLock<HashSet<HummockSstableId>>,
}

impl<S> VacuumTrigger<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: Arc<CompactorManager>,
    ) -> Self {
        Self {
            hummock_manager,
            compactor_manager,
            pending_sst_ids: Default::default(),
        }
    }

    /// Tries to make checkpoint at the minimum pinned version.
    ///
    /// Returns number of deleted deltas
    pub async fn vacuum_version_metadata(&self) -> Result<usize> {
        self.hummock_manager.proceed_version_checkpoint().await?;
        let batch_size = 64usize;
        let mut total_deleted = 0;
        loop {
            let (deleted, remain) = self
                .hummock_manager
                .delete_version_deltas(batch_size)
                .await?;
            total_deleted += deleted;
            if remain < batch_size {
                break;
            }
        }
        Ok(total_deleted)
    }

    /// Schedules deletion of SSTs from object store
    ///
    /// Returns SSTs scheduled in worker node.
    pub async fn vacuum_sst_data(&self) -> Result<Vec<HummockSstableId>> {
        // Select SSTs to delete.
        let ssts_to_delete = {
            // 1. Retry the pending SSTs first.
            // It is possible some vacuum workers have been asked to vacuum these SSTs previously,
            // but they don't report the results yet due to either latency or failure.
            // This is OK since trying to delete the same SST multiple times is safe.
            let pending_sst_ids = self.pending_sst_ids.read().iter().cloned().collect_vec();
            if !pending_sst_ids.is_empty() {
                pending_sst_ids
            } else {
                // 2. If no pending SSTs, then fetch new ones.
                let ssts_to_delete = self.hummock_manager.get_ssts_to_delete().await;
                if ssts_to_delete.is_empty() {
                    return Ok(vec![]);
                }
                // Track these SST ids, so that we can remove them from metadata later.
                self.pending_sst_ids.write().extend(ssts_to_delete.clone());
                ssts_to_delete
            }
        };

        // Dispatch the vacuum task
        let mut batch_idx = 0;
        let batch_size = 32usize;
        let mut sent_batch = Vec::with_capacity(ssts_to_delete.len());
        while batch_idx < ssts_to_delete.len() {
            let delete_batch = ssts_to_delete
                .iter()
                .skip(batch_idx)
                .take(batch_size)
                .cloned()
                .collect_vec();
            // 1. Pick a worker.
            let compactor = match self.compactor_manager.next_compactor() {
                None => {
                    tracing::warn!("No vacuum worker is available.");
                    break;
                }
                Some(compactor) => compactor,
            };

            // 2. Send task.
            match compactor
                .send_task(Task::VacuumTask(VacuumTask {
                    // The SST id doesn't necessarily have a counterpart SST file in S3, but
                    // it's OK trying to delete it.
                    sstable_ids: delete_batch.clone(),
                }))
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        "Try to vacuum SSTs {:?} in worker {}.",
                        delete_batch,
                        compactor.context_id()
                    );
                    batch_idx += batch_size;
                    sent_batch.extend(delete_batch);
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to send vacuum task to worker {}: {:#?}",
                        compactor.context_id(),
                        err
                    );
                    self.compactor_manager
                        .remove_compactor(compactor.context_id());
                }
            }
        }
        Ok(sent_batch)
    }

    /// Acknowledges deletion of SSTs and deletes corresponding metadata.
    pub async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        let deleted_sst_ids = self
            .pending_sst_ids
            .read()
            .iter()
            .filter(|p| vacuum_task.sstable_ids.contains(p))
            .cloned()
            .collect_vec();
        if !deleted_sst_ids.is_empty() {
            self.hummock_manager
                .ack_deleted_ssts(&deleted_sst_ids)
                .await?;
            self.pending_sst_ids
                .write()
                .retain(|p| !deleted_sst_ids.contains(p));
        }
        tracing::info!("Finish vacuuming SSTs {:?}", vacuum_task.sstable_ids);
        Ok(())
    }

    /// Runs a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    /// `Vacuum::full_scan_inner` in storage crate. 3. Meta node decides which SSTs to delete.
    /// See `HummockManager::extend_ssts_to_delete_from_scan`.
    pub async fn run_full_gc(&self, sst_retention_time: Duration) -> Result<()> {
        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("Try full GC but no available worker.");
                return Ok(());
            }
            Some(compactor) => compactor,
        };
        compactor
            .send_task(Task::FullScanTask(FullScanTask {
                sst_retention_time_sec: sst_retention_time.as_secs(),
            }))
            .await
            .map_err(|_| Error::CompactorUnreachable(compactor.context_id()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_pb::hummock::VacuumTask;

    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::{start_vacuum_scheduler, CompactorManager, VacuumTrigger};

    #[tokio::test]
    async fn test_shutdown_vacuum() {
        let (_env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::new());
        let vacuum = Arc::new(VacuumTrigger::new(hummock_manager, compactor_manager));
        let (join_handle, shutdown_sender) = start_vacuum_scheduler(vacuum);
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_vacuum_basic() {
        let (_env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = Arc::new(CompactorManager::default());
        let vacuum = Arc::new(VacuumTrigger::new(
            hummock_manager.clone(),
            compactor_manager.clone(),
        ));
        let _receiver = compactor_manager.add_compactor(0);

        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );

        let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
        // Current state: {v0: [], v1: [test_tables], v2: [test_tables_2, to_delete:test_tables],
        // v3: [test_tables_2, test_tables_3]}

        // Makes checkpoint and extends deltas_to_delete. Deletes deltas of v0->v1 and v2->v3.
        // Delta of v1->v2 cannot be deleted yet because it's used by ssts_to_delete.
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            2
        );

        // Found ssts_to_delete
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );

        // The deletion is not acked yet.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );

        // The delta cannot be deleted yet.
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            0
        );

        // The vacuum task is reported.
        vacuum
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_infos
                    .first()
                    .unwrap()
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
            })
            .await
            .unwrap();

        // The delta can be deleted now.
        assert_eq!(
            VacuumTrigger::vacuum_version_metadata(&vacuum)
                .await
                .unwrap(),
            1
        );

        // No ssts_to_delete.
        assert_eq!(
            VacuumTrigger::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );
    }
}
