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

use rand::Rng;
use risingwave_common::error::{ErrorCode, Result, ToErrorStr};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse, VacuumTask};
use tokio::sync::mpsc::{Receiver, Sender};

const STREAM_BUFFER_SIZE: usize = 4;

pub type CompactorManagerRef = Arc<CompactorManager>;

pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<Result<SubscribeCompactTasksResponse>>,
}

impl Compactor {
    pub async fn send_task(
        &self,
        compact_task: Option<CompactTask>,
        vacuum_task: Option<VacuumTask>,
    ) -> Result<()> {
        // TODO: compactor node backpressure
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                compact_task,
                vacuum_task,
            }))
            .await
            .map_err(|e| ErrorCode::InternalError(e.to_error_str()).into())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }
}

struct CompactorManagerInner {
    /// Senders of stream to available compactors
    compactors: Vec<Arc<Compactor>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
}

impl CompactorManagerInner {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            next_compactor: 0,
        }
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`. A compact task can be in one of these states:
/// - 1. Assigned: a compact task is assigned to a compactor via `HummockManager::get_compact_task`.
///   Assigned-->Finished/Cancelled.
/// - 2. Finished: an assigned task is reported as finished via
///   `CompactStatus::report_compact_task`. It's the final state.
/// - 3. Cancelled: an assigned task is reported as cancelled via
///   `CompactStatus::report_compact_task`. It's the final state.
pub struct CompactorManager {
    inner: parking_lot::RwLock<CompactorManagerInner>,
}

impl Default for CompactorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactorManager {
    pub fn new() -> Self {
        Self {
            inner: parking_lot::RwLock::new(CompactorManagerInner::new()),
        }
    }

    /// Gets next compactor to assign task.
    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        let mut guard = self.inner.write();
        if guard.compactors.is_empty() {
            return None;
        }
        let compactor_index = guard.next_compactor % guard.compactors.len();
        let compactor = guard.compactors[compactor_index].clone();
        guard.next_compactor += 1;
        Some(compactor)
    }

    pub fn random_compactor(&self) -> Option<Arc<Compactor>> {
        let guard = self.inner.read();
        if guard.compactors.is_empty() {
            return None;
        }

        let compactor_index = rand::thread_rng().gen::<usize>() % guard.compactors.len();
        let compactor = guard.compactors[compactor_index].clone();
        Some(compactor)
    }

    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
    ) -> Receiver<Result<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        let mut guard = self.inner.write();
        guard.compactors.retain(|c| c.context_id != context_id);
        guard.compactors.push(Arc::new(Compactor {
            context_id,
            sender: tx,
        }));
        tracing::info!("Added compactor {}", context_id);
        rx
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        tracing::info!("Removed compactor {}", context_id);
        self.inner
            .write()
            .compactors
            .retain(|c| c.context_id != context_id);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::CompactTask;
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::test_utils::{
        generate_test_tables, register_sstable_infos_to_compaction_group, setup_compute_env,
        to_local_sstable_info,
    };
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(hummock_manager: &HummockManager<S>, _context_id: u32, epoch: u64)
    where
        S: MetaStore,
    {
        let original_tables =
            generate_test_tables(epoch, hummock_manager.get_new_sst_ids(1).await.unwrap());
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        hummock_manager
            .commit_epoch(epoch, to_local_sstable_info(&original_tables))
            .await
            .unwrap();
    }

    fn dummy_compact_task(task_id: u64) -> CompactTask {
        CompactTask {
            input_ssts: vec![],
            splits: vec![],
            watermark: 0,
            sorted_output_ssts: vec![],
            task_id,
            target_level: 0,
            gc_delete_keys: false,
            task_status: false,
            compaction_group_id: StaticCompactionGroupId::StateDefault.into(),
            existing_table_ids: vec![],
            compression_algorithm: 0,
            target_file_size: 1,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
        }
    }

    #[tokio::test]
    async fn test_add_remove_compactor() {
        let compactor_manager = CompactorManager::new();
        // No compactors by default.
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);

        let mut receiver = compactor_manager.add_compactor(1);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);
        let _receiver_2 = compactor_manager.add_compactor(2);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 2);
        compactor_manager.remove_compactor(2);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123);
        let compactor = compactor_manager
            .inner
            .write()
            .compactors
            .first()
            .unwrap()
            .clone();
        compactor.send_task(Some(task.clone()), None).await.unwrap();
        // Receive a compact task.
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        compactor_manager.remove_compactor(compactor.context_id);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);
        drop(compactor);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));
    }

    #[tokio::test]
    async fn test_next_compactor() {
        let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = CompactorManager::new();
        add_compact_task(hummock_manager.as_ref(), context_id, 1).await;

        // No compactor available.
        assert!(compactor_manager.next_compactor().is_none());

        // Add a compactor.
        let mut receiver = compactor_manager.add_compactor(context_id);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);
        let compactor = compactor_manager.next_compactor().unwrap();
        // No compact task.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = hummock_manager
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap()
            .unwrap();
        compactor.send_task(Some(task.clone()), None).await.unwrap();
        // Get a compact task.
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        compactor_manager.remove_compactor(compactor.context_id());
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);
        assert!(compactor_manager.next_compactor().is_none());
    }

    #[tokio::test]
    async fn test_next_compactor_round_robin() {
        let compactor_manager = CompactorManager::new();
        let mut receivers = vec![];
        for context_id in 0..5 {
            receivers.push(compactor_manager.add_compactor(context_id));
        }
        assert_eq!(compactor_manager.inner.read().compactors.len(), 5);
        for i in 0..receivers.len() * 3 {
            let compactor = compactor_manager.next_compactor().unwrap();
            assert_eq!(compactor.context_id as usize, i % receivers.len());
        }
    }
}
