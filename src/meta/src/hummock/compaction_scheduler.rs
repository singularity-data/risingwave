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

use parking_lot::Mutex;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::CompactionGroupId;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::Receiver;

use crate::hummock::compaction::CompactStatus;
use crate::hummock::error::Error;
use crate::hummock::{CompactorManagerRef, HummockManagerRef};
use crate::storage::MetaStore;

pub type CompactionSchedulerRef<S> = Arc<CompactionScheduler<S>>;

pub type CompactionRequestChannelRef = Arc<CompactionRequestChannel>;
/// [`CompactionRequestChannel`] wrappers a mpsc channel and deduplicate requests from same
/// compaction groups.
pub struct CompactionRequestChannel {
    request_tx: UnboundedSender<CompactionGroupId>,
    scheduled: Mutex<HashSet<CompactionGroupId>>,
}

impl CompactionRequestChannel {
    fn new(request_tx: UnboundedSender<CompactionGroupId>) -> Self {
        Self {
            request_tx,
            scheduled: Default::default(),
        }
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_send(&self, compaction_group: CompactionGroupId) -> bool {
        let mut guard = self.scheduled.lock();
        if guard.contains(&compaction_group) {
            return false;
        }
        if self.request_tx.send(compaction_group).is_err() {
            return false;
        }
        guard.insert(compaction_group);
        true
    }

    fn unschedule(&self, compaction_group: CompactionGroupId) {
        self.scheduled.lock().remove(&compaction_group);
    }
}

/// Schedules compaction task picking and assignment.
pub struct CompactionScheduler<S>
where
    S: MetaStore,
{
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    pub fn new(
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Self {
        Self {
            hummock_manager,
            compactor_manager,
        }
    }

    pub async fn start(&self, mut shutdown_rx: Receiver<()>) {
        let (request_tx, mut request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionGroupId>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));
        self.hummock_manager
            .set_compaction_scheduler(request_channel.clone());
        tracing::info!("Start compaction scheduler.");
        'compaction_trigger: loop {
            let compaction_group: CompactionGroupId = tokio::select! {
                compaction_group = request_rx.recv() => {
                    match compaction_group {
                        Some(compaction_group) => compaction_group,
                        None => {
                            break 'compaction_trigger;
                        }
                    }
                },
                // Shutdown compactor
                _ = &mut shutdown_rx => {
                    break 'compaction_trigger;
                }
            };
            self.pick_and_assign(compaction_group, request_channel.clone())
                .await;
        }
        tracing::info!("Compaction scheduler is stopped");
    }

    async fn pick_and_assign(
        &self,
        compaction_group: CompactionGroupId,
        request_channel: Arc<CompactionRequestChannel>,
    ) -> bool {
        // 1. Pick a compaction task.
        let compact_task = self
            .hummock_manager
            .get_compact_task(compaction_group)
            .await;
        request_channel.unschedule(compaction_group);
        let mut compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // No compaction task available.
                return false;
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                return false;
            }
        };
        tracing::trace!(
            "Picked compaction task. {}",
            compact_task_to_string(&compact_task)
        );
        // TODO: merge this two operation in one lock guard because the target sub-level may be
        // removed by the other thread.
        if CompactStatus::is_trivial_move_task(&compact_task) {
            compact_task.task_status = true;
            compact_task.sorted_output_ssts = compact_task.input_ssts[0].table_infos.clone();
            tracing::info!(
                "trivial move task: \n{}",
                compact_task_to_string(&compact_task)
            );
            return self
                .hummock_manager
                .report_compact_task_impl(&compact_task, true)
                .await
                .is_ok();
        }

        // 2. Assign the compaction task to a compactor.
        'send_task: loop {
            // 2.1 Select a compactor.
            let compactor = match self.compactor_manager.next_compactor() {
                None => {
                    tracing::warn!("No compactor is available.");
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue 'send_task;
                }
                Some(compactor) => compactor,
            };
            // TODO: skip busy compactor

            // 2.2 Send the compaction task to the compactor.
            let send_task = async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    compactor
                        .send_task(Some(compact_task.clone()), None)
                        .await
                        .is_ok()
                })
                .await
                .unwrap_or(false)
            };
            match self
                .hummock_manager
                .assign_compaction_task(&compact_task, compactor.context_id(), send_task)
                .await
            {
                Ok(_) => {
                    // TODO: timeout assigned compaction task and move send_task after
                    // assign_compaction_task
                    tracing::trace!(
                        "Assigned compaction task. {}",
                        compact_task_to_string(&compact_task)
                    );
                    // Reschedule it in case there are more tasks from this compaction group.
                    request_channel.try_send(compaction_group);
                    return true;
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to assign compaction task to compactor {}: {:#?}",
                        compactor.context_id(),
                        err
                    );
                    match err {
                        Error::InvalidContext(_) | Error::CompactorUnreachable(_) => {
                            self.compactor_manager
                                .remove_compactor(compactor.context_id());
                        }
                        _ => {}
                    }
                    continue 'send_task;
                }
            }
        }
    }
}
