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

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::{stream, StreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::error::ErrorCode::OK;
use risingwave_common::util::worker_util::get_workers_by_parallel_unit_ids;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeNode, ExchangeSource, MergeSortExchangeNode, PlanFragment, PlanNode as PlanNodeProst,
    TaskId as TaskIdProst, TaskOutputId,
};
use risingwave_pb::common::{HostAddress, WorkerNode};
use risingwave_pb::task_service::AbortTaskRequest;
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::spawn;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::error;
use uuid::Uuid;
use StageEvent::Failed;

use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::distributed::stage::StageState::Pending;
use crate::scheduler::distributed::QueryMessage;
use crate::scheduler::plan_fragmenter::{
    ExecutionPlanNode, PartitionInfo, QueryStageRef, StageId, TaskId,
};
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::SchedulerError::Internal;
use crate::scheduler::{SchedulerError, SchedulerResult};

const TASK_SCHEDULING_PARALLELISM: usize = 10;

enum StageState {
    Pending,
    Started {
        sender: Sender<StageMessage>,
        handle: JoinHandle<SchedulerResult<JoinHandle<()>>>,
    },
    Running {
        sender: Sender<StageMessage>,
        handle: JoinHandle<SchedulerResult<JoinHandle<()>>>,
    },
    Completed,
    Failed,
}

enum StageMessage {
    Stop,
}

#[derive(Debug)]
pub enum StageEvent {
    Scheduled(StageId),
    /// Stage failed.
    Failed {
        id: StageId,
        reason: SchedulerError,
    },
    Completed(StageId),
}

#[derive(Clone)]
pub struct TaskStatus {
    _task_id: TaskId,

    // None before task is scheduled.
    location: Option<HostAddress>,
}

struct TaskStatusHolder {
    inner: ArcSwap<TaskStatus>,
}

pub struct StageExecution {
    epoch: u64,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    state: Arc<RwLock<StageState>>,
    msg_sender: Sender<QueryMessage>,

    /// Children stage executions.
    ///
    /// We use `Vec` here since children's size is usually small.
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
}

struct StageRunner {
    epoch: u64,
    state: Arc<RwLock<StageState>>,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    _receiver: Receiver<StageMessage>,
    // Send message to `QueryRunner` to notify stage state change.
    msg_sender: Sender<QueryMessage>,
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
}

impl TaskStatusHolder {
    fn new(task_id: TaskId) -> Self {
        let task_status = TaskStatus {
            _task_id: task_id,
            location: None,
        };

        Self {
            inner: ArcSwap::new(Arc::new(task_status)),
        }
    }

    fn get_status(&self) -> Arc<TaskStatus> {
        self.inner.load_full()
    }
}

impl StageExecution {
    pub fn new(
        epoch: u64,
        stage: QueryStageRef,
        worker_node_manager: WorkerNodeManagerRef,
        msg_sender: Sender<QueryMessage>,
        children: Vec<Arc<StageExecution>>,
        compute_client_pool: ComputeClientPoolRef,
    ) -> Self {
        let tasks = (0..stage.parallelism)
            .into_iter()
            .map(|task_id| (task_id, TaskStatusHolder::new(task_id)))
            .collect();
        Self {
            epoch,
            stage,
            worker_node_manager,
            tasks: Arc::new(tasks),
            state: Arc::new(RwLock::new(Pending)),
            msg_sender,
            children,
            compute_client_pool,
        }
    }

    /// Starts execution of this stage, returns error if already started.
    pub async fn start(&self) -> SchedulerResult<()> {
        let mut s = self.state.write().await;
        match &*s {
            &StageState::Pending => {
                let (sender, receiver) = channel(100);
                let runner = StageRunner {
                    epoch: self.epoch,
                    stage: self.stage.clone(),
                    worker_node_manager: self.worker_node_manager.clone(),
                    tasks: self.tasks.clone(),
                    _receiver: receiver,
                    msg_sender: self.msg_sender.clone(),
                    children: self.children.clone(),
                    state: self.state.clone(),
                    compute_client_pool: self.compute_client_pool.clone(),
                };
                let handle = spawn(async move {
                    runner.run().await.or_else(|e| {
                        error!("Stage failed: {:?}", e);
                        Err(e)
                    })
                });

                *s = StageState::Started { sender, handle };
                Ok(())
            }
            _ => {
                // This is possible since we notify stage schedule event to query runner, which may
                // receive multi events and start stage multi times.
                tracing::trace!(
                    "Staged {:?}-{:?} already started, skipping.",
                    &self.stage.query_id,
                    &self.stage.id
                );
                Ok(())
            }
        }
    }

    #[expect(clippy::unused_async)]
    pub async fn stop(&self) -> SchedulerResult<()> {
        let state = self.state.read().await;
        match *state {
            StageState::Started | StageState::Running { sender, handle } => {
                sender.send(StageMessage::Stop);
                // Here await the handle of stage runner (call abort RPC, changing states etc). If
                // the handle is done, all tasks must be abort.
                handle.await?;
            }
            _ => {}
        }

        Ok(())
    }

    pub async fn is_scheduled(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Running { .. })
    }

    pub fn get_task_status_unchecked(&self, task_id: TaskId) -> Arc<TaskStatus> {
        self.tasks[&task_id].get_status()
    }

    /// Returns all exchange sources for `output_id`. Each `ExchangeSource` is identified by
    /// producer's `TaskId` and `output_id` (consumer's `TaskId`), since each task may produce
    /// output to several channels.
    ///
    /// When this method is called, all tasks should have been scheduled, and their `worker_node`
    /// should have been set.
    fn all_exchange_sources_for(&self, output_id: u32) -> Vec<ExchangeSource> {
        self.tasks
            .iter()
            .map(|(task_id, status_holder)| {
                let task_output_id = TaskOutputId {
                    task_id: Some(TaskIdProst {
                        query_id: self.stage.query_id.id.clone(),
                        stage_id: self.stage.id,
                        task_id: *task_id,
                    }),
                    output_id,
                };

                ExchangeSource {
                    task_output_id: Some(task_output_id),
                    host: Some(status_holder.inner.load_full().location.clone().unwrap()),
                    local_execute_plan: None,
                }
            })
            .collect()
    }
}

impl StageRunner {
    async fn run(mut self) -> SchedulerResult<JoinHandle<SchedulerResult<()>>> {
        // Spawn a task to listen on receiver. Once there is a stop message, we should cancel all
        // current running tasks and change state.
        let handle = spawn(async move { self.cancel_when_required() });
        if let Err(e) = self.schedule_tasks().await {
            error!(
                "Stage {:?}-{:?} failed to schedule tasks, error: {:?}",
                self.stage.query_id, self.stage.id, e
            );
            // TODO: We should cancel all scheduled tasks
            self.send_event(QueryMessage::Stage(Failed {
                id: self.stage.id,
                reason: e,
            }))
            .await?;
            return Ok(());
        }
        // Have been canceled by query manager. No need to send anymore messages.
        if !handle.is_finished() {
            // Changing state
            let mut s = self.state.write().await;
            match mem::replace(&mut *s, StageState::Failed) {
                StageState::Started { sender, handle } => {
                    *s = StageState::Running {
                        sender: sender,
                        handle: handle,
                    };
                }
                _ => unreachable!(),
            }
            // All tasks scheduled, send `StageScheduled` event to `QueryRunner`.
            self.send_event(QueryMessage::Stage(StageEvent::Scheduled(self.stage.id)))
                .await?;
        }

        Ok(handle)
    }

    fn cancel_when_required(&mut self) -> SchedulerResult<()> {
        if let Some(s) = self._receiver.recv().await {
            assert_eq!(s, StageMessage::Stop);
            // Changing state
            let mut s = self.state.write().await;
            match mem::replace(&mut *s, StageState::Failed) {
                _ => {
                    // 1. Change state in each state runner.
                    *s = StageState::Failed;
                    for (task, task_status) in self.tasks.iter() {
                        let addr = task_status
                            .get_status()
                            .location
                            .as_ref()
                            .expect("Get address should not fail");
                        let client = self
                            .compute_client_pool
                            .get_client_for_addr(addr.clone().into())
                            .await
                            .map_err(|e| anyhow!(e))?;

                        // 2. Send RPC to each compute node for each task.
                        client
                            .abort(AbortTaskRequest {
                                task_id: Some(risingwave_pb::batch_plan::TaskId {
                                    query_id: "".to_string(),
                                    stage_id: 0,
                                    task_id: *task,
                                }),
                            })
                            .await?;
                    }
                }
                // Already completed.
                StageState::Completed => {}
            }
        }
        Ok(())
    }

    /// Send stage event to listener.
    async fn send_event(&self, event: QueryMessage) -> SchedulerResult<()> {
        self.msg_sender.send(event).await.map_err(|e| {
            {
                Internal(anyhow!(
                    "Failed to send stage scheduled event: {:?}, reason: {:?}",
                    self.stage.id,
                    e
                ))
            }
        })
    }

    async fn schedule_tasks(&self) -> SchedulerResult<()> {
        let mut futures = vec![];

        if let Some(table_scan_info) = self.stage.table_scan_info.as_ref() && let Some(vnode_bitmaps) = table_scan_info.partitions.as_ref() {
            // If the stage has table scan nodes, we create tasks according to the data distribution
            // and partition of the table.
            // We let each task read one partition by setting the `vnode_ranges` of the scan node in
            // the task.
            // We schedule the task to the worker node that owns the data partition.
            let parallel_unit_ids = vnode_bitmaps.keys().cloned().collect_vec();
            let all_workers = self.worker_node_manager.list_worker_nodes();
            let workers = match get_workers_by_parallel_unit_ids(&all_workers, &parallel_unit_ids) {
                Ok(workers) => workers,
                Err(e) => bail!("{}", e)
            };

            for (i, (parallel_unit_id, worker)) in parallel_unit_ids
                .into_iter()
                .zip_eq(workers.into_iter())
                .enumerate()
            {
                let task_id = TaskIdProst {
                    query_id: self.stage.query_id.id.clone(),
                    stage_id: self.stage.id,
                    task_id: i as u32,
                };
                let vnode_ranges = vnode_bitmaps[&parallel_unit_id].clone();
                let plan_fragment = self.create_plan_fragment(i as u32, Some(vnode_ranges));
                futures.push(self.schedule_task(task_id, plan_fragment, Some(worker)));
            }
        } else {
            for id in 0..self.stage.parallelism {
                let task_id = TaskIdProst {
                    query_id: self.stage.query_id.id.clone(),
                    stage_id: self.stage.id,
                    task_id: id,
                };
                let plan_fragment = self.create_plan_fragment(id, None);
                futures.push(self.schedule_task(task_id, plan_fragment, None));
            }
        }
        let mut buffered = stream::iter(futures).buffer_unordered(TASK_SCHEDULING_PARALLELISM);
        while let Some(result) = buffered.next().await {
            result?;
        }
        Ok(())
    }

    async fn schedule_task(
        &self,
        task_id: TaskIdProst,
        plan_fragment: PlanFragment,
        worker: Option<WorkerNode>,
    ) -> SchedulerResult<()> {
        let worker_node_addr = worker
            .unwrap_or(self.worker_node_manager.next_random()?)
            .host
            .unwrap();

        #[expect(clippy::needless_borrow)]
        let compute_client = self
            .compute_client_pool
            .get_client_for_addr((&worker_node_addr).into())
            .await
            .map_err(|e| anyhow!(e))?;

        let t_id = task_id.task_id;
        compute_client
            .create_task(task_id, plan_fragment, self.epoch)
            .await
            .map_err(|e| anyhow!(e))?;

        self.tasks[&t_id].inner.store(Arc::new(TaskStatus {
            _task_id: t_id,
            location: Some(worker_node_addr),
        }));

        Ok(())
    }

    fn create_plan_fragment(
        &self,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
    ) -> PlanFragment {
        let plan_node_prost = self.convert_plan_node(&self.stage.root, task_id, partition);
        let exchange_info = self.stage.exchange_info.clone();

        PlanFragment {
            root: Some(plan_node_prost),
            exchange_info: Some(exchange_info),
        }
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
    ) -> PlanNodeProst {
        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                // Find the stage this exchange node should fetch from and get all exchange sources.
                let child_stage = self
                    .children
                    .iter()
                    .find(|child_stage| {
                        child_stage.stage.id == execution_plan_node.source_stage_id.unwrap()
                    })
                    .unwrap();
                let exchange_sources = child_stage.all_exchange_sources_for(task_id);

                match &execution_plan_node.node {
                    NodeBody::Exchange(_exchange_node) => {
                        PlanNodeProst {
                            children: vec![],
                            // TODO: Generate meaningful identify
                            identity: Uuid::new_v4().to_string(),
                            node_body: Some(NodeBody::Exchange(ExchangeNode {
                                sources: exchange_sources,
                                input_schema: execution_plan_node.schema.clone(),
                            })),
                        }
                    }
                    NodeBody::MergeSortExchange(sort_merge_exchange_node) => {
                        PlanNodeProst {
                            children: vec![],
                            // TODO: Generate meaningful identify
                            identity: Uuid::new_v4().to_string(),
                            node_body: Some(NodeBody::MergeSortExchange(MergeSortExchangeNode {
                                exchange: Some(ExchangeNode {
                                    sources: exchange_sources,
                                    input_schema: execution_plan_node.schema.clone(),
                                }),
                                column_orders: sort_merge_exchange_node.column_orders.clone(),
                            })),
                        }
                    }
                    _ => unreachable!(),
                }
            }
            PlanNodeType::BatchSeqScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::RowSeqScan(mut scan_node) = node_body else {
                    unreachable!();
                };
                let partition = partition.unwrap();
                scan_node.vnode_bitmap = Some(partition.vnode_bitmap);
                scan_node.scan_ranges = partition.scan_ranges;
                PlanNodeProst {
                    children: vec![],
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(NodeBody::RowSeqScan(scan_node)),
                }
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| self.convert_plan_node(e, task_id, partition.clone()))
                    .collect();

                PlanNodeProst {
                    children,
                    // TODO: Generate meaningful identify
                    identity: Uuid::new_v4().to_string(),
                    node_body: Some(execution_plan_node.node.clone()),
                }
            }
        }
    }
}

impl TaskStatus {
    pub fn task_host_unchecked(&self) -> HostAddress {
        self.location.clone().unwrap()
    }
}
