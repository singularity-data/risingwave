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

use std::collections::{HashMap, HashSet, VecDeque, BTreeMap};
use std::iter::once;
use std::mem::take;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use log::debug;
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::hummock::{SstableInfo as ProseSstableInf,KeyRange as ProseKeyRange};
use risingwave_pb::stream_plan::Barrier;
use risingwave_pb::stream_service::{
    BarrierCompleteRequest, BarrierCompleteResponse as ProseBarrierCompleteResponse, InjectBarrierRequest,
};
use smallvec::SmallVec;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{oneshot, watch, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub use self::command::Command;
use self::command::CommandContext;
use self::info::BarrierActorInfo;
use self::notifier::Notifier;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::BarrierEpochState::{Complete, InFlight};
use crate::barrier::ChangedTableState::{Create, Drop};
use crate::cluster::{ClusterManagerRef, META_NODE_ID};
use crate::hummock::HummockManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv};
use crate::model::BarrierManagerState;
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

mod command;
mod info;
mod notifier;
mod progress;
mod recovery;

type Scheduled = (Command, SmallVec<[Notifier; 1]>);

#[derive(Clone, PartialEq,Hash,Eq)]
pub struct GroupedSstableInfoAaa {
    pub epoch: u64,
    pub grouped_sstable_info: Vec<GroupedSstableInfo>,
}
#[derive(Clone, PartialEq,Hash,Eq)]
pub struct GroupedSstableInfo {
    pub compaction_group_id: u64,
    pub sst:Option<SstableInfo>,
}
#[derive(Clone, PartialEq, Eq,Hash)]
pub struct SstableInfo {
    pub id: u64,
    pub key_range: Option<KeyRange>,
    pub file_size: u64,
    pub table_ids: Vec<u32>,
    pub unit_id: u64,
}
#[derive(Clone, PartialEq, Eq,Hash)]
pub struct KeyRange {
    pub left: Vec<u8>,
    pub right: Vec<u8>,
    pub inf: bool,
}
impl SstableInfo{
    pub fn to_prost(self) -> ProseSstableInf{
        let key_range = match self.key_range{
            Some(k_range) =>{Some(ProseKeyRange{
                left:k_range.left,
                right:k_range.right,
                inf:k_range.inf,
            })},
            None =>{None},
        };
        ProseSstableInf{
            id:self.id,
            key_range,
            file_size:self.file_size,
            table_ids:self.table_ids,
            unit_id:self.unit_id,
        }
    }
}
impl GroupedSstableInfoAaa{
    pub fn from_prost(barrier_complete_response: &ProseBarrierCompleteResponse) -> Vec<Self>{
        let grouped_sstable_info_vec = barrier_complete_response.sycned_sstables.clone();
        let mut return_vec = vec![];
        for grouped_sstable_info_aaa in grouped_sstable_info_vec{
            let mut grouped_sstable_info = vec![];
            for group in grouped_sstable_info_aaa.grouped_sstable_info{
                let sst = match group.sst{
                    Some(sst_table_id) =>{
                        let key_range = match sst_table_id.key_range{
                            Some(k_r) => {
                                Some(KeyRange{
                                    left: k_r.left,
                                    right: k_r.right,
                                    inf: k_r.inf,
                                })
                            },
                            None =>{None},
                        };
                        Some(SstableInfo{
                            id:sst_table_id.id,
                            key_range:key_range,
                            file_size:sst_table_id.file_size,
                            table_ids:sst_table_id.table_ids,
                            unit_id:sst_table_id.unit_id,
                        })
                    },
                    None =>{None},
                };
                grouped_sstable_info.push(GroupedSstableInfo{
                    compaction_group_id:group.compaction_group_id,
                    sst,
                });
            }
            return_vec.push(GroupedSstableInfoAaa{
                epoch:grouped_sstable_info_aaa.epoch,
                grouped_sstable_info,
            });
        }
        return_vec
    }
}
/// A buffer or queue for scheduling barriers.
struct ScheduledBarriers {
    buffer: RwLock<VecDeque<Scheduled>>,

    /// When `buffer` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,
}
/// The table state of command
#[derive(Debug, Clone)]
pub enum ChangedTableState {
    Drop(TableId),
    Create(TableId),
    NoTable,
}

impl ScheduledBarriers {
    fn new() -> Self {
        Self {
            buffer: RwLock::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
        }
    }

    /// Pop a scheduled barrier from the buffer, or a default checkpoint barrier if not exists.
    async fn pop_or_default(&self) -> Scheduled {
        let mut buffer = self.buffer.write().await;

        // If no command scheduled, create periodic checkpoint barrier by default.
        buffer
            .pop_front()
            .unwrap_or_else(|| (Command::checkpoint(), Default::default()))
    }

    /// Wait for at least one scheduled barrier in the buffer.
    async fn wait_one(&self) {
        let buffer = self.buffer.read().await;
        if buffer.len() > 0 {
            return;
        }
        let mut rx = self.changed_tx.subscribe();
        drop(buffer);

        rx.changed().await.unwrap();
    }

    /// Push a scheduled barrier into the buffer.
    async fn push(&self, scheduled: Scheduled) {
        let mut buffer = self.buffer.write().await;
        buffer.push_back(scheduled);
        if buffer.len() == 1 {
            self.changed_tx.send(()).ok();
        }
    }

    /// Attach `new_notifiers` to the very first scheduled barrier. If there's no one scheduled, a
    /// default checkpoint barrier will be created.
    async fn attach_notifiers(&self, new_notifiers: impl IntoIterator<Item = Notifier>) {
        let mut buffer = self.buffer.write().await;
        match buffer.front_mut() {
            Some((_, notifiers)) => notifiers.extend(new_notifiers),
            None => {
                // If no command scheduled, create periodic checkpoint barrier by default.
                buffer.push_back((Command::checkpoint(), new_notifiers.into_iter().collect()));
                if buffer.len() == 1 {
                    self.changed_tx.send(()).ok();
                }
            }
        }
    }

    /// Clear all buffered scheduled barriers, and notify their subscribers with failed as aborted.
    async fn abort(&self) {
        let mut buffer = self.buffer.write().await;
        while let Some((_, notifiers)) = buffer.pop_front() {
            notifiers.into_iter().for_each(|notify| {
                notify.notify_collection_failed(RwError::from(ErrorCode::InternalError(
                    "Scheduled barrier abort.".to_string(),
                )))
            })
        }
    }
}

/// [`crate::barrier::GlobalBarrierManager`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::GlobalBarrierManager`] provides a set of interfaces like a state machine,
/// accepting [`Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`Command`].
pub struct GlobalBarrierManager<S: MetaStore> {
    /// The maximal interval for sending a barrier.
    interval: Duration,

    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    scheduled_barriers: ScheduledBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    cluster_manager: ClusterManagerRef<S>,

    catalog_manager: CatalogManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,

    hummock_manager: HummockManagerRef<S>,

    metrics: Arc<MetaMetrics>,

    env: MetaSrvEnv<S>,
}

struct CheckpointControl<S: MetaStore> {
    /// Save the state and message of barrier in order
    command_ctx_queue: VecDeque<EpochNode<S>>,
    /// In addition to the actors with status `Running`.The barrier needs to send or collect the
    /// actors of these tables.
    creating_table_ids: HashSet<TableId>,
    /// The barrier does not send or collect the actors of these tables, even if they are
    /// `Running`.
    dropping_table_ids: HashSet<TableId>,
}

impl<S> CheckpointControl<S>
where
    S: MetaStore,
{
    fn new() -> Self {
        Self {
            command_ctx_queue: VecDeque::default(),
            creating_table_ids: HashSet::default(),
            dropping_table_ids: HashSet::default(),
        }
    }

    /// Try to enxtend this command's `changed_table_id` in `creating_table_ids`.
    fn pre_inject(&mut self, command: &Command) {
        if let Create(table) = command.changed_table_id() {
            self.creating_table_ids.insert(table);
        }
    }

    /// Barrier can be sent to and collected from an actor if:
    /// 1. The actor is Running and not being dropped.
    /// 2. The actor is Inactive and belongs to a creating MV
    fn can_actor_send_or_collect(&self, s: ActorState, table_id: &TableId) -> bool {
        s == ActorState::Running && !self.dropping_table_ids.contains(table_id)
            || s == ActorState::Inactive && self.creating_table_ids.contains(table_id)
    }

    /// Return the nums of barrier (the nums of in-flight-barrier , the nums of all-barrier)
    fn get_barrier_len(&self) -> (usize, usize) {
        (
            self.command_ctx_queue
                .iter()
                .filter(|x| matches!(x.state, InFlight))
                .count(),
            self.command_ctx_queue.len(),
        )
    }

    /// Inject a `command_ctx` in `command_ctx_queue`, and it's state is `InFlight`.
    fn inject(
        &mut self,
        command_ctx: Arc<CommandContext<S>>,
        notifiers: SmallVec<[Notifier; 1]>,
        timer: HistogramTimer,
    ) {
        if let Drop(table) = command_ctx.command.changed_table_id() {
            self.dropping_table_ids.insert(table);
        }
        self.command_ctx_queue.push_back(EpochNode {
            timer: Some(timer),
            result: None,
            state: InFlight,
            command_ctx,
            notifiers,
        });
    }

    /// Change the state of this `prev_epoch` to `Complete`. Return continuous nodes
    /// with `Complete` starting from first node [`Complete`..`InFlight`) and remove them.
    fn complete(
        &mut self,
        prev_epoch: u64,
        result: Result<Vec<ProseBarrierCompleteResponse>>,
    ) -> VecDeque<EpochNode<S>> {
        // change state to complete, and wait for nodes with the smaller epoch to commit
        if let Some(node) = self
            .command_ctx_queue
            .iter_mut()
            .find(|x| x.command_ctx.prev_epoch.0 == prev_epoch)
        {
            assert!(matches!(node.state, InFlight));
            node.state = Complete;
            node.result = Some(result);
        };
        // Find all continuous nodes with 'Complete' starting from first node
        let index = self
            .command_ctx_queue
            .iter()
            .position(|x| !matches!(x.state, Complete))
            .unwrap_or(self.command_ctx_queue.len());
        let complete_nodes: VecDeque<EpochNode<S>> =
            self.command_ctx_queue.drain(..index).collect();
        complete_nodes.iter().for_each(|node| {
            self.remove_changed_table_ids(node.command_ctx.command.changed_table_id())
        });
        complete_nodes
    }

    /// Remove all nodes from queue and return them.
    fn fail(&mut self) -> VecDeque<EpochNode<S>> {
        let complete_nodes: VecDeque<EpochNode<S>> = self.command_ctx_queue.drain(..).collect();
        complete_nodes.iter().for_each(|node| {
            self.remove_changed_table_ids(node.command_ctx.command.changed_table_id())
        });
        complete_nodes
    }

    /// Pause inject barrier until True
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        self.command_ctx_queue
            .iter()
            .filter(|x| matches!(x.state, InFlight ))
            .count()
            < in_flight_barrier_nums
    }

    pub fn remove_changed_table_ids(&mut self, remove_changed_table: ChangedTableState) {
        match remove_changed_table {
            Create(table_id) => {
                self.creating_table_ids.remove(&table_id);
            }
            Drop(table_id) => {
                self.dropping_table_ids.remove(&table_id);
            }
            _ => {}
        }
    }
}

/// The state and message of this barrier
pub struct EpochNode<S: MetaStore> {
    timer: Option<HistogramTimer>,
    result: Option<Result<Vec<ProseBarrierCompleteResponse>>>,
    state: BarrierEpochState,
    command_ctx: Arc<CommandContext<S>>,
    notifiers: SmallVec<[Notifier; 1]>,
}
/// The state of barrier
#[derive(PartialEq)]
enum BarrierEpochState {
    InFlight,
    Complete,
}

impl<S> GlobalBarrierManager<S>
where
    S: MetaStore,
{
    /// Create a new [`crate::barrier::GlobalBarrierManager`].
    pub fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let interval = env.opts.checkpoint_interval;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;
        tracing::info!(
            "Starting barrier manager with: interval={:?}, enable_recovery={} , in_flight_barrier_nums={},test hehehe",
            interval,
            enable_recovery,
            in_flight_barrier_nums,
        );

        Self {
            interval,
            enable_recovery,
            cluster_manager,
            catalog_manager,
            fragment_manager,
            scheduled_barriers: ScheduledBarriers::new(),
            hummock_manager,
            metrics,
            env,
            in_flight_barrier_nums,
        }
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self) -> Result<()> {
        let start = Instant::now();

        debug!("start barrier flush");
        self.wait_for_next_barrier_to_collect().await?;

        let elapsed = Instant::now().duration_since(start);
        debug!("barrier flushed in {:?}", elapsed);

        Ok(())
    }

    pub async fn start(barrier_manager: BarrierManagerRef<S>) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            barrier_manager.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(&self, mut shutdown_rx: Receiver<()>) {
        let mut tracker = CreateMviewProgressTracker::default();
        let mut state = BarrierManagerState::create(self.env.meta_store()).await;
        if self.enable_recovery {
            // handle init, here we simply trigger a recovery process to achieve the consistency. We
            // may need to avoid this when we have more state persisted in meta store.
            let new_epoch = state.in_flight_prev_epoch.next();
            assert!(new_epoch > state.in_flight_prev_epoch);
            state.in_flight_prev_epoch = new_epoch;

            let (new_epoch, actors_to_track, create_mview_progress) =
                self.recovery(state.in_flight_prev_epoch).await;
            tracker.add(new_epoch, actors_to_track, vec![]);
            for progress in create_mview_progress {
                tracker.update(progress);
            }
            state.in_flight_prev_epoch = new_epoch;
            state
                .update_inflight_prev_epoch(self.env.meta_store())
                .await
                .unwrap();
        }
        let mut min_interval = tokio::time::interval(self.interval);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut barrier_timer: Option<HistogramTimer> = None;
        let (barrier_complete_tx, mut barrier_complete_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut checkpoint_control = CheckpointControl::new();
        let mut barrier_nums = 1;
        loop {
            tokio::select! {
                biased;
                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    return;
                }
                result = barrier_complete_rx.recv() => {
                    let (in_flight_nums, all_nums) = checkpoint_control.get_barrier_len();
                    self.metrics
                        .in_flight_barrier_nums
                        .set(in_flight_nums as i64);
                    self.metrics.all_barrier_nums.set(all_nums as i64);

                    let (prev_epoch, result) = result.unwrap();
                    self.barrier_complete_and_commit(
                        prev_epoch,
                        result,
                        &mut state,
                        &mut tracker,
                        &mut checkpoint_control,
                    )
                    .await;
                    continue;
                }
                // there's barrier scheduled.
                _ = self.scheduled_barriers.wait_one(), if checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {}
                // Wait for the minimal interval,
                _ = min_interval.tick(), if checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {}
            }

            if let Some(barrier_timer) = barrier_timer {
                barrier_timer.observe_duration();
            }
            barrier_timer = Some(self.metrics.barrier_send_latency.start_timer());
            let (command, notifiers) = self.scheduled_barriers.pop_or_default().await;
            checkpoint_control.pre_inject(&command);
            let info = self.resolve_actor_info(&checkpoint_control).await;
            // When there's no actors exist in the cluster, we don't need to send the barrier. This
            // is an advance optimization. Besides if another barrier comes immediately,
            // it may send a same epoch and fail the epoch check.
            if info.nothing_to_do() {
                let mut notifiers = notifiers;
                notifiers.iter_mut().for_each(Notifier::notify_to_send);
                notifiers.iter_mut().for_each(Notifier::notify_collected);
                continue;
            }
            let prev_epoch = state.in_flight_prev_epoch;
            let new_epoch = prev_epoch.next();
            state.in_flight_prev_epoch = new_epoch;
            assert!(
                new_epoch > prev_epoch,
                "new{:?},prev{:?}",
                new_epoch,
                prev_epoch
            );
            state
                .update_inflight_prev_epoch(self.env.meta_store())
                .await
                .unwrap();
            let is_sync = if barrier_nums % 40 == 0 || !matches!(command, Command::Plain(_)) {
                barrier_nums = 1;
                true
            } else {
                barrier_nums += 1;
                false
            };
            let command_ctx = Arc::new(CommandContext::new(
                self.fragment_manager.clone(),
                self.env.stream_client_pool_ref(),
                info,
                prev_epoch,
                new_epoch,
                command,
                is_sync,
            ));
            let mut notifiers = notifiers;
            notifiers.iter_mut().for_each(Notifier::notify_to_send);
            let timer = self.metrics.barrier_latency.start_timer();
            checkpoint_control.inject(command_ctx.clone(), notifiers, timer);

            self.inject_and_send_err(command_ctx, barrier_complete_tx.clone())
                .await;
        }
    }

    /// Inject barrier and send err.
    async fn inject_and_send_err(
        &self,
        command_context: Arc<CommandContext<S>>,
        barrier_complete_tx: UnboundedSender<(u64, Result<Vec<ProseBarrierCompleteResponse>>)>,
    ) {
        let result = self
            .inject_barrier(command_context.clone(), barrier_complete_tx.clone())
            .await;
        if let Err(e) = result {
            barrier_complete_tx
                .send((command_context.prev_epoch.0, Err(e)))
                .unwrap();
        }
    }

    /// Send inject-barrier-rpc to stream service and wait for its response before returns.
    /// Then spawn a new tokio task to send barrier-complete-rpc and wait for its response
    async fn inject_barrier(
        &self,
        command_context: Arc<CommandContext<S>>,
        barrier_complete_tx: UnboundedSender<(u64, Result<Vec<ProseBarrierCompleteResponse>>)>,
    ) -> Result<()> {
        fail_point!("inject_barrier_err", |_| Err(RwError::from(
            ErrorCode::InternalError("inject_barrier_err".to_string(),)
        )));
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info.clone();
        let mut node_need_collect = HashMap::new();
        let inject_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
            let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();
            if actor_ids_to_collect.is_empty() {
                // No need to send or collect barrier for this node.
                assert!(actor_ids_to_send.is_empty());
                node_need_collect.insert(*node_id, false);
                None
            } else {
                node_need_collect.insert(*node_id, true);
                let mutation = mutation.clone();
                let request_id = Uuid::new_v4().to_string();
                let barrier = Barrier {
                    epoch: Some(risingwave_pb::data::Epoch {
                        curr: command_context.curr_epoch.0,
                        prev: command_context.prev_epoch.0,
                    }),
                    mutation,
                    // TODO(chi): add distributed tracing
                    span: vec![],
                    is_sync: command_context.is_sync,
                };
                async move {
                    let mut client = self.env.stream_client_pool().get(node).await?;

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_send,
                        actor_ids_to_collect,
                    };
                    tracing::trace!(
                        target: "events::meta::barrier::inject_barrier",
                        "inject barrier request: {:?}", request
                    );

                    // This RPC returns only if this worker node has injected this barrier.
                    client
                        .inject_barrier(request)
                        .await
                        .map(tonic::Response::<_>::into_inner)
                        .map_err(RwError::from)
                }
                .into()
            }
        });
        try_join_all(inject_futures).await?;
        let env = self.env.clone();
        tokio::spawn(async move {
            let prev_epoch = command_context.prev_epoch.0;
            let is_sync = command_context.is_sync;
            let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
                if !*node_need_collect.get(node_id).unwrap() {
                    // No need to send or collect barrier for this node.
                    None
                } else {
                    let request_id = Uuid::new_v4().to_string();
                    let env = env.clone();
                    async move {
                        let mut client = env.stream_client_pool().get(node).await?;
                        let request = BarrierCompleteRequest {
                            request_id,
                            prev_epoch,
                            is_sync,
                        };
                        tracing::trace!(
                            target: "events::meta::barrier::barrier_complete",
                            "barrier complete request: {:?}", request
                        );

                        // This RPC returns only if this worker node has collected this barrier.
                        client
                            .barrier_complete(request)
                            .await
                            .map(tonic::Response::<_>::into_inner)
                            .map_err(RwError::from)
                    }
                    .into()
                }
            });

            let result = try_join_all(collect_futures).await;
            barrier_complete_tx.send((prev_epoch, result)).unwrap();
        });
        Ok(())
    }

    /// Changes the state is `Complete`, and try commit all epoch that state is `Complete` in
    /// order. If commit is err, all nodes will be handled.
    async fn barrier_complete_and_commit(
        &self,
        prev_epoch: u64,
        result: Result<Vec<ProseBarrierCompleteResponse>>,
        state: &mut BarrierManagerState,
        tracker: &mut CreateMviewProgressTracker,
        checkpoint_control: &mut CheckpointControl<S>,
    ) {
        // change the state is Complete
        let mut complete_nodes = checkpoint_control.complete(prev_epoch, result);
        // try commit complete nodes
        let (mut index, mut err_msg) = (0, None);
        for (i, node) in complete_nodes.iter_mut().enumerate() {
            assert!(matches!(node.state, Complete));
            if let Err(err) = self.complete_barriers(node, tracker).await {
                index = i;
                err_msg = Some(err);
                break;
            }
        }
        // Handle the error node and the nodes after it
        if let Some(err) = err_msg {
            fail_point!("inject_barrier_err_success");
            let fail_nodes = complete_nodes
                .drain(index..)
                .chain(checkpoint_control.fail().into_iter());
            let mut new_epoch = Epoch::from(INVALID_EPOCH);
            for node in fail_nodes {
                if let Some(timer) = node.timer {
                    timer.observe_duration();
                }
                node.notifiers
                    .into_iter()
                    .for_each(|notifier| notifier.notify_collection_failed(err.clone()));
                new_epoch = node.command_ctx.prev_epoch;
            }
            if self.enable_recovery {
                // If failed, enter recovery mode.
                let (new_epoch, actors_to_track, create_mview_progress) =
                    self.recovery(new_epoch).await;
                *tracker = CreateMviewProgressTracker::default();
                tracker.add(new_epoch, actors_to_track, vec![]);
                for progress in create_mview_progress {
                    tracker.update(progress);
                }
                state.in_flight_prev_epoch = new_epoch;
                state
                    .update_inflight_prev_epoch(self.env.meta_store())
                    .await
                    .unwrap();
            } else {
                panic!("failed to execute barrier: {:?}", err);
            }
        }
    }

    /// Try to commit this node. It err, returns
    async fn complete_barriers(
        &self,
        node: &mut EpochNode<S>,
        tracker: &mut CreateMviewProgressTracker,
    ) -> Result<()> {
        if node.command_ctx.prev_epoch.0 != INVALID_EPOCH {
            match node.result.as_ref().expect("node result is None") {
                Ok(resps) => {
                    // We must ensure all epochs are committed in ascending order,
                    // because the storage engine will
                    // query from new to old in the order in which the L0 layer files are generated. see https://github.com/singularity-data/risingwave/issues/1251
                    
                    if resps.iter().all(|node| node.is_sync) {
                        //tracing::info!("commit{:?}",node.command_ctx.prev_epoch);

                        let mut set = HashSet::<GroupedSstableInfo>::default();
                        //let mut btree_map = BTreeMap::<u64,HashSet<GroupedSstableInfo>>::default();
                        resps
                            .iter()
                            .flat_map(|resp| GroupedSstableInfoAaa::from_prost(resp))
                            .for_each(|node|{
                                //let entry = btree_map.entry(node.epoch).or_insert(HashSet::<GroupedSstableInfo>::default());
                                node.grouped_sstable_info.into_iter().for_each(|node1| {set.insert(node1);});
                            });
                        //let mut vec = vec![];
                        // if !btree_map.contains_key(&node.command_ctx.prev_epoch.0){
                        //     btree_map.insert(node.command_ctx.prev_epoch.0, HashSet::default());
                        // }
                        // while let Some((key,value)) = btree_map.pop_first(){
                        //     let hummock = self.hummock_manager.clone();
                        //     //tracing::info!("commit{:?}",key);
                        //     let sst_info = value.into_iter().map(|x| (x.compaction_group_id,x.sst.expect("field not None").to_prost())).collect_vec();
                        //     vec.push(async move{
                        //         hummock
                        //             .commit_epoch(key, sst_info).await
                        //     })
                        // }
                        // try_join_all(vec).await?;
                        let sst_info = set.into_iter().map(|x| (x.compaction_group_id,x.sst.expect("field not None").to_prost())).collect_vec();
                        self.hummock_manager.commit_epoch(node.command_ctx.prev_epoch.0,sst_info).await?;
                        //tracing::info!("commit over {:?}",node.command_ctx.prev_epoch);
                   }
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to commit epoch {}: {:#?}",
                        node.command_ctx.prev_epoch.0,
                        err
                    );
                }
            };
        }

        node.timer.take().unwrap().observe_duration();
        let responses = node.result.take().unwrap()?;
        node.command_ctx.post_collect().await?;

        // Notify about collected first.
        let mut notifiers = take(&mut node.notifiers);
        notifiers.iter_mut().for_each(Notifier::notify_collected);
        // Then try to finish the barrier for Create MVs.
        let actors_to_finish = node.command_ctx.actors_to_track();
        tracker.add(node.command_ctx.curr_epoch, actors_to_finish, notifiers);
        for progress in responses.into_iter().flat_map(|r| r.create_mview_progress) {
            tracker.update(progress);
        }
        Ok(())
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_actor_info(
        &self,
        checkpoint_control: &CheckpointControl<S>,
    ) -> BarrierActorInfo {
        let check_state = |s: ActorState, table_id: &TableId| {
            checkpoint_control.can_actor_send_or_collect(s, table_id)
        };
        let all_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;
        let all_actor_infos = self.fragment_manager.load_all_actors(check_state).await;
        BarrierActorInfo::resolve(all_nodes, all_actor_infos)
    }

    async fn do_schedule(&self, command: Command, notifier: Notifier) -> Result<()> {
        self.scheduled_barriers
            .push((command, once(notifier).collect()))
            .await;
        Ok(())
    }

    /// Schedule a command and return immediately.
    pub async fn schedule_command(&self, command: Command) -> Result<()> {
        self.do_schedule(command, Default::default()).await
    }

    /// Schedule a command and return when its corresponding barrier is about to sent.
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.do_schedule(
            command,
            Notifier {
                to_send: Some(tx),
                ..Default::default()
            },
        )
        .await?;
        rx.await.unwrap();

        Ok(())
    }

    /// Run a command and return when it's completely finished.
    pub async fn run_command(&self, command: Command) -> Result<()> {
        let (collect_tx, collect_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();

        let is_create_mv = matches!(command, Command::CreateMaterializedView { .. });

        self.do_schedule(
            command,
            Notifier {
                collected: Some(collect_tx),
                finished: Some(finish_tx),
                ..Default::default()
            },
        )
        .await?;

        collect_rx.await.unwrap()?; // Throw the error if it occurs when collecting this barrier.

        // TODO: refactor this
        if is_create_mv {
            // The snapshot ingestion may last for several epochs, we should pin the epoch here.
            // TODO: this should be done in `post_collect`
            let _snapshot = self.hummock_manager.pin_snapshot(META_NODE_ID).await?;
            finish_rx.await.unwrap(); // Wait for this command to be finished.
            self.hummock_manager.unpin_snapshot(META_NODE_ID).await?;
        } else {
            finish_rx.await.unwrap(); // Wait for this command to be finished.
        }

        Ok(())
    }

    /// Wait for the next barrier to collect. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    pub async fn wait_for_next_barrier_to_collect(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let notifier = Notifier {
            collected: Some(tx),
            ..Default::default()
        };
        self.scheduled_barriers
            .attach_notifiers(once(notifier))
            .await;
        rx.await.unwrap()
    }
}

pub type BarrierManagerRef<S> = Arc<GlobalBarrierManager<S>>;
