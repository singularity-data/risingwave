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

use core::time::Duration;
use std::fmt::Debug;
use std::sync::Arc;

use itertools::Itertools;
use madsim::collections::{HashMap, HashSet};
use parking_lot::Mutex;
use risingwave_common::buffer::Bitmap;
use risingwave_common::config::StreamingConfig;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::{stream_plan, stream_service};
use risingwave_rpc_client::ComputeClientPool;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;

use super::{unique_executor_id, unique_operator_id, CollectResult};
use crate::executor::dispatch::*;
use crate::executor::merge::RemoteInput;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::*;
use crate::from_proto::create_executor;
use crate::task::{
    ActorId, ConsumableChannelPair, FragmentId, SharedContext, StreamEnvironment, UpDownActorIds,
    LOCAL_OUTPUT_CHANNEL_SIZE,
};

#[cfg(test)]
lazy_static::lazy_static! {
    pub static ref LOCAL_TEST_ADDR: HostAddr = "127.0.0.1:2333".parse().unwrap();
}

pub type ActorHandle = JoinHandle<()>;

pub struct LocalStreamManagerCore {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<ActorId, ActorHandle>,

    pub(crate) context: Arc<SharedContext>,

    /// Stores all actor information, taken after actor built.
    actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Stores all actor tokio runtime montioring tasks.
    actor_monitor_tasks: HashMap<ActorId, JoinHandle<()>>,

    /// Mock source, `actor_id = 0`.
    /// TODO: remove this
    mock_source: ConsumableChannelPair,

    /// The state store implement
    state_store: StateStoreImpl,

    /// Metrics of the stream manager
    pub(crate) streaming_metrics: Arc<StreamingMetrics>,

    /// The pool of compute clients.
    ///
    /// TODO: currently the client pool won't be cleared. Should remove compute clients when
    /// disconnected.
    compute_client_pool: ComputeClientPool,

    /// Config of streaming engine
    pub(crate) config: StreamingConfig,
}

/// `LocalStreamManager` manages all stream executors in this project.
pub struct LocalStreamManager {
    core: Mutex<LocalStreamManagerCore>,
}

pub struct ExecutorParams {
    pub env: StreamEnvironment,

    /// Indices of primary keys
    pub pk_indices: PkIndices,

    /// Executor id, unique across all actors.
    pub executor_id: u64,

    /// Operator id, unique for each operator in fragment.
    pub operator_id: u64,

    /// Information of the operator from plan node.
    pub op_info: String,

    /// The input executor.
    pub input: Vec<BoxedExecutor>,

    /// Id of the actor.
    pub actor_id: ActorId,

    /// FragmentId of the actor
    pub fragment_id: FragmentId,

    /// Metrics
    pub executor_stats: Arc<StreamingMetrics>,

    // Actor context
    pub actor_context: ActorContextRef,

    // Vnodes owned by this executor. Represented in bitmap.
    pub vnode_bitmap: Option<Bitmap>,
}

impl Debug for ExecutorParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorParams")
            .field("pk_indices", &self.pk_indices)
            .field("executor_id", &self.executor_id)
            .field("operator_id", &self.operator_id)
            .field("op_info", &self.op_info)
            .field("input", &self.input.len())
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}

impl LocalStreamManager {
    fn with_core(core: LocalStreamManagerCore) -> Self {
        Self {
            core: Mutex::new(core),
        }
    }

    pub fn new(
        addr: HostAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
    ) -> Self {
        Self::with_core(LocalStreamManagerCore::new(
            addr,
            state_store,
            streaming_metrics,
            config,
        ))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_core(LocalStreamManagerCore::for_test())
    }

    /// Broadcast a barrier to all senders. Save a receiver in barrier manager
    pub fn send_barrier(
        &self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> Result<()> {
        let core = self.core.lock();
        let mut barrier_manager = core.context.lock_barrier_manager();
        barrier_manager.send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)?;
        Ok(())
    }

    /// drain collect rx less than `prev_epoch` in barrier manager.
    pub fn drain_collect_rx(&self, prev_epoch: u64) {
        let core = self.core.lock();
        let mut barrier_manager = core.context.lock_barrier_manager();
        barrier_manager.drain_collect_rx(prev_epoch);
    }

    /// Use `epoch` to find collect rx. And wait for all actor to be collected before
    /// returning.
    pub async fn collect_barrier(&self, epoch: u64) -> CollectResult {
        let rx = {
            let core = self.core.lock();
            let mut barrier_manager = core.context.lock_barrier_manager();
            barrier_manager.remove_collect_rx(epoch)
        };
        // Wait for all actors finishing this barrier.
        rx.await.unwrap()
    }

    pub async fn sync_epoch(&self, epoch: u64) -> Vec<LocalSstableInfo> {
        dispatch_state_store!(self.state_store(), store, {
            match store.sync(Some(epoch)).await {
                Ok(_) => store.get_uncommitted_ssts(epoch),
                // TODO: Handle sync failure by propagating it
                // back to global barrier manager
                Err(e) => panic!(
                    "Failed to sync state store after receiving barrier prev_epoch {:?} due to {}",
                    epoch, e
                ),
            }
        })
    }

    pub async fn clear_storage_buffer(&self) {
        dispatch_state_store!(self.state_store(), store, {
            store.clear_shared_buffer().await.unwrap();
        });
    }

    /// Broadcast a barrier to all senders. Returns immediately, and caller won't be notified when
    /// this barrier is finished.
    #[cfg(test)]
    pub fn send_barrier_for_test(&self, barrier: &Barrier) -> Result<()> {
        use std::iter::empty;

        let core = self.core.lock();
        let mut barrier_manager = core.context.lock_barrier_manager();
        assert!(barrier_manager.is_local_mode());
        barrier_manager.send_barrier(barrier, empty(), empty())?;
        barrier_manager.remove_collect_rx(barrier.epoch.prev);
        Ok(())
    }

    pub fn drop_actor(&self, actors: &[ActorId]) -> Result<()> {
        let mut core = self.core.lock();
        for id in actors {
            core.drop_actor(*id);
        }
        tracing::debug!(actors = ?actors, "drop actors");
        Ok(())
    }

    /// Force stop all actors on this worker.
    pub async fn stop_all_actors(&self, epoch: Epoch) -> Result<()> {
        let (actor_ids_to_send, actor_ids_to_collect) = {
            let core = self.core.lock();
            let actor_ids_to_send = core.context.lock_barrier_manager().all_senders();
            let actor_ids_to_collect = core
                .context
                .actor_infos
                .read()
                .keys()
                .cloned()
                .collect::<HashSet<_>>();
            (actor_ids_to_send, actor_ids_to_collect)
        };
        if actor_ids_to_send.is_empty() || actor_ids_to_collect.is_empty() {
            return Ok(());
        }
        let barrier = &Barrier {
            epoch,
            mutation: Some(Arc::new(Mutation::Stop(actor_ids_to_collect.clone()))),
            span: tracing::Span::none(),
        };

        self.send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)?;

        self.collect_barrier(barrier.epoch.prev).await;
        // Clear shared buffer in storage to release memory
        self.clear_storage_buffer().await;
        self.drain_collect_rx(barrier.epoch.prev);
        self.core.lock().drop_all_actors();

        Ok(())
    }

    pub fn take_receiver(&self, ids: UpDownActorIds) -> Result<Receiver<Message>> {
        let core = self.core.lock();
        core.context.take_receiver(&ids)
    }

    pub fn update_actors(
        &self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> Result<()> {
        let mut core = self.core.lock();
        core.update_actors(actors, hanging_channels)
    }

    /// This function was called while [`LocalStreamManager`] exited.
    pub async fn wait_all(self) -> Result<()> {
        let handles = self.core.lock().take_all_handles()?;
        for (_id, handle) in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn wait_actors(&self, actor_ids: &[ActorId]) -> Result<()> {
        let handles = self.core.lock().remove_actor_handles(actor_ids)?;
        for handle in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub fn update_actor_info(
        &self,
        req: stream_service::BroadcastActorInfoTableRequest,
    ) -> Result<()> {
        let mut core = self.core.lock();
        core.update_actor_info(req)
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub fn build_actors(&self, actors: &[ActorId], env: StreamEnvironment) -> Result<()> {
        let mut core = self.core.lock();
        core.build_actors(actors, env)
    }

    #[cfg(test)]
    pub fn take_source(&self) -> tokio::sync::mpsc::Sender<Message> {
        let mut core = self.core.lock();
        core.mock_source.0.take().unwrap()
    }

    #[cfg(test)]
    pub fn take_sink(&self, ids: UpDownActorIds) -> Receiver<Message> {
        let core = self.core.lock();
        core.context.take_receiver(&ids).unwrap()
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.core.lock().state_store.clone()
    }
}

fn update_upstreams(context: &SharedContext, ids: &[UpDownActorIds]) {
    ids.iter()
        .map(|id| {
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            context.add_channel_pairs(*id, (Some(tx), Some(rx)));
        })
        .count();
}

impl LocalStreamManagerCore {
    fn new(
        addr: HostAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
    ) -> Self {
        let context = SharedContext::new(addr);
        Self::with_store_and_context(state_store, context, streaming_metrics, config)
    }

    fn with_store_and_context(
        state_store: StateStoreImpl,
        context: SharedContext,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
    ) -> Self {
        let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);

        Self {
            handles: HashMap::new(),
            context: Arc::new(context),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
            mock_source: (Some(tx), Some(rx)),
            state_store,
            streaming_metrics,
            compute_client_pool: ComputeClientPool::new(u64::MAX),
            config,
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        use risingwave_storage::monitor::StateStoreMetrics;

        let register = prometheus::Registry::new();
        let streaming_metrics = Arc::new(StreamingMetrics::new(register));
        Self::with_store_and_context(
            StateStoreImpl::shared_in_memory_store(Arc::new(StateStoreMetrics::unused())),
            SharedContext::for_test(),
            streaming_metrics,
            StreamingConfig::default(),
        )
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: BoxedExecutor,
        dispatchers: &[stream_plan::Dispatcher],
        actor_id: ActorId,
    ) -> Result<impl StreamConsumer> {
        let dispatcher_impls = dispatchers
            .iter()
            .map(|dispatcher| {
                DispatcherImpl::new(
                    &self.context,
                    actor_id,
                    dispatcher,
                    self.streaming_metrics.clone(),
                )
            })
            .try_collect()?;

        Ok(DispatchExecutor::new(
            input,
            dispatcher_impls,
            actor_id,
            self.context.clone(),
            self.streaming_metrics.clone(),
        ))
    }

    /// Create a chain(tree) of nodes, with given `store`.
    #[allow(clippy::too_many_arguments)]
    fn create_nodes_inner(
        &mut self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        node: &stream_plan::StreamNode,
        input_pos: usize,
        env: StreamEnvironment,
        store: impl StateStore,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
    ) -> Result<BoxedExecutor> {
        let op_info = node.get_identity().clone();
        // Create the input executor before creating itself
        // The node with no input must be a `get_receive_message`
        let input: Vec<_> = node
            .input
            .iter()
            .enumerate()
            .map(|(input_pos, input)| {
                self.create_nodes_inner(
                    fragment_id,
                    actor_id,
                    input,
                    input_pos,
                    env.clone(),
                    store.clone(),
                    actor_context,
                    vnode_bitmap.clone(),
                )
            })
            .try_collect()?;

        let pk_indices = node
            .get_pk_indices()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = unique_executor_id(actor_id, node.operator_id);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);

        let executor_params = ExecutorParams {
            env: env.clone(),
            pk_indices,
            executor_id,
            operator_id,
            op_info,
            input,
            actor_id,
            fragment_id,
            executor_stats: self.streaming_metrics.clone(),
            actor_context: actor_context.clone(),
            vnode_bitmap,
        };

        let executor = create_executor(executor_params, self, node, store)?;
        let executor = Self::wrap_executor_for_debug(
            executor,
            actor_id,
            executor_id,
            input_pos,
            self.streaming_metrics.clone(),
        );
        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    fn create_nodes(
        &mut self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
    ) -> Result<BoxedExecutor> {
        dispatch_state_store!(self.state_store.clone(), store, {
            self.create_nodes_inner(
                fragment_id,
                actor_id,
                node,
                0,
                env,
                store,
                actor_context,
                vnode_bitmap,
            )
        })
    }

    fn wrap_executor_for_debug(
        executor: BoxedExecutor,
        actor_id: ActorId,
        executor_id: u64,
        input_pos: usize,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> BoxedExecutor {
        DebugExecutor::new(
            executor,
            input_pos,
            actor_id,
            executor_id,
            streaming_metrics,
        )
        .boxed()
    }

    pub(crate) fn get_receive_message(
        &mut self,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstreams: &[ActorId],
        up_fragment_id: FragmentId,
    ) -> Result<Vec<Receiver<Message>>> {
        assert!(!upstreams.is_empty());
        let rxs = upstreams
            .iter()
            .map(|up_id| {
                if *up_id == 0 {
                    Ok(self.mock_source.1.take().unwrap())
                } else {
                    let upstream_addr = self.context.get_actor_info(up_id)?.get_host()?.into();
                    if !is_local_address(&upstream_addr, &self.context.addr) {
                        // Get the sender for `RemoteInput` to forward received messages to
                        // receivers in `ReceiverExecutor` or
                        // `MergerExecutor`.
                        let sender = self.context.take_sender(&(*up_id, actor_id))?;
                        // spawn the `RemoteInput`
                        let up_id = *up_id;
                        let pool = self.compute_client_pool.clone();
                        let metrics = self.streaming_metrics.clone();
                        tokio::spawn(async move {
                            let init_client = async move {
                                let remote_input = RemoteInput::create(
                                    pool.get_client_for_addr(upstream_addr).await?,
                                    (up_id, actor_id),
                                    (up_fragment_id, fragment_id),
                                    sender,
                                    metrics,
                                )
                                .await?;
                                Ok::<_, RwError>(remote_input)
                            };
                            match init_client.await {
                                Ok(remote_input) => remote_input.run().await,
                                Err(e) => {
                                    error!("Spawn remote input fails:{}", e);
                                }
                            }
                        });
                    }
                    Ok::<_, RwError>(self.context.take_receiver(&(*up_id, actor_id))?)
                }
            })
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            rxs.len(),
            upstreams.len(),
            "upstreams are not fully available: {} registered while {} required, actor_id={}",
            rxs.len(),
            upstreams.len(),
            actor_id
        );

        Ok(rxs)
    }

    fn build_actors(&mut self, actors: &[ActorId], env: StreamEnvironment) -> Result<()> {
        for actor_id in actors {
            let actor_id = *actor_id;
            let actor = self.actors.remove(&actor_id).unwrap();
            let actor_context = Arc::new(Mutex::new(ActorContext::default()));
            let vnode_bitmap = actor
                .get_vnode_bitmap()
                .ok()
                .map(|b| b.try_into())
                .transpose()?;
            let executor = self.create_nodes(
                actor.fragment_id,
                actor_id,
                actor.get_nodes()?,
                env.clone(),
                &actor_context,
                vnode_bitmap,
            )?;

            let dispatcher = self.create_dispatcher(executor, &actor.dispatcher, actor_id)?;
            let actor = Actor::new(
                dispatcher,
                actor_id,
                self.context.clone(),
                self.streaming_metrics.clone(),
                actor_context,
            );
            let monitor = tokio_metrics::TaskMonitor::new();

            self.handles.insert(
                actor_id,
                tokio::spawn(monitor.instrument(async move {
                    // unwrap the actor result to panic on error
                    actor.run().await.expect("actor failed");
                })),
            );

            let actor_id_str = actor_id.to_string();

            let metrics = self.streaming_metrics.clone();
            let task = tokio::spawn(async move {
                loop {
                    metrics
                        .actor_execution_time
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_fast_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_fast_poll_count as i64);
                    metrics
                        .actor_slow_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_slow_poll_duration.as_secs_f64());
                    metrics
                        .actor_slow_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_slow_poll_count as i64);
                    metrics
                        .actor_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_poll_duration.as_secs_f64());
                    metrics
                        .actor_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_poll_count as i64);
                    metrics
                        .actor_idle_duration
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_idle_duration.as_secs_f64());
                    metrics
                        .actor_idle_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_idled_count as i64);
                    metrics
                        .actor_scheduled_duration
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_scheduled_duration.as_secs_f64());
                    metrics
                        .actor_scheduled_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(monitor.cumulative().total_scheduled_count as i64);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            self.actor_monitor_tasks.insert(actor_id, task);
        }

        Ok(())
    }

    pub fn take_all_handles(&mut self) -> Result<HashMap<ActorId, ActorHandle>> {
        Ok(std::mem::take(&mut self.handles))
    }

    pub fn remove_actor_handles(&mut self, actor_ids: &[ActorId]) -> Result<Vec<ActorHandle>> {
        actor_ids
            .iter()
            .map(|actor_id| {
                self.handles.remove(actor_id).ok_or_else(|| {
                    RwError::from(ErrorCode::InternalError(format!(
                        "No such actor with actor id:{}",
                        actor_id
                    )))
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn update_actor_info(
        &mut self,
        req: stream_service::BroadcastActorInfoTableRequest,
    ) -> Result<()> {
        for actor in req.get_info() {
            let ret = self
                .context
                .actor_infos
                .write()
                .insert(actor.get_actor_id(), actor.clone());
            if let Some(prev_actor) = ret && actor != &prev_actor{
                return Err(ErrorCode::InternalError(format!(
                    "actor info mismatch when broadcasting {}",
                    actor.get_actor_id()
                ))
                .into());
            }
        }
        Ok(())
    }

    /// `drop_actor` is invoked by meta node via RPC once the stop barrier arrives at the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_actor(&mut self, actor_id: ActorId) {
        let handle = self.handles.remove(&actor_id).unwrap();
        self.context.retain(|&(up_id, _)| up_id != actor_id);
        self.actor_monitor_tasks.remove(&actor_id).unwrap().abort();
        self.context.actor_infos.write().remove(&actor_id);
        self.actors.remove(&actor_id);
        // Task should have already stopped when this method is invoked.
        handle.abort();
    }

    /// `drop_all_actors` is invoked by meta node via RPC once the stop barrier arrives at all the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_all_actors(&mut self) {
        for (actor_id, handle) in self.handles.drain() {
            self.context.retain(|&(up_id, _)| up_id != actor_id);
            self.actor_monitor_tasks.remove(&actor_id).unwrap().abort();
            self.actors.remove(&actor_id);
            // Task should have already stopped when this method is invoked.
            handle.abort();
        }
        self.context.actor_infos.write().clear();
    }

    fn update_actors(
        &mut self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> Result<()> {
        let local_actor_ids: HashSet<ActorId> = HashSet::from_iter(
            actors
                .iter()
                .map(|actor| actor.get_actor_id())
                .collect::<Vec<_>>()
                .into_iter(),
        );

        for actor in actors {
            let ret = self.actors.insert(actor.get_actor_id(), actor.clone());
            if ret.is_some() {
                return Err(ErrorCode::InternalError(format!(
                    "duplicated actor {}",
                    actor.get_actor_id()
                ))
                .into());
            }
        }

        for actor in actors {
            // At this time, the graph might not be complete, so we do not check if downstream
            // has `current_id` as upstream.
            let down_id = actor
                .dispatcher
                .iter()
                .flat_map(|x| x.downstream_actor_id.iter())
                .map(|id| (actor.actor_id, *id))
                .collect_vec();
            update_upstreams(&self.context, &down_id);

            // Add remote input channels.
            let mut up_id = vec![];
            for upstream_id in actor.get_upstream_actor_id() {
                if !local_actor_ids.contains(upstream_id) {
                    up_id.push((*upstream_id, actor.actor_id));
                }
            }
            update_upstreams(&self.context, &up_id);
        }

        for hanging_channel in hanging_channels {
            match (&hanging_channel.upstream, &hanging_channel.downstream) {
                (
                    Some(up),
                    Some(ActorInfo {
                        actor_id: down_id,
                        host: None,
                    }),
                ) => {
                    let up_down_ids = (up.actor_id, *down_id);
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    self.context
                        .add_channel_pairs(up_down_ids, (Some(tx), Some(rx)));
                }
                (
                    Some(ActorInfo {
                        actor_id: up_id,
                        host: None,
                    }),
                    Some(down),
                ) => {
                    let up_down_ids = (*up_id, down.actor_id);
                    let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
                    self.context
                        .add_channel_pairs(up_down_ids, (Some(tx), Some(rx)));
                }
                _ => {
                    return Err(ErrorCode::InternalError(format!(
                        "hanging channel should has exactly one remote side: {:?}",
                        hanging_channel,
                    ))
                    .into())
                }
            }
        }
        Ok(())
    }
}
