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

use std::fmt::Debug;
use std::future::Future;
use std::iter::repeat_with;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use futures_async_stream::try_stream;
use itertools::Itertools;
use madsim::collections::{HashMap, HashSet};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::error::{internal_error, Result};
use risingwave_common::types::VIRTUAL_NODE_COUNT;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_common::util::compress::decompress_data;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_pb::stream_plan::Dispatcher as ProstDispatcher;
use tokio::sync::mpsc::Sender;
use tracing::event;

use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, BoxedExecutor, Message, Mutation, StreamConsumer};
use crate::task::{ActorId, DispatcherId, SharedContext};

/// `Output` provides an interface for `Dispatcher` to send data into downstream actors.
#[async_trait]
pub trait Output: Debug + Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> Result<()>;

    fn down_actor_id(&self) -> ActorId;
}

type BoxedOutput = Box<dyn Output>;

/// `LocalOutput` sends data to a local `mpsc::Channel`
pub struct LocalOutput {
    up_id_str: String,
    down_id: ActorId,
    ch: Sender<Message>,
    metrics: Arc<StreamingMetrics>,
}

impl Debug for LocalOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalOutput")
            .field("down_id", &self.down_id)
            .finish()
    }
}

impl LocalOutput {
    pub fn new(
        up_id: ActorId,
        down_id: ActorId,
        ch: Sender<Message>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            up_id_str: up_id.to_string(),
            down_id,
            ch,
            metrics,
        }
    }
}

#[async_trait]
impl Output for LocalOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        // if the buffer is full when sending, the sender is backpressured
        if self.ch.capacity() == 0 {
            let start_time = minstant::Instant::now();
            let mut timeout_times = 0;
            // the default pull time of Prometheus is 15s
            while (tokio::time::timeout(
                std::time::Duration::from_secs(15),
                self.ch.send(message.clone()),
            ))
            .await
            .map_err(|_| internal_error("failed to send"))?
            .is_err()
            {
                self.metrics
                    .actor_output_buffer_blocking_duration_ns
                    .with_label_values(&[&self.up_id_str])
                    .inc_by(10_000_000_000);
                timeout_times += 1;
            }
            self.metrics
                .actor_output_buffer_blocking_duration_ns
                .with_label_values(&[&self.up_id_str])
                .inc_by(start_time.elapsed().as_nanos() as u64 - timeout_times * 10_000_000_000);
        } else {
            self.ch
                .send(message)
                .await
                .map_err(|_| internal_error("failed to send"))?;
        }
        Ok(())
    }

    fn down_actor_id(&self) -> ActorId {
        self.down_id
    }
}

/// `RemoteOutput` forwards data to`ExchangeServiceImpl`
pub struct RemoteOutput {
    up_id_str: String,
    down_id: ActorId,
    ch: Sender<Message>,
    metrics: Arc<StreamingMetrics>,
}

impl Debug for RemoteOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteOutput")
            .field("down_id", &self.down_id)
            .finish()
    }
}

impl RemoteOutput {
    pub fn new(
        up_id: ActorId,
        down_id: ActorId,
        ch: Sender<Message>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            up_id_str: up_id.to_string(),
            down_id,
            ch,
            metrics,
        }
    }
}

#[async_trait]
impl Output for RemoteOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        let message = match message {
            Message::Chunk(chk) => Message::Chunk(chk.compact()?),
            _ => message,
        };
        // if the buffer is full when sending, the sender is backpressured
        if self.ch.capacity() == 0 {
            let start_time = minstant::Instant::now();
            let mut timeout_times = 0;
            // the default pull time of Prometheus is 15s
            while (tokio::time::timeout(
                std::time::Duration::from_secs(15),
                self.ch.send(message.clone()),
            ))
            .await
            .map_err(|_| internal_error("failed to send"))?
            .is_err()
            {
                self.metrics
                    .actor_output_buffer_blocking_duration_ns
                    .with_label_values(&[&self.up_id_str])
                    .inc_by(10_000_000_000);
                timeout_times += 1;
            }
            self.metrics
                .actor_output_buffer_blocking_duration_ns
                .with_label_values(&[&self.up_id_str])
                .inc_by(start_time.elapsed().as_nanos() as u64 - timeout_times * 10_000_000_000);
        } else {
            self.ch
                .send(message)
                .await
                .map_err(|_| internal_error("failed to send"))?;
        }
        Ok(())
    }

    fn down_actor_id(&self) -> ActorId {
        self.down_id
    }
}

fn new_output(
    context: &SharedContext,
    addr: HostAddr,
    up_id: ActorId,
    down_id: ActorId,
    metrics: Arc<StreamingMetrics>,
) -> Result<Box<dyn Output>> {
    let tx = context.take_sender(&(up_id, down_id))?;
    if is_local_address(&addr, &context.addr) {
        // if this is a local downstream actor
        Ok(Box::new(LocalOutput::new(up_id, down_id, tx, metrics)) as Box<dyn Output>)
    } else {
        Ok(Box::new(RemoteOutput::new(up_id, down_id, tx, metrics)) as Box<dyn Output>)
    }
}

/// [`DispatchExecutor`] consumes messages and send them into downstream actors. Usually,
/// data chunks will be dispatched with some specified policy, while control message
/// such as barriers will be distributed to all receivers.
pub struct DispatchExecutor {
    input: BoxedExecutor,
    inner: DispatchExecutorInner,
}

struct DispatchExecutorInner {
    dispatchers: Vec<DispatcherImpl>,
    actor_id: u32,
    actor_id_str: String,
    context: Arc<SharedContext>,
    metrics: Arc<StreamingMetrics>,
}

impl DispatchExecutorInner {
    fn single_inner_mut(&mut self) -> &mut DispatcherImpl {
        assert_eq!(
            self.dispatchers.len(),
            1,
            "only support mutation on one-dispatcher actors"
        );
        &mut self.dispatchers[0]
    }

    async fn dispatch(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Chunk(chunk) => {
                self.metrics
                    .actor_out_record_cnt
                    .with_label_values(&[&self.actor_id_str])
                    .inc_by(chunk.cardinality() as _);
                if self.dispatchers.len() == 1 {
                    // special clone optimization when there is only one downstream dispatcher
                    self.single_inner_mut().dispatch_data(chunk).await?;
                } else {
                    for dispatcher in &mut self.dispatchers {
                        dispatcher.dispatch_data(chunk.clone()).await?;
                    }
                }
            }
            Message::Barrier(barrier) => {
                let mutation = barrier.mutation.clone();
                self.pre_mutate_dispatchers(&mutation).await?;
                for dispatcher in &mut self.dispatchers {
                    dispatcher.dispatch_barrier(barrier.clone()).await?;
                }

                self.post_mutate_dispatchers(&mutation).await?;
            }
        };
        Ok(())
    }

    /// Add new dispatchers to the executor. Will check whether their ids are unique.
    fn add_dispatchers<'a>(
        &mut self,
        new_dispatchers: impl IntoIterator<Item = &'a ProstDispatcher>,
    ) -> Result<()> {
        let new_dispatchers: Vec<_> = new_dispatchers
            .into_iter()
            .map(|d| DispatcherImpl::new(&self.context, self.actor_id, d, self.metrics.clone()))
            .try_collect()?;

        self.dispatchers.extend(new_dispatchers);

        assert!(
            self.dispatchers
                .iter()
                .map(|d| d.dispatcher_id())
                .all_unique(),
            "dispatcher ids must be unique: {:?}",
            self.dispatchers
        );

        Ok(())
    }

    /// For `Add` and `Update`, update the dispatchers before we dispatch the barrier.
    async fn pre_mutate_dispatchers(&mut self, mutation: &Option<Arc<Mutation>>) -> Result<()> {
        let Some(mutation) = mutation.as_deref() else {
            return Ok(())
        };

        match mutation {
            Mutation::AddDispatcher(adds) => {
                if let Some(new_dispatchers) = adds.map.get(&self.actor_id) {
                    self.add_dispatchers(new_dispatchers)?;
                }
            }

            Mutation::UpdateOutputs(updates) => {
                for dispatcher in &mut self.dispatchers {
                    if let Some((_, actor_infos)) =
                        updates.get_key_value(&(self.actor_id, dispatcher.dispatcher_id()))
                    {
                        let mut new_outputs = vec![];

                        let actor_id = self.actor_id;
                        // delete the old local connections in both local and remote pools;
                        self.context.retain(|&(up_id, down_id)| {
                            up_id != actor_id
                                || actor_infos.iter().any(|info| info.actor_id == down_id)
                        });

                        for actor_info in actor_infos.iter() {
                            let down_id = actor_info.get_actor_id();
                            let downstream_addr = actor_info.get_host()?.into();
                            new_outputs.push(new_output(
                                &self.context,
                                downstream_addr,
                                self.actor_id,
                                down_id,
                                self.metrics.clone(),
                            )?);
                        }
                        dispatcher.set_outputs(new_outputs)
                    }
                }
            }

            Mutation::AddOutput(adds) => {
                for dispatcher in &mut self.dispatchers {
                    if let Some(downstream_actor_infos) =
                        adds.map.get(&(self.actor_id, dispatcher.dispatcher_id()))
                    {
                        let mut outputs_to_add = Vec::with_capacity(downstream_actor_infos.len());
                        for downstream_actor_info in downstream_actor_infos {
                            let down_id = downstream_actor_info.get_actor_id();
                            let downstream_addr = downstream_actor_info.get_host()?.into();
                            outputs_to_add.push(new_output(
                                &self.context,
                                downstream_addr,
                                self.actor_id,
                                down_id,
                                self.metrics.clone(),
                            )?);
                        }
                        dispatcher.add_outputs(outputs_to_add);
                    }
                }
            }

            _ => {}
        };

        Ok(())
    }

    /// For `Stop`, update the dispatchers after we dispatch the barrier.
    async fn post_mutate_dispatchers(&mut self, mutation: &Option<Arc<Mutation>>) -> Result<()> {
        if let Some(Mutation::Stop(stops)) = mutation.as_deref() {
            // Remove outputs only if this actor itself is not to be stopped.
            if !stops.contains(&self.actor_id) {
                for dispatcher in &mut self.dispatchers {
                    dispatcher.remove_outputs(stops);
                }
            }
        }

        // After stopping the downstream mview, the outputs of some dispatcher might be empty and we
        // should clean up them.
        self.dispatchers.drain_filter(|d| d.is_empty());

        Ok(())
    }
}

impl DispatchExecutor {
    pub fn new(
        input: BoxedExecutor,
        dispatchers: Vec<DispatcherImpl>,
        actor_id: u32,
        context: Arc<SharedContext>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            input,
            inner: DispatchExecutorInner {
                dispatchers,
                actor_id,
                actor_id_str: actor_id.to_string(),
                context,
                metrics,
            },
        }
    }
}

impl StreamConsumer for DispatchExecutor {
    type BarrierStream = impl Stream<Item = Result<Barrier>> + Send;

    fn execute(mut self: Box<Self>) -> Self::BarrierStream {
        #[try_stream]
        async move {
            let input = self.input.execute();

            #[for_await]
            for msg in input {
                let msg: Message = msg?;
                let barrier = msg.as_barrier().cloned();
                self.inner.dispatch(msg).await?;
                if let Some(barrier) = barrier {
                    yield barrier;
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum DispatcherImpl {
    Hash(HashDataDispatcher),
    Broadcast(BroadcastDispatcher),
    Simple(SimpleDispatcher),
    RoundRobin(RoundRobinDataDispatcher),
}

impl DispatcherImpl {
    pub fn new(
        context: &SharedContext,
        actor_id: ActorId,
        dispatcher: &ProstDispatcher,
        metrics: Arc<StreamingMetrics>,
    ) -> Result<Self> {
        let outputs = dispatcher
            .downstream_actor_id
            .iter()
            .map(|down_id| {
                let downstream_addr = context.get_actor_info(down_id)?.get_host()?.into();
                new_output(
                    context,
                    downstream_addr,
                    actor_id,
                    *down_id,
                    metrics.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        use risingwave_pb::stream_plan::DispatcherType::*;
        let dispatcher_impl = match dispatcher.get_type()? {
            Hash => {
                assert!(!outputs.is_empty());
                let column_indices = dispatcher
                    .column_indices
                    .iter()
                    .map(|i| *i as usize)
                    .collect();
                let compressed_mapping = dispatcher.get_hash_mapping()?;
                let hash_mapping = decompress_data(
                    &compressed_mapping.original_indices,
                    &compressed_mapping.data,
                );

                DispatcherImpl::Hash(HashDataDispatcher::new(
                    dispatcher.downstream_actor_id.to_vec(),
                    outputs,
                    column_indices,
                    hash_mapping,
                    dispatcher.dispatcher_id,
                ))
            }
            Broadcast => DispatcherImpl::Broadcast(BroadcastDispatcher::new(
                outputs,
                dispatcher.dispatcher_id,
            )),
            Simple | NoShuffle => {
                let [output]: [_; 1] = outputs.try_into().unwrap();
                DispatcherImpl::Simple(SimpleDispatcher::new(output, dispatcher.dispatcher_id))
            }
            Invalid => unreachable!(),
        };

        Ok(dispatcher_impl)
    }
}

macro_rules! impl_dispatcher {
    ([], $( { $variant_name:ident } ),*) => {
        impl DispatcherImpl {
            pub async fn dispatch_data(&mut self, chunk: StreamChunk) -> Result<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.dispatch_data(chunk).await, )*
                }
            }

            pub async fn dispatch_barrier(&mut self, barrier: Barrier) -> Result<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.dispatch_barrier(barrier).await, )*
                }
            }

            pub fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
                match self {
                    $( Self::$variant_name(inner) => inner.set_outputs(outputs), )*
                }
            }

            pub fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
                match self {
                    $(Self::$variant_name(inner) => inner.add_outputs(outputs), )*
                }
            }

            pub fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
                match self {
                    $(Self::$variant_name(inner) => inner.remove_outputs(actor_ids), )*
                }
            }

            pub fn dispatcher_id(&self) -> DispatcherId {
                match self {
                    $(Self::$variant_name(inner) => inner.dispatcher_id(), )*
                }
            }

            pub fn is_empty(&self) -> bool {
                match self {
                    $(Self::$variant_name(inner) => inner.is_empty(), )*
                }
            }
        }
    }
}

macro_rules! for_all_dispatcher_variants {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x), *],
            { Hash },
            { Broadcast },
            { Simple },
            { RoundRobin }
        }
    };
}

for_all_dispatcher_variants! { impl_dispatcher }

macro_rules! define_dispatcher_associated_types {
    () => {
        type DataFuture<'a> = impl DispatchFuture<'a>;
        type BarrierFuture<'a> = impl DispatchFuture<'a>;
    };
}

pub trait DispatchFuture<'a> = Future<Output = Result<()>> + Send;

pub trait Dispatcher: Debug + 'static {
    type DataFuture<'a>: DispatchFuture<'a>;
    type BarrierFuture<'a>: DispatchFuture<'a>;

    fn dispatch_data(&mut self, chunk: StreamChunk) -> Self::DataFuture<'_>;
    fn dispatch_barrier(&mut self, barrier: Barrier) -> Self::BarrierFuture<'_>;

    fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>);
    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>);
    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>);

    fn dispatcher_id(&self) -> DispatcherId;
    fn is_empty(&self) -> bool;
}

#[derive(Debug)]
pub struct RoundRobinDataDispatcher {
    outputs: Vec<BoxedOutput>,
    cur: usize,
    dispatcher_id: DispatcherId,
}

impl RoundRobinDataDispatcher {
    pub fn new(outputs: Vec<BoxedOutput>, dispatcher_id: DispatcherId) -> Self {
        Self {
            outputs,
            cur: 0,
            dispatcher_id,
        }
    }
}

impl Dispatcher for RoundRobinDataDispatcher {
    define_dispatcher_associated_types!();

    fn dispatch_data(&mut self, chunk: StreamChunk) -> Self::DataFuture<'_> {
        async move {
            self.outputs[self.cur].send(Message::Chunk(chunk)).await?;
            self.cur += 1;
            self.cur %= self.outputs.len();
            Ok(())
        }
    }

    fn dispatch_barrier(&mut self, barrier: Barrier) -> Self::BarrierFuture<'_> {
        async move {
            // always broadcast barrier
            for output in &mut self.outputs {
                output.send(Message::Barrier(barrier.clone())).await?;
            }
            Ok(())
        }
    }

    fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs = outputs.into_iter().collect();
        self.cur = self.cur.min(self.outputs.len() - 1);
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(outputs.into_iter());
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .drain_filter(|output| actor_ids.contains(&output.down_actor_id()))
            .count();
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

pub struct HashDataDispatcher {
    fragment_ids: Vec<u32>,
    outputs: Vec<BoxedOutput>,
    keys: Vec<usize>,
    /// Mapping from virtual node to actor id, used for hash data dispatcher to dispatch tasks to
    /// different downstream actors.
    hash_mapping: Vec<ActorId>,
    dispatcher_id: DispatcherId,
}

impl Debug for HashDataDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashDataDispatcher")
            .field("fragment_ids", &self.fragment_ids)
            .field("outputs", &self.outputs)
            .field("keys", &self.keys)
            .field("dispatcher_id", &self.dispatcher_id)
            .finish_non_exhaustive()
    }
}

impl HashDataDispatcher {
    pub fn new(
        fragment_ids: Vec<u32>,
        outputs: Vec<BoxedOutput>,
        keys: Vec<usize>,
        hash_mapping: Vec<ActorId>,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            fragment_ids,
            outputs,
            keys,
            hash_mapping,
            dispatcher_id,
        }
    }
}

impl Dispatcher for HashDataDispatcher {
    define_dispatcher_associated_types!();

    fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs = outputs.into_iter().collect()
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(outputs.into_iter());
    }

    fn dispatch_barrier(&mut self, barrier: Barrier) -> Self::BarrierFuture<'_> {
        async move {
            // always broadcast barrier
            for output in &mut self.outputs {
                output.send(Message::Barrier(barrier.clone())).await?;
            }
            Ok(())
        }
    }

    fn dispatch_data(&mut self, chunk: StreamChunk) -> Self::DataFuture<'_> {
        async move {
            // A chunk can be shuffled into multiple output chunks that to be sent to downstreams.
            // In these output chunks, the only difference are visibility map, which is calculated
            // by the hash value of each line in the input chunk.
            let num_outputs = self.outputs.len();

            // get hash value of every line by its key
            let hash_builder = CRC32FastBuilder {};
            let hash_values = chunk
                .get_hash_values(&self.keys, hash_builder)
                .unwrap()
                .iter()
                .map(|hash| *hash as usize % VIRTUAL_NODE_COUNT)
                .collect_vec();

            tracing::trace!(target: "events::stream::dispatch::hash", "\n{}\n keys {:?} => {:?}", chunk.to_pretty_string(), self.keys, hash_values);

            let mut vis_maps = repeat_with(|| BitmapBuilder::with_capacity(chunk.capacity()))
                .take(num_outputs)
                .collect_vec();
            let mut last_hash_value_when_update_delete: usize = 0;
            let mut new_ops: Vec<Op> = Vec::with_capacity(chunk.capacity());

            let (ops, columns, visibility) = chunk.into_inner();

            match visibility {
                None => {
                    hash_values.iter().zip_eq(ops).for_each(|(hash, op)| {
                        // get visibility map for every output chunk
                        for (output, vis_map) in self.outputs.iter().zip_eq(vis_maps.iter_mut()) {
                            vis_map.append(self.hash_mapping[*hash] == output.down_actor_id());
                        }
                        // The 'update' message, noted by an UpdateDelete and a successive
                        // UpdateInsert, need to be rewritten to common
                        // Delete and Insert if they were dispatched to
                        // different actors.
                        if op == Op::UpdateDelete {
                            last_hash_value_when_update_delete = *hash;
                        } else if op == Op::UpdateInsert {
                            if *hash != last_hash_value_when_update_delete {
                                new_ops.push(Op::Delete);
                                new_ops.push(Op::Insert);
                            } else {
                                new_ops.push(Op::UpdateDelete);
                                new_ops.push(Op::UpdateInsert);
                            }
                        } else {
                            new_ops.push(op);
                        }
                    });
                }
                Some(visibility) => {
                    hash_values
                        .iter()
                        .zip_eq(visibility.iter())
                        .zip_eq(ops)
                        .for_each(|((hash, visible), op)| {
                            for (output, vis_map) in self.outputs.iter().zip_eq(vis_maps.iter_mut())
                            {
                                vis_map.append(
                                    visible && self.hash_mapping[*hash] == output.down_actor_id(),
                                );
                            }
                            if !visible {
                                new_ops.push(op);
                                return;
                            }
                            if op == Op::UpdateDelete {
                                last_hash_value_when_update_delete = *hash;
                            } else if op == Op::UpdateInsert {
                                if *hash != last_hash_value_when_update_delete {
                                    new_ops.push(Op::Delete);
                                    new_ops.push(Op::Insert);
                                    panic!("Update of the same pk is shuffled to different partitions, which might cause problems. We forbid this for now.");
                                } else {
                                    new_ops.push(Op::UpdateDelete);
                                    new_ops.push(Op::UpdateInsert);
                                }
                            } else {
                                new_ops.push(op);
                            }
                        });
                }
            }

            let ops = new_ops;

            // individually output StreamChunk integrated with vis_map
            for ((vis_map, output), downstream) in vis_maps
                .into_iter()
                .zip_eq(self.outputs.iter_mut())
                .zip_eq(self.fragment_ids.iter())
            {
                let vis_map = vis_map.finish();
                // columns is not changed in this function
                let new_stream_chunk =
                    StreamChunk::new(ops.clone(), columns.clone(), Some(vis_map));
                if new_stream_chunk.cardinality() > 0 {
                    event!(
                        tracing::Level::TRACE,
                        msg = "chunk",
                        downstream = downstream,
                        "send = \n{:#?}",
                        new_stream_chunk
                    );
                    output.send(Message::Chunk(new_stream_chunk)).await?;
                }
            }
            Ok(())
        }
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .drain_filter(|output| actor_ids.contains(&output.down_actor_id()))
            .count();
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

/// `BroadcastDispatcher` dispatches message to all outputs.
#[derive(Debug)]
pub struct BroadcastDispatcher {
    outputs: HashMap<ActorId, BoxedOutput>,
    dispatcher_id: DispatcherId,
}

impl BroadcastDispatcher {
    pub fn new(
        outputs: impl IntoIterator<Item = BoxedOutput>,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            outputs: Self::into_pairs(outputs).collect(),
            dispatcher_id,
        }
    }

    fn into_pairs(
        outputs: impl IntoIterator<Item = BoxedOutput>,
    ) -> impl Iterator<Item = (ActorId, BoxedOutput)> {
        outputs
            .into_iter()
            .map(|output| (output.down_actor_id(), output))
    }
}

impl Dispatcher for BroadcastDispatcher {
    define_dispatcher_associated_types!();

    fn dispatch_data(&mut self, chunk: StreamChunk) -> Self::DataFuture<'_> {
        async move {
            for output in self.outputs.values_mut() {
                output.send(Message::Chunk(chunk.clone())).await?;
            }
            Ok(())
        }
    }

    fn dispatch_barrier(&mut self, barrier: Barrier) -> Self::BarrierFuture<'_> {
        async move {
            for output in self.outputs.values_mut() {
                output.send(Message::Barrier(barrier.clone())).await?;
            }
            Ok(())
        }
    }

    fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs = Self::into_pairs(outputs).collect()
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(Self::into_pairs(outputs));
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .drain_filter(|actor_id, _| actor_ids.contains(actor_id))
            .count();
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

/// `SimpleDispatcher` dispatches message to a single output.
#[derive(Debug)]
pub struct SimpleDispatcher {
    output: Option<BoxedOutput>,
    dispatcher_id: DispatcherId,
}

impl SimpleDispatcher {
    pub fn new(output: BoxedOutput, dispatcher_id: DispatcherId) -> Self {
        Self {
            output: Some(output),
            dispatcher_id,
        }
    }

    /// Get the output of this dispatcher.
    /// The field should always be `Some`. After `remove_output` is called, the field becomes `None`
    /// and this dispatcher should be dropped immediately by checking `is_empty`.
    fn output(&mut self) -> &mut BoxedOutput {
        self.output.as_mut().expect("no output")
    }
}

impl Dispatcher for SimpleDispatcher {
    define_dispatcher_associated_types!();

    fn set_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.output = Some(outputs.into_iter().next().unwrap());
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        // TODO: ban this after removing `AddOutputs` mutation.
        self.output = Some(outputs.into_iter().next().unwrap());
    }

    fn dispatch_barrier(&mut self, barrier: Barrier) -> Self::BarrierFuture<'_> {
        async move {
            self.output()
                .send(Message::Barrier(barrier.clone()))
                .await?;
            Ok(())
        }
    }

    fn dispatch_data(&mut self, chunk: StreamChunk) -> Self::DataFuture<'_> {
        async move {
            self.output().send(Message::Chunk(chunk)).await?;
            Ok(())
        }
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        if actor_ids.contains(&self.output().down_actor_id()) {
            self.output = None;
        }
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn is_empty(&self) -> bool {
        self.output.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::{Arc, Mutex};

    use futures::{pin_mut, StreamExt};
    use itertools::Itertools;
    use madsim::collections::HashMap;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Array, ArrayBuilder, I32ArrayBuilder, Op};
    use risingwave_common::catalog::Schema;
    use risingwave_common::types::VIRTUAL_NODE_COUNT;
    use risingwave_pb::common::{ActorInfo, HostAddress};
    use static_assertions::const_assert_eq;
    use tokio::sync::mpsc::channel;

    use super::*;
    use crate::executor::receiver::ReceiverExecutor;
    use crate::executor::{ActorContext, AddOutput};
    use crate::task::{LOCAL_OUTPUT_CHANNEL_SIZE, LOCAL_TEST_ADDR};

    #[derive(Debug)]
    pub struct MockOutput {
        actor_id: ActorId,
        data: Arc<Mutex<Vec<Message>>>,
    }

    impl MockOutput {
        pub fn new(actor_id: ActorId, data: Arc<Mutex<Vec<Message>>>) -> Self {
            Self { actor_id, data }
        }
    }

    #[async_trait]
    impl Output for MockOutput {
        async fn send(&mut self, message: Message) -> Result<()> {
            self.data.lock().unwrap().push(message);
            Ok(())
        }

        fn down_actor_id(&self) -> ActorId {
            self.actor_id
        }
    }

    // TODO: this test contains update being shuffled to different partitions, which is not
    // supported for now.
    #[tokio::test]
    #[ignore]
    async fn test_hash_dispatcher_complex() {
        test_hash_dispatcher_complex_inner().await
    }

    async fn test_hash_dispatcher_complex_inner() {
        // This test only works when VIRTUAL_NODE_COUNT is 256.
        const_assert_eq!(VIRTUAL_NODE_COUNT, 256);

        let num_outputs = 2; // actor id ranges from 1 to 2
        let key_indices = &[0, 2];
        let output_data_vecs = (0..num_outputs)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect::<Vec<_>>();
        let outputs = output_data_vecs
            .iter()
            .enumerate()
            .map(|(actor_id, data)| {
                Box::new(MockOutput::new(1 + actor_id as u32, data.clone())) as BoxedOutput
            })
            .collect::<Vec<_>>();
        let mut hash_mapping = (1..num_outputs + 1)
            .flat_map(|id| vec![id as ActorId; VIRTUAL_NODE_COUNT / num_outputs])
            .collect_vec();
        hash_mapping.resize(VIRTUAL_NODE_COUNT, num_outputs as u32);
        let mut hash_dispatcher = HashDataDispatcher::new(
            (0..outputs.len() as u32).collect(),
            outputs,
            key_indices.to_vec(),
            hash_mapping,
            0,
        );

        let chunk = StreamChunk::from_pretty(
            "  I I I
            +  4 6 8
            +  5 7 9
            +  0 0 0
            -  1 1 1 D
            U- 2 0 2
            U+ 2 0 2
            U- 3 3 2
            U+ 3 3 4",
        );
        hash_dispatcher.dispatch_data(chunk).await.unwrap();

        assert_eq!(
            *output_data_vecs[0].lock().unwrap()[0].as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I
                +  4 6 8
                +  5 7 9
                +  0 0 0
                -  1 1 1 D
                U- 2 0 2
                U+ 2 0 2
                -  3 3 2 D  // Should rewrite UpdateDelete to Delete
                +  3 3 4    // Should rewrite UpdateInsert to Insert",
            )
        );
        assert_eq!(
            *output_data_vecs[1].lock().unwrap()[0].as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I
                +  4 6 8 D
                +  5 7 9 D
                +  0 0 0 D
                -  1 1 1 D  // Should keep original invisible mark
                U- 2 0 2 D  // Should keep UpdateDelete
                U+ 2 0 2 D  // Should keep UpdateInsert
                -  3 3 2    // Should rewrite UpdateDelete to Delete
                +  3 3 4 D  // Should rewrite UpdateInsert to Insert",
            )
        );
    }

    fn add_local_channels(ctx: Arc<SharedContext>, up_down_ids: Vec<(u32, u32)>) {
        for up_down_id in up_down_ids {
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            ctx.add_channel_pairs(up_down_id, (Some(tx), Some(rx)));
        }
    }

    fn add_remote_channels(ctx: Arc<SharedContext>, up_id: u32, down_ids: Vec<u32>) {
        for down_id in down_ids {
            let (tx, rx) = channel(LOCAL_OUTPUT_CHANNEL_SIZE);
            ctx.add_channel_pairs((up_id, down_id), (Some(tx), Some(rx)));
        }
    }

    fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: LOCAL_TEST_ADDR.host.clone(),
                port: LOCAL_TEST_ADDR.port as i32,
            }),
        }
    }

    fn helper_make_remote_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: "172.1.1.2".to_string(),
                port: 2334,
            }),
        }
    }

    #[tokio::test]
    async fn test_configuration_change() {
        let schema = Schema { fields: vec![] };
        let (tx, rx) = channel(16);
        let input = Box::new(ReceiverExecutor::new(
            schema.clone(),
            vec![],
            rx,
            ActorContext::create(),
            0,
            0,
            Arc::new(StreamingMetrics::unused()),
        ));
        let data_sink = Arc::new(Mutex::new(vec![]));
        let actor_id = 233;
        let output = Box::new(MockOutput::new(actor_id, data_sink));
        let ctx = Arc::new(SharedContext::for_test());
        let dispatcher_id = 666;
        let metrics = Arc::new(StreamingMetrics::unused());

        let executor = Box::new(DispatchExecutor::new(
            input,
            vec![DispatcherImpl::Simple(SimpleDispatcher::new(
                output,
                dispatcher_id,
            ))],
            actor_id,
            ctx.clone(),
            metrics,
        ))
        .execute();
        pin_mut!(executor);

        let mut updates1: HashMap<(u32, u64), Vec<ActorInfo>> = HashMap::new();

        updates1.insert(
            (actor_id, 0),
            vec![
                helper_make_local_actor(234),
                helper_make_local_actor(235),
                helper_make_remote_actor(238),
            ],
        );
        add_local_channels(ctx.clone(), vec![(233, 234), (233, 235)]);
        add_remote_channels(ctx.clone(), 233, vec![238]);

        let b1 = Barrier::new_test_barrier(1).with_mutation(Mutation::UpdateOutputs(updates1));
        tx.send(Message::Barrier(b1)).await.unwrap();
        executor.next().await.unwrap().unwrap();
        let tctx = ctx.clone();
        {
            assert_eq!(tctx.get_channel_pair_number(), 3);
        }

        let mut updates2: HashMap<(u32, u64), Vec<ActorInfo>> = HashMap::new();
        updates2.insert(
            (actor_id, dispatcher_id),
            vec![helper_make_local_actor(235)],
        );
        let b2 = Barrier::new_test_barrier(1).with_mutation(Mutation::UpdateOutputs(updates2));

        tx.send(Message::Barrier(b2)).await.unwrap();
        executor.next().await.unwrap().unwrap();
        let tctx = ctx.clone();
        {
            assert_eq!(tctx.get_channel_pair_number(), 1);
        }

        add_local_channels(ctx.clone(), vec![(233, 245)]);
        add_remote_channels(ctx.clone(), 233, vec![246]);
        tx.send(Message::Barrier(
            Barrier::new_test_barrier(1).with_mutation(Mutation::AddOutput(AddOutput {
                map: {
                    let mut actors = HashMap::default();
                    actors.insert(
                        (233, 666),
                        vec![helper_make_local_actor(245), helper_make_remote_actor(246)],
                    );
                    actors
                },
                ..Default::default()
            })),
        ))
        .await
        .unwrap();
        executor.next().await.unwrap().unwrap();
        let tctx = ctx.clone();
        {
            assert_eq!(tctx.get_channel_pair_number(), 3);
        }
    }

    #[tokio::test]
    async fn test_hash_dispatcher() {
        let num_outputs = 5; // actor id ranges from 1 to 5
        let cardinality = 10;
        let dimension = 4;
        let key_indices = &[0, 2];
        let output_data_vecs = (0..num_outputs)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect::<Vec<_>>();
        let outputs = output_data_vecs
            .iter()
            .enumerate()
            .map(|(actor_id, data)| {
                Box::new(MockOutput::new(1 + actor_id as u32, data.clone())) as BoxedOutput
            })
            .collect::<Vec<_>>();
        let mut hash_mapping = (1..num_outputs + 1)
            .flat_map(|id| vec![id as ActorId; VIRTUAL_NODE_COUNT / num_outputs])
            .collect_vec();
        hash_mapping.resize(VIRTUAL_NODE_COUNT, num_outputs as u32);
        let mut hash_dispatcher = HashDataDispatcher::new(
            (0..outputs.len() as u32).collect(),
            outputs,
            key_indices.to_vec(),
            hash_mapping.clone(),
            0,
        );

        let mut ops = Vec::new();
        for idx in 0..cardinality {
            if idx % 2 == 0 {
                ops.push(Op::Insert);
            } else {
                ops.push(Op::Delete);
            }
        }

        let mut start = 19260817i32..;
        let mut builders = (0..dimension)
            .map(|_| I32ArrayBuilder::new(cardinality))
            .collect_vec();
        let mut output_cols = vec![vec![vec![]; dimension]; num_outputs];
        let mut output_ops = vec![vec![]; num_outputs];
        for op in &ops {
            let hash_builder = CRC32FastBuilder {};
            let mut hasher = hash_builder.build_hasher();
            let one_row = (0..dimension).map(|_| start.next().unwrap()).collect_vec();
            for key_idx in key_indices.iter() {
                let val = one_row[*key_idx];
                let bytes = val.to_le_bytes();
                hasher.update(&bytes);
            }
            let output_idx =
                hash_mapping[hasher.finish() as usize % VIRTUAL_NODE_COUNT] as usize - 1;
            for (builder, val) in builders.iter_mut().zip_eq(one_row.iter()) {
                builder.append(Some(*val)).unwrap();
            }
            output_cols[output_idx]
                .iter_mut()
                .zip_eq(one_row.iter())
                .for_each(|(each_column, val)| each_column.push(*val));
            output_ops[output_idx].push(op);
        }

        let columns = builders
            .into_iter()
            .map(|builder| {
                let array = builder.finish().unwrap();
                Column::new(Arc::new(array.into()))
            })
            .collect::<Vec<_>>();

        let chunk = StreamChunk::new(ops, columns, None);
        hash_dispatcher.dispatch_data(chunk).await.unwrap();

        for (output_idx, output) in output_data_vecs.into_iter().enumerate() {
            let guard = output.lock().unwrap();
            // It is possible that there is no chunks, as a key doesn't belong to any hash bucket.
            assert!(guard.len() <= 1);
            if guard.is_empty() {
                assert!(output_cols[output_idx].iter().all(|x| { x.is_empty() }));
            } else {
                let message = guard.get(0).unwrap();
                let real_chunk = match message {
                    Message::Chunk(chunk) => chunk,
                    _ => panic!(),
                };
                real_chunk
                    .columns()
                    .iter()
                    .zip_eq(output_cols[output_idx].iter())
                    .for_each(|(real_col, expect_col)| {
                        let real_vals = real_chunk
                            .visibility()
                            .as_ref()
                            .unwrap()
                            .iter()
                            .enumerate()
                            .filter(|(_, vis)| *vis)
                            .map(|(row_idx, _)| {
                                real_col.array_ref().as_int32().value_at(row_idx).unwrap()
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(real_vals.len(), expect_col.len());
                        assert_eq!(real_vals, *expect_col);
                    });
            }
        }
    }
}
