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
use std::sync::Arc;

use enum_as_inner::EnumAsInner;
use error::StreamExecutorResult;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use madsim::collections::{HashMap, HashSet};
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::types::DataType;
use risingwave_connector::source::{ConnectorState, SplitImpl};
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::Epoch as ProstEpoch;
use risingwave_pb::stream_plan::add_dispatcher_mutation::Dispatchers;
use risingwave_pb::stream_plan::barrier::Mutation as ProstMutation;
use risingwave_pb::stream_plan::stream_message::StreamMessage;
use risingwave_pb::stream_plan::{
    AddDispatcherMutation, AddMutation, Barrier as ProstBarrier, Dispatcher as ProstDispatcher,
    DispatcherMutation, PauseMutation, ResumeMutation, SourceChangeSplitMutation, StopMutation,
    StreamMessage as ProstStreamMessage, UpdateMutation,
};
use smallvec::SmallVec;
use tracing::trace_span;

use crate::task::{ActorId, DispatcherId, ENABLE_BARRIER_AGGREGATION};

mod actor;
pub mod aggregation;
mod barrier_align;
mod batch_query;
mod chain;
mod debug;
pub mod dispatch;
mod dynamic_filter;
mod error;
mod expand;
mod filter;
mod global_simple_agg;
mod hash_agg;
pub mod hash_join;
mod hop_window;
mod local_simple_agg;
mod lookup;
mod lookup_union;
mod managed_state;
pub mod merge;
pub mod monitor;
mod mview;
mod project;
mod rearranged_chain;
pub mod receiver;
mod simple;
mod sink;
mod source;
mod top_n_appendonly;
mod top_n_executor;
mod top_n_new;
mod union;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod test_utils;

pub use actor::{Actor, ActorContext, ActorContextRef, OperatorInfo, OperatorInfoStatus};
pub use batch_query::BatchQueryExecutor;
pub use chain::ChainExecutor;
pub use debug::DebugExecutor;
pub use dispatch::DispatchExecutor;
pub use dynamic_filter::DynamicFilterExecutor;
pub use expand::ExpandExecutor;
pub use filter::FilterExecutor;
pub use global_simple_agg::GlobalSimpleAggExecutor;
pub use hash_agg::HashAggExecutor;
pub use hash_join::*;
pub use hop_window::HopWindowExecutor;
pub use local_simple_agg::LocalSimpleAggExecutor;
pub use lookup::*;
pub use lookup_union::LookupUnionExecutor;
pub use merge::MergeExecutor;
pub use mview::*;
pub use project::ProjectExecutor;
pub use rearranged_chain::RearrangedChainExecutor;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use simple::{SimpleExecutor, SimpleExecutorWrapper};
pub use sink::SinkExecutor;
pub use source::*;
pub use top_n_appendonly::AppendOnlyTopNExecutor;
pub use top_n_new::TopNExecutorNew;
pub use union::UnionExecutor;

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedMessageStream = BoxStream<'static, StreamExecutorResult<Message>>;
pub type MessageStreamItem = StreamExecutorResult<Message>;

pub trait MessageStream = futures::Stream<Item = MessageStreamItem> + Send;

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

/// Static information of an executor.
#[derive(Debug, Default)]
pub struct ExecutorInfo {
    /// See [`Executor::schema`].
    pub schema: Schema,

    /// See [`Executor::pk_indices`].
    pub pk_indices: PkIndices,

    /// See [`Executor::identity`].
    pub identity: String,
}

/// `Executor` supports handling of control messages.
pub trait Executor: Send + 'static {
    fn execute(self: Box<Self>) -> BoxedMessageStream;

    /// Return the schema of the OUTPUT of the executor.
    fn schema(&self) -> &Schema;

    /// Return the primary key indices of the OUTPUT of the executor.
    /// Schema is used by both OLAP and streaming, therefore
    /// pk indices are maintained independently.
    fn pk_indices(&self) -> PkIndicesRef;

    /// Identity of the executor.
    fn identity(&self) -> &str;

    fn execute_with_epoch(self: Box<Self>, _epoch: u64) -> BoxedMessageStream {
        self.execute()
    }

    #[inline(always)]
    fn info(&self) -> ExecutorInfo {
        let schema = self.schema().to_owned();
        let pk_indices = self.pk_indices().to_owned();
        let identity = self.identity().to_owned();
        ExecutorInfo {
            schema,
            pk_indices,
            identity,
        }
    }

    fn boxed(self) -> BoxedExecutor
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

pub const INVALID_EPOCH: u64 = 0;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct AddOutput {
    pub map: HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>,
    pub splits: HashMap<ActorId, Vec<SplitImpl>>,
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct AddDispatcher {
    pub map: HashMap<ActorId, Vec<ProstDispatcher>>,
    // TODO: remove this and use `SourceChangesSplit` after we support multiple mutations.
    pub splits: HashMap<ActorId, Vec<SplitImpl>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Stop(HashSet<ActorId>),
    UpdateOutputs(HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>),
    AddOutput(AddOutput),
    AddDispatcher(AddDispatcher),
    SourceChangeSplit(HashMap<ActorId, ConnectorState>),
    Pause,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Epoch {
    pub curr: u64,
    pub prev: u64,
}

impl Epoch {
    pub fn new(curr: u64, prev: u64) -> Self {
        assert!(curr > prev);
        Self { curr, prev }
    }

    #[cfg(test)]
    pub fn inc(&self) -> Self {
        Self {
            curr: self.curr + 1,
            prev: self.prev + 1,
        }
    }

    pub fn new_test_epoch(curr: u64) -> Self {
        assert!(curr > 0);
        Self::new(curr, curr - 1)
    }
}

impl Default for Epoch {
    fn default() -> Self {
        Self {
            curr: 1,
            prev: INVALID_EPOCH,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Barrier {
    pub epoch: Epoch,
    pub mutation: Option<Arc<Mutation>>,
    pub span: tracing::Span,
}

impl Default for Barrier {
    fn default() -> Self {
        Self {
            span: tracing::Span::none(),
            epoch: Epoch::default(),
            mutation: None,
        }
    }
}

impl Barrier {
    /// Create a plain barrier.
    pub fn new_test_barrier(epoch: u64) -> Self {
        Self {
            epoch: Epoch::new_test_epoch(epoch),
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_mutation(self, mutation: Mutation) -> Self {
        Self {
            mutation: Some(Arc::new(mutation)),
            ..self
        }
    }

    #[must_use]
    pub fn with_stop(self) -> Self {
        self.with_mutation(Mutation::Stop(HashSet::default()))
    }

    // TODO: The barrier should always contain trace info after we migrated barrier generation to
    // meta service.
    #[must_use]
    pub fn with_span(self, span: tracing::span::Span) -> Self {
        Self { span, ..self }
    }

    pub fn is_to_stop_actor(&self, actor_id: ActorId) -> bool {
        matches!(self.mutation.as_deref(), Some(Mutation::Stop(actors)) if actors.contains(&actor_id))
    }

    pub fn is_to_add_output(&self, actor_id: ActorId) -> bool {
        // TODO: remove `AddOutput`
        matches!(
            self.mutation.as_deref(),
            Some(Mutation::AddOutput(map)) if map.map
                .values()
                .flatten()
                .any(|info| info.actor_id == actor_id)
        ) || matches!(
            self.mutation.as_deref(),
            Some(Mutation::AddDispatcher(map)) if map.map
                .values()
                .flatten()
                .any(|dispatcher| dispatcher.downstream_actor_id.contains(&actor_id))
        )
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.mutation == other.mutation
    }
}

impl Mutation {
    /// Return true if the mutation is stop.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(self, Mutation::Stop(_))
    }

    fn to_protobuf(&self) -> ProstMutation {
        match self {
            Mutation::Stop(actors) => ProstMutation::Stop(StopMutation {
                actors: actors.iter().cloned().collect::<Vec<_>>(),
            }),
            Mutation::UpdateOutputs(updates) => ProstMutation::Update(UpdateMutation {
                mutations: updates
                    .iter()
                    .map(|(&(actor_id, dispatcher_id), actors)| DispatcherMutation {
                        actor_id,
                        dispatcher_id,
                        info: actors.clone(),
                    })
                    .collect(),
            }),
            Mutation::AddOutput(adds) => ProstMutation::Add(AddMutation {
                mutations: adds
                    .map
                    .iter()
                    .map(|(&(actor_id, dispatcher_id), actors)| DispatcherMutation {
                        actor_id,
                        dispatcher_id,
                        info: actors.clone(),
                    })
                    .collect(),
                ..Default::default()
            }),
            Mutation::AddDispatcher(adds) => ProstMutation::AddDispatcher(AddDispatcherMutation {
                actor_dispatchers: adds
                    .map
                    .iter()
                    .map(|(&actor_id, dispatchers)| {
                        (
                            actor_id,
                            Dispatchers {
                                dispatchers: dispatchers.clone(),
                            },
                        )
                    })
                    .collect(),
                ..Default::default()
            }),
            Mutation::SourceChangeSplit(changes) => {
                ProstMutation::Splits(SourceChangeSplitMutation {
                    actor_splits: changes
                        .iter()
                        .map(|(&actor_id, splits)| {
                            (
                                actor_id,
                                ConnectorSplits {
                                    splits: splits
                                        .clone()
                                        .unwrap_or_default()
                                        .iter()
                                        .map(ConnectorSplit::from)
                                        .collect(),
                                },
                            )
                        })
                        .collect(),
                })
            }
            Mutation::Pause => ProstMutation::Pause(PauseMutation {}),
            Mutation::Resume => ProstMutation::Resume(ResumeMutation {}),
        }
    }

    fn from_protobuf(prost: &ProstMutation) -> Result<Self> {
        let mutation = match prost {
            ProstMutation::Stop(stop) => {
                Mutation::Stop(HashSet::from_iter(stop.get_actors().clone()))
            }

            ProstMutation::Update(update) => Mutation::UpdateOutputs(
                update
                    .mutations
                    .iter()
                    .map(|mutation| {
                        (
                            (mutation.actor_id, mutation.dispatcher_id),
                            mutation.get_info().clone(),
                        )
                    })
                    .collect::<HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>>(),
            ),
            // TODO: remove this
            ProstMutation::Add(adds) => Mutation::AddOutput(AddOutput {
                map: adds
                    .mutations
                    .iter()
                    .map(|mutation| {
                        (
                            (mutation.actor_id, mutation.dispatcher_id),
                            mutation.get_info().clone(),
                        )
                    })
                    .collect::<HashMap<(ActorId, DispatcherId), Vec<ActorInfo>>>(),
                splits: adds
                    .actor_splits
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(|split| split.try_into().unwrap())
                                .collect(),
                        )
                    })
                    .collect(),
            }),
            ProstMutation::AddDispatcher(adds) => Mutation::AddDispatcher(AddDispatcher {
                map: adds
                    .actor_dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| (actor_id, dispatchers.dispatchers.clone()))
                    .collect(),
                // TODO: remove this and use `SourceChangesSplit` after we support multiple
                // mutations.
                splits: adds
                    .actor_splits
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            splits
                                .splits
                                .iter()
                                .map(|split| split.try_into().unwrap())
                                .collect(),
                        )
                    })
                    .collect(),
            }),

            ProstMutation::Splits(s) => {
                let mut change_splits: Vec<(ActorId, ConnectorState)> =
                    Vec::with_capacity(s.actor_splits.len());
                for (&actor_id, splits) in &s.actor_splits {
                    if splits.splits.is_empty() {
                        change_splits.push((actor_id, None));
                    } else {
                        change_splits.push((
                            actor_id,
                            Some(
                                splits
                                    .splits
                                    .iter()
                                    .map(SplitImpl::try_from)
                                    .collect::<anyhow::Result<Vec<SplitImpl>>>()
                                    .to_rw_result()?,
                            ),
                        ));
                    }
                }
                Mutation::SourceChangeSplit(
                    change_splits
                        .into_iter()
                        .collect::<HashMap<ActorId, ConnectorState>>(),
                )
            }
            ProstMutation::Pause(_) => Mutation::Pause,
            ProstMutation::Resume(_) => Mutation::Resume,
        };
        Ok(mutation)
    }
}

impl Barrier {
    pub fn to_protobuf(&self) -> ProstBarrier {
        let Barrier {
            epoch, mutation, ..
        }: Barrier = self.clone();
        ProstBarrier {
            epoch: Some(ProstEpoch {
                curr: epoch.curr,
                prev: epoch.prev,
            }),
            mutation: mutation.map(|mutation| mutation.to_protobuf()),
            span: vec![],
        }
    }

    pub fn from_protobuf(prost: &ProstBarrier) -> Result<Self> {
        let mutation = prost
            .mutation
            .as_ref()
            .map(Mutation::from_protobuf)
            .transpose()?
            .map(Arc::new);
        let epoch = prost.get_epoch().unwrap();
        Ok(Barrier {
            span: if ENABLE_BARRIER_AGGREGATION {
                trace_span!("barrier", epoch = ?epoch, mutation = ?mutation)
            } else {
                tracing::Span::none()
            },
            epoch: Epoch::new(epoch.curr, epoch.prev),
            mutation,
        })
    }
}

#[derive(Debug, EnumAsInner, PartialEq, Clone)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(Barrier),
}

impl<'a> TryFrom<&'a Message> for &'a Barrier {
    type Error = ();

    fn try_from(m: &'a Message) -> std::result::Result<Self, Self::Error> {
        match m {
            Message::Chunk(_) => Err(()),
            Message::Barrier(b) => Ok(b),
        }
    }
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    ///
    /// Note that this does not mean we will stop the current actor.
    #[cfg(test)]
    pub fn is_stop(&self) -> bool {
        matches!(
            self,
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_ref().unwrap().is_stop()
        )
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamMessage> {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf();
                StreamMessage::StreamChunk(prost_stream_chunk)
            }
            Self::Barrier(barrier) => StreamMessage::Barrier(barrier.clone().to_protobuf()),
        };
        let prost_stream_msg = ProstStreamMessage {
            stream_message: Some(prost),
        };
        Ok(prost_stream_msg)
    }

    pub fn from_protobuf(prost: &ProstStreamMessage) -> Result<Self> {
        let res = match prost.get_stream_message()? {
            StreamMessage::StreamChunk(ref stream_chunk) => {
                Message::Chunk(StreamChunk::from_protobuf(stream_chunk)?)
            }
            StreamMessage::Barrier(ref barrier) => {
                Message::Barrier(Barrier::from_protobuf(barrier)?)
            }
        };
        Ok(res)
    }

    pub fn get_encoded_len(msg: &impl ::prost::Message) -> usize {
        ::prost::Message::encoded_len(msg)
    }
}

pub type PkIndices = Vec<usize>;
pub type PkIndicesRef<'a> = &'a [usize];
pub type PkDataTypes = SmallVec<[DataType; 1]>;

/// Get clones of inputs by given `pk_indices` from `columns`.
pub fn pk_input_arrays(pk_indices: PkIndicesRef, columns: &[Column]) -> Vec<ArrayRef> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array())
        .collect()
}

/// Get references to inputs by given `pk_indices` from `columns`.
pub fn pk_input_array_refs<'a>(
    pk_indices: PkIndicesRef,
    columns: &'a [Column],
) -> Vec<&'a ArrayImpl> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array_ref())
        .collect()
}

/// Expect the first message of the given `stream` as a barrier.
pub async fn expect_first_barrier(
    stream: &mut (impl MessageStream + Unpin),
) -> StreamExecutorResult<Barrier> {
    let message = stream.next().await.unwrap()?;
    let barrier = message
        .into_barrier()
        .expect("the first message must be a barrier");
    Ok(barrier)
}

/// `StreamConsumer` is the last step in an actor.
pub trait StreamConsumer: Send + 'static {
    type BarrierStream: Stream<Item = Result<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream;
}
