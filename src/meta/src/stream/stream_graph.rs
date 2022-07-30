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

use std::collections::hash_map::HashMap;
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Deref, Range};
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::catalog::{generate_intertable_name_with_type, TableId};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, DispatcherType, MergeNode, StreamActor,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode,
};

use super::{BuildGraphInfo, CreateMaterializedViewContext, FragmentManagerRef};
use crate::cluster::WorkerId;
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;

/// Id of an Actor, maybe local or global
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
enum LocalActorId {
    /// The global allocated id of a fragment.
    Global(u32),
    /// The local id of a fragment, need to be converted to global id if being used in the meta
    /// service.
    Local(u32),
}

/// Id of a fragment, maybe local or global
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
struct GlobalFragmentId(u32);

impl LocalActorId {
    pub fn as_global_id(&self) -> u32 {
        match self {
            Self::Global(id) => *id,
            _ => panic!("actor id is not global id"),
        }
    }

    pub fn as_local_id(&self) -> u32 {
        match self {
            Self::Local(id) => *id,
            _ => panic!("actor id is not local id"),
        }
    }

    #[expect(dead_code)]
    pub fn is_global(&self) -> bool {
        matches!(self, Self::Global(_))
    }

    #[expect(dead_code)]
    pub fn is_local(&self) -> bool {
        matches!(self, Self::Local(_))
    }

    /// Convert local id to global id. Panics if the actor id is not local, or actor id >=
    /// len.
    pub fn to_global_id(self, offset: u32, len: u32) -> Self {
        let id = self.as_local_id();
        assert!(id < len, "actor id {} is out of range (len: {})", id, len);
        Self::Global(id + offset)
    }
}

impl GlobalFragmentId {
    pub fn as_global_id(&self) -> u32 {
        self.0
    }

    /// Convert local id to global id. Panics if the fragment id is not local, or fragment id >=
    /// len.
    pub fn from_local_id(id: u32, offset: u32, len: u32) -> Self {
        assert!(
            id < len,
            "fragment id {} is out of range (len: {})",
            id,
            len
        );
        Self(id + offset)
    }
}

/// A list of actors with order.
#[derive(Debug, Clone)]
struct OrderedActorLink(pub Vec<LocalActorId>);

impl OrderedActorLink {
    pub fn to_global_ids(&self, actor_id_offset: u32, actor_id_len: u32) -> Self {
        Self(
            self.0
                .iter()
                .map(|x| x.to_global_id(actor_id_offset, actor_id_len))
                .collect(),
        )
    }

    pub fn as_global_ids(&self) -> Vec<u32> {
        Self::slice_as_global_ids(self.0.as_slice())
    }

    pub fn slice_as_global_ids(data: &[LocalActorId]) -> Vec<u32> {
        data.iter().map(|x| x.as_global_id()).collect()
    }
}

struct StreamActorDownstream {
    dispatch_strategy: DispatchStrategy,
    dispatcher_id: u64,

    /// Downstream actors.
    actors: OrderedActorLink,

    /// Whether to place the downstream actors on the same node
    same_worker_node: bool,
}

struct StreamActorUpstream {
    /// Upstream actors
    actors: OrderedActorLink,
    /// associate fragment id
    fragment_id: GlobalFragmentId,
    /// Whether to place the upstream actors on the same node
    same_worker_node: bool,
}

/// [`StreamActorBuilder`] builds a stream actor in a stream DAG.
struct StreamActorBuilder {
    /// actor id field
    actor_id: LocalActorId,

    /// associated fragment id
    fragment_id: GlobalFragmentId,

    /// associated stream node
    nodes: Arc<StreamNode>,

    /// downstream dispatchers (dispatcher, downstream actor, hash mapping)
    downstreams: Vec<StreamActorDownstream>,

    /// upstreams, exchange node operator_id -> upstream actor ids
    upstreams: HashMap<u64, StreamActorUpstream>,

    /// Whether to place this actors on the same node as chain's upstream MVs.
    chain_same_worker_node: bool,

    /// whether this actor builder has been sealed
    sealed: bool,
}

impl StreamActorBuilder {
    fn is_chain_same_worker_node(stream_node: &StreamNode) -> bool {
        fn visit(stream_node: &StreamNode) -> bool {
            if let Some(NodeBody::Chain(ref chain)) = stream_node.node_body {
                return chain.same_worker_node;
            }
            stream_node.input.iter().any(visit)
        }
        visit(stream_node)
    }

    pub fn new(
        actor_id: LocalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) -> Self {
        Self {
            actor_id,
            fragment_id,
            chain_same_worker_node: Self::is_chain_same_worker_node(&node),
            nodes: node,
            downstreams: vec![],
            upstreams: HashMap::new(),
            sealed: false,
        }
    }

    pub fn get_fragment_id(&self) -> GlobalFragmentId {
        self.fragment_id
    }

    /// Add a dispatcher to this actor.
    pub fn add_dispatcher(
        &mut self,
        dispatch_strategy: DispatchStrategy,
        dispatcher_id: u64,
        downstream_actors: OrderedActorLink,
        same_worker_node: bool,
    ) {
        assert!(!self.sealed);

        self.downstreams.push(StreamActorDownstream {
            dispatch_strategy,
            dispatcher_id,
            actors: downstream_actors,
            same_worker_node,
        });
    }

    /// Build an actor from given information. At the same time, convert local actor id to global
    /// actor id.
    pub fn seal(&mut self, actor_id_offset: u32, actor_id_len: u32) {
        assert!(!self.sealed);

        self.actor_id = self.actor_id.to_global_id(actor_id_offset, actor_id_len);
        self.downstreams = std::mem::take(&mut self.downstreams)
            .into_iter()
            .map(
                |StreamActorDownstream {
                     dispatch_strategy,
                     dispatcher_id,
                     actors: downstreams,
                     same_worker_node,
                 }| {
                    let downstreams = downstreams.to_global_ids(actor_id_offset, actor_id_len);

                    if dispatch_strategy.r#type == DispatcherType::NoShuffle as i32 {
                        assert_eq!(
                            downstreams.0.len(),
                            1,
                            "no shuffle should only have one actor downstream"
                        );
                        assert!(
                            dispatch_strategy.column_indices.is_empty(),
                            "should leave `column_indices` empty"
                        );
                    }

                    StreamActorDownstream {
                        dispatch_strategy,
                        dispatcher_id,
                        actors: downstreams,
                        same_worker_node,
                    }
                },
            )
            .collect();

        self.upstreams = std::mem::take(&mut self.upstreams)
            .into_iter()
            .map(
                |(
                    exchange_id,
                    StreamActorUpstream {
                        actors,
                        same_worker_node,
                        fragment_id,
                    },
                )| {
                    (
                        exchange_id,
                        StreamActorUpstream {
                            actors: actors.to_global_ids(actor_id_offset, actor_id_len),
                            same_worker_node,
                            fragment_id,
                        },
                    )
                },
            )
            .collect();
        self.sealed = true;
    }

    /// Build an actor after seal.
    pub fn build(&self) -> StreamActor {
        assert!(self.sealed);

        let dispatcher = self
            .downstreams
            .iter()
            .map(
                |StreamActorDownstream {
                     dispatch_strategy,
                     dispatcher_id,
                     actors,
                     same_worker_node: _,
                 }| Dispatcher {
                    downstream_actor_id: actors.as_global_ids(),
                    r#type: dispatch_strategy.r#type,
                    column_indices: dispatch_strategy.column_indices.clone(),
                    // will be filled later by stream manager
                    hash_mapping: None,
                    dispatcher_id: *dispatcher_id,
                },
            )
            .collect_vec();

        StreamActor {
            actor_id: self.actor_id.as_global_id(),
            fragment_id: self.fragment_id.as_global_id(),
            nodes: Some(self.nodes.deref().clone()),
            dispatcher,
            upstream_actor_id: self
                .upstreams
                .iter()
                .flat_map(|(_, StreamActorUpstream { actors, .. })| actors.0.iter().copied())
                .map(|x| x.as_global_id())
                .collect(), // TODO: store each upstream separately
            same_worker_node_as_upstream: self.chain_same_worker_node
                || self.upstreams.values().any(|u| u.same_worker_node),
            vnode_bitmap: None,
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
#[derive(Default)]
struct StreamGraphBuilder {
    actor_builders: BTreeMap<LocalActorId, StreamActorBuilder>,

    table_node_actors: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,

    table_sink_actor_ids: HashMap<TableId, Vec<ActorId>>,
}

impl StreamGraphBuilder {
    /// Resolve infos at first to avoid blocking call inside.
    pub fn fill_info(&mut self, info: BuildGraphInfo) {
        self.table_node_actors = info.table_node_actors;
        self.table_sink_actor_ids = info.table_sink_actor_ids;
    }

    /// Insert new generated actor.
    pub fn add_actor(
        &mut self,
        actor_id: LocalActorId,
        fragment_id: GlobalFragmentId,
        node: Arc<StreamNode>,
    ) {
        self.actor_builders.insert(
            actor_id,
            StreamActorBuilder::new(actor_id, fragment_id, node),
        );
    }

    /// Number of actors in the graph builder
    pub fn actor_len(&self) -> usize {
        self.actor_builders.len()
    }

    /// Add dependency between two connected node in the graph.
    pub fn add_link(
        &mut self,
        upstream_fragment_id: GlobalFragmentId,
        upstream_actor_ids: &[LocalActorId],
        downstream_actor_ids: &[LocalActorId],
        exchange_operator_id: u64,
        dispatch_strategy: DispatchStrategy,
        same_worker_node: bool,
    ) {
        if dispatch_strategy.get_type().unwrap() == DispatcherType::NoShuffle {
            assert_eq!(
                upstream_actor_ids.len(),
                downstream_actor_ids.len(),
                "mismatched length when procssing no-shuffle exchange: {:?} -> {:?} on exchange {}",
                upstream_actor_ids,
                downstream_actor_ids,
                exchange_operator_id
            );

            // update 1v1 relationship
            upstream_actor_ids
                .iter()
                .zip_eq(downstream_actor_ids.iter())
                .for_each(|(upstream_id, downstream_id)| {
                    self.actor_builders
                        .get_mut(upstream_id)
                        .unwrap()
                        .add_dispatcher(
                            dispatch_strategy.clone(),
                            exchange_operator_id,
                            OrderedActorLink(vec![*downstream_id]),
                            same_worker_node,
                        );

                    let ret = self
                        .actor_builders
                        .get_mut(downstream_id)
                        .unwrap()
                        .upstreams
                        .insert(
                            exchange_operator_id,
                            StreamActorUpstream {
                                actors: OrderedActorLink(vec![*upstream_id]),
                                fragment_id: upstream_fragment_id,
                                same_worker_node,
                            },
                        );

                    assert!(
                        ret.is_none(),
                        "duplicated exchange input {} for no-shuffle actors {:?} -> {:?}",
                        exchange_operator_id,
                        upstream_id,
                        downstream_id
                    );
                });

            return;
        }

        // otherwise, make m * n links between actors.

        assert!(
            !same_worker_node,
            "same_worker_node only applies to 1v1 dispatchers."
        );

        // update actors to have dispatchers, link upstream -> downstream.
        upstream_actor_ids.iter().for_each(|upstream_id| {
            self.actor_builders
                .get_mut(upstream_id)
                .unwrap()
                .add_dispatcher(
                    dispatch_strategy.clone(),
                    exchange_operator_id,
                    OrderedActorLink(downstream_actor_ids.to_vec()),
                    same_worker_node,
                );
        });

        // update actors to have upstreams, link downstream <- upstream.
        downstream_actor_ids.iter().for_each(|downstream_id| {
            let ret = self
                .actor_builders
                .get_mut(downstream_id)
                .unwrap()
                .upstreams
                .insert(
                    exchange_operator_id,
                    StreamActorUpstream {
                        actors: OrderedActorLink(upstream_actor_ids.to_vec()),
                        fragment_id: upstream_fragment_id,
                        same_worker_node,
                    },
                );
            assert!(
                ret.is_none(),
                "duplicated exchange input {} for actors {:?} -> {:?}",
                exchange_operator_id,
                upstream_actor_ids,
                downstream_actor_ids
            );
        });
    }

    /// Build final stream DAG with dependencies with current actor builders.
    #[allow(clippy::type_complexity)]
    pub fn build(
        mut self,
        ctx: &mut CreateMaterializedViewContext,
        actor_id_offset: u32,
        actor_id_len: u32,
    ) -> Result<HashMap<GlobalFragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();

        for builder in self.actor_builders.values_mut() {
            builder.seal(actor_id_offset, actor_id_len);
        }

        for builder in self.actor_builders.values() {
            let actor_id = builder.actor_id;
            let mut actor = builder.build();
            let mut upstream_actors = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { actors, .. })| (*id, actors.clone()))
                .collect();
            let mut upstream_fragments = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { fragment_id, .. })| (*id, *fragment_id))
                .collect();
            let stream_node = self.build_inner(
                ctx,
                actor.get_nodes()?,
                actor_id,
                &mut upstream_actors,
                &mut upstream_fragments,
            )?;

            actor.nodes = Some(stream_node);
            graph
                .entry(builder.get_fragment_id())
                .or_insert(vec![])
                .push(actor);
        }
        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is `ExchangeNode`, and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's `ExchangeNode`.
    /// 3. replace node's `ExchangeNode` input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a `ChainNode`.
    fn build_inner(
        &self,
        ctx: &mut CreateMaterializedViewContext,
        stream_node: &StreamNode,
        actor_id: LocalActorId,
        upstream_actor_id: &mut HashMap<u64, OrderedActorLink>,
        upstream_fragment_id: &mut HashMap<u64, GlobalFragmentId>,
    ) -> Result<StreamNode> {
        let table_id_offset = ctx.table_id_offset;
        let mut check_and_fill_internal_table = |table_id: u32, table: Option<Table>| {
            ctx.internal_table_id_map.entry(table_id).or_insert(table);
        };

        match stream_node.get_node_body()? {
            NodeBody::Exchange(_) => {
                panic!("ExchangeNode should be eliminated from the top of the plan node when converting fragments to actors: {:#?}", stream_node)
            }
            NodeBody::Chain(_) => Ok(self.resolve_chain_node(stream_node)?),
            _ => {
                let mut new_stream_node = stream_node.clone();

                // Table id rewrite done below.
                match new_stream_node.node_body.as_mut().unwrap() {
                    NodeBody::HashJoin(node) => {
                        if let Some(table) = &mut node.left_table {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "HashJoinLeft",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                        if let Some(table) = &mut node.right_table {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "HashJoinRight",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                    }

                    NodeBody::Lookup(node) => {
                        if let Some(ArrangementTableId::TableId(table_id)) =
                            &mut node.arrangement_table_id
                        {
                            *table_id += table_id_offset;
                            node.arrangement_table.as_mut().unwrap().id = *table_id;
                            // We do not need check and fill internal table for Lookup, cuz it's
                            // already been set by ArrangeNode.
                        }
                    }

                    NodeBody::Arrange(node) => {
                        if let Some(table) = &mut node.table {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "ArrangeNode",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                    }

                    NodeBody::HashAgg(node) => {
                        assert_eq!(node.internal_tables.len(), node.agg_calls.len());
                        // In-place update the table id. Convert from local to global.
                        for table in &mut node.internal_tables {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "HashAgg",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                    }

                    NodeBody::TopN(node) => {
                        node.table_id_l += table_id_offset;
                        node.table_id_h += table_id_offset;

                        // TODO add catalog::Table to TopNNode
                        check_and_fill_internal_table(node.table_id_l, None);
                        check_and_fill_internal_table(node.table_id_h, None);
                    }

                    NodeBody::AppendOnlyTopN(node) => {
                        node.table_id_l += table_id_offset;
                        node.table_id_h += table_id_offset;

                        // TODO add catalog::Table to AppendOnlyTopN
                        check_and_fill_internal_table(node.table_id_l, None);
                        check_and_fill_internal_table(node.table_id_h, None);
                    }

                    NodeBody::GlobalSimpleAgg(node) | NodeBody::LocalSimpleAgg(node) => {
                        assert_eq!(node.internal_tables.len(), node.agg_calls.len());
                        // In-place update the table id. Convert from local to global.
                        for table in &mut node.internal_tables {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "GlobalSimpleAgg",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                    }

                    NodeBody::DynamicFilter(node) => {
                        if let Some(table) = &mut node.left_table {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "DynamicFilterLeft",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                        if let Some(table) = &mut node.right_table {
                            table.id += table_id_offset;
                            table.schema_id = ctx.schema_id;
                            table.database_id = ctx.database_id;
                            table.name = generate_intertable_name_with_type(
                                &ctx.mview_name,
                                table.id,
                                "DynamicFilterRight",
                            );
                            check_and_fill_internal_table(table.id, Some(table.clone()));
                        }
                    }
                    _ => {}
                }

                for (idx, input) in stream_node.input.iter().enumerate() {
                    match input.get_node_body()? {
                        NodeBody::Exchange(_) => {
                            assert!(!input.get_fields().is_empty());
                            new_stream_node.input[idx] = StreamNode {
                                input: vec![],
                                pk_indices: input.pk_indices.clone(),
                                node_body: Some(NodeBody::Merge(MergeNode {
                                    upstream_actor_id: upstream_actor_id
                                        .remove(&input.get_operator_id())
                                        .expect("failed to find upstream actor id for given exchange node").as_global_ids(),
                                    upstream_fragment_id: upstream_fragment_id.get(&input.get_operator_id()).unwrap().as_global_id(),
                                    fields: input.get_fields().clone(),
                                })),
                                fields: input.get_fields().clone(),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                                append_only: input.append_only,
                            };
                        }
                        NodeBody::Chain(_) => {
                            new_stream_node.input[idx] = self.resolve_chain_node(input)?;
                        }
                        _ => {
                            new_stream_node.input[idx] = self.build_inner(
                                ctx,
                                input,
                                actor_id,
                                upstream_actor_id,
                                upstream_fragment_id,
                            )?;
                        }
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    /// Resolve the chain node, only rewrite the schema of input `MergeNode`.
    fn resolve_chain_node(&self, stream_node: &StreamNode) -> Result<StreamNode> {
        let NodeBody::Chain(chain_node) = stream_node.get_node_body().unwrap() else {
            unreachable!()
        };
        let input = stream_node.get_input();
        assert_eq!(input.len(), 2);

        let merge_node = &input[0];
        assert_matches!(merge_node.node_body, Some(NodeBody::Merge(_)));
        let batch_plan_node = &input[1];
        assert_matches!(batch_plan_node.node_body, Some(NodeBody::BatchPlan(_)));

        let chain_input = vec![
            StreamNode {
                input: vec![],
                pk_indices: stream_node.pk_indices.clone(),
                node_body: Some(NodeBody::Merge(MergeNode {
                    upstream_actor_id: vec![],
                    upstream_fragment_id: 0,
                    fields: chain_node.upstream_fields.clone(),
                })),
                fields: chain_node.upstream_fields.clone(),
                operator_id: merge_node.operator_id,
                identity: "MergeExecutor".to_string(),
                append_only: stream_node.append_only,
            },
            batch_plan_node.clone(),
        ];

        Ok(StreamNode {
            input: chain_input,
            pk_indices: stream_node.pk_indices.clone(),
            node_body: Some(NodeBody::Chain(chain_node.clone())),
            operator_id: stream_node.operator_id,
            identity: "ChainExecutor".to_string(),
            fields: chain_node.upstream_fields.clone(),
            append_only: stream_node.append_only,
        })
    }
}

/// The mutable state when building actor graph.
#[derive(Default)]
struct BuildActorGraphState {
    /// stream graph builder, to build streaming DAG.
    stream_graph_builder: StreamGraphBuilder,
    /// when converting fragment graph to actor graph, we need to know which actors belong to a
    /// fragment.
    fragment_actors: HashMap<GlobalFragmentId, Vec<LocalActorId>>,
    /// local actor id
    next_local_actor_id: u32,
}

impl BuildActorGraphState {
    fn gen_actor_ids(&mut self, parallel_degree: u32) -> Range<u32> {
        let start_actor_id = self.next_local_actor_id;
        self.next_local_actor_id += parallel_degree;
        start_actor_id..start_actor_id + parallel_degree
    }
}

/// [`ActorGraphBuilder`] generates the proto for interconnected actors for a streaming pipeline.
pub struct ActorGraphBuilder {
    /// GlobalFragmentId -> parallel_degree
    parallelisms: HashMap<FragmentId, u32>,

    fragment_graph: StreamFragmentGraph,
}

impl ActorGraphBuilder {
    pub async fn new<S>(
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_graph: &StreamFragmentGraphProto,
        default_parallelism: u32,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<Self>
    where
        S: MetaStore,
    {
        // save dependent table ids in ctx
        ctx.dependent_table_ids = fragment_graph
            .dependent_table_ids
            .iter()
            .map(|table_id| TableId::new(*table_id))
            .collect();

        let fragment_len = fragment_graph.fragments.len() as u32;
        let offset = id_gen_manager
            .generate_interval::<{ IdCategory::Fragment }>(fragment_len as i32)
            .await? as _;

        // Compute how many table ids should be allocated for all actors.
        // Allocate all needed table ids for current MV.
        let table_ids_cnt = fragment_graph.table_ids_cnt;
        let start_table_id = id_gen_manager
            .generate_interval::<{ IdCategory::Table }>(table_ids_cnt as i32)
            .await? as _;
        ctx.table_id_offset = start_table_id;

        let fragment_graph = StreamFragmentGraph::from_protobuf(fragment_graph.clone(), offset);

        // TODO(Kexiang): now simply use Count(ParallelUnit) as parallelism of each fragment
        let parallelisms: HashMap<FragmentId, u32> = fragment_graph
            .fragments()
            .iter()
            .map(|(id, fragment)| {
                let id = id.as_global_id();
                let parallel_degree = if fragment.is_singleton {
                    1
                } else {
                    default_parallelism
                };
                (id, parallel_degree)
            })
            .collect();

        Ok(Self {
            parallelisms,
            fragment_graph,
        })
    }

    pub async fn generate_graph<S>(
        &mut self,
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        self.generate_graph_inner(id_gen_manager, fragment_manager, ctx)
            .await
    }

    /// Build a stream graph by duplicating each fragment as parallel actors.
    async fn generate_graph_inner<S>(
        &self,
        id_gen_manager: IdGeneratorManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>>
    where
        S: MetaStore,
    {
        let stream_graph = {
            let BuildActorGraphState {
                stream_graph_builder,
                next_local_actor_id,
                ..
            } = {
                let mut state = BuildActorGraphState::default();
                // resolve upstream table infos first
                // TODO: this info is only used by `resolve_chain_node`. We can move that logic to
                // stream manager and remove dependency on fragment manager.
                let info = fragment_manager
                    .get_build_graph_info(&ctx.dependent_table_ids)
                    .await?;
                ctx.table_sink_map = info.table_sink_actor_ids.clone();
                state.stream_graph_builder.fill_info(info);

                // Generate actors of the streaming plan
                self.build_actor_graph(&mut state, &self.fragment_graph)?;
                state
            };

            // generates global ids
            let (actor_len, start_actor_id) = {
                let actor_len = stream_graph_builder.actor_len() as u32;
                assert_eq!(actor_len, next_local_actor_id);
                let start_actor_id = id_gen_manager
                    .generate_interval::<{ IdCategory::Actor }>(actor_len as i32)
                    .await? as _;

                (actor_len, start_actor_id)
            };

            stream_graph_builder.build(ctx, start_actor_id, actor_len)?
        };

        // Serialize the graph
        let stream_graph = stream_graph
            .into_iter()
            .map(|(fragment_id, actors)| {
                let fragment = self.fragment_graph.get_fragment(fragment_id).unwrap();
                let fragment_id = fragment_id.as_global_id();
                (
                    fragment_id,
                    Fragment {
                        fragment_id,
                        fragment_type: fragment.fragment_type,
                        distribution_type: if fragment.is_singleton {
                            FragmentDistributionType::Single
                        } else {
                            FragmentDistributionType::Hash
                        } as i32,
                        actors,
                        vnode_mapping: None,
                    },
                )
            })
            .collect();

        Ok(stream_graph)
    }

    /// Build actor graph from fragment graph using topological sort. Setup dispatcher in actor and
    /// generate actors by their parallelism.
    fn build_actor_graph(
        &self,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
    ) -> Result<()> {
        // Use topological sort to build the graph from downstream to upstream. (The first fragment
        // poped out from the heap will be the top-most node in plan, or the sink in stream graph.)
        let mut actionable_fragment_id = VecDeque::new();
        let mut downstream_cnts = HashMap::new();

        // Iterate all fragments
        for (fragment_id, _) in fragment_graph.fragments().iter() {
            // Count how many downstreams we have for a given fragment
            let downstream_cnt = fragment_graph.get_downstreams(*fragment_id).len();
            if downstream_cnt == 0 {
                actionable_fragment_id.push_back(*fragment_id);
            } else {
                downstream_cnts.insert(*fragment_id, downstream_cnt);
            }
        }

        while let Some(fragment_id) = actionable_fragment_id.pop_front() {
            // Build the actors corresponding to the fragment
            self.build_actor_graph_fragment(fragment_id, state, fragment_graph)?;

            // Find if we can process more fragments
            for upstream_id in fragment_graph.get_upstreams(fragment_id).keys() {
                let downstream_cnt = downstream_cnts
                    .get_mut(upstream_id)
                    .expect("the upstream should exist");
                *downstream_cnt -= 1;
                if *downstream_cnt == 0 {
                    downstream_cnts.remove(upstream_id);
                    actionable_fragment_id.push_back(*upstream_id);
                }
            }
        }

        if !downstream_cnts.is_empty() {
            // There are fragments that are not processed yet.
            return Err(ErrorCode::InternalError("graph is not a DAG".into()).into());
        }

        Ok(())
    }

    fn build_actor_graph_fragment(
        &self,
        fragment_id: GlobalFragmentId,
        state: &mut BuildActorGraphState,
        fragment_graph: &StreamFragmentGraph,
    ) -> Result<()> {
        let current_fragment = fragment_graph.get_fragment(fragment_id).unwrap().clone();

        let parallel_degree = self
            .parallelisms
            .get(&fragment_id.as_global_id())
            .unwrap()
            .to_owned();

        let node = Arc::new(current_fragment.node.unwrap());
        let actor_ids = state
            .gen_actor_ids(parallel_degree)
            .into_iter()
            .map(LocalActorId::Local)
            .collect_vec();

        for id in &actor_ids {
            state
                .stream_graph_builder
                .add_actor(*id, fragment_id, node.clone());
        }

        for (downstream_fragment_id, dispatch_edge) in fragment_graph.get_downstreams(fragment_id) {
            let downstream_actors = state
                .fragment_actors
                .get(downstream_fragment_id)
                .expect("downstream fragment not processed yet");

            let dispatch_strategy = dispatch_edge.dispatch_strategy.as_ref().unwrap();
            match dispatch_strategy.get_type()? {
                DispatcherType::Hash
                | DispatcherType::Simple
                | DispatcherType::Broadcast
                | DispatcherType::NoShuffle => {
                    state.stream_graph_builder.add_link(
                        fragment_id,
                        &actor_ids,
                        downstream_actors,
                        dispatch_edge.link_id,
                        dispatch_strategy.clone(),
                        dispatch_edge.same_worker_node,
                    );
                }
                DispatcherType::Unspecified => unreachable!(),
            }
        }

        let ret = state
            .fragment_actors
            .insert(fragment_id, actor_ids.to_vec());
        assert!(
            ret.is_none(),
            "fragment {:?} already processed",
            fragment_id
        );

        Ok(())
    }
}

#[derive(Default)]
struct StreamFragmentGraph {
    /// stores all the fragments in the graph.
    fragments: HashMap<GlobalFragmentId, StreamFragment>,

    /// stores edges between fragments: upstream => downstream.
    downstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,

    /// stores edges between fragments: downstream -> upstream.
    upstreams: HashMap<GlobalFragmentId, HashMap<GlobalFragmentId, StreamFragmentEdge>>,
}

impl StreamFragmentGraph {
    /// Will convert all local ids to global ids by `local_id + offset`
    pub fn from_protobuf(mut proto: StreamFragmentGraphProto, offset: u32) -> Self {
        let mut graph = Self::default();

        let len = proto.fragments.len() as u32;

        graph.fragments = std::mem::take(&mut proto.fragments)
            .into_iter()
            .map(|(id, fragment)| {
                let id = GlobalFragmentId::from_local_id(id, offset, len);
                (
                    id,
                    StreamFragment {
                        fragment_id: id.as_global_id(),
                        ..fragment
                    },
                )
            })
            .collect();

        for edge in proto.edges {
            let upstream_id = GlobalFragmentId::from_local_id(edge.upstream_id, offset, len);
            let downstream_id = GlobalFragmentId::from_local_id(edge.downstream_id, offset, len);
            let res = graph.upstreams.entry(downstream_id).or_default().insert(
                upstream_id,
                StreamFragmentEdge {
                    upstream_id: upstream_id.as_global_id(),
                    downstream_id: downstream_id.as_global_id(),
                    ..edge.clone()
                },
            );
            assert!(res.is_none());
            let res = graph.downstreams.entry(upstream_id).or_default().insert(
                downstream_id,
                StreamFragmentEdge {
                    upstream_id: upstream_id.as_global_id(),
                    downstream_id: downstream_id.as_global_id(),
                    ..edge
                },
            );
            assert!(res.is_none());
        }

        graph
    }

    pub fn fragments(&self) -> &HashMap<GlobalFragmentId, StreamFragment> {
        &self.fragments
    }

    pub fn get_fragment(&self, fragment_id: GlobalFragmentId) -> Option<&StreamFragment> {
        self.fragments.get(&fragment_id)
    }

    pub fn get_downstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        lazy_static::lazy_static! {
            static ref EMPTY_HASHMAP: HashMap<GlobalFragmentId, StreamFragmentEdge> = HashMap::new();
        }
        self.downstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }

    pub fn get_upstreams(
        &self,
        fragment_id: GlobalFragmentId,
    ) -> &HashMap<GlobalFragmentId, StreamFragmentEdge> {
        lazy_static::lazy_static! {
            static ref EMPTY_HASHMAP: HashMap<GlobalFragmentId, StreamFragmentEdge> = HashMap::new();
        }
        self.upstreams.get(&fragment_id).unwrap_or(&EMPTY_HASHMAP)
    }
}
