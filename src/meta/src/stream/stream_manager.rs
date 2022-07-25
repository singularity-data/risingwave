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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use async_recursion::async_recursion;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_pb::catalog::{Source, Table};
use risingwave_pb::common::{ActorInfo, ParallelUnitMapping, WorkerNode, WorkerType};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ActorMapping, AddMutation, Dispatcher, DispatcherType, StreamActor, StreamNode, UpdateMutation};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::update_mutation::DispatcherUpdate;

use super::ScheduledLocations;
use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::{ClusterManagerRef, WorkerId};
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::manager::{DatabaseId, HashMappingManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv, SchemaId};
use crate::model::{ActorId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{fetch_source_fragments, FragmentManagerRef, Scheduler, SourceManagerRef};

pub type GlobalStreamManagerRef<S> = Arc<GlobalStreamManager<S>>;

/// [`CreateMaterializedViewContext`] carries one-time infos.
#[derive(Default)]
pub struct CreateMaterializedViewContext {
    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
    /// Upstream mview actor ids grouped by node id.
    pub upstream_node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    /// Upstream mview actor ids grouped by table id.
    pub table_sink_map: HashMap<TableId, Vec<ActorId>>,
    /// Dependent table ids
    pub dependent_table_ids: HashSet<TableId>,
    /// Temporary source info used during `create_materialized_source`
    pub affiliated_source: Option<Source>,
    /// Table id offset get from meta id generator. Used to calculate global unique table id.
    pub table_id_offset: u32,
    /// Internal TableID to Table mapping
    pub internal_table_id_map: HashMap<u32, Option<Table>>,
    /// SchemaId of mview
    pub schema_id: SchemaId,
    /// DatabaseId of mview
    pub database_id: DatabaseId,
    /// Name of mview, for internal table name generation.
    pub mview_name: String,
    pub table_properties: HashMap<String, String>,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager<S: MetaStore> {
    /// Manages definition and status of fragments and actors
    fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    barrier_manager: BarrierManagerRef<S>,

    /// Maintains information of the cluster
    cluster_manager: ClusterManagerRef<S>,

    /// Maintains streaming sources from external system like kafka
    source_manager: SourceManagerRef<S>,

    /// Maintains vnode mapping of all fragments and state tables.
    _hash_mapping_manager: HashMappingManagerRef,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Client Pool to stream service on compute nodes
    client_pool: StreamClientPoolRef,

    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef<S>,

    compaction_group_manager: CompactionGroupManagerRef<S>,
}

impl<S> GlobalStreamManager<S>
    where
        S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            scheduler: Scheduler::new(cluster_manager.clone(), env.hash_mapping_manager_ref()),
            fragment_manager,
            barrier_manager,
            cluster_manager,
            source_manager,
            _hash_mapping_manager: env.hash_mapping_manager_ref(),
            client_pool: env.stream_client_pool_ref(),
            compaction_group_manager,
            id_gen_manager: env.id_gen_manager_ref(),
        })
    }

    async fn resolve_chain_node(
        &self,
        table_fragments: &mut TableFragments,
        dependent_table_ids: &HashSet<TableId>,
        dispatchers: &mut HashMap<ActorId, Vec<Dispatcher>>,
        upstream_node_actors: &mut HashMap<WorkerId, HashSet<ActorId>>,
        locations: &ScheduledLocations,
    ) -> Result<()> {
        // The closure environment. Used to simulate recursive closure.
        struct Env<'a> {
            /// Records what's the correspoding actor of each parallel unit of one table.
            upstream_parallel_unit_info: &'a HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>>,
            /// Records what's the actors on each worker of one table.
            tables_node_actors: &'a HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
            /// Schedule information of all actors.
            locations: &'a ScheduledLocations,
            /// New dispatchers for this mview.
            dispatchers: &'a mut HashMap<ActorId, Vec<Dispatcher>>,
            /// Upstream Materialize actor ids grouped by worker id.
            upstream_node_actors: &'a mut HashMap<WorkerId, HashSet<ActorId>>,
        }

        impl Env<'_> {
            fn resolve_chain_node_inner(
                &mut self,
                stream_node: &mut StreamNode,
                actor_id: ActorId,
                same_worker_node_as_upstream: bool,
            ) -> Result<()> {
                let Some(NodeBody::Chain(ref mut chain)) = stream_node.node_body else {
                    // If node is not chain node, recursively deal with input nodes
                    for input in &mut stream_node.input {
                        self.resolve_chain_node_inner(input, actor_id, same_worker_node_as_upstream)?;
                    }
                    return Ok(());
                };
                // If node is chain node, we insert upstream ids into chain's input (merge)

                // get upstream table id
                let table_id = TableId::new(chain.table_id);

                let upstream_actor_id = {
                    // 1. use table id to get upstream parallel_unit -> actor_id mapping
                    let upstream_parallel_actor_mapping =
                        &self.upstream_parallel_unit_info[&table_id];
                    // 2. use our actor id to get parallel unit id of the chain actor
                    let parallel_unit_id = self.locations.actor_locations[&actor_id].id;
                    // 3. and use chain actor's parallel unit id to get the corresponding upstream
                    // actor id
                    upstream_parallel_actor_mapping[&parallel_unit_id]
                };

                // The current implementation already ensures chain and upstream are on the same
                // worker node. So we do a sanity check here, in case that the logic get changed but
                // `same_worker_node` constraint is not satisfied.
                if same_worker_node_as_upstream {
                    // Parallel unit id is a globally unique id across all worker nodes. It can be
                    // seen as something like CPU core id. Therefore, we verify that actor's unit id
                    // == upstream's unit id.

                    let actor_parallel_unit_id =
                        self.locations.actor_locations.get(&actor_id).unwrap().id;

                    assert_eq!(
                        *self
                            .upstream_parallel_unit_info
                            .get(&table_id)
                            .unwrap()
                            .get(&actor_parallel_unit_id)
                            .unwrap(),
                        upstream_actor_id
                    );
                }

                // fill upstream node-actor info for later use
                let upstream_table_node_actors = self.tables_node_actors.get(&table_id).unwrap();

                let chain_upstream_node_actors = upstream_table_node_actors
                    .iter()
                    .flat_map(|(node_id, actor_ids)| {
                        actor_ids.iter().map(|actor_id| (*node_id, *actor_id))
                    })
                    .filter(|(_, actor_id)| upstream_actor_id == *actor_id)
                    .into_group_map();
                for (node_id, actor_ids) in chain_upstream_node_actors {
                    self.upstream_node_actors
                        .entry(node_id)
                        .or_default()
                        .extend(actor_ids.iter());
                }

                // deal with merge and batch query node, setting upstream infos.
                let batch_stream_node = &stream_node.input[1];
                assert!(
                    matches!(batch_stream_node.node_body, Some(NodeBody::BatchPlan(_))),
                    "chain's input[1] should always be batch query"
                );

                let merge_stream_node = &mut stream_node.input[0];
                let Some(NodeBody::Merge(ref mut merge)) = merge_stream_node.node_body else {
                    unreachable!("chain's input[0] should always be merge");
                };
                merge.upstream_actor_id.push(upstream_actor_id);

                // finally, we should also build dispatcher infos here.
                //
                // Note: currently we ensure that the downstream chain operator has the same
                // parallel unit and distribution as the upstream mview, so we can simply use
                // `NoShuffle` dispatcher here.
                // TODO: support different parallel unit and distribution for new MV.
                self.dispatchers
                    .entry(upstream_actor_id)
                    .or_default()
                    .push(Dispatcher {
                        r#type: DispatcherType::NoShuffle as _,
                        // Use chain actor id as dispatcher id to avoid collision in this
                        // Dispatch executor.
                        dispatcher_id: actor_id as _,
                        downstream_actor_id: vec![actor_id],
                        ..Default::default()
                    });

                Ok(())
            }
        }

        let upstream_parallel_unit_info = &self
            .fragment_manager
            .get_sink_parallel_unit_ids(dependent_table_ids)
            .await?;

        let tables_node_actors = &self
            .fragment_manager
            .get_tables_node_actors(dependent_table_ids)
            .await?;

        let mut env = Env {
            upstream_parallel_unit_info,
            tables_node_actors,
            locations,
            dispatchers,
            upstream_node_actors,
        };

        for fragment in table_fragments.fragments.values_mut() {
            for actor in &mut fragment.actors {
                if let Some(ref mut stream_node) = actor.nodes {
                    env.resolve_chain_node_inner(
                        stream_node,
                        actor.actor_id,
                        actor.same_worker_node_as_upstream,
                    )?;
                }
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn resolve_migrate_dependent_actors(&self,
                                              table_ids: HashSet<TableId>,
                                              actor_map: &mut HashMap<ActorId, StreamActor>,
                                              actor_id_to_worker_id: &mut HashMap<ActorId, WorkerId>,
                                              actors: &mut HashMap<TableId, HashMap<ActorId, WorkerId>>,
                                              chain_actor_ids: &mut HashSet<ActorId>,
                                              cache: &mut HashSet<TableId>) -> Result<()> {
        for table_id in table_ids {
            if cache.contains(&table_id) {
                continue;
            }

            let table_fragments = self.fragment_manager.select_table_fragments_by_table_id(&table_id).await?;
            actor_id_to_worker_id.extend(table_fragments.actor_to_node());

            let table_actor_map = table_fragments.actor_map();

            for (actor_id, actor) in table_actor_map {
                actor_map.insert(actor_id, actor.clone());
            }

            let table_chain_actor_ids = table_fragments.chain_actor_ids();
            if !table_chain_actor_ids.is_empty() {
                chain_actor_ids.extend(table_chain_actor_ids);
                let dependent_table_ids = table_fragments.dependent_table_ids();
                self.resolve_migrate_dependent_actors(dependent_table_ids, actor_map, actor_id_to_worker_id, actors, chain_actor_ids, cache).await?;
            }

            cache.insert(table_id);
        }

        Ok(())
    }

    pub async fn migrate_actors(
        &self,
        actors: HashMap<TableId, HashMap<ActorId, WorkerId>>,
    ) -> Result<HashMap<ActorId, ActorId>> {
        let worker_nodes: HashMap<WorkerId, WorkerNode> = self
            .cluster_manager
            .list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            )
            .await
            .into_iter()
            .map(|worker_node| (worker_node.id, worker_node))
            .collect();

        if worker_nodes.is_empty() {
            bail!("no available compute node in the cluster");
        }

        let mut actors = actors;

        let mut chain_actor_ids = HashSet::new();
        let mut actor_id_to_worker_id = HashMap::new();
        let mut actor_map = HashMap::new();
        let mut _cache = HashSet::new();
        self.resolve_migrate_dependent_actors(actors.keys().cloned().collect(), &mut actor_map, &mut actor_id_to_worker_id, &mut actors, &mut chain_actor_ids, &mut _cache).await?;

        let mut actor_id_to_target_id = HashMap::new();
        let mut actor_id_to_table_id = HashMap::new();
        for (table_id, map) in &actors {
            for actor_id in map.keys() {
                if !actor_map.contains_key(actor_id) {
                    bail!("actor {} not found", actor_id);
                }

                actor_id_to_table_id.insert(*actor_id, *table_id);
            }

            for (&actor_id, &worker_id) in map {
                if !worker_nodes.contains_key(&worker_id) {
                    //bail!("worker {} not found", worker_id);
                }

                actor_id_to_target_id.insert(actor_id, worker_id);
            }
        }

        let mut downstream_actors = HashMap::new();
        let mut upstream_actors = HashMap::new();
        for (actor_id, stream_actor) in &actor_map {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    downstream_actors
                        .entry(*actor_id as ActorId)
                        .or_insert(vec![])
                        .push(*downstream_actor_id as ActorId);
                    upstream_actors
                        .entry(*downstream_actor_id as ActorId)
                        .or_insert(vec![])
                        .push((*actor_id, dispatcher.dispatcher_id));
                }
            }
        }

        let actor_ids: BTreeSet<ActorId> = actors.values().flat_map(|value| value.keys().into_iter().cloned()).collect();

        let mut old_actor_id_to_new_actor_id = HashMap::new();
        let mut new_actor_id_to_old_actor_id = HashMap::new();
        let mut new_actor_map = HashMap::new();

        for actor_id in &actor_ids {
            let id = self.id_gen_manager.generate::<{ IdCategory::Actor }>().await? as ActorId;
            old_actor_id_to_new_actor_id.insert(*actor_id, id);
            new_actor_id_to_old_actor_id.insert(id, *actor_id);

            let old_actor = actor_map.get(actor_id).unwrap();
            let mut new_actor = old_actor.clone();

            if chain_actor_ids.contains(actor_id) {
                let upstream_actor_ids = upstream_actors.get(actor_id).unwrap();
                assert_eq!(upstream_actor_ids.len(), 1);
                let (upstream_actor_id, _) = upstream_actor_ids.iter().next().unwrap();
                if actor_ids.contains(upstream_actor_id) {
                    new_actor.same_worker_node_as_upstream = false;
                }
            }

            for upstream_actor_id in &mut new_actor.upstream_actor_id {
                if let Some(new_actor_id) = old_actor_id_to_new_actor_id.get(upstream_actor_id) {
                    *upstream_actor_id = *new_actor_id as u32;
                }
            }

            for dispatcher in &mut new_actor.dispatcher {
                for downstream_actor_id in &mut dispatcher.downstream_actor_id {
                    if let Some(new_actor_id) = old_actor_id_to_new_actor_id.get(downstream_actor_id) {
                        *downstream_actor_id = *new_actor_id as u32;
                    }
                }
            }

            new_actor.actor_id = id;

            new_actor_map.insert(*actor_id as ActorId, new_actor);
        }

        let mut node_hanging_channels: HashMap<WorkerId, Vec<HangingChannel>> = HashMap::new();

        for actor_id in &actor_ids {
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                let worker_id = actor_id_to_target_id.get(actor_id).unwrap();
                let worker = worker_nodes.get(worker_id).unwrap();

                for (upstream_actor_id, _upstream_dispatcher_id) in upstream_actor_ids {
                    if actor_ids.contains(upstream_actor_id) {
                        continue;
                    }

                    let new_actor_id = old_actor_id_to_new_actor_id.get(actor_id).unwrap();

                    // note: must exists
                    let upstream_worker_id = actor_id_to_worker_id.get(upstream_actor_id).unwrap();

                    // note: we will create local channel later
                    // if worker_id == upstream_worker_id {
                    //     continue;
                    // }

                    node_hanging_channels.entry(*upstream_worker_id).or_default()
                        .push(HangingChannel {
                            upstream: Some(ActorInfo {
                                actor_id: *upstream_actor_id,
                                host: None,
                            }),
                            downstream: Some(ActorInfo {
                                actor_id: *new_actor_id,
                                host: worker.host.clone(),
                            }),
                        })
                }
            }
        }

        let mut actor_infos_to_broadcast = vec![];
        let mut node_actors: HashMap<WorkerId, Vec<_>> = HashMap::new();
        for (actor_id, &worker_id) in &actor_id_to_target_id {
            let new_actor = new_actor_map.get(actor_id).unwrap();
            node_actors.entry(worker_id).or_default().push(new_actor.clone());

            let worker = worker_nodes.get(&worker_id).unwrap();
            actor_infos_to_broadcast.push(ActorInfo {
                actor_id: new_actor.actor_id,
                host: worker.host.clone(),
            })
        }


        let mut broadcast_node_ids = HashSet::new();
        for actor_id in &actor_ids {
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                for (upstream_actor_id, _) in upstream_actor_ids {
                    let node_id = actor_id_to_worker_id.get(upstream_actor_id).unwrap();
                    broadcast_node_ids.insert(node_id);
                }
            }
        }

        for node_id in broadcast_node_ids {
            let node = worker_nodes.get(node_id).unwrap();
            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await?;
        }

        for (node_id, stream_actors) in &node_actors {
            let node = worker_nodes.get(node_id).unwrap();
            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            let request = UpdateActorsRequest {
                request_id,
                actors: stream_actors.clone(),
                hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
            };

            client
                .to_owned()
                .update_actors(request)
                .await?;
        }


        // Build remaining hanging channels on compute nodes.
        for (node_id, hanging_channels) in node_hanging_channels {
            let node = worker_nodes.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await?;
        }

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, stream_actors) in node_actors {
            let node = worker_nodes.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: stream_actors
                        .iter()
                        .map(|stream_actor| stream_actor.actor_id)
                        .collect(),
                })
                .await?;
        }

        let mut actor_dispatcher_update = HashMap::new();
        for actor_id in &actor_ids {
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                for (upstream_actor_id, upstream_dispatcher_id) in upstream_actor_ids {
                    if actor_ids.contains(upstream_actor_id) {
                        continue;
                    }

                    let new_actor_id = old_actor_id_to_new_actor_id.get(actor_id).unwrap();

                    let upstream_actor = actor_map.get(upstream_actor_id).unwrap();

                    let dispatcher = upstream_actor.dispatcher.iter().find(|&dispatcher| {
                        dispatcher.dispatcher_id == *upstream_dispatcher_id
                    }).unwrap();

                    let mut new_hash_mapping = dispatcher.hash_mapping.clone();

                    if dispatcher.get_type().unwrap() == DispatcherType::Hash {
                        if let Some(actor_mapping) = new_hash_mapping.as_mut() {
                            for mapping_actor_id in &mut actor_mapping.data {
                                if let Some(new_actor_id) = old_actor_id_to_new_actor_id.get(mapping_actor_id) {
                                    *mapping_actor_id = *new_actor_id;
                                }
                            }
                        }
                    }

                    let dispatcher_update = actor_dispatcher_update.entry(*upstream_actor_id).or_insert(DispatcherUpdate {
                        dispatcher_id: dispatcher.dispatcher_id,
                        hash_mapping: new_hash_mapping,
                        added_downstream_actor_id: vec![],
                        removed_downstream_actor_id: vec![],
                    });

                    dispatcher_update.added_downstream_actor_id.push(*new_actor_id);
                    dispatcher_update.removed_downstream_actor_id.push(*actor_id);
                }
            }
        }

        for (actor_id, x) in &actor_dispatcher_update {
            println!("actor_id {}", actor_id);
            println!("\tdispatcher {}", x.dispatcher_id);
            println!("\tmapping {:?}", x.hash_mapping);
            println!("\tadd {:?}", x.added_downstream_actor_id);
            println!("\trem {:?}", x.removed_downstream_actor_id);
        }

        self.barrier_manager.run_command(Command::Plain(Some(Mutation::Update(UpdateMutation {
            actor_dispatcher_update,
            actor_merge_update: Default::default(),
            dropped_actor_id: vec![]
        })))).await?;

        Ok(old_actor_id_to_new_actor_id)
    }

    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// (optional) get the split information of the `StreamSource` via source manager and patch
    /// actors .
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    ///
    /// Note the `table_fragments` is required to be sorted in topology order. (Downstream first,
    /// then upstream.)
    pub async fn create_materialized_view(
        &self,
        mut table_fragments: TableFragments,
        CreateMaterializedViewContext {
            dispatchers,
            upstream_node_actors,
            table_sink_map,
            dependent_table_ids,
            table_properties,
            ..
        }: &mut CreateMaterializedViewContext,
    ) -> Result<()> {
        // This scope guard does clean up jobs ASYNCHRONOUSLY before Err returns.
        // It MUST be cleared before Ok returns.
        let mut revert_funcs = scopeguard::guard(
            vec![],
            |revert_funcs: Vec<futures::future::BoxFuture<()>>| {
                tokio::spawn(async move {
                    for revert_func in revert_funcs.into_iter().rev() {
                        revert_func.await;
                    }
                });
            },
        );

        // Schedule actors to parallel units. `locations` will record the parallel unit that an
        // actor is scheduled to, and the worker node this parallel unit is on.
        let locations = {
            // List all running worker nodes.
            let workers = self
                .cluster_manager
                .list_worker_node(
                    WorkerType::ComputeNode,
                    Some(risingwave_pb::common::worker_node::State::Running),
                )
                .await;
            if workers.is_empty() {
                bail!("no available compute node in the cluster");
            }

            // Create empty locations.
            let mut locations = ScheduledLocations::with_workers(workers);

            // Schedule each fragment(actors) to nodes, recorded in `locations`.
            // Vnode mapping in fragment will be filled in as well.
            let topological_order = table_fragments.generate_topological_order();
            for fragment_id in topological_order {
                let fragment = table_fragments.fragments.get_mut(&fragment_id).unwrap();
                self.scheduler.schedule(fragment, &mut locations).await?;
            }

            locations
        };

        // Resolve chain node infos, including:
        // 1. insert upstream actor id in merge node
        // 2. insert parallel unit id in batch query node
        self.resolve_chain_node(
            &mut table_fragments,
            dependent_table_ids,
            dispatchers,
            upstream_node_actors,
            &locations,
        )
            .await?;

        let dispatchers = &*dispatchers;

        // println!("dispatchers {:?}", dispatchers);
        //
        // for (a, b) in dispatchers {
        //     println!("actor {}", a);
        //     for dispatcher in b {
        //         println!("\tdispatcher {:?}", dispatcher);
        //     }
        // }

        // Record vnode to parallel unit mapping for actors.
        let actor_to_vnode_mapping = {
            let mut mapping = HashMap::new();
            for fragment in table_fragments.fragments.values() {
                for actor in &fragment.actors {
                    mapping
                        .try_insert(actor.actor_id, fragment.vnode_mapping.clone())
                        .unwrap();
                }
            }
            mapping
        };

        // Fill hash dispatcher's mapping with scheduled locations.
        for fragment in table_fragments.fragments.values_mut() {
            // Filter out hash dispatchers in this fragment.
            let dispatchers = fragment
                .actors
                .iter_mut()
                .flat_map(|actor| actor.dispatcher.iter_mut())
                .filter(|d| d.get_type().unwrap() == DispatcherType::Hash);

            for dispatcher in dispatchers {
                match dispatcher.downstream_actor_id.as_slice() {
                    [] => panic!("hash dispatcher should have at least one downstream actor"),

                    // There exists some unoptimized situation where a hash dispatcher has ONLY ONE
                    // downstream actor, which makes it behave like a simple dispatcher. As a
                    // workaround, we specially compute the consistent hash mapping here.
                    // This arm could be removed after the optimizer has been fully implemented.
                    &[single_downstream_actor] => {
                        dispatcher.hash_mapping = Some(ActorMapping {
                            original_indices: vec![VIRTUAL_NODE_COUNT as u64 - 1],
                            data: vec![single_downstream_actor],
                        });
                    }

                    // For normal cases, we can simply transform the mapping from downstream actors
                    // to current hash dispatchers.
                    downstream_actors @ &[first_downstream_actor, ..] => {
                        // All actors in the downstream fragment should have the same parallel unit
                        // mapping, find it with the first downstream actor.
                        let downstream_vnode_mapping = actor_to_vnode_mapping
                            .get(&first_downstream_actor)
                            .unwrap()
                            .as_ref()
                            .unwrap_or_else(|| {
                                panic!("no vnode mapping for actor {}", &first_downstream_actor);
                            });

                        // Mapping from the parallel unit to downstream actors.
                        let parallel_unit_actor_map = downstream_actors
                            .iter()
                            .map(|actor_id| {
                                (
                                    locations.actor_locations.get(actor_id).unwrap().id,
                                    *actor_id,
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        // Trasform the mapping of parallel unit to the mapping of actor.
                        let ParallelUnitMapping {
                            original_indices,
                            data,
                            ..
                        } = downstream_vnode_mapping;
                        let data = data
                            .iter()
                            .map(|parallel_unit_id| parallel_unit_actor_map[parallel_unit_id])
                            .collect_vec();
                        dispatcher.hash_mapping = Some(ActorMapping {
                            original_indices: original_indices.clone(),
                            data,
                        });
                    }
                }
            }
        }

        // Mark the actors to be built as `State::Building`.
        let actor_info = locations
            .actor_locations
            .iter()
            .map(|(&actor_id, parallel_unit)| {
                (
                    actor_id,
                    ActorStatus {
                        parallel_unit: Some(parallel_unit.clone()),
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();
        table_fragments.set_actor_status(actor_info);
        let actor_map = table_fragments.actor_map();

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains:
        // 1. actors in the current create-materialized-view request.
        // 2. all upstream actors.
        let actor_infos_to_broadcast = {
            let current = locations.actor_infos();
            let upstream = upstream_node_actors
                .iter()
                .flat_map(|(node_id, upstreams)| {
                    upstreams.iter().map(|up_id| ActorInfo {
                        actor_id: *up_id,
                        host: locations
                            .worker_locations
                            .get(node_id)
                            .unwrap()
                            .host
                            .clone(),
                    })
                });
            current.chain(upstream).collect_vec()
        };

        let actor_host_infos = locations.actor_info_map();
        let node_actors = locations.worker_actors();

        // Hanging channels for each worker node.
        let mut node_hanging_channels = {
            // upstream_actor_id -> Vec<downstream_actor_info>
            let up_id_to_down_info = dispatchers
                .iter()
                .map(|(&up_id, dispatchers)| {
                    let down_infos = dispatchers
                        .iter()
                        .flat_map(|d| d.downstream_actor_id.iter())
                        .map(|down_id| actor_host_infos[down_id].clone())
                        .collect_vec();
                    (up_id, down_infos)
                })
                .collect::<HashMap<_, _>>();

            upstream_node_actors
                .iter()
                .map(|(node_id, up_ids)| {
                    (
                        *node_id,
                        up_ids
                            .iter()
                            .flat_map(|up_id| {
                                up_id_to_down_info[up_id]
                                    .iter()
                                    .map(|down_info| HangingChannel {
                                        upstream: Some(ActorInfo {
                                            actor_id: *up_id,
                                            host: None,
                                        }),
                                        downstream: Some(down_info.clone()),
                                    })
                            })
                            .collect_vec(),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        
        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (node_id, actors) in &node_actors {
            let node = locations.worker_locations.get(node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await?;

            let stream_actors = actors
                .iter()
                .map(|actor_id| actor_map.get(actor_id).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                    hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
                })
                .await?;
        }

        // Build remaining hanging channels on compute nodes.
        for (node_id, hanging_channels) in node_hanging_channels {
            let node = locations.worker_locations.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await?;
        }

        // Register to compaction group beforehand.
        let registered_table_ids = self
            .compaction_group_manager
            .register_table_fragments(&table_fragments, table_properties)
            .await?;
        let compaction_group_manager_ref = self.compaction_group_manager.clone();
        revert_funcs.push(Box::pin(async move {
            if let Err(e) = compaction_group_manager_ref.unregister_table_ids(&registered_table_ids).await {
                tracing::warn!("Failed to unregister_table_ids {:#?}.\nThey will be cleaned up on node restart.\n{:#?}", registered_table_ids, e);
            }
        }));

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, actors) in node_actors {
            let node = locations.worker_locations.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await?;
        }

        // Extract the fragments that include source operators.
        let source_fragments = {
            let mut source_fragments = HashMap::new();
            fetch_source_fragments(&mut source_fragments, &table_fragments);
            source_fragments
        };

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;

        let table_id = table_fragments.table_id();
        let init_split_assignment = self
            .source_manager
            .pre_allocate_splits(&table_id, source_fragments.clone())
            .await?;

        if let Err(err) = self
            .barrier_manager
            .run_command(Command::CreateMaterializedView {
                table_fragments,
                table_sink_map: table_sink_map.clone(),
                dispatchers: dispatchers.clone(),
                source_state: init_split_assignment.clone(),
            })
            .await
        {
            self.fragment_manager
                .cancel_create_table_fragments(&table_id)
                .await?;
            return Err(err);
        }

        self.source_manager
            .patch_update(Some(source_fragments), Some(init_split_assignment))
            .await?;

        revert_funcs.clear();
        Ok(())
    }

    /// Dropping materialized view is done by barrier manager. Check
    /// [`Command::DropMaterializedView`] for details.
    pub async fn drop_materialized_view(&self, table_id: &TableId) -> Result<()> {
        let table_fragments = self
            .fragment_manager
            .select_table_fragments_by_table_id(table_id)
            .await?;

        // Extract the fragments that include source operators.
        let source_fragments = {
            let mut source_fragments = HashMap::new();
            fetch_source_fragments(&mut source_fragments, &table_fragments);
            source_fragments
        };

        self.barrier_manager
            .run_command(Command::DropMaterializedView(*table_id))
            .await?;

        let mut actor_ids = HashSet::new();
        for fragment_ids in source_fragments.values() {
            for fragment_id in fragment_ids {
                if let Some(fragment) = table_fragments.fragments.get(fragment_id) {
                    for actor in &fragment.actors {
                        actor_ids.insert(actor.actor_id);
                    }
                }
            }
        }
        self.source_manager
            .drop_update(Some(source_fragments), Some(actor_ids))
            .await?;

        // Unregister from compaction group afterwards.
        if let Err(e) = self
            .compaction_group_manager
            .unregister_table_fragments(&table_fragments)
            .await
        {
            tracing::warn!(
                "Failed to unregister table {}. It wll be unregistered eventually.\n{:#?}",
                table_id,
                e
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_common::error::tonic_err;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse, *,
    };
    use tokio::sync::oneshot::Sender;
    #[cfg(feature = "failpoints")]
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::cluster::ClusterManager;
    use crate::hummock::compaction_group::manager::CompactionGroupManager;
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::manager::{CatalogManager, MetaSrvEnv};
    use crate::model::ActorId;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;
    use crate::stream::{FragmentManager, SourceManager};
    use crate::MetaOpts;

    struct FakeFragmentState {
        actor_streams: Mutex<HashMap<ActorId, StreamActor>>,
        actor_ids: Mutex<HashSet<ActorId>>,
        actor_infos: Mutex<HashMap<ActorId, HostAddress>>,
    }

    struct FakeStreamService {
        inner: Arc<FakeFragmentState>,
    }

    #[async_trait::async_trait]
    impl StreamService for FakeStreamService {
        async fn update_actors(
            &self,
            request: Request<UpdateActorsRequest>,
        ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_streams.lock().unwrap();
            for actor in req.get_actors() {
                guard.insert(actor.get_actor_id(), actor.clone());
            }

            Ok(Response::new(UpdateActorsResponse { status: None }))
        }

        async fn build_actors(
            &self,
            request: Request<BuildActorsRequest>,
        ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_ids.lock().unwrap();
            for id in req.get_actor_id() {
                guard.insert(*id);
            }

            Ok(Response::new(BuildActorsResponse {
                request_id: "".to_string(),
                status: None,
            }))
        }

        async fn broadcast_actor_info_table(
            &self,
            request: Request<BroadcastActorInfoTableRequest>,
        ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_infos.lock().unwrap();
            for info in req.get_info() {
                guard.insert(
                    info.get_actor_id(),
                    info.get_host().map_err(tonic_err)?.clone(),
                );
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_actors(
            &self,
            _request: Request<DropActorsRequest>,
        ) -> std::result::Result<Response<DropActorsResponse>, Status> {
            Ok(Response::new(DropActorsResponse::default()))
        }

        async fn force_stop_actors(
            &self,
            _request: Request<ForceStopActorsRequest>,
        ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
            Ok(Response::new(ForceStopActorsResponse::default()))
        }

        async fn inject_barrier(
            &self,
            _request: Request<InjectBarrierRequest>,
        ) -> std::result::Result<Response<InjectBarrierResponse>, Status> {
            Ok(Response::new(InjectBarrierResponse::default()))
        }

        async fn create_source(
            &self,
            _request: Request<CreateSourceRequest>,
        ) -> std::result::Result<Response<CreateSourceResponse>, Status> {
            unimplemented!()
        }

        async fn sync_sources(
            &self,
            _request: Request<SyncSourcesRequest>,
        ) -> std::result::Result<Response<SyncSourcesResponse>, Status> {
            Ok(Response::new(SyncSourcesResponse::default()))
        }

        async fn drop_source(
            &self,
            _request: Request<DropSourceRequest>,
        ) -> std::result::Result<Response<DropSourceResponse>, Status> {
            unimplemented!()
        }

        async fn barrier_complete(
            &self,
            _request: Request<BarrierCompleteRequest>,
        ) -> std::result::Result<Response<BarrierCompleteResponse>, Status> {
            Ok(Response::new(BarrierCompleteResponse::default()))
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManager<MemStore>,
        fragment_manager: FragmentManagerRef<MemStore>,
        state: Arc<FakeFragmentState>,
        join_handles: Vec<JoinHandle<()>>,
        shutdown_txs: Vec<Sender<()>>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16) -> Result<Self> {
            let addr = SocketAddr::new(host.parse().unwrap(), port);
            let state = Arc::new(FakeFragmentState {
                actor_streams: Mutex::new(HashMap::new()),
                actor_ids: Mutex::new(HashSet::new()),
                actor_infos: Mutex::new(HashMap::new()),
            });

            let fake_service = FakeStreamService {
                inner: state.clone(),
            };

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let stream_srv = StreamServiceServer::new(fake_service);
            let join_handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(stream_srv)
                    .serve_with_shutdown(addr, async move { shutdown_rx.await.unwrap() })
                    .await
                    .unwrap();
            });

            sleep(Duration::from_secs(1));

            let env = MetaSrvEnv::for_test_opts(Arc::new(MetaOpts::test(true, false))).await;
            let cluster_manager =
                Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            let fake_parallelism = 4;
            cluster_manager
                .add_worker_node(WorkerType::ComputeNode, host.clone(), fake_parallelism)
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);
            let meta_metrics = Arc::new(MetaMetrics::new());
            let compaction_group_manager =
                Arc::new(CompactionGroupManager::new(env.clone()).await.unwrap());
            let compactor_manager = Arc::new(CompactorManager::new());

            let hummock_manager = Arc::new(
                HummockManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    meta_metrics.clone(),
                    compaction_group_manager.clone(),
                    compactor_manager.clone(),
                )
                    .await?,
            );

            let barrier_manager = Arc::new(GlobalBarrierManager::new(
                env.clone(),
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
                hummock_manager,
                meta_metrics.clone(),
            ));

            let compaction_group_manager =
                Arc::new(CompactionGroupManager::new(env.clone()).await?);

            let source_manager = Arc::new(
                SourceManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    barrier_manager.clone(),
                    catalog_manager.clone(),
                    fragment_manager.clone(),
                    compaction_group_manager.clone(),
                )
                    .await?,
            );

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_manager.clone(),
                cluster_manager.clone(),
                source_manager.clone(),
                compaction_group_manager.clone(),
            )?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager).await;

            Ok(Self {
                global_stream_manager: stream_manager,
                fragment_manager,
                state,
                join_handles: vec![join_handle_2, join_handle],
                shutdown_txs: vec![shutdown_tx_2, shutdown_tx],
            })
        }

        async fn stop(self) {
            for shutdown_tx in self.shutdown_txs {
                shutdown_tx.send(()).unwrap();
            }
            for join_handle in self.join_handles {
                join_handle.await.unwrap();
            }
        }
    }

    fn make_mview_stream_actors(table_id: &TableId, count: usize) -> Vec<StreamActor> {
        (0..count)
            .map(|i| StreamActor {
                actor_id: i as u32,
                // A dummy node to avoid panic.
                nodes: Some(StreamNode {
                    node_body: Some(NodeBody::Materialize(MaterializeNode {
                        table_id: table_id.table_id(),
                        ..Default::default()
                    })),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect_vec()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12333).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                vnode_mapping: None,
            },
        );
        let table_fragments = TableFragments::new(table_id, fragments, HashSet::default());

        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, &mut ctx)
            .await?;

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap()
                .clone();
            scheduled_actor.vnode_bitmap.take().unwrap();
            assert_eq!(scheduled_actor, actor);
            assert!(services
                .state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                services
                    .state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12333,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await?;
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());

        services.stop().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12334).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                vnode_mapping: None,
            },
        );
        let internal_table_id = HashSet::from([2, 3, 5, 7]);

        let table_fragments = TableFragments::new(table_id, fragments, internal_table_id.clone());

        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, &mut ctx)
            .await?;

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap();
            scheduled_actor.vnode_bitmap.take().unwrap();
            assert_eq!(scheduled_actor, actor);
            assert!(services
                .state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                services
                    .state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12334,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await?;
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());

        let table_fragments = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id)
            .await?;
        assert_eq!(4, table_fragments.internal_table_ids().len());

        // test drop materialized_view
        // the table_fragments will be deleted when barrier_manager run_command DropMaterializedView
        // via drop_table_fragments
        services
            .global_stream_manager
            .drop_materialized_view(&table_fragments.table_id())
            .await?;

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        // TODO: check memory and metastore consistent
        assert_eq!(
            select_err_1.to_string(),
            "internal error: table_fragment not exist: id=0"
        );

        services.stop().await;
        Ok(())
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints_drop_mv_recovery() {
        let inject_barrier_err = "inject_barrier_err";
        let inject_barrier_err_success = "inject_barrier_err_success";
        let services = MockServices::start("127.0.0.1", 12335).await.unwrap();

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                vnode_mapping: None,
            },
        );
        let internal_table_id = HashSet::from([2, 3, 5, 7]);

        let table_fragments = TableFragments::new(table_id, fragments, internal_table_id.clone());

        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, &mut ctx)
            .await
            .unwrap();

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap();
            scheduled_actor.vnode_bitmap.take().unwrap();
            assert_eq!(scheduled_actor, actor);
            assert!(services
                .state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                services
                    .state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12335,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await
            .unwrap();
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await
            .unwrap();
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());
        let notify = Arc::new(Notify::new());
        let notify1 = notify.clone();

        // test recovery.
        fail::cfg(inject_barrier_err, "return").unwrap();
        tokio::spawn(async move {
            fail::cfg_callback(inject_barrier_err_success, move || {
                fail::remove(inject_barrier_err);
                fail::remove(inject_barrier_err_success);
                notify.notify_one();
            })
                .unwrap();
        });
        notify1.notified().await;

        let table_fragments = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id)
            .await
            .unwrap();
        assert_eq!(4, table_fragments.internal_table_ids().len());

        // test drop materialized_view
        services
            .global_stream_manager
            .drop_materialized_view(&table_fragments.table_id())
            .await
            .unwrap();

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        // TODO: check memory and metastore consistent
        assert_eq!(
            select_err_1.to_string(),
            "internal error: table_fragment not exist: id=0"
        );

        services.stop().await;
    }

    fn make_mview_stream_actors_custom(
        start: usize,
        stop: usize,
        node_body: NodeBody,
        dispatchers: Vec<Dispatcher>,
        // upstream_actor_ids: Vec<ActorId>,
    ) -> Vec<StreamActor> {
        let mut actors = vec![];
        for i in start..stop {
            let mut new_dispatchers = vec![];

            for dispatcher in &dispatchers {
                let mut new_dispatcher = dispatcher.clone();
                new_dispatcher.dispatcher_id = dispatcher.dispatcher_id * 1000 + i as u64;
                new_dispatchers.push(new_dispatcher);
            }

            actors.push(StreamActor {
                actor_id: i as u32,
                nodes: Some(StreamNode {
                    node_body: Some(node_body.clone()),
                    ..Default::default()
                }),
                dispatcher: new_dispatchers,
                // upstream_actor_id: upstream_actor_ids,
                ..Default::default()
            })
        }

        actors
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_migrate_actors() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12333).await?;

        let table_id = TableId::new(0);

        let actor_groups = vec![
            make_mview_stream_actors_custom(
                100,
                102,
                NodeBody::Source(SourceNode {
                    table_id: table_id.table_id(),
                    ..Default::default()
                }),
                vec![Dispatcher {
                    dispatcher_id: 0,
                    downstream_actor_id: (102..105).collect_vec(),
                    ..Default::default()
                }],
            ),
            make_mview_stream_actors_custom(
                102,
                105,
                NodeBody::Filter(FilterNode {
                    ..Default::default()
                }),
                vec![Dispatcher {
                    dispatcher_id: 1,
                    downstream_actor_id: (105..109).collect_vec(),
                    ..Default::default()
                }],
            ),
            make_mview_stream_actors_custom(
                105,
                109,
                NodeBody::Filter(FilterNode {
                    ..Default::default()
                }),
                vec![Dispatcher {
                    dispatcher_id: 2,
                    downstream_actor_id: (109..111).collect_vec(),
                    ..Default::default()
                }],
            ),
            make_mview_stream_actors_custom(
                109,
                111,
                NodeBody::Filter(FilterNode {
                    ..Default::default()
                }),
                vec![Dispatcher {
                    dispatcher_id: 3,
                    downstream_actor_id: (111..112).collect_vec(),
                    ..Default::default()
                }],
            ),
            make_mview_stream_actors_custom(
                111,
                112,
                NodeBody::Materialize(MaterializeNode {
                    table_id: table_id.table_id(),
                    ..Default::default()
                }),
                vec![],
            ),
        ];

        let mut fragments = BTreeMap::default();

        let actor_groups_len = actor_groups.len();
        for (i, actor_group) in actor_groups.into_iter().enumerate() {
            let fragment_type = if i == 0 {
                FragmentType::Source
            } else if i == actor_groups_len - 1 {
                FragmentType::Sink
            } else {
                FragmentType::Others
            };
            let fragment = Fragment {
                fragment_id: i as u32,
                fragment_type: fragment_type as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actor_group.clone(),
                vnode_mapping: None,
            };

            fragments.insert(i as crate::model::FragmentId, fragment);
        }

        let table_fragments = TableFragments::new(table_id, fragments, HashSet::default());

        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, &mut ctx)
            .await?;

        let mut actor_workers = HashMap::new();
        actor_workers.insert(100, 1);
        actor_workers.insert(106, 1);
        actor_workers.insert(108, 1);
        actor_workers.insert(110, 1);

        let mut actors = HashMap::new();
        actors.insert(table_id, actor_workers);

        services
            .global_stream_manager
            .migrate_actors(actors)
            .await
            .unwrap();

        services.stop().await;
        Ok(())
    }
}
