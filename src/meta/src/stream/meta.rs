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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_common::util::compress::decompress_data;

use risingwave_pb::common::{ParallelUnit, ParallelUnitType, WorkerNode};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::{Dispatcher, FragmentType, StreamActor};
use tokio::sync::RwLock;

use crate::cluster::WorkerId;
use crate::manager::{HashMappingManagerRef, MetaSrvEnv};
use crate::model::{ActorId, MetadataModel, TableFragments, Transactional};
use crate::storage::{MetaStore, Transaction};
use crate::stream::record_table_vnode_mappings;

struct FragmentManagerCore {
    table_fragments: HashMap<TableId, TableFragments>,
}

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager<S: MetaStore> {
    meta_store: Arc<S>,

    core: RwLock<FragmentManagerCore>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<WorkerId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<WorkerId, Vec<ActorId>>,
}

#[derive(Default)]
pub struct BuildGraphInfo {
    pub table_node_actors: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
    pub table_sink_actor_ids: HashMap<TableId, Vec<ActorId>>,
}

pub type FragmentManagerRef<S> = Arc<FragmentManager<S>>;

impl<S: MetaStore> FragmentManager<S>
    where
        S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let meta_store = env.meta_store_ref();
        let table_fragments = try_match_expand!(
            TableFragments::list(&*meta_store).await,
            Ok,
            "TableFragments::list fail"
        )?;

        let table_fragments = table_fragments
            .into_iter()
            .map(|tf| (tf.table_id(), tf))
            .collect();

        // Extract vnode mapping info from listed `table_fragments` to hash mapping manager.
        Self::restore_vnode_mappings(env.hash_mapping_manager_ref(), &table_fragments)?;

        Ok(Self {
            meta_store,
            core: RwLock::new(FragmentManagerCore { table_fragments }),
        })
    }

    pub async fn list_table_fragments(&self) -> Result<Vec<TableFragments>> {
        let map = &self.core.read().await.table_fragments;

        Ok(map.values().cloned().collect())
    }

    pub async fn batch_update_table_fragments(
        &self,
        table_fragments: &[TableFragments],
    ) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        let mut transaction = Transaction::default();
        for table_fragment in table_fragments {
            if map.contains_key(&table_fragment.table_id()) {
                table_fragment.upsert_in_transaction(&mut transaction)?;
            } else {
                return Err(RwError::from(InternalError(format!(
                    "table_fragment not exist: id={}",
                    table_fragment.table_id()
                ))));
            }
        }

        self.meta_store.txn(transaction).await?;
        for table_fragment in table_fragments {
            map.insert(table_fragment.table_id(), table_fragment.clone());
        }

        Ok(())
    }

    pub async fn select_table_fragments_by_table_id(
        &self,
        table_id: &TableId,
    ) -> Result<TableFragments> {
        let map = &self.core.read().await.table_fragments;
        if let Some(table_fragments) = map.get(table_id) {
            Ok(table_fragments.clone())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Start create a new `TableFragments` and insert it into meta store, currently the actors'
    /// state is `ActorState::Inactive`.
    pub async fn start_create_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => Err(RwError::from(InternalError(format!(
                "table_fragment already exist: id={}",
                table_fragment.table_id()
            )))),
            Entry::Vacant(v) => {
                table_fragment.insert(&*self.meta_store).await?;
                v.insert(table_fragment);
                Ok(())
            }
        }
    }

    /// Cancel creation of a new `TableFragments` and delete it from meta store.
    pub async fn cancel_create_table_fragments(&self, table_id: &TableId) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(*table_id) {
            Entry::Occupied(o) => {
                TableFragments::delete(&*self.meta_store, &table_id.table_id).await?;
                o.remove();
                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    /// Finish create a new `TableFragments` and update the actors' state to `ActorState::Running`,
    /// besides also update all dependent tables' downstream actors info.
    pub async fn finish_create_table_fragments(
        &self,
        table_id: &TableId,
        dependent_table_actors: Vec<(TableId, HashMap<ActorId, Vec<Dispatcher>>)>,
    ) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();

            let mut table_fragments = table_fragments.clone();
            table_fragments.update_actors_state(ActorState::Running);
            table_fragments.upsert_in_transaction(&mut transaction)?;

            let mut dependent_tables = Vec::with_capacity(dependent_table_actors.len());
            for (dependent_table_id, mut new_dispatchers) in dependent_table_actors {
                let mut dependent_table = map
                    .get(&dependent_table_id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "table_fragment not exist: id={}",
                            dependent_table_id
                        )))
                    })?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    for actor in &mut fragment.actors {
                        // Extend new dispatchers to table fragments.
                        if let Some(new_dispatchers) = new_dispatchers.remove(&actor.actor_id) {
                            actor.dispatcher.extend(new_dispatchers);
                        }
                    }
                }
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            map.insert(*table_id, table_fragments);
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }

            Ok(())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Drop table fragments info and remove downstream actor infos in fragments from its dependent
    /// tables.
    pub async fn drop_table_fragments(&self, table_id: &TableId) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();
            table_fragments.delete_in_transaction(&mut transaction)?;

            let dependent_table_ids = table_fragments.dependent_table_ids();
            let chain_actor_ids = table_fragments.chain_actor_ids();
            let mut dependent_tables = Vec::with_capacity(dependent_table_ids.len());
            for dependent_table_id in dependent_table_ids {
                let mut dependent_table = map
                    .get(&dependent_table_id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "table_fragment not exist: id={}",
                            dependent_table_id
                        )))
                    })?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    if fragment.fragment_type == FragmentType::Sink as i32 {
                        for actor in &mut fragment.actors {
                            // Remove these downstream actor ids from all dispatchers.
                            for dispatcher in &mut actor.dispatcher {
                                dispatcher
                                    .downstream_actor_id
                                    .retain(|x| !chain_actor_ids.contains(x));
                            }
                            // Remove empty dispatchers.
                            actor
                                .dispatcher
                                .retain(|d| !d.downstream_actor_id.is_empty());
                        }
                    }
                }
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            map.remove(table_id).unwrap();
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }
            Ok(())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all actor that need to be sent or
    /// collected
    pub async fn load_all_actors(
        &self,
        check_state: impl Fn(ActorState, &TableId) -> bool,
    ) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut source_actor_ids = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (node_id, actor_states) in fragments.node_actor_states() {
                for actor_state in actor_states {
                    if check_state(actor_state.1, &fragments.table_id()) {
                        actor_maps
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_state.0);
                    }
                }
            }

            let source_actors = fragments.node_source_actor_states();
            for (&node_id, actor_states) in &source_actors {
                for actor_state in actor_states {
                    if check_state(actor_state.1, &fragments.table_id()) {
                        source_actor_ids
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_state.0);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            source_actor_maps: source_actor_ids,
        }
    }

    pub async fn recreate_actors(
        &self,
        migrate_map: &HashMap<ActorId, WorkerId>,
        recreate_actor_id_map: &HashMap<ActorId, ActorId>,
        recreated_actors: &HashMap<ActorId, StreamActor>,
        node_map: &HashMap<WorkerId, WorkerNode>,
        table_fragments: Vec<TableFragments>,
    ) -> Result<(Vec<TableFragments>, HashMap<ParallelUnitId, ParallelUnit>)> {
        let mut parallel_unit_migrate_map = HashMap::new();
        let (mut pu_hash_map, mut pu_single_map) = Self::fetch_parallel_unit_map(node_map);
        let mut table_fragments = table_fragments;
        let mut new_fragments = Vec::new();

        for fragment in &mut table_fragments {
            let mut flag = false;

            for fragment in fragment.fragments.values_mut() {
                for actor in &mut fragment.actors {
                    if let Some(recreated_actor) = recreated_actors.get(&actor.actor_id) {
                        *actor = recreated_actor.clone();
                        continue;
                    }

                    for upstream_actor_id in &mut actor.upstream_actor_id {
                        if let Some(recreated_actor_id) = recreate_actor_id_map.get(upstream_actor_id) {
                            *upstream_actor_id = *recreated_actor_id;
                        }
                    }

                    for dispatcher in &mut actor.dispatcher {
                        for downstream_actor_id in &mut dispatcher.downstream_actor_id {
                            if let Some(recreated_actor_id) = recreate_actor_id_map.get(downstream_actor_id) {
                                *downstream_actor_id = *recreated_actor_id;
                            }
                        }
                    }
                }
            }

            let fragment_migrated_actor_ids = fragment.actor_status.keys().cloned().filter(|actor_id| {
                migrate_map.contains_key(actor_id)
            }).collect_vec();

            let mut recreate_actor_status_map = HashMap::new();
            for actor_id in &fragment_migrated_actor_ids {
                if let Some(status) = fragment.actor_status.remove(actor_id) {
                    recreate_actor_status_map.insert(*actor_id, status);
                }
            }

            let mut new_actor_status_map = HashMap::new();
            for (actor_id, mut status) in recreate_actor_status_map {
                if let Some(new_node_id) = migrate_map.get(&actor_id) {
                    flag = Self::update_parallel_unit_for_actor_status(&mut parallel_unit_migrate_map, &mut pu_hash_map, &mut pu_single_map, &mut status, new_node_id);
                    let new_actor_id = recreate_actor_id_map.get(&actor_id).unwrap();
                    new_actor_status_map.insert(*new_actor_id, status);
                }
            }

            for (actor_id, status) in new_actor_status_map {
                fragment.actor_status.insert(actor_id, status);
            }

            if flag {
                // update vnode mapping of updated fragments
                fragment.update_vnode_mapping(&parallel_unit_migrate_map);
                new_fragments.push(fragment.clone());
            }
        };
        // update fragments
        self.batch_update_table_fragments(&new_fragments).await?;
        Ok((new_fragments, parallel_unit_migrate_map))
    }

    fn update_parallel_unit_for_actor_status(
        parallel_unit_migrate_map: &mut HashMap<u32, ParallelUnit>,
        pu_hash_map: &mut HashMap<WorkerId, Vec<&ParallelUnit>>,
        pu_single_map: &mut HashMap<WorkerId, Vec<&ParallelUnit>>,
        status: &mut ActorStatus,
        new_node_id: &WorkerId,
    ) -> bool {
        let mut flag = false;
        if let Some(ref old_parallel_unit) = status.parallel_unit {
            if let Entry::Vacant(e) =
            parallel_unit_migrate_map.entry(old_parallel_unit.id)
            {
                if old_parallel_unit.r#type == ParallelUnitType::Hash as i32 {
                    let new_parallel_unit =
                        pu_hash_map.get_mut(new_node_id).unwrap().pop().unwrap();
                    e.insert(new_parallel_unit.clone());
                    status.parallel_unit = Some(new_parallel_unit.clone());
                } else {
                    let new_parallel_unit =
                        pu_single_map.get_mut(new_node_id).unwrap().pop().unwrap();
                    e.insert(new_parallel_unit.clone());
                    status.parallel_unit = Some(new_parallel_unit.clone());
                }
                flag = true;
            } else {
                status.parallel_unit = Some(
                    parallel_unit_migrate_map
                        .get(&old_parallel_unit.id)
                        .unwrap()
                        .clone(),
                );
            }
        }

        flag
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`]
    /// migrate actors and update fragments, generate migrate info
    pub async fn migrate_actors(
        &self,
        migrate_map: &HashMap<ActorId, WorkerId>,
        node_map: &HashMap<WorkerId, WorkerNode>,
    ) -> Result<(Vec<TableFragments>, HashMap<ParallelUnitId, ParallelUnit>)> {
        let mut parallel_unit_migrate_map = HashMap::new();
        let (mut pu_hash_map, mut pu_single_map) = Self::fetch_parallel_unit_map(node_map);
        // update actor status and generate pu to pu migrate info
        let mut table_fragments = self.list_table_fragments().await?;
        let mut new_fragments = Vec::new();
        table_fragments.iter_mut().for_each(|fragment| {
            let mut flag = false;
            fragment
                .actor_status
                .iter_mut()
                .for_each(|(actor_id, status)| {
                    if let Some(new_node_id) = migrate_map.get(actor_id) {
                        flag = Self::update_parallel_unit_for_actor_status(&mut parallel_unit_migrate_map, &mut pu_hash_map, &mut pu_single_map, status, new_node_id);
                    };
                });
            if flag {
                // update vnode mapping of updated fragments
                fragment.update_vnode_mapping(&parallel_unit_migrate_map);
                new_fragments.push(fragment.clone());
            }
        });
        // update fragments
        self.batch_update_table_fragments(&new_fragments).await?;
        Ok((new_fragments, parallel_unit_migrate_map))
    }

    #[allow(clippy::type_complexity)]
    fn fetch_parallel_unit_map(node_map: &HashMap<WorkerId, WorkerNode>) -> (HashMap<WorkerId, Vec<&ParallelUnit>>, HashMap<WorkerId, Vec<&ParallelUnit>>) {
        let mut pu_hash_map: HashMap<WorkerId, Vec<&ParallelUnit>> = HashMap::new();
        let mut pu_single_map: HashMap<WorkerId, Vec<&ParallelUnit>> = HashMap::new();
        // split parallel units of node into types, map them with WorkerId
        for (node_id, node) in node_map {
            let pu_hash = node
                .parallel_units
                .iter()
                .filter(|pu| pu.r#type == ParallelUnitType::Hash as i32)
                .collect_vec();
            pu_hash_map.insert(*node_id, pu_hash);
            let pu_single = node
                .parallel_units
                .iter()
                .filter(|pu| pu.r#type == ParallelUnitType::Single as i32)
                .collect_vec();
            pu_single_map.insert(*node_id, pu_single);
        }
        (pu_hash_map, pu_single_map)
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> HashMap<WorkerId, Vec<StreamActor>> {
        let mut actor_maps = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (node_id, actor_ids) in fragments.node_actors(include_inactive) {
                let node_actor_ids = actor_maps.entry(node_id).or_insert_with(Vec::new);
                node_actor_ids.extend(actor_ids);
            }
        }

        actor_maps
    }

    pub async fn all_chain_actor_ids(&self) -> HashSet<ActorId> {
        let map = &self.core.read().await.table_fragments;

        map.values()
            .flat_map(|table_fragment| table_fragment.chain_actor_ids())
            .collect::<HashSet<_>>()
    }

    pub async fn table_node_actors(
        &self,
        table_id: &TableId,
    ) -> Result<BTreeMap<WorkerId, Vec<ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.node_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_sink_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    // we will read three things at once, avoiding locking too much.
    pub async fn get_build_graph_info(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<BuildGraphInfo> {
        let map = &self.core.read().await.table_fragments;
        let mut info: BuildGraphInfo = Default::default();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.table_node_actors
                        .insert(*table_id, table_fragment.node_actor_ids());
                    info.table_sink_actor_ids
                        .insert(*table_id, table_fragment.sink_actor_ids());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }
        Ok(info)
    }

    pub async fn get_sink_parallel_unit_ids(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.parallel_unit_sink_actor_id());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }

        Ok(info)
    }

    pub async fn get_tables_worker_actors(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.node_actor_ids());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }

        Ok(info)
    }

    fn restore_vnode_mappings(
        hash_mapping_manager: HashMappingManagerRef,
        table_fragments: &HashMap<TableId, TableFragments>,
    ) -> Result<()> {
        for fragments in table_fragments.values() {
            for (fragment_id, fragment) in &fragments.fragments {
                let mapping = fragment.vnode_mapping.as_ref().unwrap();
                let vnode_mapping = decompress_data(&mapping.original_indices, &mapping.data);
                assert_eq!(vnode_mapping.len(), VIRTUAL_NODE_COUNT);
                hash_mapping_manager.set_fragment_hash_mapping(*fragment_id, vnode_mapping);

                // Looking at the first actor is enough, since all actors in one fragment have
                // identical state table id.
                let actor = fragment.actors.first().unwrap();
                let stream_node = actor.get_nodes()?;
                record_table_vnode_mappings(
                    &hash_mapping_manager,
                    stream_node,
                    fragment.fragment_id,
                )?;
            }
        }
        Ok(())
    }
}
