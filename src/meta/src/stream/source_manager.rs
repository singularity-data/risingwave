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

use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::{internal_error, Result, RwError, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_connector::source::{
    ConnectorProperties, SplitEnumeratorImpl, SplitImpl, SplitMetaData,
};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::source::Info::StreamSource;
use risingwave_pb::catalog::Source;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::source::{
    ConnectorSplit, ConnectorSplits, SourceActorInfo as ProstSourceActorInfo,
};
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::SourceChangeSplitMutation;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use risingwave_rpc_client::StreamClient;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};
use tokio_retry::strategy::FixedInterval;

use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::ClusterManagerRef;
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::manager::{CatalogManagerRef, MetaSrvEnv, SourceId};
use crate::model::{
    ActorId, FragmentId, MetadataModel, MetadataModelResult, TableFragments, Transactional,
};
use crate::storage::{MetaStore, Transaction};
use crate::stream::FragmentManagerRef;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

const SOURCE_CF_NAME: &str = "cf/source";

#[expect(dead_code)]
pub struct SourceManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,
    compaction_group_manager: CompactionGroupManagerRef<S>,
    core: Arc<Mutex<SourceManagerCore<S>>>,
}

pub struct SharedSplitMap {
    splits: Option<BTreeMap<String, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

#[expect(dead_code)]
pub struct ConnectorSourceWorker {
    source_id: SourceId,
    current_splits: SharedSplitMapRef,
    enumerator: SplitEnumeratorImpl,
    period: Duration,
}

#[derive(Debug, Default)]
pub struct SourceActorInfo {
    actor_id: ActorId,
    splits: Vec<SplitImpl>,
}

impl MetadataModel for SourceActorInfo {
    type KeyType = u32;
    type ProstType = ProstSourceActorInfo;

    fn cf_name() -> String {
        SOURCE_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        Self::ProstType {
            actor_id: self.actor_id,
            splits: Some(ConnectorSplits {
                splits: self.splits.iter().map(ConnectorSplit::from).collect(),
            }),
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            actor_id: prost.actor_id,
            splits: prost
                .splits
                .unwrap_or_default()
                .splits
                .into_iter()
                .map(|split| SplitImpl::try_from(&split).unwrap())
                .collect(),
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.actor_id)
    }
}

impl ConnectorSourceWorker {
    pub async fn create(source: &Source, period: Duration) -> Result<Self> {
        let source_id = source.get_id();
        let info = source
            .info
            .clone()
            .ok_or_else(|| internal_error("source info is empty"))?;
        let stream_source_info = try_match_expand!(info, Info::StreamSource)?;
        let properties =
            ConnectorProperties::extract(stream_source_info.properties).to_rw_result()?;
        let enumerator = SplitEnumeratorImpl::create(properties)
            .await
            .to_rw_result()?;
        let current_splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
        Ok(Self {
            source_id,
            current_splits,
            enumerator,
            period,
        })
    }

    pub async fn run(&mut self, mut sync_call_rx: UnboundedReceiver<oneshot::Sender<Result<()>>>) {
        let mut interval = time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            select! {
                biased;
                tx = sync_call_rx.borrow_mut().recv() => {
                    if let Some(tx) = tx {
                        let _ = tx.send(self.tick().await);
                    }
                }
                _ = interval.tick() => {
                    if let Err(e) = self.tick().await {
                        log::error!("error happened when tick from connector source worker: {}", e.to_string());
                    }
                }
            }
        }
    }

    async fn tick(&mut self) -> Result<()> {
        let splits = self.enumerator.list_splits().await.to_rw_result()?;
        let mut current_splits = self.current_splits.lock().await;
        current_splits.splits.replace(
            splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect(),
        );

        Ok(())
    }
}

pub struct ConnectorSourceWorkerHandle {
    handle: JoinHandle<()>,
    sync_call_tx: UnboundedSender<oneshot::Sender<Result<()>>>,
    splits: SharedSplitMapRef,
}

pub struct SourceManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,
    pub managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    pub source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl<S> SourceManagerCore<S>
where
    S: MetaStore,
{
    fn new(
        fragment_manager: FragmentManagerRef<S>,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    ) -> Self {
        Self {
            fragment_manager,
            managed_sources,
            source_fragments,
            actor_splits,
        }
    }

    async fn diff(&mut self) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        // first, list all fragment, so that we can get `FragmentId` -> `Vec<ActorId>` map
        let table_frags = self.fragment_manager.list_table_fragments().await?;
        let mut frag_actors: HashMap<FragmentId, Vec<ActorId>> = HashMap::new();
        for table_frag in table_frags {
            for (frag_id, mut frag) in table_frag.fragments {
                let mut actors = frag.actors.iter_mut().map(|x| x.actor_id).collect_vec();
                frag_actors
                    .entry(frag_id)
                    .or_insert(vec![])
                    .append(&mut actors);
            }
        }

        // then we diff the splits
        let mut changed_actors: HashMap<ActorId, Vec<SplitImpl>> = HashMap::new();

        for (source_id, ConnectorSourceWorkerHandle { splits, .. }) in &self.managed_sources {
            let frag_ids = match self.source_fragments.get(source_id) {
                Some(fragment_ids) if !fragment_ids.is_empty() => fragment_ids,
                _ => {
                    continue;
                }
            };

            let discovered_splits = {
                let splits_guard = splits.lock().await;
                match splits_guard.splits.clone() {
                    None => continue,
                    Some(splits) => splits,
                }
            };

            for frag_id in frag_ids {
                let actor_ids = match frag_actors.remove(frag_id) {
                    None => {
                        // target fragment has gone?
                        continue;
                    }
                    Some(actors) => actors,
                };

                let mut prev_splits = HashMap::new();
                for actor_id in actor_ids {
                    prev_splits.insert(
                        actor_id,
                        self.actor_splits
                            .get(&actor_id)
                            .cloned()
                            .unwrap_or_default(),
                    );
                }

                let diff = diff_splits(prev_splits, &discovered_splits);
                if let Some(change) = diff {
                    for (actor_id, splits) in change {
                        changed_actors.insert(actor_id, splits);
                    }
                }
            }
        }

        Ok(changed_actors)
    }

    pub fn patch_diff(
        &mut self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        actor_splits: Option<HashMap<ActorId, Vec<SplitImpl>>>,
    ) {
        if let Some(source_fragments) = source_fragments {
            for (source_id, mut fragment_ids) in source_fragments {
                self.source_fragments
                    .entry(source_id)
                    .or_insert_with(BTreeSet::default)
                    .append(&mut fragment_ids);
            }
        }

        if let Some(actor_splits) = actor_splits {
            for (actor_id, splits) in actor_splits {
                self.actor_splits.insert(actor_id, splits.clone());
            }
        }
    }

    pub fn drop_diff(
        &mut self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        actor_splits: Option<HashSet<ActorId>>,
    ) {
        if let Some(source_fragments) = source_fragments {
            for (source_id, fragment_ids) in source_fragments {
                if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                    let managed_fragment_ids = entry.get_mut();
                    for fragment_id in &fragment_ids {
                        managed_fragment_ids.remove(fragment_id);
                    }

                    if managed_fragment_ids.is_empty() {
                        entry.remove();
                    }
                }

                if let Some(managed_fragment_ids) = self.source_fragments.get_mut(&source_id) {
                    for fragment_id in fragment_ids {
                        managed_fragment_ids.remove(&fragment_id);
                    }
                }
            }
        }

        if let Some(actor_splits) = actor_splits {
            for actor_id in actor_splits {
                self.actor_splits.remove(&actor_id);
            }
        }
    }

    pub fn get_actor_splits(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        self.actor_splits.clone()
    }
}

pub(crate) fn fetch_source_fragments(
    source_fragments: &mut HashMap<SourceId, BTreeSet<FragmentId>>,
    table_fragments: &TableFragments,
) {
    for fragment in table_fragments.fragments() {
        for actor in &fragment.actors {
            if let Some(source_id) =
                TableFragments::fetch_stream_source_id(actor.nodes.as_ref().unwrap())
            {
                source_fragments
                    .entry(source_id)
                    .or_insert(BTreeSet::new())
                    .insert(fragment.fragment_id as FragmentId);

                break;
            }
        }
    }
}

// todo use min heap to optimize
fn diff_splits(
    mut prev_actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    discovered_splits: &BTreeMap<String, SplitImpl>,
) -> Option<HashMap<ActorId, Vec<SplitImpl>>> {
    let prev_split_ids: HashSet<_> = prev_actor_splits
        .values()
        .flat_map(|splits| splits.iter().map(SplitImpl::id))
        .collect();

    if discovered_splits
        .keys()
        .all(|split_id| prev_split_ids.contains(split_id))
    {
        return None;
    }

    let mut new_discovered_splits = HashSet::new();
    for (split_id, split) in discovered_splits {
        if !prev_split_ids.contains(split_id) {
            new_discovered_splits.insert(split.id());
        }
    }

    let mut result = HashMap::new();

    let mut actors = prev_actor_splits.keys().cloned().collect_vec();

    // sort actors
    actors.sort();

    let actor_len = actors.len();

    for (index, split_id) in new_discovered_splits.into_iter().enumerate() {
        let target_actor_id = actors[index % actor_len];
        let split = discovered_splits.get(&split_id).unwrap().clone();

        result
            .entry(target_actor_id)
            .or_insert_with(|| prev_actor_splits.remove(&target_actor_id).unwrap());

        result.get_mut(&target_actor_id).unwrap().push(split);
    }

    Some(result)
}

impl<S> SourceManager<S>
where
    S: MetaStore,
{
    const SOURCE_RETRY_INTERVAL: Duration = Duration::from_secs(10);
    const SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
    ) -> Result<Self> {
        let mut managed_sources = HashMap::new();
        {
            let catalog_guard = catalog_manager.get_catalog_core_guard().await;
            let sources = catalog_guard.list_sources().await?;

            for source in sources {
                if let Some(StreamSource(_)) = source.info {
                    Self::create_source_worker(&source, &mut managed_sources).await?
                }
            }
        }

        let mut source_fragments = HashMap::new();
        for table_fragments in fragment_manager.list_table_fragments().await? {
            fetch_source_fragments(&mut source_fragments, &table_fragments)
        }

        let actor_splits = SourceActorInfo::list(env.meta_store())
            .await?
            .into_iter()
            .map(|source_actor_info| (source_actor_info.actor_id, source_actor_info.splits))
            .collect();

        let core = Arc::new(Mutex::new(SourceManagerCore::new(
            fragment_manager,
            managed_sources,
            source_fragments,
            actor_splits,
        )));

        Ok(Self {
            env,
            cluster_manager,
            catalog_manager,
            barrier_manager,
            compaction_group_manager,
            core,
        })
    }

    pub async fn drop_update(
        &self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        actor_splits: Option<HashSet<ActorId>>,
    ) -> Result<()> {
        {
            let mut core = self.core.lock().await;
            core.drop_diff(source_fragments, actor_splits.clone());
        }

        let mut trx = Transaction::default();
        if let Some(actor_ids) = actor_splits {
            for actor_id in actor_ids {
                let source_actor_info = SourceActorInfo {
                    actor_id,
                    ..Default::default()
                };
                source_actor_info.delete_in_transaction(&mut trx)?;
            }
        }

        self.env
            .meta_store()
            .txn(trx)
            .await
            .map_err(|e| internal_error(e.to_string()))
    }

    pub async fn patch_update(
        &self,
        source_fragments: Option<HashMap<SourceId, BTreeSet<FragmentId>>>,
        actor_splits: Option<HashMap<ActorId, Vec<SplitImpl>>>,
    ) -> Result<()> {
        let mut trx = Transaction::default();
        if let Some(actor_splits) = actor_splits.clone() {
            for (actor_id, splits) in actor_splits {
                let source_actor_info = SourceActorInfo { actor_id, splits };
                source_actor_info.upsert_in_transaction(&mut trx)?;
            }
        }

        self.env
            .meta_store()
            .txn(trx)
            .await
            .map_err(|e| internal_error(e.to_string()))?;

        let mut core = self.core.lock().await;
        core.patch_diff(source_fragments, actor_splits);

        Ok(())
    }

    pub async fn pre_allocate_splits(
        &self,
        table_id: &TableId,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    ) -> Result<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;
        let table_fragments = core
            .fragment_manager
            .select_table_fragments_by_table_id(table_id)
            .await?;

        let mut assigned = HashMap::new();

        for (source_id, fragments) in source_fragments {
            let handle = core
                .managed_sources
                .get(&source_id)
                .ok_or_else(|| internal_error(format!("could not found source {}", source_id)))?;

            if handle.splits.lock().await.splits.is_none() {
                // force refresh source
                let (tx, rx) = oneshot::channel();
                handle.sync_call_tx.send(tx).to_rw_result()?;
                rx.await.map_err(|e| internal_error(e.to_string()))??;
            }

            if let Some(splits) = &handle.splits.lock().await.splits {
                if splits.is_empty() {
                    tracing::warn!("no splits detected for source {}", source_id);
                    continue;
                }

                for fragment_id in fragments {
                    let empty_actor_splits = table_fragments
                        .fragments
                        .get(&fragment_id)
                        .ok_or_else(|| {
                            internal_error(format!("could not found source {}", source_id))
                        })?
                        .actors
                        .iter()
                        .map(|actor| (actor.actor_id, vec![]))
                        .collect();

                    if let Some(diff) = diff_splits(empty_actor_splits, splits) {
                        assigned.extend(diff);
                    }
                }
            } else {
                unreachable!();
            }
        }

        Ok(assigned)
    }

    async fn all_stream_clients(&self) -> Result<impl Iterator<Item = StreamClient>> {
        // FIXME: there is gap between the compute node activate itself and source ddl operation,
        // create/drop source(non-stateful source like TableSource) before the compute node
        // activate itself will cause an inconsistent state. This situation will happen when some
        // compute node scale in.
        let all_compute_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;

        let all_stream_clients = try_join_all(
            all_compute_nodes
                .iter()
                .map(|worker| self.env.stream_client_pool().get(worker)),
        )
        .await?
        .into_iter();

        Ok(all_stream_clients)
    }

    /// Broadcast the create source request to all compute nodes.
    pub async fn create_source(&self, source: &Source) -> Result<()> {
        // This scope guard does clean up jobs ASYNCHRONOUSLY before Err returns.
        // It MUST be cleared before Ok returns.
        let mut revert_funcs = scopeguard::guard(
            vec![],
            |revert_funcs: Vec<futures::future::BoxFuture<()>>| {
                tokio::spawn(async move {
                    for revert_func in revert_funcs {
                        revert_func.await;
                    }
                });
            },
        );

        // Register beforehand and is safeguarded by CompactionGroupManager::purge_stale_members.
        let registered_table_ids = self
            .compaction_group_manager
            .register_source(source.id, &HashMap::new())
            .await?;
        let compaction_group_manager_ref = self.compaction_group_manager.clone();
        revert_funcs.push(Box::pin(async move {
            if let Err(e) = compaction_group_manager_ref.unregister_table_ids(&registered_table_ids).await {
                tracing::warn!("Failed to unregister_table_ids {:#?}.\nThey will be cleaned up on node restart.\n{:#?}", registered_table_ids, e);
            }
        }));
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSourceRequest {
                    source: Some(source.clone()),
                };
                async move { client.create_source(request).await.map_err(RwError::from) }
            });

        // ignore response body, always none
        let _ = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if core.managed_sources.contains_key(&source.get_id()) {
            log::warn!("source {} already registered", source.get_id());
            revert_funcs.clear();
            return Ok(());
        }

        if let Some(StreamSource(_)) = source.info {
            Self::create_source_worker(source, &mut core.managed_sources).await?;
        }

        revert_funcs.clear();
        Ok(())
    }

    async fn create_source_worker(
        source: &Source,
        managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
    ) -> Result<()> {
        let mut worker = ConnectorSourceWorker::create(source, Duration::from_secs(10)).await?;
        let current_splits_ref = worker.current_splits.clone();
        log::info!("spawning new watcher for source {}", source.id);

        let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();

        let handle = tokio::spawn(async move { worker.run(sync_call_rx).await });
        managed_sources.insert(
            source.id,
            ConnectorSourceWorkerHandle {
                handle,
                sync_call_tx,
                splits: current_splits_ref,
            },
        );

        Ok(())
    }

    pub async fn drop_source(&self, source_id: SourceId) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSourceRequest { source_id };
                async move { client.drop_source(request).await.map_err(RwError::from) }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        let mut core = self.core.lock().await;
        if let Some(handle) = core.managed_sources.remove(&source_id) {
            handle.handle.abort();
        }

        if core.source_fragments.contains_key(&source_id) {
            log::warn!(
                "dropping source {}, but associated fragments still exists",
                source_id
            );
            core.source_fragments.remove(&source_id);
        } else {
            // Unregister afterwards and is safeguarded by
            // CompactionGroupManager::purge_stale_members.
            if let Err(e) = self
                .compaction_group_manager
                .unregister_source(source_id)
                .await
            {
                tracing::warn!(
                    "Failed to unregister source {}. It wll be unregistered eventually.\n{:#?}",
                    source_id,
                    e
                );
            }
        }

        Ok(())
    }

    async fn tick(&self) -> Result<()> {
        let diff = {
            let mut core_guard = self.core.lock().await;
            core_guard.diff().await?
        };

        if !diff.is_empty() {
            let command = Command::Plain(Some(Mutation::Splits(SourceChangeSplitMutation {
                actor_splits: diff
                    .iter()
                    .map(|(&actor_id, splits)| {
                        (
                            actor_id,
                            ConnectorSplits {
                                splits: splits.iter().map(ConnectorSplit::from).collect(),
                            },
                        )
                    })
                    .collect(),
            })));
            log::debug!("pushing down mutation {:#?}", command);

            tokio_retry::Retry::spawn(FixedInterval::new(Self::SOURCE_RETRY_INTERVAL), || async {
                let command = command.clone();
                self.barrier_manager.run_command(command).await
            })
            .await
            .expect("source manager barrier push down failed");

            self.patch_update(None, Some(diff))
                .await
                .expect("patch update failed");
        }

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        let mut ticker = time::interval(Self::SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(e) = self.tick().await {
                log::error!(
                    "error happened while running source manager tick: {}",
                    e.to_string()
                );
            }
        }
    }

    pub async fn get_source_ids_in_fragments(&self) -> Vec<SourceId> {
        self.core
            .lock()
            .await
            .source_fragments
            .keys()
            .cloned()
            .collect_vec()
    }

    pub async fn get_actor_splits(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        self.core.lock().await.get_actor_splits()
    }
}
