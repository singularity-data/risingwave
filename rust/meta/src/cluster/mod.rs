use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::{HostAddress, ParallelUnit, ParallelUnitType, WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::hummock::HummockManager;
use crate::manager::{
    HashDispatchManager, HashDispatchManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv,
    NotificationManagerRef,
};
use crate::model::{MetadataModel, Worker, INVALID_EXPIRE_AT};
use crate::storage::MetaStore;

pub type NodeId = u32;
pub type ParallelUnitId = u32;
pub type NodeLocations = HashMap<NodeId, WorkerNode>;
pub type StoredClusterManagerRef<S> = Arc<StoredClusterManager<S>>;

const DEFAULT_WORK_NODE_PARALLEL_DEGREE: usize = 8;

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct WorkerKey(pub HostAddress);

impl PartialEq<Self> for WorkerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for WorkerKey {}

impl Hash for WorkerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.host.hash(state);
        self.0.port.hash(state);
    }
}

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct StoredClusterManager<S> {
    meta_store_ref: Arc<S>,
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    hummock_manager_ref: Option<Arc<HummockManager<S>>>,
    dispatch_manager_ref: HashDispatchManagerRef<S>,
    max_heartbeat_interval: Duration,
    notification_manager_ref: NotificationManagerRef,
    core: RwLock<StoredClusterManagerCore>,
}

impl<S> StoredClusterManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        hummock_manager_ref: Option<Arc<HummockManager<S>>>,
        notification_manager_ref: NotificationManagerRef,
        max_heartbeat_interval: Duration,
    ) -> Result<Self> {
        let meta_store_ref = env.meta_store_ref();
        let core = StoredClusterManagerCore::new(meta_store_ref.clone()).await?;
        let compute_nodes = core.list_worker_node(WorkerType::ComputeNode, None);
        let dispatch_manager_ref =
            Arc::new(HashDispatchManager::new(&compute_nodes, meta_store_ref.clone()).await?);

        Ok(Self {
            meta_store_ref,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            hummock_manager_ref,
            dispatch_manager_ref,
            notification_manager_ref,
            max_heartbeat_interval,
            core: RwLock::new(core),
        })
    }

    pub async fn add_worker_node(
        &self,
        host_address: HostAddress,
        r#type: WorkerType,
    ) -> Result<(WorkerNode, bool)> {
        let mut core = self.core.write().await;
        match core.get_worker_by_host(host_address.clone()) {
            Some(worker) => Ok((worker.to_protobuf(), false)),
            None => {
                // Generate worker id.
                let worker_id = self
                    .id_gen_manager_ref
                    .generate::<{ IdCategory::Worker }>()
                    .await?;

                // Generate parallel units.
                let parallel_units = if r#type == WorkerType::ComputeNode {
                    self.generate_cn_parallel_units(
                        DEFAULT_WORK_NODE_PARALLEL_DEGREE as usize,
                        worker_id as u32,
                    )
                    .await?
                } else {
                    vec![]
                };

                // Construct worker.
                let worker_node = WorkerNode {
                    id: worker_id as u32,
                    r#type: r#type as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                };
                let worker = Worker::from_protobuf(worker_node.clone());

                // Alter consistent hash mapping.
                if r#type == WorkerType::ComputeNode {
                    self.dispatch_manager_ref
                        .add_worker_mapping(&worker.to_protobuf())
                        .await?;
                }

                // Persist worker node.
                worker.insert(&*self.meta_store_ref).await?;

                // Update core.
                core.add_worker_node(worker);

                Ok((worker_node, true))
            }
        }
    }

    pub async fn activate_worker_node(&self, host_address: HostAddress) -> Result<()> {
        let mut core = self.core.write().await;
        match core.get_worker_by_host(host_address.clone()) {
            Some(worker) => {
                let mut worker_node = worker.to_protobuf();
                if worker_node.state == State::Running as i32 {
                    return Ok(());
                }
                worker_node.state = State::Running as i32;
                let worker = Worker::from_protobuf(worker_node.clone());

                worker.insert(self.meta_store_ref.as_ref()).await?;

                core.activate_worker_node(worker);

                // Notify frontends of new compute node.
                if worker_node.r#type == WorkerType::ComputeNode as i32 {
                    self.notification_manager_ref
                        .notify(
                            Operation::Add,
                            &Info::Node(worker_node),
                            crate::manager::NotificationTarget::Frontend,
                        )
                        .await?
                }

                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
        }
    }

    pub async fn delete_worker_node(&self, host_address: HostAddress) -> Result<()> {
        let mut core = self.core.write().await;
        match core.get_worker_by_host(host_address.clone()) {
            Some(worker) => {
                let worker_node = worker.to_protobuf();

                if let Some(hummock_manager_ref) = self.hummock_manager_ref.as_ref() {
                    // It's desirable these operations are committed atomically.
                    // But meta store transaction across *Manager is not intuitive.
                    // TODO #93: So we rely on a safe guard that periodically purges hummock context
                    // resource owned by stale worker nodes.
                    hummock_manager_ref
                        .release_context_resource(worker_node.id)
                        .await?;
                }

                // Alter consistent hash mapping.
                if worker_node.r#type == WorkerType::ComputeNode as i32 {
                    self.dispatch_manager_ref
                        .delete_worker_mapping(&worker_node)
                        .await?;
                }

                // Persist deletion.
                Worker::delete(&*self.meta_store_ref, &host_address).await?;

                // Update core.
                core.delete_worker_node(worker);

                // Notify frontends to delete compute node.
                if worker_node.r#type == WorkerType::ComputeNode as i32 {
                    self.notification_manager_ref
                        .notify(
                            Operation::Delete,
                            &Info::Node(worker_node),
                            crate::manager::NotificationTarget::Frontend,
                        )
                        .await?
                }

                Ok(())
            }
            None => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
        }
    }

    pub async fn heartbeat(&self, worker_id: u32) -> Result<()> {
        tracing::debug!("heartbeat from {}", worker_id);
        let mut core = self.core.write().await;
        // 1. Get unique key. TODO: avoid this step
        let key = match core.get_worker_by_id(worker_id) {
            None => {
                return Ok(());
            }
            Some(worker_2) => worker_2.to_protobuf().host.unwrap(),
        };

        // 2. Update expire_at
        let expire_at = SystemTime::now()
            .add(self.max_heartbeat_interval)
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        core.update_worker_ttl(key, expire_at);

        Ok(())
    }

    async fn update_worker_ttl(&self, host_address: HostAddress, expire_at: u64) {
        let mut core = self.core.write().await;
        core.update_worker_ttl(host_address, expire_at);
    }

    /// TODO: Java frontend doesn't send heartbeat. Remove this function after we use
    /// Rust frontend.
    async fn get_workers_to_init_or_delete(&self, now: u64) -> Vec<Worker> {
        let core = self.core.read().await;
        core.get_workers_to_init_or_delete(now)
    }

    pub async fn start_heartbeat_checker(
        cluster_manager_ref: StoredClusterManagerRef<S>,
        check_interval: Duration,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Heartbeat checker is shutting down");
                        return;
                    }
                }
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs();
                let workers_to_init_or_delete =
                    cluster_manager_ref.get_workers_to_init_or_delete(now).await;
                for worker in workers_to_init_or_delete {
                    let key = worker.key().expect("illegal key");
                    if worker.expire_at() == INVALID_EXPIRE_AT {
                        // Initialize expire_at
                        cluster_manager_ref
                            .update_worker_ttl(
                                key,
                                now + cluster_manager_ref.max_heartbeat_interval.as_secs(),
                            )
                            .await;
                        continue;
                    }
                    match cluster_manager_ref.delete_worker_node(key.clone()).await {
                        Ok(_) => {
                            tracing::warn!(
                                "Deleted expired worker {} {}:{}; expired at {}, now {}",
                                worker.worker_id(),
                                key.host,
                                key.port,
                                worker.expire_at(),
                                now,
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                "Failed to delete expired worker {} {}:{}; expired at {}, now {}. {}",
                                worker.worker_id(),
                                key.host,
                                key.port,
                                worker.expire_at(),
                                now,
                                err,
                            );
                        }
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Get live nodes with the specified type and state.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `worker_state` Filter by this state if it is not None.
    pub async fn list_worker_node(
        &self,
        worker_type: WorkerType,
        worker_state: Option<State>,
    ) -> Vec<WorkerNode> {
        let core = self.core.read().await;
        core.list_worker_node(worker_type, worker_state)
    }

    pub async fn get_worker_count(&self, worker_type: Option<WorkerType>) -> usize {
        let core = self.core.read().await;
        core.get_worker_count(worker_type)
    }

    pub async fn list_parallel_units(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> Vec<ParallelUnit> {
        let core = self.core.read().await;
        core.list_parallel_units(parallel_unit_type)
    }

    pub async fn get_parallel_unit_count(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> usize {
        let core = self.core.read().await;
        core.get_parallel_unit_count(parallel_unit_type)
    }

    async fn generate_cn_parallel_units(
        &self,
        parallel_degree: usize,
        node_id: u32,
    ) -> Result<Vec<ParallelUnit>> {
        let start_id = self
            .id_gen_manager_ref
            .generate_interval::<{ IdCategory::ParallelUnit }>(parallel_degree as i32)
            .await? as usize;
        let parallel_degree = DEFAULT_WORK_NODE_PARALLEL_DEGREE;
        let mut parallel_units = Vec::with_capacity(parallel_degree);
        let single_parallel_unit = ParallelUnit {
            id: start_id as u32,
            r#type: ParallelUnitType::Single as i32,
            node_id,
        };
        parallel_units.push(single_parallel_unit);
        (start_id + 1..start_id + parallel_degree).for_each(|id| {
            let hash_parallel_unit = ParallelUnit {
                id: id as u32,
                r#type: ParallelUnitType::Hash as i32,
                node_id,
            };
            parallel_units.push(hash_parallel_unit);
        });
        Ok(parallel_units)
    }
}

pub struct StoredClusterManagerCore {
    /// Record for workers in the cluster.
    workers: HashMap<WorkerKey, Worker>,
    /// Record for parallel units of different types.
    single_parallel_units: Vec<ParallelUnit>,
    hash_parallel_units: Vec<ParallelUnit>,
}

impl StoredClusterManagerCore {
    async fn new<S>(meta_store_ref: Arc<S>) -> Result<Self>
    where
        S: MetaStore,
    {
        let workers = try_match_expand!(
            Worker::list(&*meta_store_ref).await,
            Ok,
            "Worker::list fail"
        )?;
        let mut worker_map = HashMap::new();
        let mut single_parallel_units = Vec::new();
        let mut hash_parallel_units = Vec::new();

        workers.iter().for_each(|w| {
            worker_map.insert(WorkerKey(w.key().unwrap()), w.clone());
            let node = w.to_protobuf();
            node.parallel_units.iter().for_each(|parallel_unit| {
                if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                    single_parallel_units.push(parallel_unit.clone());
                } else {
                    hash_parallel_units.push(parallel_unit.clone());
                }
            });
        });

        Ok(Self {
            workers: worker_map,
            single_parallel_units,
            hash_parallel_units,
        })
    }

    fn get_worker_by_host(&self, host_address: HostAddress) -> Option<Worker> {
        self.workers.get(&WorkerKey(host_address)).cloned()
    }

    fn get_worker_by_id(&self, id: u32) -> Option<Worker> {
        self.workers
            .iter()
            .find(|(_, worker)| worker.worker_id() == id)
            .map(|(_, worker)| worker.clone())
    }

    fn add_worker_node(&mut self, worker: Worker) {
        let worker_node = worker.to_protobuf();
        worker_node.parallel_units.iter().for_each(|parallel_unit| {
            if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                self.single_parallel_units.push(parallel_unit.clone());
            } else {
                self.hash_parallel_units.push(parallel_unit.clone());
            }
        });
        self.workers
            .insert(WorkerKey(worker_node.host.unwrap()), worker);
    }

    fn activate_worker_node(&mut self, worker: Worker) {
        self.workers
            .insert(WorkerKey(worker.to_protobuf().host.unwrap()), worker);
    }

    fn delete_worker_node(&mut self, worker: Worker) {
        let worker_node = worker.to_protobuf();
        worker_node.parallel_units.iter().for_each(|parallel_unit| {
            if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                self.single_parallel_units
                    .retain(|p| p.id != parallel_unit.id);
            } else {
                self.hash_parallel_units
                    .retain(|p| p.id != parallel_unit.id);
            }
        });
        self.workers.remove(&WorkerKey(worker_node.host.unwrap()));
    }

    fn list_worker_node(
        &self,
        worker_type: WorkerType,
        worker_state: Option<State>,
    ) -> Vec<WorkerNode> {
        self.workers
            .iter()
            .map(|(_, worker)| worker.to_protobuf())
            .filter(|w| w.r#type == worker_type as i32)
            .filter(|w| match worker_state {
                None => true,
                Some(state) => state as i32 == w.state,
            })
            .collect::<Vec<_>>()
    }

    fn get_worker_count(&self, worker_type: Option<WorkerType>) -> usize {
        self.workers
            .iter()
            .filter(|(_, worker)| match worker_type {
                Some(worker_type) => worker.worker_type() == worker_type,
                None => true,
            })
            .count()
    }

    fn list_parallel_units(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> Vec<ParallelUnit> {
        match parallel_unit_type {
            Some(ParallelUnitType::Single) => self.single_parallel_units.clone(),
            Some(ParallelUnitType::Hash) => self.hash_parallel_units.clone(),
            None => {
                let mut all_parallel_units = self.single_parallel_units.clone();
                all_parallel_units.extend(self.hash_parallel_units.clone());
                all_parallel_units
            }
        }
    }

    fn get_parallel_unit_count(&self, parallel_unit_type: Option<ParallelUnitType>) -> usize {
        match parallel_unit_type {
            Some(ParallelUnitType::Single) => self.single_parallel_units.len(),
            Some(ParallelUnitType::Hash) => self.hash_parallel_units.len(),
            None => self.single_parallel_units.len() + self.hash_parallel_units.len(),
        }
    }

    fn update_worker_ttl(&mut self, host_address: HostAddress, expire_at: u64) {
        match self.workers.entry(WorkerKey(host_address)) {
            Entry::Occupied(mut worker) => {
                worker.get_mut().set_expire_at(expire_at);
            }
            Entry::Vacant(_) => {}
        }
    }

    fn get_workers_to_init_or_delete(&self, now: u64) -> Vec<Worker> {
        self.workers
            .iter()
            .filter(|(_, worker)| worker.worker_type() != WorkerType::Frontend)
            .filter(|(_, worker)| worker.expire_at() < now)
            .map(|(_, worker)| worker.clone())
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::hummock::test_utils::setup_compute_env;
    use crate::manager::NotificationManager;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let hummock_manager = Arc::new(
            HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
                .await
                .unwrap(),
        );
        let cluster_manager = Arc::new(
            StoredClusterManager::new(
                env.clone(),
                Some(hummock_manager.clone()),
                Arc::new(NotificationManager::new()),
                Duration::new(0, 0),
            )
            .await
            .unwrap(),
        );

        let mut worker_nodes = Vec::new();
        let worker_count = 5usize;
        for i in 0..worker_count {
            let fake_host_address = HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5000 + i as i32,
            };
            let (worker_node, _) = cluster_manager
                .add_worker_node(fake_host_address, WorkerType::ComputeNode)
                .await
                .unwrap();
            worker_nodes.push(worker_node);
        }

        let single_parallel_count = worker_count;
        let hash_parallel_count = (DEFAULT_WORK_NODE_PARALLEL_DEGREE - 1) * worker_count;
        assert_cluster_manager(&cluster_manager, single_parallel_count, hash_parallel_count).await;

        let worker_to_delete_count = 4usize;
        for i in 0..worker_to_delete_count {
            let fake_host_address = HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5000 + i as i32,
            };
            cluster_manager
                .delete_worker_node(fake_host_address)
                .await
                .unwrap();
        }
        assert_cluster_manager(&cluster_manager, 1, DEFAULT_WORK_NODE_PARALLEL_DEGREE - 1).await;

        let mapping = cluster_manager
            .dispatch_manager_ref
            .get_worker_mapping()
            .await
            .unwrap();
        let unique_parallel_units = HashSet::<u32>::from_iter(mapping.into_iter());
        assert_eq!(
            unique_parallel_units.len(),
            DEFAULT_WORK_NODE_PARALLEL_DEGREE - 1
        );

        Ok(())
    }

    async fn assert_cluster_manager(
        cluster_manager: &StoredClusterManager<MemStore>,
        single_parallel_count: usize,
        hash_parallel_count: usize,
    ) {
        let single_parallel_units = cluster_manager
            .list_parallel_units(Some(ParallelUnitType::Single))
            .await;
        let hash_parallel_units = cluster_manager
            .list_parallel_units(Some(ParallelUnitType::Hash))
            .await;
        assert_eq!(single_parallel_units.len(), single_parallel_count);
        assert_eq!(hash_parallel_units.len(), hash_parallel_count);
    }

    // This test takes seconds because the TTL is measured in seconds.
    #[tokio::test]
    #[ignore]
    async fn test_heartbeat() {
        let (_env, _hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
        let context_id_1 = worker_node.id;
        let fake_host_address_2 = HostAddress {
            host: "127.0.0.1".to_string(),
            port: 2,
        };
        let (_worker_node_2, _) = cluster_manager
            .add_worker_node(fake_host_address_2, WorkerType::ComputeNode)
            .await
            .unwrap();
        // Two live nodes
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .await
                .len(),
            2
        );

        let ttl = cluster_manager.max_heartbeat_interval;
        let check_interval = std::cmp::min(Duration::from_millis(100), ttl / 4);

        // Keep worker 1 alive
        let cluster_manager_ref = cluster_manager.clone();
        let keep_alive_join_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(cluster_manager_ref.max_heartbeat_interval / 3).await;
                cluster_manager_ref.heartbeat(context_id_1).await.unwrap();
            }
        });

        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One node has actually expired but still got two, because heartbeat check is not
        // started.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .await
                .len(),
            2
        );

        let (join_handle, shutdown_sender) =
            StoredClusterManager::start_heartbeat_checker(cluster_manager.clone(), check_interval)
                .await;
        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One live node left.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .await
                .len(),
            1
        );

        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
        keep_alive_join_handle.abort();
    }
}
