use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use rand::distributions::{Distribution as RandDistribution, Uniform};
use risingwave_common::error::Result;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_rpc_client::MetaClient;
use tokio::sync::mpsc;

use crate::optimizer::plan_node::PlanNodeType;
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;
use crate::scheduler::plan_fragmenter::{Query, QueryStageRef, StageId};

type ScheduledStageSender = std::sync::mpsc::Sender<ScheduledStage>;
type ScheduledStageReceiver = std::sync::mpsc::Receiver<ScheduledStage>;
use risingwave_pb::plan;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{plan_node as pb_batch_node, ExchangeNode, ExchangeSource, PlanFragment};

pub(crate) type TaskId = u64;

/// `BatchScheduler` dispatches query fragments to compute nodes.
pub(crate) struct BatchScheduler {
    worker_manager: WorkerNodeManagerRef,
    scheduled_stage_sender: mpsc::UnboundedSender<ScheduledStage>,
    scheduled_stage_receiver: mpsc::UnboundedReceiver<ScheduledStage>,
    scheduled_stages_map: HashMap<StageId, ScheduledStageRef>,
}

#[derive(Debug)]
pub(crate) struct ScheduledStage {
    pub assignments: HashMap<TaskId, WorkerNode>,
    pub augmented_stage: AugmentedStageRef,
}

impl ScheduledStage {
    pub fn from_augmented_stage(
        augmented_stage: AugmentedStageRef,
        assignments: HashMap<TaskId, WorkerNode>,
    ) -> Self {
        Self {
            assignments,
            augmented_stage,
        }
    }

    pub fn id(&self) -> StageId {
        self.augmented_stage.query_stage.id
    }
}
pub(crate) type ScheduledStageRef = Arc<ScheduledStage>;

#[derive(Debug)]
pub(crate) struct AugmentedStage {
    pub query_stage: QueryStageRef,
    pub exchange_source: HashMap<StageId, ScheduledStageRef>,
    pub parallelism: u32,
    pub workers: Vec<WorkerNode>,
}

impl AugmentedStage {
    /// Construct augment stage from query stage.
    pub fn new_with_query_stage(
        query_stage: QueryStageRef,
        exchange_source: &HashMap<StageId, ScheduledStageRef>,
        workers: Vec<WorkerNode>,
        parallelism: u32,
    ) -> Self {
        Self {
            query_stage,
            exchange_source: exchange_source.clone(),
            parallelism,
            workers,
        }
    }

    /// Serialize augmented stage into plan node. Used by task manager to construct task.
    pub fn to_prost(&self, task_id: TaskId, query: &Query) -> Result<PlanFragment> {
        let prost_root = self.rewrite_exchange(self.query_stage.root.clone(), task_id, query)?;
        Ok(PlanFragment {
            root: Some(prost_root),
            exchange_info: Some(self.query_stage.distribution.to_prost(self.parallelism)),
        })
    }

    fn rewrite_exchange(
        &self,
        plan_node: PlanRef,
        task_id: TaskId,
        query: &Query,
    ) -> Result<plan::PlanNode> {
        let mut current_node = plan_node.to_batch_prost();
        // Clear children first.
        current_node.children.clear();

        // If current plan node is exchange node, it must be root.
        let cur_plan_node_type = plan_node.node_type();
        if cur_plan_node_type == PlanNodeType::BatchExchange {
            let source_stage_id = query
                .stage_graph
                .exchange_id_to_stage
                .get(&plan_node.id().0)
                .unwrap();
            let scheduled_stage = self.exchange_source.get(source_stage_id).unwrap();
            let exchange_node =
                Self::create_exchange_node(scheduled_stage, plan_node, task_id, query)?;
            current_node.node_body = Some(exchange_node);
        } else {
            for child in plan_node.inputs() {
                current_node
                    .children
                    .push(self.rewrite_exchange(child, task_id, query)?)
            }
        }

        Ok(current_node)
    }

    fn create_exchange_node(
        stage: &ScheduledStageRef,
        plan_node: PlanRef,
        task_id: TaskId,
        query: &Query,
    ) -> Result<pb_batch_node::NodeBody> {
        let mut exchange_node = ExchangeNode {
            ..Default::default()
        };
        for (child_task_id, worker_node) in &stage.assignments {
            let host = &worker_node.host;
            let task_output_id = Some(risingwave_pb::plan::TaskOutputId {
                output_id: task_id as u32,
                task_id: Self::construct_prost_task_id(
                    *child_task_id,
                    stage.id(),
                    query.query_id.to_string(),
                ),
            });
            // Construct exchange source into exchange node.
            // Consider a HashJoin -> Exchange -> Scan. Assume parallelism of HashJoin is 4, while
            // the Scan is 3. (HashJoin -> Exchange will be one stage, Scan will be
            // another stage). 3 Scan Tasks will run in 3 different nodes. Each nodes
            // will create 4 local channel (0, 1, 2, 3), after local hash shuffle.
            // Task 0 of HashJoin will pull all 0 channels from 3 nodes,  Task 1 of HashJoin will
            // pull all 1 channels from 3 nodes, etc.
            let exchange_source = ExchangeSource {
                host: host.clone(),
                task_output_id,
            };
            exchange_node.sources.push(exchange_source);
        }
        // Construct input schema in exchange node.
        let input = plan_node.inputs()[0].clone();
        let schema = input.schema();
        for field in &schema.fields {
            exchange_node.input_schema.push(field.to_prost()?);
        }
        Ok(NodeBody::Exchange(exchange_node))
    }

    // Construct TaskId in prost.
    fn construct_prost_task_id(
        task_id: TaskId,
        stage_id: StageId,
        query_id: String,
    ) -> Option<risingwave_pb::plan::TaskId> {
        Some(risingwave_pb::plan::TaskId {
            task_id: task_id as u32,
            stage_id: stage_id as u32,
            query_id,
        })
    }
}

pub(crate) type AugmentedStageRef = Arc<AugmentedStage>;

impl BatchScheduler {
    /// Used in tests.
    pub fn mock(worker_manager: WorkerNodeManagerRef) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            worker_manager,
            scheduled_stage_sender: sender,
            scheduled_stage_receiver: receiver,
            scheduled_stages_map: HashMap::new(),
        }
    }

    pub fn get_scheduled_stage_unchecked(&self, stage_id: &StageId) -> &ScheduledStageRef {
        self.scheduled_stages_map.get(stage_id).unwrap()
    }
}

/// `WorkerNodeManager` manages live worker nodes.
pub(crate) struct WorkerNodeManager {
    worker_nodes: RwLock<Vec<WorkerNode>>,
}

pub(crate) type WorkerNodeManagerRef = Arc<WorkerNodeManager>;

impl WorkerNodeManager {
    pub async fn new(client: MetaClient) -> Result<Self> {
        let worker_nodes = RwLock::new(
            client
                .list_all_nodes(WorkerType::ComputeNode, false)
                .await?,
        );
        Ok(Self { worker_nodes })
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let worker_nodes = RwLock::new(worker_nodes);
        Self { worker_nodes }
    }

    pub fn list_worker_nodes(&self) -> Vec<WorkerNode> {
        self.worker_nodes.read().unwrap().clone()
    }

    pub fn add_worker_node(&self, node: WorkerNode) {
        self.worker_nodes.write().unwrap().push(node);
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        self.worker_nodes.write().unwrap().retain(|x| *x != node);
    }

    /// Get a random worker node.
    pub fn next_random(&self) -> WorkerNode {
        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..self.worker_nodes.read().unwrap().len());
        self.worker_nodes
            .read()
            .unwrap()
            .get(die.sample(&mut rng))
            .unwrap()
            .clone()
    }
}

/// Where the query execution handler to pull results.
pub(crate) struct QueryResultLocation {
    task_id: TaskId,
    worker_node: WorkerNode,
}

impl BatchScheduler {
    /// Given a `Query` (Already split by plan fragmenter)
    pub async fn schedule(&mut self, query: &Query) -> QueryResultLocation {
        // First schedule all leaf stages.
        for leaf_stage_id in &query.leaf_stages() {
            let stage = query.stage_graph.get_stage_unchecked(leaf_stage_id);
            let child_stages = query.stage_graph.get_child_stages_unchecked(leaf_stage_id);
            self.schedule_stage(stage, child_stages);
        }

        loop {
            let scheduled_stage = self.scheduled_stage_receiver.recv().await.unwrap();
            let cur_stage_id = scheduled_stage.id();
            self.scheduled_stages_map
                .insert(cur_stage_id, Arc::new(scheduled_stage));

            let parent_ids = query.get_parents(&cur_stage_id);
            for parent_id in parent_ids {
                let stage = query.stage_graph.get_stage_unchecked(parent_id);
                let child_stages = query.stage_graph.get_child_stages_unchecked(parent_id);
                if self.all_child_scheduled(child_stages) {
                    self.schedule_stage(stage, child_stages);
                }
            }

            if cur_stage_id == query.stage_graph.id {
                // All child stages have been scheduled.
                let root_stage = self.scheduled_stages_map.get(&cur_stage_id).unwrap();
                let (task_id, worker_node) = root_stage.assignments.iter().next().unwrap();
                return QueryResultLocation {
                    task_id: *task_id,
                    worker_node: worker_node.clone(),
                };
            }
        }
    }

    /// Get scheduled stages from `stage_ids`.
    fn get_scheduled_stages(
        &self,
        stage_ids: &HashSet<StageId>,
    ) -> HashMap<StageId, ScheduledStageRef> {
        let mut ret = HashMap::new();
        for stage_id in stage_ids {
            ret.insert(
                *stage_id,
                self.scheduled_stages_map.get(stage_id).unwrap().clone(),
            );
        }
        ret
    }

    /// Schedule each query stage (`QueryStageRef` -> `AugmentedStageRef` -> `ScheduledStageRef`)
    /// and write results into channel.
    ///
    /// Calculate available workers, parallelism for each stage.
    fn schedule_stage(
        &mut self,
        query_stage_ref: QueryStageRef,
        child_scheduled_stage: &HashSet<StageId>,
    ) {
        let all_nodes = self.worker_manager.list_worker_nodes();
        let distribution_schema = query_stage_ref.distribution.clone();
        let mut next_stage_parallelism = 1;
        if distribution_schema != Distribution::Single {
            next_stage_parallelism = all_nodes.len();
        }

        let scheduled_children = self.get_scheduled_stages(child_scheduled_stage);

        // Determine how many worker nodes for current stage.
        let mut cur_stage_worker_nodes = vec![];
        if scheduled_children.is_empty() {
            // If current plan has scan node, use all workers (the data may be in any of them).
            if Self::include_table_scan(query_stage_ref.root.clone()) {
                cur_stage_worker_nodes = all_nodes;
            } else {
                // Otherwise just choose a random worker.
                cur_stage_worker_nodes.push(self.worker_manager.next_random());
            }
        } else {
            let mut use_num_nodes = all_nodes.len();
            for stage in scheduled_children.values() {
                // If distribution is single, one worker.
                if stage.augmented_stage.query_stage.distribution == Distribution::Single {
                    use_num_nodes = 1;
                    break;
                }
            }

            if use_num_nodes == all_nodes.len() {
                cur_stage_worker_nodes = all_nodes;
            } else {
                cur_stage_worker_nodes.push(self.worker_manager.next_random());
            }
        }

        self.do_stage_execution(Arc::new(AugmentedStage::new_with_query_stage(
            query_stage_ref,
            &scheduled_children,
            cur_stage_worker_nodes,
            next_stage_parallelism as u32,
        )));
    }

    /// Check whether plan node has a table scan node. If true, the parallelism should be
    /// all the compute nodes.
    fn include_table_scan(plan_node: PlanRef) -> bool {
        if plan_node.node_type() == PlanNodeType::BatchSeqScan {
            return true;
        }

        for child in plan_node.inputs() {
            if Self::include_table_scan(child) {
                return true;
            }
        }

        false
    }

    /// Check whether all child stages are scheduled.
    fn all_child_scheduled(&self, child_stages: &HashSet<StageId>) -> bool {
        for child_stage_id in child_stages {
            if !self.scheduled_stages_map.contains_key(child_stage_id) {
                return false;
            }
        }
        true
    }

    /// Wrap scheduled stages into task and send to compute node for execution.
    fn do_stage_execution(&mut self, augmented_stage: AugmentedStageRef) {
        let mut scheduled_tasks = HashMap::new();
        for task_id in 0..augmented_stage.parallelism {
            // TODO(Bowen): Introduce Compute Client to do task distribution.
            scheduled_tasks.insert(
                task_id as TaskId,
                augmented_stage.workers[task_id as usize].clone(),
            );
        }

        let scheduled_stage =
            ScheduledStage::from_augmented_stage(augmented_stage, scheduled_tasks);
        self.scheduled_stage_sender.send(scheduled_stage).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_rpc_client::MetaClient;
    use tokio::sync::watch;

    use super::WorkerNodeManager;
    use crate::catalog::catalog_service::CatalogCache;
    use crate::observer::observer_manager::ObserverManager;
    use crate::test_utils::FrontendMockMetaClient;

    #[tokio::test]
    async fn test_add_and_delete_worker_node() {
        let mut meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);

        let (catalog_updated_tx, _) = watch::channel(0);
        let catalog_cache = Arc::new(RwLock::new(
            CatalogCache::new(meta_client.clone()).await.unwrap(),
        ));
        let worker_node_manager =
            Arc::new(WorkerNodeManager::new(meta_client.clone()).await.unwrap());

        let observer_manager = ObserverManager::new(
            meta_client.clone(),
            "127.0.0.1:12345".parse().unwrap(),
            worker_node_manager.clone(),
            catalog_cache,
            catalog_updated_tx,
        )
        .await;
        observer_manager.start();

        // Add worker node
        let socket_addr = "127.0.0.1:6789".parse().unwrap();
        meta_client
            .register(socket_addr, WorkerType::ComputeNode)
            .await
            .unwrap();
        meta_client.activate(socket_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut worker_nodes = worker_node_manager.list_worker_nodes();
        assert_eq!(1, worker_nodes.len());
        let worker_node_0 = worker_nodes.pop().unwrap();
        assert_eq!(WorkerType::ComputeNode, worker_node_0.r#type());
        assert_eq!(
            &HostAddress {
                host: "127.0.0.1".to_string(),
                port: 6789
            },
            worker_node_0.get_host().unwrap()
        );

        // Delete worker node
        meta_client.unregister(socket_addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let worker_nodes = worker_node_manager.list_worker_nodes();
        assert_eq!(0, worker_nodes.len());
    }
}
