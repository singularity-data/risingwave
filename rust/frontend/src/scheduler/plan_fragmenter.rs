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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::error::Result;
use uuid::Uuid;

use crate::optimizer::plan_node::{BatchExchange, PlanNodeType, PlanTreeNode};
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;

pub(crate) type StageId = u64;

/// `BatchPlanFragmenter` splits a query plan into fragments.
struct BatchPlanFragmenter {
    stage_graph_builder: StageGraphBuilder,
    next_stage_id: u64,
}

impl BatchPlanFragmenter {
    pub fn new() -> Self {
        Self {
            stage_graph_builder: StageGraphBuilder::new(),
            next_stage_id: 0,
        }
    }
}

/// Contains the connection info of each stage.
pub(crate) struct Query {
    /// Query id should always be unique.
    pub(crate) query_id: Uuid,
    pub(crate) stage_graph: StageGraph,
}

impl Query {
    pub fn leaf_stages(&self) -> Vec<StageId> {
        let mut ret_leaf_stages = Vec::new();
        for stage_id in self.stage_graph.stages.keys() {
            if self
                .stage_graph
                .get_child_stages_unchecked(stage_id)
                .is_empty()
            {
                ret_leaf_stages.push(*stage_id);
            }
        }
        ret_leaf_stages
    }

    pub fn get_parents(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.stage_graph.parent_edges.get(stage_id).unwrap()
    }
}

/// Fragment part of `Query`.
#[derive(Debug)]
pub(crate) struct QueryStage {
    pub id: StageId,
    pub root: PlanRef,
    pub distribution: Distribution,
}
pub(crate) type QueryStageRef = Arc<QueryStage>;

/// Maintains how each stage are connected.
pub(crate) struct StageGraph {
    pub(crate) id: StageId,
    stages: HashMap<StageId, QueryStageRef>,
    /// Traverse from top to down. Used in split plan into stages.
    child_edges: HashMap<StageId, HashSet<StageId>>,
    /// Traverse from down to top. Used in schedule each stage.
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    /// Indicates which stage the exchange executor is running on.
    /// Look up child stage for exchange source so that parent stage knows where to pull data.
    pub(crate) exchange_id_to_stage: HashMap<i32, StageId>,
}

impl StageGraph {
    pub fn get_stage_unchecked(&self, stage_id: &StageId) -> QueryStageRef {
        self.stages.get(stage_id).unwrap().clone()
    }

    pub fn get_child_stages_unchecked(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.child_edges.get(stage_id).unwrap()
    }
}

struct StageGraphBuilder {
    stages: HashMap<StageId, QueryStageRef>,
    child_edges: HashMap<StageId, HashSet<StageId>>,
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    exchange_id_to_stage: HashMap<i32, StageId>,
}

impl StageGraphBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            child_edges: HashMap::new(),
            parent_edges: HashMap::new(),
            exchange_id_to_stage: HashMap::new(),
        }
    }

    pub fn build(mut self, stage_id: StageId) -> StageGraph {
        for stage_id in self.stages.keys() {
            if self.child_edges.get(stage_id).is_none() {
                self.child_edges.insert(*stage_id, HashSet::new());
            }

            if self.parent_edges.get(stage_id).is_none() {
                self.parent_edges.insert(*stage_id, HashSet::new());
            }
        }

        StageGraph {
            id: stage_id,
            stages: self.stages,
            child_edges: self.child_edges,
            parent_edges: self.parent_edges,
            exchange_id_to_stage: self.exchange_id_to_stage,
        }
    }

    /// Link parent stage and child stage. Maintain the mappings of parent -> child and child ->
    /// parent.
    ///
    /// # Arguments
    ///
    /// * `exchange_id` - The operator id of exchange executor.
    pub fn link_to_child(&mut self, parent_id: StageId, exchange_id: i32, child_id: StageId) {
        let child_ids = self.child_edges.get_mut(&parent_id);
        // If the parent id does not exist, create a new set containing the child ids. Otherwise
        // just insert.
        match child_ids {
            Some(childs) => {
                childs.insert(child_id);
            }

            None => {
                let mut childs = HashSet::new();
                childs.insert(child_id);
                self.child_edges.insert(parent_id, childs);
            }
        };

        let parent_ids = self.parent_edges.get_mut(&child_id);
        // If the child id does not exist, create a new set containing the parent ids. Otherwise
        // just insert.
        match parent_ids {
            Some(parent_ids) => {
                parent_ids.insert(parent_id);
            }

            None => {
                let mut parents = HashSet::new();
                parents.insert(parent_id);
                self.parent_edges.insert(child_id, parents);
            }
        };
        self.exchange_id_to_stage.insert(exchange_id, child_id);
    }

    pub fn add_node(&mut self, stage: QueryStageRef) {
        self.stages.insert(stage.id, stage);
    }
}

impl BatchPlanFragmenter {
    /// Split the plan node into each stages, based on exchange node.
    pub fn split(mut self, batch_node: PlanRef) -> Result<Query> {
        let root_stage_graph =
            self.new_query_stage(batch_node.clone(), batch_node.distribution().clone());
        self.build_stage(&root_stage_graph, batch_node.clone());
        let stage_graph = self.stage_graph_builder.build(root_stage_graph.id);
        Ok(Query {
            stage_graph,
            query_id: Uuid::new_v4(),
        })
    }

    fn new_query_stage(&mut self, node: PlanRef, distribution: Distribution) -> QueryStageRef {
        let next_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let stage = Arc::new(QueryStage {
            id: next_stage_id,
            root: node.clone(),
            distribution,
        });
        self.stage_graph_builder.add_node(stage.clone());
        stage
    }

    /// Based on current stage, use stage graph builder to recursively build the DAG plan (splits
    /// the plan by exchange node.). Children under pipeline-breaker separately forms a stage
    /// (aka plan fragment).
    fn build_stage(&mut self, cur_stage: &QueryStage, node: PlanRef) {
        // NOTE: The breaker's children will not be logically removed after plan slicing,
        // but their serialized plan will ignore the children. Therefore, the compute-node
        // will eventually only receive the sliced part.
        if node.node_type() == PlanNodeType::BatchExchange {
            let exchange_node = node.downcast_ref::<BatchExchange>().unwrap();
            for child_node in exchange_node.inputs() {
                // If plan node is a exchange node, for each inputs (child), new a query stage and
                // link with current stage.
                let child_query_stage =
                    self.new_query_stage(child_node.clone(), child_node.distribution().clone());
                self.stage_graph_builder.link_to_child(
                    cur_stage.id,
                    node.id().0,
                    child_query_stage.id,
                );
                self.build_stage(&child_query_stage, child_node);
            }
        } else {
            for child_node in node.inputs() {
                // All child nodes still belongs to current stage if no exchange.
                self.build_stage(cur_stage, child_node);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnDesc, TableDesc};
    use risingwave_common::types::DataType;
    use risingwave_pb::common::{
        HostAddress, ParallelUnit, ParallelUnitType, WorkerNode, WorkerType,
    };
    use risingwave_pb::plan::exchange_info::DistributionMode;
    use risingwave_pb::plan::JoinType;

    use crate::optimizer::plan_node::{
        BatchExchange, BatchHashJoin, BatchSeqScan, EqJoinPredicate, LogicalJoin, LogicalScan,
        PlanNodeType,
    };
    use crate::optimizer::property::{Distribution, Order};
    use crate::optimizer::PlanRef;
    use crate::scheduler::plan_fragmenter::BatchPlanFragmenter;
    use crate::scheduler::schedule::{BatchScheduler, WorkerNodeManager};
    use crate::session::QueryContext;
    use crate::utils::Condition;

    #[tokio::test]
    async fn test_fragmenter() {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Scan
        //
        let ctx = QueryContext::mock().await;

        let batch_plan_node: PlanRef = BatchSeqScan::new(LogicalScan::new(
            "".to_string(),
            vec![0, 1],
            Rc::new(TableDesc {
                table_id: 0.into(),
                pk: vec![],
                columns: vec![
                    ColumnDesc {
                        data_type: DataType::Int32,
                        column_id: 0.into(),
                        name: "a".to_string(),
                        type_name: String::new(),
                        field_descs: vec![],
                    },
                    ColumnDesc {
                        data_type: DataType::Float64,
                        column_id: 1.into(),
                        name: "b".to_string(),
                        type_name: String::new(),
                        field_descs: vec![],
                    },
                ],
            }),
            ctx,
        ))
        .into();
        let batch_exchange_node1: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let batch_exchange_node2: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let hash_join_node: PlanRef = BatchHashJoin::new(
            LogicalJoin::new(
                batch_exchange_node1.clone(),
                batch_exchange_node2.clone(),
                JoinType::Inner,
                Condition::true_cond(),
            ),
            EqJoinPredicate::create(0, 0, Condition::true_cond()),
        )
        .into();
        let batch_exchange_node3: PlanRef = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into();

        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new();
        let query = fragmenter.split(batch_exchange_node3.clone()).unwrap();

        assert_eq!(query.stage_graph.id, 0);
        assert_eq!(query.stage_graph.stages.len(), 4);

        // Check the mappings of child edges.
        assert_eq!(query.stage_graph.child_edges.get(&0).unwrap().len(), 1);
        assert_eq!(query.stage_graph.child_edges.get(&1).unwrap().len(), 2);
        assert_eq!(query.stage_graph.child_edges.get(&2).unwrap().len(), 0);
        assert_eq!(query.stage_graph.child_edges.get(&3).unwrap().len(), 0);

        // Check the mappings of parent edges.
        assert_eq!(query.stage_graph.parent_edges.get(&0).unwrap().len(), 0);
        assert_eq!(query.stage_graph.parent_edges.get(&1).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&2).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&3).unwrap().len(), 1);

        // Check plan node in each stages.
        let root_exchange = query.stage_graph.stages.get(&0).unwrap();
        assert_eq!(root_exchange.root.node_type(), PlanNodeType::BatchExchange);
        let join_node = query.stage_graph.stages.get(&1).unwrap();
        assert_eq!(join_node.root.node_type(), PlanNodeType::BatchHashJoin);
        let scan_node1 = query.stage_graph.stages.get(&2).unwrap();
        assert_eq!(scan_node1.root.node_type(), PlanNodeType::BatchSeqScan);
        let scan_node2 = query.stage_graph.stages.get(&3).unwrap();
        assert_eq!(scan_node2.root.node_type(), PlanNodeType::BatchSeqScan);

        // -- Check augment phase --
        let worker1 = WorkerNode {
            id: 0,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5687,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(0, 0),
        };
        let worker2 = WorkerNode {
            id: 1,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5688,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(8, 1),
        };
        let worker3 = WorkerNode {
            id: 2,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5689,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(16, 2),
        };
        let workers = vec![worker1.clone(), worker2.clone(), worker3.clone()];
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(workers));
        let mut scheduler = BatchScheduler::mock(worker_node_manager);
        let _query_result_loc = scheduler.schedule(&query).await;

        let root = scheduler.get_scheduled_stage_unchecked(&0);
        assert_eq!(root.augmented_stage.exchange_source.len(), 1);
        assert!(root.augmented_stage.exchange_source.get(&1).is_some());
        assert_eq!(root.assignments.len(), 1);
        assert_eq!(root.assignments.get(&0).unwrap(), &worker1);

        let join_node = scheduler.get_scheduled_stage_unchecked(&1);
        assert_eq!(join_node.augmented_stage.exchange_source.len(), 2);
        assert!(join_node.augmented_stage.exchange_source.get(&2).is_some());
        assert!(join_node.augmented_stage.exchange_source.get(&3).is_some());
        assert_eq!(join_node.assignments.len(), 3);
        assert_eq!(join_node.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(join_node.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(join_node.assignments.get(&2).unwrap(), &worker3);

        let scan_node_1 = scheduler.get_scheduled_stage_unchecked(&2);
        assert_eq!(scan_node_1.augmented_stage.exchange_source.len(), 0);
        assert_eq!(scan_node_1.assignments.len(), 3);
        assert_eq!(scan_node_1.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(scan_node_1.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(scan_node_1.assignments.get(&2).unwrap(), &worker3);

        let scan_node_2 = scheduler.get_scheduled_stage_unchecked(&2);
        assert_eq!(scan_node_2.augmented_stage.exchange_source.len(), 0);
        assert_eq!(scan_node_2.assignments.len(), 3);
        assert_eq!(scan_node_2.assignments.get(&0).unwrap(), &worker1);
        assert_eq!(scan_node_2.assignments.get(&1).unwrap(), &worker2);
        assert_eq!(scan_node_2.assignments.get(&2).unwrap(), &worker3);

        // Check that the serialized exchange source node has been filled with correct info.
        let prost_node_root = root.augmented_stage.to_prost(0, &query);
        assert_eq!(
            prost_node_root.exchange_info.unwrap().mode,
            DistributionMode::Single as i32
        );
        assert_eq!(prost_node_root.root.clone().unwrap().children.len(), 0);
        if let risingwave_pb::plan::plan_node::NodeBody::Exchange(exchange) =
            prost_node_root.root.unwrap().node_body.unwrap()
        {
            assert_eq!(exchange.sources.len(), 3);
            assert_eq!(exchange.input_schema.len(), 4);
        } else {
            panic!("The root node should be exchange single");
        }

        let prost_join_node = join_node.augmented_stage.to_prost(0, &query);
        assert_eq!(prost_join_node.root.as_ref().unwrap().children.len(), 2);
        assert_eq!(
            prost_join_node.exchange_info.unwrap().mode,
            DistributionMode::Hash as i32
        );
        if let risingwave_pb::plan::plan_node::NodeBody::HashJoin(_) = prost_join_node
            .root
            .as_ref()
            .unwrap()
            .node_body
            .as_ref()
            .unwrap()
        {
        } else {
            panic!("The node should be hash join node");
        }

        let exchange_1 = prost_join_node.root.as_ref().unwrap().children[0].clone();
        if let risingwave_pb::plan::plan_node::NodeBody::Exchange(exchange) =
            exchange_1.node_body.unwrap()
        {
            assert_eq!(exchange.sources.len(), 3);
            assert_eq!(exchange.input_schema.len(), 2);
        } else {
            panic!("The node should be exchange node");
        }
    }

    fn generate_parallel_units(start_id: u32, node_id: u32) -> Vec<ParallelUnit> {
        let parallel_degree = 8;
        let mut parallel_units = vec![ParallelUnit {
            id: start_id,
            r#type: ParallelUnitType::Single as i32,
            worker_node_id: node_id,
        }];
        for id in start_id + 1..start_id + parallel_degree {
            parallel_units.push(ParallelUnit {
                id,
                r#type: ParallelUnitType::Hash as i32,
                worker_node_id: node_id,
            });
        }
        parallel_units
    }
}
