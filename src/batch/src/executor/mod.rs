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
use anyhow::anyhow;
mod delete;
mod expand;
mod filter;
mod generic_exchange;
mod hash_agg;
mod hop_window;
mod insert;
mod join;
mod limit;
mod merge_sort_exchange;
pub mod monitor;
mod order_by;
mod project;
mod project_set;
mod row_seq_scan;
mod sort_agg;
mod sys_row_seq_scan;
mod table_function;
pub mod test_utils;
mod top_n;
mod trace;
mod update;
mod values;

use async_recursion::async_recursion;
pub use delete::*;
pub use expand::*;
pub use filter::*;
use futures::stream::BoxStream;
pub use generic_exchange::*;
pub use hash_agg::*;
pub use hop_window::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use merge_sort_exchange::*;
pub use monitor::*;
pub use order_by::*;
pub use project::*;
pub use project_set::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::PlanNode;
pub use row_seq_scan::*;
pub use sort_agg::*;
pub use table_function::*;
pub use top_n::*;
pub use trace::*;
pub use update::*;
pub use values::*;

use crate::executor::sys_row_seq_scan::SysRowSeqScanExecutorBuilder;
use crate::task::{BatchTaskContext, TaskId};

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedDataChunkStream = BoxStream<'static, Result<DataChunk>>;

pub struct ExecutorInfo {
    pub schema: Schema,
    pub id: String,
}

/// Refactoring of `Executor` using `Stream`.
pub trait Executor: Send + 'static {
    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Executes to return the data chunk stream.
    ///
    /// The implementation should guaranteed that each `DataChunk`'s cardinality is not zero.
    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor`
/// from proto and global environment.
#[async_trait::async_trait]
pub trait BoxedExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor>;
}

pub struct ExecutorBuilder<'a, C> {
    pub plan_node: &'a PlanNode,
    pub task_id: &'a TaskId,
    context: C,
    epoch: u64,
}

macro_rules! build_executor {
    ($source: expr, $inputs: expr, $($proto_type_name:path => $data_type:ty),* $(,)?) => {
        match $source.plan_node().get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor($source, $inputs)
                },
            )*
        }
    }
}

impl<'a, C: Clone> ExecutorBuilder<'a, C> {
    pub fn new(plan_node: &'a PlanNode, task_id: &'a TaskId, context: C, epoch: u64) -> Self {
        Self {
            plan_node,
            task_id,
            context,
            epoch,
        }
    }

    #[must_use]
    pub fn clone_for_plan(&self, plan_node: &'a PlanNode) -> Self {
        ExecutorBuilder::new(plan_node, self.task_id, self.context.clone(), self.epoch)
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl<'a, C: BatchTaskContext> ExecutorBuilder<'a, C> {
    pub async fn build(&self) -> Result<BoxedExecutor> {
        self.try_build().await.map_err(|e| {
            anyhow!(format!(
                "[PlanNode: {:?}] Failed to build executor: {}",
                self.plan_node.get_node_body(),
                e,
            ))
            .into()
        })
    }

    #[async_recursion]
    async fn try_build(&self) -> Result<BoxedExecutor> {
        let mut inputs = Vec::with_capacity(self.plan_node.children.len());
        for input_node in &self.plan_node.children {
            let input = self.clone_for_plan(input_node).build().await?;
            inputs.push(input);
        }

        let real_executor = build_executor! { self, inputs,
            NodeBody::RowSeqScan => RowSeqScanExecutorBuilder,
            NodeBody::Insert => InsertExecutor,
            NodeBody::Delete => DeleteExecutor,
            NodeBody::Exchange => GenericExchangeExecutorBuilder,
            NodeBody::Update => UpdateExecutor,
            NodeBody::Filter => FilterExecutor,
            NodeBody::Project => ProjectExecutor,
            NodeBody::SortAgg => SortAggExecutor,
            NodeBody::OrderBy => OrderByExecutor,
            NodeBody::TopN => TopNExecutor,
            NodeBody::Limit => LimitExecutor,
            NodeBody::Values => ValuesExecutor,
            NodeBody::NestedLoopJoin => NestedLoopJoinExecutor,
            NodeBody::HashJoin => HashJoinExecutor<()>,
            NodeBody::SortMergeJoin => SortMergeJoinExecutor,
            NodeBody::HashAgg => HashAggExecutorBuilder,
            NodeBody::MergeSortExchange => MergeSortExchangeExecutorBuilder,
            NodeBody::TableFunction => TableFunctionExecutorBuilder,
            NodeBody::HopWindow => HopWindowExecutor,
            NodeBody::SysRowSeqScan => SysRowSeqScanExecutorBuilder,
            NodeBody::Expand => ExpandExecutor,
            NodeBody::LookupJoin => LookupJoinExecutorBuilder,
            NodeBody::ProjectSet => ProjectSetExecutor,
        }
        .await?;
        let input_desc = real_executor.identity().to_string();
        Ok(Box::new(TraceExecutor::new(real_executor, input_desc)) as BoxedExecutor)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::batch_plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{ComputeNodeContext, TaskId};

    #[test]
    fn test_clone_for_plan() {
        let plan_node = PlanNode {
            ..Default::default()
        };
        let task_id = &TaskId {
            task_id: 1,
            stage_id: 1,
            query_id: "test_query_id".to_string(),
        };
        let builder = ExecutorBuilder::new(
            &plan_node,
            task_id,
            ComputeNodeContext::new_for_test(),
            u64::MAX,
        );
        let child_plan = &PlanNode {
            ..Default::default()
        };
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
