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

use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::TableDesc;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::StreamNode as ProstStreamPlan;

use super::{LogicalScan, PlanBase, PlanNodeId, StreamIndexScan, ToStreamProst};
use crate::catalog::ColumnId;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::property::{Distribution, DistributionDisplay};

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone)]
pub struct StreamTableScan {
    pub base: PlanBase,
    logical: LogicalScan,
    batch_plan_id: PlanNodeId,
}

impl StreamTableScan {
    pub fn new(logical: LogicalScan) -> Self {
        let ctx = logical.base.ctx.clone();

        let batch_plan_id = ctx.next_plan_node_id();

        let distribution = {
            let distribution_key = logical
                .distribution_key()
                .expect("distribution key of stream chain must exist in output columns");
            if distribution_key.is_empty() {
                Distribution::Single
            } else {
                // Follows upstream distribution from TableCatalog
                Distribution::HashShard(distribution_key)
            }
        };
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.pk_indices.clone(),
            distribution,
            logical.table_desc().appendonly,
        );
        Self {
            base,
            logical,
            batch_plan_id,
        }
    }

    pub fn table_name(&self) -> &str {
        self.logical.table_name()
    }

    pub fn logical(&self) -> &LogicalScan {
        &self.logical
    }

    pub fn to_index_scan(&self, index_name: &str, index: &Rc<TableDesc>) -> StreamIndexScan {
        StreamIndexScan::new(self.logical.to_index_scan(index_name, index))
    }
}

impl_plan_tree_node_for_leaf! { StreamTableScan }

impl fmt::Display for StreamTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("StreamTableScan");

        builder
            .field("table", &format_args!("{}", self.logical.table_name()))
            .field(
                "columns",
                &format_args!(
                    "[{}]",
                    match verbose {
                        false => self.logical.column_names(),
                        true => self.logical.column_names_with_table_prefix(),
                    }
                    .join(", ")
                ),
            );

        if verbose {
            builder.field(
                "pk",
                &IndicesDisplay {
                    indices: self.pk_indices(),
                    input_schema: &self.base.schema,
                },
            );
            builder.field(
                "distribution",
                &DistributionDisplay {
                    distribution: self.distribution(),
                    input_schema: &self.base.schema,
                },
            );
        }

        builder.finish()
    }
}

impl ToStreamProst for StreamTableScan {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamTableScan {
    pub fn adhoc_to_stream_prost(&self, auto_fields: bool) -> ProstStreamPlan {
        use risingwave_pb::plan_common::*;
        use risingwave_pb::stream_plan::*;

        let batch_plan_node = BatchPlanNode {
            table_desc: Some(self.logical.table_desc().to_protobuf()),
            column_ids: self
                .logical
                .output_column_ids()
                .iter()
                .map(ColumnId::get_id)
                .collect(),
        };

        let pk_indices = self.base.pk_indices.iter().map(|x| *x as u32).collect_vec();

        ProstStreamPlan {
            fields: self.schema().to_prost(),
            input: vec![
                // The merge node should be empty
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::Merge(Default::default())),
                    ..Default::default()
                },
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::BatchPlan(batch_plan_node)),
                    operator_id: if auto_fields {
                        self.batch_plan_id.0 as u64
                    } else {
                        0
                    },
                    identity: if auto_fields { "BatchPlanNode" } else { "" }.into(),
                    pk_indices: pk_indices.clone(),
                    input: vec![],
                    fields: vec![], // TODO: fill this later
                    append_only: true,
                },
            ],
            node_body: Some(ProstStreamNode::Chain(ChainNode {
                table_id: self.logical.table_desc().table_id.table_id,
                same_worker_node: false,
                disable_rearrange: false,
                // The fields from upstream
                upstream_fields: self
                    .logical
                    .table_desc()
                    .columns
                    .iter()
                    .map(|x| Field {
                        data_type: Some(x.data_type.to_protobuf()),
                        name: x.name.clone(),
                    })
                    .collect(),
                // The column idxs need to be forwarded to the downstream
                column_ids: self
                    .logical
                    .column_descs()
                    .iter()
                    .map(|x| x.column_id.get_id())
                    .collect(),
                is_singleton: *self.distribution() == Distribution::Single,
            })),
            pk_indices,
            operator_id: if auto_fields {
                self.base.id.0 as u64
            } else {
                0
            },
            identity: if auto_fields {
                format!("{}", self)
            } else {
                "".into()
            },
            append_only: self.append_only(),
        }
    }
}
