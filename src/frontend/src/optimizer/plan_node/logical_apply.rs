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
//
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::expr::CorrelatedId;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalApply` represents a correlated join, where the right side may refer to columns from the
/// left side.
#[derive(Debug, Clone)]
pub struct LogicalApply {
    pub base: PlanBase,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,

    /// Id of the Apply operator.
    /// So correlated_input_ref can refer the Apply operator exactly by correlated_id.
    correlated_id: CorrelatedId,
    /// The indices of `CorrelatedInputRef`s in `right`.
    correlated_indices: Vec<usize>,
}

impl fmt::Display for LogicalApply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalApply {{ type: {:?}, on: {}, correlated_id: {} }}",
            &self.join_type,
            {
                let mut concat_schema = self.left().schema().fields.clone();
                concat_schema.extend(self.right().schema().fields.clone());
                let concat_schema = Schema::new(concat_schema);
                format!(
                    "{}",
                    ConditionDisplay {
                        condition: &self.on,
                        input_schema: &concat_schema
                    }
                )
            },
            self.correlated_id
        )
    }
}

impl LogicalApply {
    pub(crate) fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
    ) -> Self {
        let ctx = left.ctx();
        let out_column_num =
            LogicalJoin::out_column_num(left.schema().len(), right.schema().len(), join_type);
        let output_indices = (0..out_column_num).collect::<Vec<_>>();
        let schema =
            LogicalJoin::derive_schema(left.schema(), right.schema(), join_type, &output_indices);
        let pk_indices = LogicalJoin::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.pk_indices(),
            right.pk_indices(),
            join_type,
            &output_indices,
        );
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalApply {
            base,
            left,
            right,
            on,
            join_type,
            correlated_id,
            correlated_indices,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
    ) -> PlanRef {
        Self::new(
            left,
            right,
            join_type,
            on,
            correlated_id,
            correlated_indices,
        )
        .into()
    }

    /// Get the join type of the logical apply.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn decompose(
        self,
    ) -> (
        PlanRef,
        PlanRef,
        Condition,
        JoinType,
        CorrelatedId,
        Vec<usize>,
    ) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.correlated_id,
            self.correlated_indices,
        )
    }

    pub fn correlated_id(&self) -> CorrelatedId {
        self.correlated_id
    }
}

impl PlanTreeNodeBinary for LogicalApply {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            left,
            right,
            self.join_type,
            self.on.clone(),
            self.correlated_id,
            self.correlated_indices.clone(),
        )
    }
}

impl_plan_tree_node_for_binary! { LogicalApply }

impl ColPrunable for LogicalApply {
    fn prune_col(&self, _: &[usize]) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl PredicatePushdown for LogicalApply {
    fn predicate_pushdown(&self, _predicate: Condition) -> PlanRef {
        panic!("LogicalApply should be unnested")
    }
}

impl ToBatch for LogicalApply {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }
}

impl ToStream for LogicalApply {
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Err(RwError::from(ErrorCode::InternalError(
            "LogicalApply should be unnested".to_string(),
        )))
    }
}
