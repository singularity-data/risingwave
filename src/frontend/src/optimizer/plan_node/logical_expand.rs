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

use itertools::Itertools;
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::types::DataType;

use super::{
    gen_filter_and_pushdown, BatchExpand, ColPrunable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamExpand, ToBatch, ToStream,
};
use crate::expr::InputRef;
use crate::risingwave_common::error::Result;
use crate::utils::{ColIndexMapping, Condition};

/// [`LogicalExpand`] expand one row multiple times according to `column_subsets`.
///
/// It can be used to implement distinct aggregation and group set.
#[derive(Debug, Clone)]
pub struct LogicalExpand {
    pub base: PlanBase,
    // `column_subsets` has many `subset`s which specifies the columns that need to be
    // reserved and other columns will be filled with NULL.
    column_subsets: Vec<Vec<usize>>,
    input: PlanRef,
}

impl LogicalExpand {
    pub fn new(input: PlanRef, column_subsets: Vec<Vec<usize>>) -> Self {
        let input_schema_len = input.schema().len();
        for key in column_subsets.iter().flatten() {
            assert!(*key < input_schema_len);
        }
        // The last column should be the flag.
        let mut pk_indices = input.pk_indices().to_vec();
        pk_indices.push(input_schema_len);

        let schema = Self::derive_schema(input.schema());
        let ctx = input.ctx();
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalExpand {
            base,
            column_subsets,
            input,
        }
    }

    pub fn create(input: PlanRef, column_subsets: Vec<Vec<usize>>) -> PlanRef {
        Self::new(input, column_subsets).into()
    }

    fn derive_schema(input_schema: &Schema) -> Schema {
        let mut fields = input_schema.clone().into_fields();
        fields.push(Field::with_name(DataType::Int64, "flag"));
        Schema::new(fields)
    }

    pub fn column_subsets(&self) -> &Vec<Vec<usize>> {
        &self.column_subsets
    }

    pub fn column_subsets_display(&self) -> Vec<Vec<FieldDisplay>> {
        self.column_subsets()
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .map(|&i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
                    .collect_vec()
            })
            .collect_vec()
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ column_subsets: {:?} }}",
            name,
            self.column_subsets_display()
        )
    }
}

impl PlanTreeNodeUnary for LogicalExpand {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.column_subsets.clone())
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let column_subsets = self
            .column_subsets
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .filter_map(|i| input_col_change.try_map(*i))
                    .collect_vec()
            })
            .collect_vec();
        let (mut map, new_input_col_num) = input_col_change.into_parts();
        assert_eq!(new_input_col_num, input.schema().len());
        map.push(Some(new_input_col_num));

        (Self::new(input, column_subsets), ColIndexMapping::new(map))
    }
}

impl_plan_tree_node_for_unary! {LogicalExpand}

impl fmt::Display for LogicalExpand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalExpand")
    }
}

impl ColPrunable for LogicalExpand {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let pos_of_flag = self.input.schema().len();
        let input_required_cols = required_cols
            .iter()
            .copied()
            .filter(|i| *i != pos_of_flag)
            .collect_vec();
        let new_input = self.input.prune_col(&input_required_cols);
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input.schema().len(),
        );
        let (new_expand, col_change) = self.rewrite_with_input(new_input, input_col_change);

        let exprs = required_cols
            .iter()
            .map(|col| {
                let mapped_col = col_change.map(*col);
                let data_type = new_expand.base.schema.fields[mapped_col].data_type();
                InputRef::new(mapped_col, data_type).into()
            })
            .collect_vec();
        LogicalProject::create(new_expand.into(), exprs)
    }
}

impl PredicatePushdown for LogicalExpand {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        // TODO: how to do predicate pushdown for Expand?
        //
        // let new_input = self.input.predicate_pushdown(predicate);
        // self.clone_with_input(new_input).into()

        gen_filter_and_pushdown(self, predicate, Condition::true_cond())
    }
}

impl ToBatch for LogicalExpand {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input.to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchExpand::new(new_logical).into())
    }
}

impl ToStream for LogicalExpand {
    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (expand, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((expand.into(), out_col_change))
    }

    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        todo!() // TODO(kaige):
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use crate::optimizer::plan_node::{
        LogicalExpand, LogicalProject, LogicalValues, PlanTreeNodeUnary,
    };
    use crate::session::OptimizerContext;

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Expand(column_subsets: [[0, 1], [2]]
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0, 2, 3, 1] will result in
    /// ```text
    /// Project(input_ref(0), input_ref(1), input_ref(3), input_ref(2))
    ///   Expand(column_subsets: [[0, 2], [1]])
    ///     TableScan(v1, v3, v2)
    /// ```
    async fn test_prune_expand() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int32, "v3"),
        ];
        let values = LogicalValues::new(vec![], Schema { fields }, ctx);

        let column_subsets = vec![vec![0, 1], vec![2]];
        let expand = LogicalExpand::create(values.into(), column_subsets);

        // Perform the prune
        let required_cols = vec![0, 2, 3, 1];
        let plan = expand.prune_col(&required_cols);

        // Check the result
        let project: &LogicalProject = plan.as_logical_project().unwrap();
        let expected_schema = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v3"),
            Field::with_name(DataType::Int64, "flag"),
            Field::with_name(DataType::Int32, "v2"),
        ];
        assert_eq!(expected_schema, project.base.schema.fields().to_owned());

        let expand = project.input();
        let expand: &LogicalExpand = expand.as_logical_expand().unwrap();
        let expected_schema = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v3"),
            Field::with_name(DataType::Int32, "v2"),
            Field::with_name(DataType::Int64, "flag"),
        ];
        let expected_column_subsets = vec![vec![0, 2], vec![1]];
        assert_eq!(expected_schema, expand.base.schema.fields().to_owned());
        assert_eq!(expected_column_subsets, expand.column_subsets.to_owned());

        let values = expand.input();
        let values: &LogicalValues = values.as_logical_values().unwrap();
        let expected_schema = vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v3"),
            Field::with_name(DataType::Int32, "v2"),
        ];
        assert_eq!(expected_schema, values.base.schema.fields().to_owned());
    }
}
