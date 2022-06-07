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
use std::fmt;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Schema, TableDesc};
use risingwave_common::error::Result;

use super::{
    ColPrunable, PlanBase, PlanRef, PredicatePushdown, StreamTableScan, ToBatch, ToStream,
};
use crate::expr::{CollectInputRef, ExprImpl, InputRef};
use crate::optimizer::plan_node::{BatchSeqScan, LogicalFilter, LogicalProject};
use crate::session::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalScan {
    pub base: PlanBase,
    table_name: String, // explain-only
    /// Include `output_col_idx` and columns required in `predicate`
    required_col_idx: Vec<usize>,
    output_col_idx: Vec<usize>,
    // Descriptor of the table
    table_desc: Rc<TableDesc>,
    // Descriptors of all indexes on this table
    indexes: Vec<(String, Rc<TableDesc>)>,
    /// The pushed down predicates. It refers to column indexes of the table.
    predicate: Condition,
}

impl LogicalScan {
    /// Create a `LogicalScan` node. Used internally by optimizer.
    fn new(
        table_name: String,         // explain-only
        output_col_idx: Vec<usize>, // the column index in the table
        table_desc: Rc<TableDesc>,
        indexes: Vec<(String, Rc<TableDesc>)>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
    ) -> Self {
        // here we have 3 concepts
        // 1. column_id: ColumnId, stored in catalog and a ID to access data from storage.
        // 2. table_idx: usize, column index in the TableDesc or tableCatalog.
        // 3. operator_idx: usize,  column index in the ScanOperator's schema.
        // in a query we get the same version of catalog, so the mapping from column_id and
        // table_idx will not changes. and the `required_col_idx is the `table_idx` of the
        // required columns, in other word, is the mapping from operator_idx to table_idx.

        let mut id_to_op_idx = HashMap::new();

        let fields = output_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| {
                let col = &table_desc.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
                col.into()
            })
            .collect();

        let pk_indices = table_desc
            .pks
            .iter()
            .map(|&c| id_to_op_idx.get(&table_desc.columns[c].column_id).copied())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();

        let schema = Schema { fields };
        let base = PlanBase::new_logical(ctx, schema, pk_indices);

        let mut required_col_idx = output_col_idx.clone();
        let mut visitor =
            CollectInputRef::new(FixedBitSet::with_capacity(table_desc.columns.len()));
        predicate.visit_expr(&mut visitor);
        let predicate_col_idx: FixedBitSet = visitor.into();
        predicate_col_idx.ones().for_each(|idx| {
            if !required_col_idx.contains(&idx) {
                required_col_idx.push(idx);
            }
        });

        Self {
            base,
            table_name,
            required_col_idx,
            output_col_idx,
            table_desc,
            indexes,
            predicate,
        }
    }

    /// Create a [`LogicalScan`] node. Used by planner.
    pub fn create(
        table_name: String, // explain-only
        table_desc: Rc<TableDesc>,
        indexes: Vec<(String, Rc<TableDesc>)>,
        ctx: OptimizerContextRef,
    ) -> Self {
        Self::new(
            table_name,
            (0..table_desc.columns.len()).into_iter().collect(),
            table_desc,
            indexes,
            ctx,
            Condition::true_cond(),
        )
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].name.clone())
            .collect()
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get a reference to the logical scan's table desc.
    #[must_use]
    pub fn table_desc(&self) -> &TableDesc {
        self.table_desc.as_ref()
    }

    /// Get a reference to the logical scan's table desc. The columns are not exactly the same as
    /// the original table as being pruned, but will have the same order as the required input.
    #[must_use]
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|i| self.table_desc.columns[*i].clone())
            .collect()
    }

    /// Get all indexes on this table
    #[must_use]
    pub fn indexes(&self) -> &[(String, Rc<TableDesc>)] {
        &self.indexes
    }

    /// distribution keys stored in catalog only contains column index of the table (`table_idx`),
    /// so we need to convert it to `operator_idx` when filling distributions.
    pub fn map_distribution_keys(&self) -> Vec<usize> {
        let tb_idx_to_op_idx = self
            .required_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_desc
            .distribution_keys
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx[&tb_idx])
            .collect()
    }

    pub fn to_index_scan(&self, index_name: &str, index: &Rc<TableDesc>) -> LogicalScan {
        let mut new_required_col_idx = Vec::with_capacity(self.required_col_idx.len());
        let all_columns = index
            .columns
            .iter()
            .enumerate()
            .map(|(idx, desc)| (desc.column_id, idx))
            .collect::<HashMap<_, _>>();

        // create index scan plan to match the output order of the current table scan
        for &col_idx in &self.required_col_idx {
            let column_idx_in_index = all_columns[&self.table_desc.columns[col_idx].column_id];
            new_required_col_idx.push(column_idx_in_index);
        }

        Self::new(
            index_name.to_string(),
            new_required_col_idx,
            index.clone(),
            vec![],
            self.ctx(),
            self.predicate.clone(),
        )
    }

    /// a vec of `InputRef` corresponding to `output_col_idx`, which can represent a pulled project.
    fn output_idx_to_input_ref(&self) -> Vec<ExprImpl> {
        let output_idx = self
            .output_col_idx
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                InputRef::new(i, self.table_desc.columns[col_idx].data_type.clone()).into()
            })
            .collect_vec();
        output_idx
    }

    /// Undo predicate push down when predicate in scan is not supported.
    fn predicate_pull_up(&self) -> PlanRef {
        let mut predicate = self.predicate.clone();
        if predicate.always_true() {
            return self.clone().into();
        }

        let mut mapping =
            ColIndexMapping::new(self.required_col_idx.iter().map(|i| Some(*i)).collect())
                .inverse();
        predicate = predicate.rewrite_expr(&mut mapping);

        let scan_without_predicate = Self::new(
            self.table_name.clone(),
            self.required_col_idx.clone(),
            self.table_desc.clone(),
            self.indexes.clone(),
            self.ctx(),
            Condition::true_cond(),
        );
        let mut plan = LogicalFilter::create(scan_without_predicate.into(), predicate);
        if self.required_col_idx != self.output_col_idx {
            plan = LogicalProject::create(plan, self.output_idx_to_input_ref());
        }
        assert_eq!(self.schema(), plan.schema());
        plan
    }

    fn clone_with_predicate(&self, predicate: Condition) -> Self {
        Self::new(
            self.table_name.clone(),
            self.required_col_idx.clone(),
            self.table_desc.clone(),
            self.indexes.clone(),
            self.base.ctx.clone(),
            predicate,
        )
    }

    fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        Self::new(
            self.table_name.clone(),
            output_col_idx,
            self.table_desc.clone(),
            self.indexes.clone(),
            self.base.ctx.clone(),
            self.predicate.clone(),
        )
    }
}

impl_plan_tree_node_for_leaf! {LogicalScan}

impl fmt::Display for LogicalScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.predicate.always_true() {
            write!(
                f,
                "LogicalScan {{ table: {}, columns: [{}] }}",
                self.table_name,
                self.column_names().join(", "),
            )
        } else {
            let required_col_names = self
                .required_col_idx
                .iter()
                .map(|i| format!("${}:{}", i, self.table_desc.columns[*i].name))
                .collect_vec();

            write!(
                f,
                "LogicalScan {{ table: {}, output_columns: [{}], required_columns: [{}], predicate: {} }}",
                self.table_name,
                self.column_names().join(", "),
                required_col_names.join(", "),
                self.predicate,
            )
        }
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.required_col_idx[*i])
            .collect();
        assert!(output_col_idx
            .iter()
            .all(|i| self.output_col_idx.contains(i)));

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl PredicatePushdown for LogicalScan {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        let predicate = predicate.rewrite_expr(&mut ColIndexMapping::new(
            self.output_col_idx.iter().map(|i| Some(*i)).collect(),
        ));

        self.clone_with_predicate(predicate.and(self.predicate.clone()))
            .into()
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> Result<PlanRef> {
        if self.predicate.always_true() {
            Ok(BatchSeqScan::new(self.clone()).into())
        } else {
            self.predicate_pull_up().to_batch()
        }
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self) -> Result<PlanRef> {
        if self.predicate.always_true() {
            Ok(StreamTableScan::new(self.clone()).into())
        } else {
            self.predicate_pull_up().to_stream()
        }
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        match self.base.pk_indices.is_empty() {
            true => {
                let mut col_ids = HashSet::new();

                for idx in &self.required_col_idx {
                    col_ids.insert(self.table_desc.columns[*idx].column_id);
                }
                let mut col_id_to_tb_idx = HashMap::new();
                for (tb_idx, c) in self.table_desc().columns.iter().enumerate() {
                    col_id_to_tb_idx.insert(c.column_id, tb_idx);
                }
                let col_need_to_add = self
                    .table_desc
                    .order_desc
                    .iter()
                    .filter(|c| !col_ids.contains(&c.column_desc.column_id))
                    .map(|c| col_id_to_tb_idx.get(&c.column_desc.column_id).unwrap())
                    .collect_vec();

                let mut output_col_idx = self.output_col_idx.clone();
                output_col_idx.extend(col_need_to_add);
                let new_len = output_col_idx.len();
                Ok((
                    self.clone_with_output_indices(output_col_idx).into(),
                    ColIndexMapping::identity_or_none(self.schema().len(), new_len),
                ))
            }
            false => Ok((
                self.clone().into(),
                ColIndexMapping::identity(self.schema().len()),
            )),
        }
    }
}
