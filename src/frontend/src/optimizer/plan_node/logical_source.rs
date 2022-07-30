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

use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};

use super::{
    ColPrunable, LogicalFilter, LogicalProject, PlanBase, PlanRef, PredicatePushdown, StreamSource,
    ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::session::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalSource {
    pub base: PlanBase,
    pub source_catalog: Rc<SourceCatalog>,
}

impl LogicalSource {
    pub fn new(source_catalog: Rc<SourceCatalog>, ctx: OptimizerContextRef) -> Self {
        let mut id_to_idx = HashMap::new();

        let fields = source_catalog
            .columns
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                id_to_idx.insert(c.column_id(), idx);
                (&c.column_desc).into()
            })
            .collect();
        let pk_indices = source_catalog
            .pk_col_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let schema = Schema { fields };
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalSource {
            base,
            source_catalog,
        }
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn source_catalog(&self) -> Rc<SourceCatalog> {
        self.source_catalog.clone()
    }
}

impl_plan_tree_node_for_leaf! {LogicalSource}

impl fmt::Display for LogicalSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalSource {{ source: {}, columns: [{}] }}",
            self.source_catalog.name,
            self.column_names().join(", ")
        )
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::NotImplemented(
            "there is no batch source operator".to_string(),
            None.into(),
        )))
    }
}

impl ToStream for LogicalSource {
    fn to_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            StreamSource::new(self.clone()).into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
