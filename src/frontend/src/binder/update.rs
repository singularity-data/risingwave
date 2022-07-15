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

use std::assert_matches::assert_matches;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::ensure;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_sqlparser::ast::{Assignment, Expr, TableFactor, TableWithJoins};

use super::{Binder, BoundTableSource, Relation};
use crate::expr::{Expr as _, ExprImpl};

#[derive(Debug)]
pub struct BoundUpdate {
    /// Used for injecting new chunks to the source.
    pub table_source: BoundTableSource,

    /// Used for scanning the records to update with the `selection`.
    pub table: Relation,

    pub selection: Option<ExprImpl>,

    /// Expression used to project to the updated row. The assigned columns will use the new
    /// expression, and the other columns will be simply `InputRef`.
    pub exprs: Vec<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    ) -> Result<BoundUpdate> {
        let table_source = {
            ensure!(table.joins.is_empty());
            let name = match &table.relation {
                TableFactor::Table { name, .. } => name.clone(),
                _ => unreachable!(),
            };
            let (schema_name, table_name) = Self::resolve_table_name(name.clone())?;
            self.bind_table_source(name).map_err(|err| {
                if let Ok(table) = self.catalog.get_table_by_name(&self.db_name, &schema_name,
                &table_name)      && table.associated_source_id().is_none(){
                    return RwError::from(ErrorCode::InvalidInputSyntax(
                        format!("cannot change materialized view {}",table_name)
                    ));
                }
                err
            })?
        };

        if table_source.append_only {
            return Err(ErrorCode::BindError(
                "Append-only table source doesn't support update".to_string(),
            )
            .into());
        }

        let table = self.bind_vec_table_with_joins(vec![table])?.unwrap();
        assert_matches!(table, Relation::BaseTable(_));

        let selection = selection.map(|expr| self.bind_expr(expr)).transpose()?;

        let mut assignment_exprs = HashMap::new();
        for Assignment { id, value } in assignments {
            // FIXME: Parsing of `id` is not strict. It will even treat `a.b` as `(a, b)`.
            let assignments = match (id.as_slice(), value) {
                // col = expr
                ([id], value) => {
                    vec![(id.clone(), value)]
                }

                // (col1, col2) = (subquery)
                (_ids, Expr::Subquery(_)) => {
                    return Err(ErrorCode::NotImplemented(
                        "subquery on the right side of multi-assignment".to_owned(),
                        None.into(),
                    )
                    .into())
                }
                // (col1, col2) = (expr1, expr2)
                (ids, Expr::Row(values)) if ids.len() == values.len() => {
                    id.into_iter().zip_eq(values.into_iter()).collect()
                }
                // (col1, col2) = <other expr>
                _ => {
                    return Err(ErrorCode::BindError(
                        "number of columns does not match number of values".to_owned(),
                    )
                    .into())
                }
            };

            for (id, value) in assignments {
                let id_expr = self.bind_expr(Expr::Identifier(id.clone()))?;
                let value_expr = self.bind_expr(value)?.cast_assign(id_expr.return_type())?;

                match assignment_exprs.entry(id_expr) {
                    Entry::Occupied(_) => {
                        return Err(ErrorCode::BindError(
                            "multiple assignments to same column".to_owned(),
                        )
                        .into())
                    }
                    Entry::Vacant(v) => {
                        v.insert(value_expr);
                    }
                }
            }
        }

        let all_columns = Self::iter_bound_columns(self.context.columns.iter()).0;
        let exprs = all_columns
            .into_iter()
            .map(|c| assignment_exprs.remove(&c).unwrap_or(c))
            .collect_vec();

        Ok(BoundUpdate {
            table_source,
            table,
            selection,
            exprs,
        })
    }
}
