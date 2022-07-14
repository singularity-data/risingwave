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

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Cte, Expr, OrderByExpr, Query, Value, With};

use crate::binder::{Binder, BoundSetExpr};
use crate::expr::ExprImpl;
use crate::optimizer::property::{Direction, FieldOrder};

/// A validated sql query, including order and union.
/// An example of its relationship with `BoundSetExpr` and `BoundSelect` can be found here: <https://bit.ly/3GQwgPz>
#[derive(Debug, Clone)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
    pub order: Vec<FieldOrder>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub extra_order_exprs: Vec<ExprImpl>,
}

impl BoundQuery {
    /// The schema returned by this [`BoundQuery`].
    pub fn schema(&self) -> &Schema {
        self.body.schema()
    }

    /// The types returned by this [`BoundQuery`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.schema().data_types()
    }

    pub fn is_correlated(&self) -> bool {
        self.body.is_correlated()
            || self
                .extra_order_exprs
                .iter()
                .any(|e| e.has_correlated_input_ref())
    }

    pub fn collect_correlated_indices(&self) -> Vec<usize> {
        // TODO: collect `correlated_input_ref` in `extra_order_exprs`.
        self.body.collect_correlated_indices()
    }
}

impl Binder {
    /// Bind a [`Query`].
    ///
    /// Before binding the [`Query`], we push the current [`BindContext`](super::BindContext) to the
    /// stack and create a new context, because it may be a subquery.
    ///
    /// After finishing binding, we pop the previous context from the stack.
    pub fn bind_query(&mut self, query: Query) -> Result<BoundQuery> {
        self.push_context();
        let result = self.bind_query_inner(query);
        self.pop_context()?;
        result
    }

    /// Bind a [`Query`] using the current [`BindContext`](super::BindContext).
    pub(super) fn bind_query_inner(&mut self, query: Query) -> Result<BoundQuery> {
        let limit = query.get_limit_value();
        let offset = query.get_offset_value();
        if let Some(with) = query.with {
            self.bind_with(with)?;
        }
        let body = self.bind_set_expr(query.body)?;
        let mut name_to_index = HashMap::new();
        body.schema()
            .fields()
            .iter()
            .enumerate()
            .for_each(|(index, field)| {
                name_to_index
                    .entry(field.name.clone())
                    // Ambiguous (duplicate) output names are marked with usize::MAX.
                    // This is not necessarily an error as long as not actually referenced by order
                    // by.
                    .and_modify(|v| *v = usize::MAX)
                    .or_insert(index);
            });
        let mut extra_order_exprs = vec![];
        let visible_output_num = body.schema().len();
        let order = query
            .order_by
            .into_iter()
            .map(|order_by_expr| {
                self.bind_order_by_expr(
                    order_by_expr,
                    &name_to_index,
                    &mut extra_order_exprs,
                    visible_output_num,
                )
            })
            .collect::<Result<_>>()?;
        Ok(BoundQuery {
            body,
            order,
            limit,
            offset,
            extra_order_exprs,
        })
    }

    fn bind_order_by_expr(
        &mut self,
        order_by_expr: OrderByExpr,
        name_to_index: &HashMap<String, usize>,
        extra_order_exprs: &mut Vec<ExprImpl>,
        visible_output_num: usize,
    ) -> Result<FieldOrder> {
        let direct = match order_by_expr.asc {
            None | Some(true) => Direction::Asc,
            Some(false) => Direction::Desc,
        };
        let index = match order_by_expr.expr {
            Expr::Identifier(name) if let Some(index) = name_to_index.get(&name.value) => match *index != usize::MAX {
                true => *index,
                false => return Err(ErrorCode::BindError(format!("ORDER BY \"{}\" is ambiguous", name.value)).into()),
            }
            Expr::Value(Value::Number(number, _)) => match number.parse::<usize>() {
                Ok(index) if 1 <= index && index <= visible_output_num => index - 1,
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Invalid value in ORDER BY: {}",
                        number
                    ))
                    .into())
                }
            },
            expr => {
                extra_order_exprs.push(self.bind_expr(expr)?);
                visible_output_num + extra_order_exprs.len() - 1
            }
        };
        Ok(FieldOrder { index, direct })
    }

    fn bind_with(&mut self, with: With) -> Result<()> {
        if with.recursive {
            Err(ErrorCode::NotImplemented("recursive cte".into(), None.into()).into())
        } else {
            for cte_table in with.cte_tables {
                let Cte { alias, query, .. } = cte_table;
                let table_name = alias.name.value.clone();
                let bound_query = self.bind_query(query)?;
                self.cte_to_relation
                    .insert(table_name, (bound_query, alias));
            }
            Ok(())
        }
    }
}
