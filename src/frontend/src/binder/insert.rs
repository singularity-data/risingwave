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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, Query, SetExpr};

use super::{BoundQuery, BoundSetExpr};
use crate::binder::{Binder, BoundTableSource};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef, Literal};

#[derive(Debug)]
pub struct BoundInsert {
    /// Used for injecting deletion chunks to the source.
    pub table_source: BoundTableSource,

    pub source: BoundQuery,

    /// Used as part of an extra `Project` when the column types of `source` query does not match
    /// `table_source`. This does not include a simple `VALUE`. See comments in code for details.
    pub cast_exprs: Vec<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_insert(
        &mut self,
        source_name: ObjectName,
        _columns: Vec<Ident>,
        source: Query,
    ) -> Result<BoundInsert> {
        let (schema_name, table_name) = Self::resolve_table_name(source_name.clone())?;

        let table_source = self.bind_table_source(source_name).map_err(|err| {
            if let Ok(table) = self.catalog.get_table_by_name(&self.db_name, &schema_name,
                    &table_name)          && table.associated_source_id().is_none(){
                                return RwError::from(ErrorCode::InvalidInputSyntax(
                                    format!("cannot change materialized view {}",table_name)
                                ));
                            }
            err
        })?;
        let expected_types = table_source
            .columns
            .iter()
            .map(|c| c.data_type.clone())
            .collect();

        // When the column types of `source` query does not match `expected_types`, casting is
        // needed.
        //
        // In PG, when the `source` is a `VALUES` without order / limit / offset, special treatment
        // is given and it is NOT equivalent to assignment cast over potential implicit cast inside.
        // For example, the following is valid:
        // ```
        //   create table t (v1 time);
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        // ```
        // But the followings are not:
        // ```
        //   values (timestamp '2020-01-01 01:02:03'), (time '03:04:05');
        //   insert into t values (timestamp '2020-01-01 01:02:03'), (time '03:04:05') limit 1;
        // ```
        // Because `timestamp` can cast to `time` in assignment context, but no casting between them
        // is allowed implicitly.
        //
        // In this case, assignment cast should be used directly in `VALUES`, suppressing its
        // internal implicit cast.
        // In other cases, the `source` query is handled on its own and assignment cast is done
        // afterwards.
        let (source, cast_exprs) = match source {
            Query {
                with: None,
                body: SetExpr::Values(values),
                order_by: order,
                limit: None,
                offset: None,
                fetch: None,
            } if order.is_empty() => {
                let values = self.bind_values(values, Some(expected_types))?;
                let body = BoundSetExpr::Values(values.into());
                (
                    BoundQuery {
                        body,
                        order: vec![],
                        limit: None,
                        offset: None,
                        extra_order_exprs: vec![],
                    },
                    vec![],
                )
            }
            query => {
                let bound = self.bind_query(query)?;
                let actual_types = bound.data_types();
                let cast_exprs = match expected_types == actual_types {
                    true => vec![],
                    false => Self::cast_on_insert(
                        expected_types,
                        actual_types
                            .into_iter()
                            .enumerate()
                            .map(|(i, t)| InputRef::new(i, t).into())
                            .collect(),
                    )?,
                };
                (bound, cast_exprs)
            }
        };

        let insert = BoundInsert {
            table_source,
            source,
            cast_exprs,
        };

        Ok(insert)
    }

    /// Cast a list of `exprs` to corresponding `expected_types` IN ASSIGNMENT CONTEXT. Make sure
    /// you understand the difference of implicit, assignment and explicit cast before reusing it.
    pub(super) fn cast_on_insert(
        expected_types: Vec<DataType>,
        exprs: Vec<ExprImpl>,
    ) -> Result<Vec<ExprImpl>> {
        let msg = match expected_types.len().cmp(&exprs.len()) {
            std::cmp::Ordering::Equal => {
                return exprs
                    .into_iter()
                    .zip_eq(expected_types)
                    .map(|(mut e, t)| {
                        e = Self::change_null_struct_type(e, t.clone())?;
                        e.cast_assign(t)
                    })
                    .try_collect();
            }
            std::cmp::Ordering::Less => "INSERT has more expressions than target columns",
            std::cmp::Ordering::Greater => "INSERT has more target columns than expressions",
        };
        Err(ErrorCode::BindError(msg.into()).into())
    }

    /// If expr is struct type function and some inputs are null,
    /// we need to change the data type of these fields to target type.
    pub fn change_null_struct_type(expr: ExprImpl, target: DataType) -> Result<ExprImpl> {
        if let ExprImpl::FunctionCall(func) = &expr {
            if func.get_expr_type() == ExprType::Row {
                if let DataType::Struct { fields } = target {
                    let inputs = func
                        .inputs()
                        .iter()
                        .zip_eq(fields.iter())
                        .map(|(e, d)| {
                            if e.is_null() {
                                Ok(ExprImpl::Literal(Literal::new(None, d.clone()).into()))
                            } else if let DataType::Struct { fields: _ } = d {
                                Self::change_null_struct_type(e.clone(), d.clone())
                            } else {
                                Ok(e.clone())
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let data_type = inputs.iter().map(|i| i.return_type()).collect_vec();
                    return Ok(ExprImpl::FunctionCall(
                        FunctionCall::new_unchecked(
                            func.get_expr_type(),
                            inputs,
                            DataType::Struct {
                                fields: data_type.into(),
                            },
                        )
                        .into(),
                    ));
                }
                return Err(ErrorCode::BindError(
                    "target type of struct function is not struct type".to_string(),
                )
                .into());
            }
        }
        Ok(expr)
    }
}
