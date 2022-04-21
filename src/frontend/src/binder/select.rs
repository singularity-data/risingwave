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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Scalar};
use risingwave_sqlparser::ast::{Expr, Ident, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, Relation};
use crate::catalog::check_valid_column_name;
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall, InputRef, Literal};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: Vec<ExprImpl>,
    pub schema: Schema,
}

impl BoundSelect {
    /// The names returned by this [`BoundSelect`].
    pub fn names(&self) -> Vec<String> {
        self.aliases
            .iter()
            .cloned()
            .map(|alias| alias.unwrap_or_else(|| UNNAMED_COLUMN.to_string()))
            .collect()
    }

    /// The types returned by this [`BoundSelect`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.select_items
            .iter()
            .map(|item| item.return_type())
            .collect()
    }

    pub fn is_correlated(&self) -> bool {
        self.select_items
            .iter()
            .chain(self.group_by.iter())
            .chain(self.where_clause.iter())
            .any(|expr| expr.has_correlated_input_ref())
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(select.from)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        self.context.clause = None;

        if let Some(selection) = &selection {
            let return_type = selection.return_type();
            if return_type != DataType::Boolean {
                return Err(ErrorCode::InternalError(format!(
                    "argument of WHERE must be boolean, not type {:?}",
                    return_type
                ))
                .into());
            }
        }

        // Bind GROUP BY clause.
        let group_by = select
            .group_by
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_project(select.projection)?;

        // Get index from `select_item` to find the `column_desc` in bindings,
        // and then get the `field_desc` in expr
        // If `select_item` not have index, use `alias` and `data_type` to form
        // field.
        let fields = select_items
            .iter()
            .zip_eq(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                match s.get_index() {
                    Some(index) => {
                        let column = s.get_field(self.context.columns[index].desc.clone())?;
                        Ok(Field::with_struct(
                            s.return_type(),
                            name,
                            column.field_descs.iter().map(|f| f.into()).collect_vec(),
                            column.type_name,
                        ))
                    }
                    None => Ok(Field::with_name(s.return_type(), name)),
                }
            })
            .collect::<Result<Vec<Field>>>()?;

        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            where_clause: selection,
            group_by,
            schema: Schema { fields },
        })
    }

    pub fn bind_project(
        &mut self,
        select_items: Vec<SelectItem>,
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let mut select_list = vec![];
        let mut aliases = vec![];
        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (select_expr, alias) = match &expr.clone() {
                        Expr::Identifier(ident) => {
                            (self.bind_expr(expr)?, Some(ident.value.clone()))
                        }
                        Expr::CompoundIdentifier(idents) => (
                            self.bind_expr(expr)?,
                            idents.last().map(|ident| ident.value.clone()),
                        ),
                        Expr::FieldIdentifier(field_expr, idents) => {
                            self.bind_single_field_column(*field_expr.clone(), idents)?
                        }
                        _ => (self.bind_expr(expr)?, None),
                    };
                    select_list.push(select_expr);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    check_valid_column_name(&alias.value)?;

                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.value));
                }
                SelectItem::QualifiedWildcard(obj_name) => {
                    let table_name = &obj_name.0.last().unwrap().value;
                    let (begin, end) = self.context.range_of.get(table_name).ok_or_else(|| {
                        ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name))
                    })?;
                    let (exprs, names) =
                        Self::bind_columns_iter(self.context.columns[*begin..*end].iter());
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::ExprQualifiedWildcard(expr, idents) => {
                    let (exprs, names) = self.bind_wildcard_field_column(expr, &idents.0)?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::Wildcard => {
                    let (exprs, names) = Self::bind_columns_iter(
                        self.context.columns[..].iter().filter(|c| !c.is_hidden),
                    );
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
            }
        }
        Ok((select_list, aliases))
    }

    /// This function will accept three expr type: `CompoundIdentifier`,`Identifier`,`Cast(Todo)`
    /// We will extract ident from `expr` to get the `column_binding`.
    /// Will return `column_binding` and field `idents`.
    pub fn extract_binding_and_idents(
        &mut self,
        expr: Expr,
        ids: Vec<Ident>,
    ) -> Result<(&ColumnBinding, Vec<Ident>)> {
        match expr {
            // For CompoundIdentifier, we will use first ident as table name and second ident as
            // column name to get `column_desc`.
            Expr::CompoundIdentifier(idents) => {
                let (table_name, column): (&String, &String) = match &idents[..] {
                    [table, column] => (&table.value, &column.value),
                    _ => {
                        return Err(ErrorCode::InternalError(format!(
                            "Too many idents: {:?}",
                            idents
                        ))
                        .into());
                    }
                };
                let index = self
                    .context
                    .get_column_binding_index(Some(table_name), column)?;
                Ok((&self.context.columns[index], ids))
            }
            // For Identifier, we will first use the ident as
            // column name to get `column_desc`.
            // If column name not exist, we will use the ident as table name.
            // The reason is that in pgsql, for table name v3 have a column name v3 which
            // have a field name v3. Select (v3).v3 from v3 will return the field value instead
            // of column value.
            Expr::Identifier(ident) => match self.context.indexs_of.get(&ident.value) {
                Some(indexs) => {
                    if indexs.len() == 1 {
                        let index = self.context.get_column_binding_index(None, &ident.value)?;
                        Ok((&self.context.columns[index], ids))
                    } else {
                        let column = &ids[0].value;
                        let index = self
                            .context
                            .get_column_binding_index(Some(&ident.value), column)?;
                        Ok((&self.context.columns[index], ids[1..].to_vec()))
                    }
                }
                None => {
                    let column = &ids[0].value;
                    let index = self
                        .context
                        .get_column_binding_index(Some(&ident.value), column)?;
                    Ok((&self.context.columns[index], ids[1..].to_vec()))
                }
            },
            Expr::Cast { .. } => {
                todo!()
            }
            _ => unreachable!(),
        }
    }

    /// Bind wildcard field column, e.g. `(table.v1).*`.
    /// Will return vector of Field type `FunctionCall` and alias.
    pub fn bind_wildcard_field_column(
        &mut self,
        expr: Expr,
        ids: &[Ident],
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, column) = Self::bind_field(
            InputRef::new(binding.index, binding.desc.data_type.clone()).into(),
            &idents,
            binding.desc.clone(),
            true,
        )?;
        Ok((
            exprs,
            column
                .field_descs
                .iter()
                .map(|f| Some(f.name.clone()))
                .collect_vec(),
        ))
    }

    /// Bind single field column, e.g. `(table.v1).v2`.
    /// Will return Field type `FunctionCall` and alias.
    pub fn bind_single_field_column(
        &mut self,
        expr: Expr,
        ids: &[Ident],
    ) -> Result<(ExprImpl, Option<String>)> {
        let (binding, idents) = self.extract_binding_and_idents(expr, ids.to_vec())?;
        let (exprs, column) = Self::bind_field(
            InputRef::new(binding.index, binding.desc.data_type.clone()).into(),
            &idents,
            binding.desc.clone(),
            false,
        )?;
        Ok((exprs[0].clone(), Some(column.name)))
    }

    /// Bind field in recursive way, each field in the binding path will store as `literal` in
    /// exprs and return.
    /// `col` describes what `expr` contains.
    pub fn bind_field(
        expr: ExprImpl,
        idents: &[Ident],
        desc: ColumnDesc,
        wildcard: bool,
    ) -> Result<(Vec<ExprImpl>, ColumnDesc)> {
        match idents.get(0) {
            Some(ident) => {
                let (field, field_index) = desc.field(&ident.value)?;
                let expr = FunctionCall::new_with_return_type(
                    ExprType::Field,
                    vec![
                        expr,
                        Literal::new(Some(field_index.to_scalar_value()), DataType::Int32).into(),
                    ],
                    field.data_type.clone(),
                )
                .into();
                Self::bind_field(expr, &idents[1..], field, wildcard)
            }
            None => {
                if wildcard {
                    Self::bind_wildcard_field(expr, desc)
                } else {
                    Ok((vec![expr], desc))
                }
            }
        }
    }

    /// Will fail if it's an atomic value.
    /// Rewrite (expr:Struct).* to [Field(expr, 0), Field(expr, 1), ... Field(expr, n)].
    pub fn bind_wildcard_field(
        expr: ExprImpl,
        desc: ColumnDesc,
    ) -> Result<(Vec<ExprImpl>, ColumnDesc)> {
        if desc.field_descs.is_empty() {
            Err(
                ErrorCode::BindError(format!("The field {} is not the nested column", desc.name))
                    .into(),
            )
        } else {
            Ok((
                desc.field_descs
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        FunctionCall::new_with_return_type(
                            ExprType::Field,
                            vec![
                                expr.clone(),
                                Literal::new(Some((i as i32).to_scalar_value()), DataType::Int32)
                                    .into(),
                            ],
                            f.data_type.clone(),
                        )
                        .into()
                    })
                    .collect_vec(),
                desc,
            ))
        }
    }

    pub fn bind_columns_iter<'a>(
        column_binding: impl Iterator<Item = &'a ColumnBinding>,
    ) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        column_binding
            .map(|c| {
                (
                    InputRef::new(c.index, c.desc.data_type.clone()).into(),
                    Some(c.desc.name.clone()),
                )
            })
            .unzip()
    }
}
