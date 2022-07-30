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

use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::ScalarImpl;

use crate::binder::{
    BoundBaseTable, BoundJoin, BoundSource, BoundSystemTable, BoundWindowTableFunction, Relation,
    WindowTableFunctionKind,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, TableFunction};
use crate::optimizer::plan_node::{
    LogicalHopWindow, LogicalJoin, LogicalProject, LogicalScan, LogicalSource,
    LogicalTableFunction, PlanRef,
};
use crate::planner::Planner;

impl Planner {
    pub fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(*t),
            Relation::SystemTable(st) => self.plan_sys_table(*st),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.as_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
            Relation::Source(s) => self.plan_source(*s),
            Relation::TableFunction(tf) => self.plan_table_function(*tf),
        }
    }

    pub(crate) fn plan_sys_table(&mut self, sys_table: BoundSystemTable) -> Result<PlanRef> {
        Ok(LogicalScan::create(
            sys_table.name,
            true,
            Rc::new(sys_table.sys_table_catalog.table_desc()),
            vec![],
            self.ctx(),
        )
        .into())
    }

    pub(super) fn plan_base_table(&mut self, base_table: BoundBaseTable) -> Result<PlanRef> {
        Ok(LogicalScan::create(
            base_table.name,
            false,
            Rc::new(base_table.table_catalog.table_desc()),
            base_table
                .table_indexes
                .iter()
                .map(|x| (x.name.clone(), Rc::new(x.table_desc())))
                .collect(),
            self.ctx(),
        )
        .into())
    }

    pub(super) fn plan_source(&mut self, source: BoundSource) -> Result<PlanRef> {
        Ok(LogicalSource::new(Rc::new(source.catalog), self.ctx()).into())
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        let join_type = join.join_type;
        let on_clause = join.cond;
        if let Some(proj) = join.projection {
            Ok(LogicalProject::create(
                LogicalJoin::create(left, right, join_type, on_clause),
                proj,
            ))
        } else {
            Ok(LogicalJoin::create(left, right, join_type, on_clause))
        }
    }

    pub(super) fn plan_window_table_function(
        &mut self,
        table_function: BoundWindowTableFunction,
    ) -> Result<PlanRef> {
        use WindowTableFunctionKind::*;
        match table_function.kind {
            Tumble => self.plan_tumble_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
            Hop => self.plan_hop_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
        }
    }

    pub(super) fn plan_table_function(&mut self, table_function: TableFunction) -> Result<PlanRef> {
        Ok(LogicalTableFunction::new(table_function, self.ctx()).into())
    }

    fn plan_tumble_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let mut args = args.into_iter();

        let cols = match &input {
            Relation::Source(s) => s.catalog.columns.to_vec(),
            Relation::BaseTable(t) => t.table_catalog.columns().to_vec(),
            _ => return Err(ErrorCode::BindError("the ".to_string()).into()),
        };

        match (args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None) => {
                let mut exprs = Vec::with_capacity(cols.len() + 2);
                for (idx, col) in cols.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col.data_type().clone()).into());
                }
                let window_start: ExprImpl = FunctionCall::new(
                    ExprType::TumbleStart,
                    vec![ExprImpl::InputRef(Box::new(time_col)), window_size.clone()],
                )?
                .into();
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    FunctionCall::new(ExprType::Add, vec![window_start.clone(), window_size])?
                        .into();
                exprs.push(window_start);
                exprs.push(window_end);
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs);
                Ok(project)
            }
            _ => Err(ErrorCode::BindError(
                "Invalid arguments for TUMBLE window function".to_string(),
            )
            .into()),
        }
    }

    fn plan_hop_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let input = self.plan_relation(input)?;
        let mut args = args.into_iter();
        let Some((ExprImpl::Literal(window_slide), ExprImpl::Literal(window_size))) = args.next_tuple() else {
            return Err(ErrorCode::BindError("Invalid arguments for HOP window function".to_string()).into());
        };
        let Some(ScalarImpl::Interval(window_slide)) = *window_slide.get_data() else {
            return Err(ErrorCode::BindError("Invalid arguments for HOP window function".to_string()).into());
        };
        let Some(ScalarImpl::Interval(window_size)) = *window_size.get_data() else {
            return Err(ErrorCode::BindError("Invalid arguments for HOP window function".to_string()).into());
        };
        if window_size.exact_div(&window_slide).is_none() {
            return Err(ErrorCode::BindError(format!("Invalid arguments for HOP window function: window_size {} cannot be divided by window_slide {}",window_size, window_slide)).into());
        }
        Ok(LogicalHopWindow::create(
            input,
            time_col,
            window_slide,
            window_size,
        ))
    }
}
