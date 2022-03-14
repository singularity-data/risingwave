use std::fmt::Debug;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Select;

use super::bind_context::Clause;
use crate::binder::{Binder, TableRef};
use crate::expr::{Expr, ExprImpl};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<TableRef>,
    pub selection: Option<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        let from = self.bind_vec_table_with_joins(select.from)?;

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
        let select_items = self.bind_project(select.projection)?;
        let aliases = vec![None; select_items.len()];
        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            selection,
        })
    }
}
