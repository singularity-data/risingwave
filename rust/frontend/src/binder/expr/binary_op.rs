use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{BinaryOperator, Expr};

use crate::binder::Binder;
use crate::expr::{Expr as _, ExprType, FunctionCall};

impl Binder {
    pub(super) fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<FunctionCall> {
        let bound_left = self.bind_expr(left)?;
        let bound_right = self.bind_expr(right)?;
        let func_type = match op {
            BinaryOperator::Plus => ExprType::Add,
            BinaryOperator::Minus => ExprType::Subtract,
            BinaryOperator::Multiply => ExprType::Multiply,
            BinaryOperator::Divide => ExprType::Divide,
            BinaryOperator::Modulo => ExprType::Modulus,
            _ => return Err(ErrorCode::NotImplementedError(format!("{:?}", op)).into()),
        };
        let desc = format!(
            "{:?} {:?} {:?}",
            bound_left.return_type(),
            op,
            bound_right.return_type(),
        );
        FunctionCall::new(func_type, vec![bound_left, bound_right])
            .ok_or_else(|| ErrorCode::NotImplementedError(desc).into())
    }
}
