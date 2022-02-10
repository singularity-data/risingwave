use risingwave_common::types::DataType;
mod input_ref;
pub use input_ref::*;
mod literal;
pub use literal::*;
mod function_call;
pub use function_call::*;
mod agg_call;
pub use agg_call::*;
mod type_inference;
pub use type_inference::*;
mod utils;
pub use utils::*;

pub type ExprType = risingwave_pb::expr::expr_node::Type;

/// the trait of bound exprssions
pub trait Expr {
    fn return_type(&self) -> DataType;
    fn bound_expr(self) -> ExprImpl;
}
#[derive(Clone, Debug, PartialEq)]
pub enum ExprImpl {
    // ColumnRef(Box<BoundColumnRef>), might be used in binder.
    InputRef(Box<InputRef>),
    Literal(Box<Literal>),
    FunctionCall(Box<FunctionCall>),
    AggCall(Box<AggCall>),
}
impl Expr for ExprImpl {
    fn return_type(&self) -> DataType {
        match self {
            ExprImpl::InputRef(expr) => expr.return_type(),
            ExprImpl::Literal(expr) => expr.return_type(),
            ExprImpl::FunctionCall(expr) => expr.return_type(),
            ExprImpl::AggCall(expr) => expr.return_type(),
        }
    }
    fn bound_expr(self) -> ExprImpl {
        self
    }
}
