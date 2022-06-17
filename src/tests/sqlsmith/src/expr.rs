use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_frontend::expr::{func_sig_map, DataTypeName, ExprType, FuncSign};
use risingwave_sqlparser::ast::{BinaryOperator, DataType, Expr, Value};

use crate::SqlGenerator;

lazy_static::lazy_static! {
    static ref FUNC_TABLE: HashMap<DataTypeName, Vec<FuncSign>> = {
        init_op_table()
    };
}

fn init_op_table() -> HashMap<DataTypeName, Vec<FuncSign>> {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sig_map()
        .iter()
        .for_each(|(func, ret)| funcs.entry(*ret).or_default().push(func.clone()));
    funcs
}

impl SqlGenerator {
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName) -> Expr {
        match self.rng.gen_range(0..=99) {
            0..=49 => self.gen_func(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            50..=99 => self.gen_simple_scalar(typ),
            _ => unreachable!(),
        }
    }

    fn gen_func(&mut self, ret: DataTypeName) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return sql_null(),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func.inputs_type.iter().map(|t| self.gen_expr(*t)).collect();
        if exprs.len() == 2 {
            make_bin_op(func.func, exprs)
        } else {
            Expr::Value(Value::Null)
        }
    }

    fn gen_simple_scalar(&mut self, typ: DataTypeName) -> Expr {
        use DataTypeName as T;
        match typ {
            T::Int32 | T::Int16 | T::Int64 => {
                Expr::Value(Value::Number(self.rng.gen_range(0..100).to_string(), false))
            }
            T::Varchar => Expr::Value(Value::SingleQuotedString(
                (0..10)
                    .map(|_| self.rng.sample(Alphanumeric) as char)
                    .collect(),
            )),
            T::Decimal | T::Float64 | T::Float32 => Expr::Value(Value::Number(
                self.rng.gen_range(0.0..99.9).to_string(),
                false,
            )),
            T::Boolean => Expr::Value(Value::Boolean(self.rng.gen_bool(0.5))),
            T::Date => Expr::TypedString {
                data_type: DataType::Date,
                value: self.gen_temporal_scalar(typ),
            },
            T::Time => Expr::TypedString {
                data_type: DataType::Time(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Timestamp | T::Timestampz => Expr::TypedString {
                data_type: DataType::Timestamp(false),
                value: self.gen_temporal_scalar(typ),
            },
            T::Interval => Expr::TypedString {
                data_type: DataType::Interval,
                value: self.gen_temporal_scalar(typ),
            },
            _ => Expr::Value(Value::Null),
        }
    }

    fn gen_temporal_scalar(&mut self, typ: DataTypeName) -> String {
        use DataTypeName as T;

        let secs = self.rng.gen_range(0..1000000) as u64;
        let tm = DateTime::<Utc>::from(SystemTime::now() - Duration::from_secs(secs));
        match typ {
            T::Date => tm.format("%F").to_string(),
            T::Timestamp | T::Timestampz => tm.format("%Y-%m-%d %H:%M:%S").to_string(),
            T::Time => tm.format("%T").to_string(),
            T::Interval => secs.to_string(),
            _ => unreachable!(),
        }
    }
}

fn make_bin_op(func: ExprType, exprs: Vec<Expr>) -> Expr {
    use {BinaryOperator as B, ExprType as E};
    let bin_op = match func {
        E::Add => B::Plus,
        E::Subtract => B::Minus,
        E::Multiply => B::Multiply,
        E::Divide => B::Divide,
        E::Modulus => B::Modulo,
        E::GreaterThan => B::Gt,
        E::GreaterThanOrEqual => B::GtEq,
        E::LessThan => B::Lt,
        E::LessThanOrEqual => B::LtEq,
        E::Equal => B::Eq,
        E::NotEqual => B::NotEq,
        E::And => B::And,
        E::Or => B::Or,
        E::Like => B::Like,
        E::BitwiseAnd => B::BitwiseAnd,
        E::BitwiseOr => B::BitwiseOr,
        E::BitwiseXor => B::PGBitwiseXor,
        E::BitwiseShiftLeft => B::PGBitwiseShiftLeft,
        E::BitwiseShiftRight => B::PGBitwiseShiftRight,
        _ => {
            return Expr::Value(Value::Null);
        }
    };
    Expr::BinaryOp {
        left: Box::new(exprs[0].clone()),
        op: bin_op,
        right: Box::new(exprs[1].clone()),
    }
}

fn sql_null() -> Expr {
    Expr::Value(Value::Null)
}
