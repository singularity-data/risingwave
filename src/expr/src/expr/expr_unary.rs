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

//! For expression that only accept one value as input (e.g. CAST)

use risingwave_common::array::*;
use risingwave_common::types::*;
use risingwave_pb::expr::expr_node::Type as ProstType;

use super::template::{UnaryBytesExpression, UnaryExpression};
use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
use crate::expr::template::UnaryNullableExpression;
use crate::expr::BoxedExpression;
use crate::vector_op::arithmetic_op::{decimal_abs, general_abs, general_neg};
use crate::vector_op::ascii::ascii;
use crate::vector_op::bitwise_op::general_bitnot;
use crate::vector_op::cast::*;
use crate::vector_op::cmp::{is_false, is_not_false, is_not_true, is_true};
use crate::vector_op::conjunction;
use crate::vector_op::length::{bit_length, length_default, octet_length};
use crate::vector_op::lower::lower;
use crate::vector_op::ltrim::ltrim;
use crate::vector_op::md5::md5;
use crate::vector_op::round::*;
use crate::vector_op::rtrim::rtrim;
use crate::vector_op::trim::trim;
use crate::vector_op::upper::upper;
use crate::{ExprError, Result};

/// This macro helps to create cast expression.
/// It receives all the combinations of `gen_cast` and generates corresponding match cases
/// In `[]`, the parameters are for constructing new expression
/// * `$child`: child expression
/// * `$ret`: return expression
///
/// In `()*`, the parameters are for generating match cases
/// * `$input`: input type
/// * `$cast`: The cast type in that the operation will calculate
/// * `$func`: The scalar function for expression, it's a generic function and specialized by the
///   type of `$input, $cast`
macro_rules! gen_cast_impl {
    ([$child:expr, $ret:expr], $( { $input:ident, $cast:ident, $func:expr } ),*) => {
        match ($child.return_type(), $ret.clone()) {
            $(
                ($input! { type_match_pattern }, $cast! { type_match_pattern }) => Box::new(
                    UnaryExpression::< $input! { type_array }, $cast! { type_array }, _>::new(
                        $child,
                        $ret.clone(),
                        $func
                    )
                ),
            )*
            _ => {
                return Err(ExprError::Cast2($child.return_type(), $ret));
            }
        }
    };
}

macro_rules! gen_cast {
    ($($x:tt, )* ) => {
        gen_cast_impl! {
            [$($x),*],

            { varchar, date, str_to_date },
            { varchar, time, str_to_time },
            { varchar, timestamp, str_to_timestamp },
            { varchar, timestampz, str_to_timestampz },
            { varchar, int16, str_parse },
            { varchar, int32, str_parse },
            { varchar, int64, str_parse },
            { varchar, float32, str_parse },
            { varchar, float64, str_parse },
            { varchar, decimal, str_parse },
            { varchar, boolean, str_to_bool },

            { boolean, varchar, general_to_string },
            { int16, varchar, general_to_string },
            { int32, varchar, general_to_string },
            { int64, varchar, general_to_string },
            { float32, varchar, general_to_string },
            { float64, varchar, general_to_string },
            { decimal, varchar, general_to_string },

            { boolean, int32, general_cast },
            { int32, boolean, int32_to_bool },

            { int16, int32, general_cast },
            { int16, int64, general_cast },
            { int16, float32, general_cast },
            { int16, float64, general_cast },
            { int16, decimal, general_cast },
            { int32, int16, general_cast },
            { int32, int64, general_cast },
            { int32, float32, to_f32 }, // lossy
            { int32, float64, general_cast },
            { int32, decimal, general_cast },
            { int64, int16, general_cast },
            { int64, int32, general_cast },
            { int64, float32, to_f32 }, // lossy
            { int64, float64, to_f64 }, // lossy
            { int64, decimal, general_cast },

            { float32, float64, general_cast },
            { float32, decimal, general_cast },
            { float32, int16, to_i16 },
            { float32, int32, to_i32 },
            { float32, int64, to_i64 },
            { float64, decimal, general_cast },
            { float64, int16, to_i16 },
            { float64, int32, to_i32 },
            { float64, int64, to_i64 },
            { float64, float32, to_f32 }, // lossy

            { decimal, int16, dec_to_i16 },
            { decimal, int32, dec_to_i32 },
            { decimal, int64, dec_to_i64 },
            { decimal, float32, to_f32 },
            { decimal, float64, to_f64 },

            { date, timestamp, general_cast },
            { time, interval, general_cast }
        }
    };
}

/// This macro helps to create unary expression.
/// In [], the parameters are for constructing new expression
/// * $`expr_name`: expression name, used for print error message
/// * $child: child expression
/// * $ret: return array type
/// In ()*, the parameters are for generating match cases
/// * $input: child array type
/// * $rt: The return type in that the operation will calculate
/// * $func: The scalar function for expression
macro_rules! gen_unary_impl {
    ([$expr_name: literal, $child:expr, $ret:expr], $( { $input:ident, $rt: ident, $func:ident },)*) => {
        match ($child.return_type()) {
            $(
                $input! { type_match_pattern } => Box::new(
                        UnaryExpression::<$input! { type_array}, $rt! {type_array}, _>::new(
                            $child,
                            $ret.clone(),
                            $func,
                        )
                ),
            )*
            _ => {
                return Err(ExprError::UnsupportedFunction(format!("{}({:?}) -> {:?}", $expr_name, $child.return_type(), $ret)));
            }
        }
    };
}

macro_rules! gen_unary_atm_expr  {
    (
        $expr_name: literal,
        $child:expr,
        $ret:expr,
        $general_func:ident,
        {
            $( { $input:ident, $rt:ident, $func:ident }, )*
        } $(,)?
    ) => {
        gen_unary_impl! {
            [$expr_name, $child, $ret],
            { int16, int16, $general_func },
            { int32, int32, $general_func },
            { int64, int64, $general_func },
            { float32, float32, $general_func },
            { float64, float64, $general_func },
            $(
                { $input, $rt, $func },
            )*
        }
    };
}

macro_rules! gen_round_expr {
    (
        $expr_name:literal,
        $child:expr,
        $ret:expr,
        $float64_round_func:ident,
        $decimal_round_func:ident
    ) => {
        gen_unary_impl! {
            [$expr_name, $child, $ret],
            { float64, float64, $float64_round_func },
            { decimal, decimal, $decimal_round_func },
        }
    };
}

pub fn new_unary_expr(
    expr_type: ProstType,
    return_type: DataType,
    child_expr: BoxedExpression,
) -> Result<BoxedExpression> {
    use crate::expr::data_types::*;

    let expr: BoxedExpression = match (expr_type, return_type.clone(), child_expr.return_type()) {
        (ProstType::Cast, _, _) => gen_cast! { child_expr, return_type, },
        (ProstType::BoolOut, _, DataType::Boolean) => {
            Box::new(UnaryExpression::<BoolArray, Utf8Array, _>::new(
                child_expr,
                return_type,
                bool_out,
            ))
        }
        (ProstType::Not, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                conjunction::not,
            ))
        }
        (ProstType::IsTrue, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_true,
            ))
        }
        (ProstType::IsNotTrue, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_not_true,
            ))
        }
        (ProstType::IsFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_false,
            ))
        }
        (ProstType::IsNotFalse, _, _) => {
            Box::new(UnaryNullableExpression::<BoolArray, BoolArray, _>::new(
                child_expr,
                return_type,
                is_not_false,
            ))
        }
        (ProstType::IsNull, _, _) => Box::new(IsNullExpression::new(child_expr)),
        (ProstType::IsNotNull, _, _) => Box::new(IsNotNullExpression::new(child_expr)),
        (ProstType::Upper, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
            child_expr,
            return_type,
            upper,
        )),
        (ProstType::Lower, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
            child_expr,
            return_type,
            lower,
        )),
        (ProstType::Md5, _, _) => Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
            child_expr,
            return_type,
            md5,
        )),
        (ProstType::Ascii, _, _) => Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
            child_expr,
            return_type,
            ascii,
        )),
        (ProstType::CharLength, _, _) => Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
            child_expr,
            return_type,
            length_default,
        )),
        (ProstType::OctetLength, _, _) => Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
            child_expr,
            return_type,
            octet_length,
        )),
        (ProstType::BitLength, _, _) => Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
            child_expr,
            return_type,
            bit_length,
        )),
        (ProstType::Neg, _, _) => {
            gen_unary_atm_expr! { "Neg", child_expr, return_type, general_neg,
                {
                    { decimal, decimal, general_neg },
                }
            }
        }
        (ProstType::Abs, _, _) => {
            gen_unary_atm_expr! { "Abs", child_expr, return_type, general_abs,
                {
                    {decimal, decimal, decimal_abs},
                }
            }
        }
        (ProstType::BitwiseNot, _, _) => {
            gen_unary_impl! {
                [ "BitwiseNot", child_expr, return_type],
                { int16, int16, general_bitnot },
                { int32, int32, general_bitnot },
                { int64, int64, general_bitnot },

            }
        }
        (ProstType::Ceil, _, _) => {
            gen_round_expr! {"Ceil", child_expr, return_type, ceil_f64, ceil_decimal}
        }
        (ProstType::Floor, _, _) => {
            gen_round_expr! {"Floor", child_expr, return_type, floor_f64, floor_decimal}
        }
        (ProstType::Round, _, _) => {
            gen_round_expr! {"Ceil", child_expr, return_type, round_f64, round_decimal}
        }
        (expr, ret, child) => {
            return Err(ExprError::UnsupportedFunction(format!(
                "{:?}({:?}) -> {:?}",
                expr, child, ret
            )));
        }
    };

    Ok(expr)
}

pub fn new_length_default(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryExpression::<Utf8Array, I32Array, _>::new(
        expr_ia1,
        return_type,
        length_default,
    ))
}

pub fn new_trim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        trim,
    ))
}

pub fn new_ltrim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        ltrim,
    ))
}

pub fn new_rtrim_expr(expr_ia1: BoxedExpression, return_type: DataType) -> BoxedExpression {
    Box::new(UnaryBytesExpression::<Utf8Array, _>::new(
        expr_ia1,
        return_type,
        rtrim,
    ))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::types::{NaiveDateWrapper, Scalar};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::FunctionCall;

    use super::super::*;
    use crate::expr::test_utils::{make_expression, make_input_ref};
    use crate::vector_op::cast::{general_cast, str_parse};

    #[test]
    fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, Type::Not);
        test_unary_date::<NaiveDateTimeArray, _>(|x| general_cast(x).unwrap(), Type::Cast);
        test_str_to_int16::<I16Array, _>(|x| str_parse(x).unwrap());
    }

    #[test]
    fn test_i16_to_i32() {
        let mut input = Vec::<Option<i16>>::new();
        let mut target = Vec::<Option<i32>>::new();
        for i in 0..100i16 {
            if i % 2 == 0 {
                target.push(Some(i as i32));
                input.push(Some(i as i16));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            I16Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::new(vec![col1], 100);
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int16)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = Row::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_neg() {
        let mut input = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<i32>>::new();

        input.push(Some(1));
        input.push(Some(0));
        input.push(Some(-1));

        target.push(Some(-1));
        target.push(Some(0));
        target.push(Some(1));

        let col1 = Column::new(
            I32Array::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::new(vec![col1], 3);
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Neg as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int32)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = Row::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_str_to_int16<A, F>(f: F)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(&str) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<String>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..1u32 {
            if i % 2 == 0 {
                let s = i.to_string();
                target.push(Some(f(&s)));
                input.push(Some(s));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = Column::new(
            Utf8Array::from_slice(&input.iter().map(|x| x.as_ref().map(|x| &**x)).collect_vec())
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::new(vec![col1], 1);
        let return_type = DataType {
            type_name: TypeName::Int16 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Varchar)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = Row::new(vec![input[i]
                .as_ref()
                .cloned()
                .map(|str| str.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_unary_bool<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(true));
                target.push(Some(f(true)));
            } else if i % 3 == 0 {
                input.push(Some(false));
                target.push(Some(f(false)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            BoolArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = make_expression(kind, &[TypeName::Boolean], &[0]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = Row::new(vec![input[i].map(|b| b.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_unary_date<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<NaiveDateWrapper>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = NaiveDateWrapper::new(NaiveDate::from_num_days_from_ce(i));
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = Column::new(
            NaiveDateArray::from_slice(&input)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = make_expression(kind, &[TypeName::Date], &[0]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = Row::new(vec![input[i].map(|d| d.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
