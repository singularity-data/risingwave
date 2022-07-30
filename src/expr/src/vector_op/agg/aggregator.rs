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

use std::sync::Arc;

use risingwave_common::array::*;
use risingwave_common::ensure;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::*;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::expr::AggCall;
use risingwave_pb::plan_common::OrderType as ProstOrderType;

use super::string_agg::StringAgg;
use crate::expr::{
    build_from_prost, AggKind, Expression, ExpressionRef, InputRefExpression, LiteralExpression,
};
use crate::vector_op::agg::approx_count_distinct::ApproxCountDistinct;
use crate::vector_op::agg::count_star::CountStar;
use crate::vector_op::agg::functions::*;
use crate::vector_op::agg::general_agg::*;
use crate::vector_op::agg::general_distinct_agg::*;

/// An `Aggregator` supports `update` data and `output` result.
pub trait Aggregator: Send + 'static {
    fn return_type(&self) -> DataType;

    /// `update_single` update the aggregator with a single row with type checked at runtime.
    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()>;

    /// `update_multi` update the aggregator with multiple rows with type checked at runtime.
    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()>;

    /// `output` the aggregator to `ArrayBuilder` with input with type checked at runtime.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    /// `output_and_reset` output the aggregator to `ArrayBuilder` and reset the internal state.
    fn output_and_reset(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}

pub type BoxedAggState = Box<dyn Aggregator>;

pub struct AggStateFactory {
    agg_kind: AggKind,
    return_type: DataType,
    // When agg func is count(*), the args is empty and input type is None.
    input_type: Option<DataType>,
    input_col_idx: usize,
    extra_arg: Option<ExpressionRef>,
    distinct: bool,
    order_pairs: Vec<OrderPair>,
    order_col_types: Vec<DataType>,
    filter: ExpressionRef,
}

impl AggStateFactory {
    pub fn new(prost: &AggCall) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type()?);
        let agg_kind = AggKind::try_from(prost.get_type()?)?;
        let distinct = prost.distinct;
        let mut order_pairs = vec![];
        let mut order_col_types = vec![];
        prost.get_order_by_fields().iter().for_each(|field| {
            let col_idx = field.get_input().unwrap().get_column_idx() as usize;
            let col_type = DataType::from(field.get_type().unwrap());
            let order_type =
                OrderType::from_prost(&ProstOrderType::from_i32(field.direction).unwrap());
            // TODO(yuchao): `nulls first/last` is not supported yet, so it's ignore here,
            // see also `risingwave_common::util::sort_util::compare_values`
            order_pairs.push(OrderPair::new(col_idx, order_type));
            order_col_types.push(col_type);
        });
        let filter: ExpressionRef = match prost.filter {
            Some(ref expr) => Arc::from(build_from_prost(expr)?),
            None => Arc::from(
                LiteralExpression::new(DataType::Boolean, Some(ScalarImpl::Bool(true))).boxed(),
            ),
        };
        match &prost.get_args()[..] {
            [] => match (&agg_kind, return_type.clone()) {
                (AggKind::Count, DataType::Int64) => Ok(Self {
                    agg_kind,
                    return_type,
                    input_type: None,
                    input_col_idx: 0,
                    extra_arg: None,
                    distinct,
                    order_pairs,
                    order_col_types,
                    filter,
                }),
                _ => Err(ErrorCode::InternalError(format!(
                    "Agg {:?} without args not supported",
                    agg_kind
                ))
                .into()),
            },
            [ref arg] if agg_kind != AggKind::StringAgg => {
                let input_type = DataType::from(arg.get_type()?);
                let input_col_idx = arg.get_input()?.get_column_idx() as usize;
                Ok(Self {
                    agg_kind,
                    return_type,
                    input_type: Some(input_type),
                    input_col_idx,
                    extra_arg: None,
                    distinct,
                    order_pairs,
                    order_col_types,
                    filter,
                })
            }
            [ref agg_arg, ref extra_arg] if agg_kind == AggKind::StringAgg => {
                let input_type = DataType::from(agg_arg.get_type()?);
                let input_col_idx = agg_arg.get_input()?.get_column_idx() as usize;
                let extra_arg_type = DataType::from(extra_arg.get_type()?);
                ensure!(
                    extra_arg_type == DataType::Varchar,
                    ErrorCode::ExprError("Delimiter must be Varchar".into())
                );
                let extra_arg = Arc::from(
                    InputRefExpression::new(
                        extra_arg_type,
                        extra_arg.get_input()?.get_column_idx() as usize,
                    )
                    .boxed(),
                );
                Ok(Self {
                    agg_kind,
                    return_type,
                    input_type: Some(input_type),
                    input_col_idx,
                    extra_arg: Some(extra_arg),
                    distinct,
                    order_pairs,
                    order_col_types,
                    filter,
                })
            }
            _ => Err(ErrorCode::ExprError(
                format!("Too many/few arguments for {:?}", agg_kind).into(),
            )
            .into()),
        }
    }

    pub fn create_agg_state(&self) -> Result<Box<dyn Aggregator>> {
        if let AggKind::ApproxCountDistinct = self.agg_kind {
            Ok(Box::new(ApproxCountDistinct::new(
                self.return_type.clone(),
                self.input_col_idx,
                self.filter.clone(),
            )))
        } else if let AggKind::StringAgg = self.agg_kind {
            Ok(Box::new(StringAgg::new(
                self.input_col_idx,
                self.extra_arg.clone().unwrap(),
                self.order_pairs.clone(),
                self.order_col_types.clone(),
            )))
        } else if let Some(input_type) = self.input_type.clone() {
            create_agg_state_unary(
                input_type,
                self.input_col_idx,
                &self.agg_kind,
                self.return_type.clone(),
                self.distinct,
                self.filter.clone(),
            )
        } else {
            Ok(Box::new(CountStar::new(
                self.return_type.clone(),
                0,
                self.filter.clone(),
            )))
        }
    }

    pub fn get_return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

pub fn create_agg_state_unary(
    input_type: DataType,
    input_col_idx: usize,
    agg_type: &AggKind,
    return_type: DataType,
    distinct: bool,
    filter: ExpressionRef,
) -> Result<Box<dyn Aggregator>> {
    use crate::expr::data_types::*;

    macro_rules! gen_arms {
        [$(($agg:ident, $fn:expr, $in:tt, $ret:tt, $init_result:expr)),* $(,)?] => {
            match (
                input_type,
                agg_type,
                return_type.clone(),
                distinct,
            ) {
                $(
                    ($in! { type_match_pattern }, AggKind::$agg, $ret! { type_match_pattern }, false) => {
                        Box::new(GeneralAgg::<$in! { type_array }, _, $ret! { type_array }>::new(
                            return_type,
                            input_col_idx,
                            $fn,
                            $init_result,
                            filter
                        ))
                    },
                    ($in! { type_match_pattern }, AggKind::$agg, $ret! { type_match_pattern }, true) => {
                        Box::new(GeneralDistinctAgg::<$in! { type_array }, _, $ret! { type_array }>::new(
                            return_type,
                            input_col_idx,
                            $fn,
                            filter,
                        ))
                    },
                )*
                (unimpl_input, unimpl_agg, unimpl_ret, distinct) => {
                    return Err(
                        ErrorCode::InternalError(format!(
                        "unsupported aggregator: type={:?} input={:?} output={:?} distinct={}",
                        unimpl_agg, unimpl_input, unimpl_ret, distinct
                        ))
                        .into(),
                    )
                }
            }
        };
    }

    let state: Box<dyn Aggregator> = gen_arms![
        (Count, count, int16, int64, Some(0)),
        (Count, count, int32, int64, Some(0)),
        (Count, count, int64, int64, Some(0)),
        (Count, count, float32, int64, Some(0)),
        (Count, count, float64, int64, Some(0)),
        (Count, count, decimal, int64, Some(0)),
        (Count, count_str, varchar, int64, Some(0)),
        (Count, count, boolean, int64, Some(0)),
        (Count, count, interval, int64, Some(0)),
        (Count, count, date, int64, Some(0)),
        (Count, count, timestamp, int64, Some(0)),
        (Count, count, time, int64, Some(0)),
        (Count, count_struct, struct_type, int64, Some(0)),
        (Count, count_list, list, int64, Some(0)),
        (Sum, sum, int16, int64, None),
        (Sum, sum, int32, int64, None),
        (Sum, sum, int64, decimal, None),
        (Sum, sum, float32, float32, None),
        (Sum, sum, float64, float64, None),
        (Sum, sum, decimal, decimal, None),
        (Min, min, int16, int16, None),
        (Min, min, int32, int32, None),
        (Min, min, int64, int64, None),
        (Min, min, float32, float32, None),
        (Min, min, float64, float64, None),
        (Min, min, decimal, decimal, None),
        (Min, min, boolean, boolean, None), // TODO(#359): remove once unnecessary
        (Min, min, interval, interval, None),
        (Min, min, date, date, None),
        (Min, min, timestamp, timestamp, None),
        (Min, min, time, time, None),
        (Min, min_struct, struct_type, struct_type, None),
        (Min, min_str, varchar, varchar, None),
        (Min, min_list, list, list, None),
        (Max, max, int16, int16, None),
        (Max, max, int32, int32, None),
        (Max, max, int64, int64, None),
        (Max, max, float32, float32, None),
        (Max, max, float64, float64, None),
        (Max, max, decimal, decimal, None),
        (Max, max, boolean, boolean, None), // TODO(#359): remove once unnecessary
        (Max, max, interval, interval, None),
        (Max, max, date, date, None),
        (Max, max, timestamp, timestamp, None),
        (Max, max, time, time, None),
        (Max, max_struct, struct_type, struct_type, None),
        (Max, max_str, varchar, varchar, None),
        (Max, max_list, list, list, None),
        // Global Agg
        (Sum, sum, int64, int64, None),
        // We remark that SingleValue does not produce a runtime error when it receives zero row.
        // Therefore, we do NOT need to change the logic in GeneralAgg::output_concrete.
        (SingleValue, SingleValue::new(), int16, int16, None),
        (SingleValue, SingleValue::new(), int32, int32, None),
        (SingleValue, SingleValue::new(), int64, int64, None),
        (SingleValue, SingleValue::new(), float32, float32, None),
        (SingleValue, SingleValue::new(), float64, float64, None),
        (SingleValue, SingleValue::new(), decimal, decimal, None),
        (SingleValue, SingleValue::new(), boolean, boolean, None),
        (SingleValue, SingleValue::new(), varchar, varchar, None),
    ];
    Ok(state)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    #[test]
    fn test_create_agg_state() {
        let int64_type = DataType::Int64;
        let decimal_type = DataType::Decimal;
        let bool_type = DataType::Boolean;
        let char_type = DataType::Varchar;
        let filter: ExpressionRef = Arc::from(
            LiteralExpression::new(DataType::Boolean, Some(ScalarImpl::Bool(true))).boxed(),
        );
        macro_rules! test_create {
            ($input_type:expr, $agg:ident, $return_type:expr, $expected:ident) => {
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    &AggKind::$agg,
                    $return_type.clone(),
                    false,
                    filter.clone(),
                )
                .$expected());
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    &AggKind::$agg,
                    $return_type.clone(),
                    true,
                    filter.clone(),
                )
                .$expected());
            };
        }

        test_create! { int64_type, Count, int64_type, is_ok }
        test_create! { decimal_type, Count, int64_type, is_ok }
        test_create! { bool_type, Count, int64_type, is_ok }
        test_create! { char_type, Count, int64_type, is_ok }

        test_create! { int64_type, Sum, decimal_type, is_ok }
        test_create! { decimal_type, Sum, decimal_type, is_ok }
        test_create! { bool_type, Sum, bool_type, is_err }
        test_create! { char_type, Sum, char_type, is_err }

        test_create! { int64_type, Min, int64_type, is_ok }
        test_create! { decimal_type, Min, decimal_type, is_ok }
        test_create! { bool_type, Min, bool_type, is_ok } // TODO(#359): revert to is_err
        test_create! { char_type, Min, char_type, is_ok }

        test_create! { int64_type, SingleValue, int64_type, is_ok }
        test_create! { decimal_type, SingleValue, decimal_type, is_ok }
        test_create! { bool_type, SingleValue, bool_type, is_ok }
        test_create! { char_type, SingleValue, char_type, is_ok }
    }
}
