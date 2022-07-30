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

use std::collections::BinaryHeap;
use std::sync::Arc;

use risingwave_common::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, Row};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::encoding_for_comparison::{encode_row, is_type_encodable};
use risingwave_common::util::sort_util::{DescOrderedRow, OrderPair};

use crate::expr::ExpressionRef;
use crate::vector_op::agg::aggregator::Aggregator;

enum StringAggState {
    WithoutOrder {
        result: Option<String>,
    },
    WithOrder {
        order_pairs: Arc<Vec<OrderPair>>,
        min_heap: BinaryHeap<DescOrderedRow>,
        encodable: bool,
    },
}

pub struct StringAgg {
    agg_col_idx: usize,
    delimiter: ExpressionRef,
    state: StringAggState,
}

impl StringAgg {
    pub fn new(
        agg_col_idx: usize,
        delimiter: ExpressionRef,
        order_pairs: Vec<OrderPair>,
        order_col_types: Vec<DataType>,
    ) -> Self {
        StringAgg {
            agg_col_idx,
            delimiter,
            state: if order_pairs.is_empty() {
                StringAggState::WithoutOrder { result: None }
            } else {
                StringAggState::WithOrder {
                    order_pairs: Arc::new(order_pairs),
                    min_heap: BinaryHeap::new(),
                    encodable: order_col_types
                        .iter()
                        .map(Clone::clone)
                        .all(is_type_encodable),
                }
            },
        }
    }

    fn eval_delimiter(expr: &ExpressionRef, row: &Row) -> Result<String> {
        match expr.eval_row(row)? {
            None => Ok("".to_string()),
            Some(delim_str) => Ok(delim_str.into_utf8()),
        }
    }

    fn push_row(&mut self, s: &str, chunk: &DataChunk, row_id: usize) -> Result<()> {
        match &mut self.state {
            StringAggState::WithoutOrder { result } => match result {
                Some(result) => {
                    let (row_ref, vis) = chunk.row_at(row_id)?;
                    assert!(vis);
                    let row = row_ref.to_owned_row();
                    let delim = Self::eval_delimiter(&self.delimiter, &row)?;
                    result.push_str(&delim);
                    result.push_str(s);
                }
                None => {
                    *result = Some(s.to_string());
                }
            },
            StringAggState::WithOrder {
                order_pairs,
                min_heap,
                encodable,
            } => {
                let (row_ref, vis) = chunk.row_at(row_id)?;
                assert!(vis);
                let row = row_ref.to_owned_row();
                let encoded_row = if *encodable {
                    Some(encode_row(&row, order_pairs))
                } else {
                    None
                };
                min_heap.push(DescOrderedRow {
                    row,
                    encoded_row,
                    order_pairs: order_pairs.clone(),
                });
            }
        }
        Ok(())
    }

    fn get_result(&self) -> Result<Option<String>> {
        match &self.state {
            StringAggState::WithoutOrder { result } => Ok(result.clone()),
            StringAggState::WithOrder {
                order_pairs: _,
                min_heap,
                encodable: _,
            } => {
                if min_heap.is_empty() {
                    Ok(None)
                } else {
                    let mut result = String::new();
                    let mut first = true;
                    for orow in min_heap.clone().into_iter_sorted() {
                        let mut row = orow.row;
                        if !first {
                            let delim = Self::eval_delimiter(&self.delimiter, &row)?;
                            result.push_str(&delim);
                        }
                        let value = match row.0[self.agg_col_idx] {
                            Some(ScalarImpl::Utf8(ref mut s)) => std::mem::take(s),
                            _ => panic!("Expected Utf8"),
                        };
                        result.push_str(&value);
                        first = false;
                    }
                    Ok(Some(result))
                }
            }
        }
    }

    fn get_result_and_reset(&mut self) -> Result<Option<String>> {
        match &mut self.state {
            StringAggState::WithoutOrder { result } => {
                let res = result.clone();
                *result = None;
                Ok(res)
            }
            StringAggState::WithOrder {
                order_pairs: _,
                min_heap,
                encodable: _,
            } => {
                if min_heap.is_empty() {
                    Ok(None)
                } else {
                    let mut result = String::new();
                    let mut first = true;
                    for orow in min_heap.drain_sorted() {
                        let mut row = orow.row;
                        if !first {
                            let delim = Self::eval_delimiter(&self.delimiter, &row)?;
                            result.push_str(&delim);
                        }
                        let value = match row.0[self.agg_col_idx] {
                            Some(ScalarImpl::Utf8(ref mut s)) => std::mem::take(s),
                            _ => panic!("Expected Utf8"),
                        };
                        result.push_str(&value);
                        first = false;
                    }
                    Ok(Some(result))
                }
            }
        }
    }
}

impl Aggregator for StringAgg {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let ArrayImpl::Utf8(col) = input.column_at(self.agg_col_idx).array_ref() {
            if let Some(s) = col.value_at(row_id) {
                // only need to save rows with non-empty string value to aggregate
                self.push_row(s, input, row_id)?;
            }
            Ok(())
        } else {
            Err(
                ErrorCode::InternalError(format!("Input fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        if let ArrayImpl::Utf8(col) = input.column_at(self.agg_col_idx).array_ref() {
            for (i, s) in col
                .iter()
                .enumerate()
                .skip(start_row_id)
                .take(end_row_id - start_row_id)
                .filter(|(_, s)| s.is_some())
            {
                self.push_row(s.unwrap(), input, i)?;
            }
            Ok(())
        } else {
            Err(
                ErrorCode::InternalError(format!("Input fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::Utf8(builder) = builder {
            let s = self.get_result()?;
            if let Some(s) = s {
                builder.append(Some(&s))
            } else {
                builder.append(None)
            }
            .map_err(Into::into)
        } else {
            Err(
                ErrorCode::InternalError(format!("Builder fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn output_and_reset(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::Utf8(builder) = builder {
            let s = self.get_result_and_reset()?;
            if let Some(s) = s {
                builder.append(Some(&s))
            } else {
                builder.append(None)
            }
            .map_err(Into::into)
        } else {
            Err(
                ErrorCode::InternalError(format!("Builder fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt, Utf8ArrayBuilder};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};

    use super::*;
    use crate::expr::{Expression, ExpressionRef, LiteralExpression};
    use crate::vector_op::agg::aggregator::Aggregator;

    fn empty_delimiter() -> ExpressionRef {
        Arc::from(LiteralExpression::new(DataType::Varchar, Some("".to_string().into())).boxed())
    }

    #[test]
    fn test_basic_string_agg() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T
             aaa
             bbb
             ccc
             ddd",
        );
        let mut agg = StringAgg::new(0, empty_delimiter(), vec![], vec![]);
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
        agg.output(&mut builder)?;
        agg.output_and_reset(&mut builder)?;
        let output = builder.finish()?;
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "aaabbbcccddd";
        assert_eq!(actual, &[Some(expected), Some(expected)]);
        Ok(())
    }

    #[test]
    fn test_string_agg_with_order() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   i i
             aaa 1 3
             bbb 0 4
             ccc 0 8
             ddd 1 3",
        );
        let mut agg = StringAgg::new(
            0,
            empty_delimiter(),
            vec![
                OrderPair::new(1, OrderType::Ascending),
                OrderPair::new(2, OrderType::Descending),
                OrderPair::new(0, OrderType::Descending),
            ],
            vec![DataType::Int32, DataType::Int32, DataType::Varchar],
        );
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
        agg.output(&mut builder)?;
        agg.output_and_reset(&mut builder)?;
        let output = builder.finish()?;
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "cccbbbdddaaa";
        assert_eq!(actual, &[Some(expected), Some(expected)]);
        Ok(())
    }
}
