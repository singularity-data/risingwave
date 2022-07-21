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

use std::convert::TryFrom;

use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::*;
use crate::executor::aggregation::{AggArgs, AggCall};

pub fn build_agg_call_from_prost(
    append_only: bool,
    agg_call_proto: &risingwave_pb::expr::AggCall,
) -> Result<AggCall> {
    let args = match &agg_call_proto.get_args()[..] {
        [] => AggArgs::None,
        [arg] => AggArgs::Unary(
            DataType::from(arg.get_type()?),
            arg.get_input()?.column_idx as usize,
        ),
        _ => {
            return Err(RwError::from(ErrorCode::NotImplemented(
                "multiple aggregation args".to_string(),
                None.into(),
            )))
        }
    };
    let filter = match agg_call_proto.filter {
        Some(ref prost_filter) => Some(Arc::from(build_from_prost(prost_filter)?)),
        None => None,
    };
    Ok(AggCall {
        kind: AggKind::try_from(agg_call_proto.get_type()?)?,
        args,
        return_type: DataType::from(agg_call_proto.get_return_type()?),
        append_only,
        filter,
    })
}