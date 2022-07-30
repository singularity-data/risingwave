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

use anyhow::{anyhow, Result};
use num_traits::FromPrimitive;
use risingwave_common::array::StructValue;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_time, str_to_timestamp};
use serde_json::Value;

macro_rules! ensure_float {
    ($v:ident, $t:ty) => {
        $v.as_f64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

macro_rules! ensure_int {
    ($v:ident, $t:ty) => {
        $v.as_i64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

macro_rules! ensure_str {
    ($v:ident, $t:literal) => {
        $v.as_str()
            .ok_or_else(|| anyhow!(concat!("expect ", $t, ", but found {}"), $v))?
    };
}

fn do_parse_json_value(column: &ColumnDesc, v: &Value) -> Result<ScalarImpl> {
    let v = match column.data_type.clone() {
        DataType::Boolean => v.as_bool().ok_or_else(|| anyhow!("expect bool"))?.into(),
        DataType::Int16 => ScalarImpl::Int16(
            ensure_int!(v, i16)
                .try_into()
                .map_err(|e| anyhow!("expect i16: {}", e))?,
        ),
        DataType::Int32 => ScalarImpl::Int32(
            ensure_int!(v, i32)
                .try_into()
                .map_err(|e| anyhow!("expect i32: {}", e))?,
        ),
        DataType::Int64 => ensure_int!(v, i64).into(),
        DataType::Float32 => ScalarImpl::Float32((ensure_float!(v, f32) as f32).into()),
        DataType::Float64 => ScalarImpl::Float64((ensure_float!(v, f64) as f64).into()),
        DataType::Decimal => Decimal::from_f64(ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_time(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => str_to_timestamp(ensure_str!(v, "timestamp"))?.into(),
        DataType::Timestampz => unimplemented!(),
        DataType::Interval => unimplemented!(),
        DataType::List { .. } => unimplemented!(),
        DataType::Struct { .. } => {
            let fields = column
                .field_descs
                .iter()
                .map(|field| json_parse_value(field, v.get(&field.name)))
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::Struct(StructValue::new(fields))
        }
    };
    Ok(v)
}

pub(crate) fn json_parse_value(column: &ColumnDesc, value: Option<&Value>) -> Result<Datum> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(do_parse_json_value(column, v).map_err(|e| {
            anyhow!("failed to parse column '{}' from json: {}", column.name, e)
        })?)),
    }
}
