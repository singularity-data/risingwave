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

use itertools::Itertools;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::types::Row;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;

pub fn to_pg_rows(chunk: DataChunk) -> Vec<Row> {
    chunk
        .rows()
        .map(|r| {
            Row::new(
                r.0.into_iter()
                    .map(|data| data.map(|d| d.to_string()))
                    .collect_vec(),
            )
        })
        .collect_vec()
}

/// Convert from [`Field`] to [`PgFieldDescriptor`].
pub fn to_pg_field(f: &Field) -> PgFieldDescriptor {
    PgFieldDescriptor::new(f.name.clone(), data_type_to_type_oid(f.data_type()))
}

pub fn data_type_to_type_oid(data_type: DataType) -> TypeOid {
    match data_type {
        DataType::Int16 => TypeOid::SmallInt,
        DataType::Int32 => TypeOid::Int,
        DataType::Int64 => TypeOid::BigInt,
        DataType::Float32 => TypeOid::Float4,
        DataType::Float64 => TypeOid::Float8,
        DataType::Boolean => TypeOid::Boolean,
        DataType::Char => TypeOid::CharArray,
        DataType::Varchar => TypeOid::Varchar,
        DataType::Date => TypeOid::Date,
        DataType::Time => TypeOid::Time,
        DataType::Timestamp => TypeOid::Timestamp,
        DataType::Timestampz => TypeOid::Timestampz,
        DataType::Decimal => TypeOid::Decimal,
        DataType::Interval => TypeOid::Varchar,
        DataType::Struct { .. } => TypeOid::Varchar,
        DataType::List { .. } => TypeOid::Varchar,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::{column, column_nonnull};

    use super::*;

    #[test]
    fn test_to_pg_field() {
        let field = Field::with_name(DataType::Int32, "v1");
        let pg_field = to_pg_field(&field);
        assert_eq!(pg_field.get_name(), "v1");
        assert_eq!(
            pg_field.get_type_oid().as_number(),
            TypeOid::Int.as_number()
        );
    }

    #[test]
    fn test_to_pg_rows() {
        let chunk = DataChunk::new(
            vec![
                column_nonnull!(I32Array, [1, 2, 3, 4]),
                column!(I64Array, [Some(6), None, Some(7), None]),
                column!(F32Array, [Some(6.01), None, Some(7.01), None]),
                column!(Utf8Array, [Some("aaa"), None, Some("vvv"), None]),
            ],
            None,
        );
        let rows = to_pg_rows(chunk);
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("6".to_string()),
                Some("6.01".to_string()),
                Some("aaa".to_string()),
            ],
            vec![Some("2".to_string()), None, None, None],
            vec![
                Some("3".to_string()),
                Some("7".to_string()),
                Some("7.01".to_string()),
                Some("vvv".to_string()),
            ],
            vec![Some("4".to_string()), None, None, None],
        ];
        let vec = rows
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }
}
