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

use futures::future::try_join_all;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use crate::executor::ExecutorBuilder;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2};

/// [`UpdateExecutor`] implements table updation with values from its child executor and given
/// expressions.
// TODO: multiple `UPDATE`s in a single epoch may cause problems. Need validation on materialize.
// TODO: concurrent `UPDATE` may cause problems. A scheduler might be required.
pub struct UpdateExecutor {
    /// Target table id.
    table_id: TableId,
    source_manager: SourceManagerRef,
    child: BoxedExecutor2,
    exprs: Vec<BoxedExpression>,
    schema: Schema,
    identity: String,
}

impl UpdateExecutor {
    pub fn new(
        table_id: TableId,
        source_manager: SourceManagerRef,
        child: BoxedExecutor2,
        exprs: Vec<BoxedExpression>,
    ) -> Self {
        Self {
            table_id,
            source_manager,
            child,
            exprs,
            // TODO: support `RETURNING`
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            identity: "UpdateExecutor".to_string(),
        }
    }
}

impl Executor2 for UpdateExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl UpdateExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let source_desc = self.source_manager.get_source(&self.table_id)?;
        let source = source_desc.source.as_table_v2().expect("not table source");

        let schema = self.child.schema().clone();
        let mut notifiers = Vec::new();

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?.compact()?;
            let len = data_chunk.cardinality();

            let updated_data_chunk = {
                let columns = self
                    .exprs
                    .iter_mut()
                    .map(|expr| expr.eval(&data_chunk).map(Column::new))
                    .collect::<Result<Vec<_>>>()?;

                DataChunk::builder().columns(columns).build()
            };

            // Merge two data chunks into (U-, U+) pairs.
            // TODO: split chunks
            let mut builders = schema.create_array_builders(len * 2)?;
            for row in data_chunk
                .rows()
                .zip_eq(updated_data_chunk.rows())
                .flat_map(|(a, b)| [a, b])
            {
                for (datum_ref, builder) in row.values().zip_eq(builders.iter_mut()) {
                    builder.append_datum_ref(datum_ref)?;
                }
            }
            let columns = builders
                .into_iter()
                .map(|b| b.finish().map(|a| a.into()))
                .collect::<Result<Vec<_>>>()?;

            let ops = [Op::UpdateDelete, Op::UpdateInsert]
                .into_iter()
                .cycle()
                .take(len * 2)
                .collect();

            let stream_chunk = StreamChunk::new(ops, columns, None);

            let notifier = source.write_chunk(stream_chunk)?;
            notifiers.push(notifier);
        }

        // Wait for all chunks to be taken / written.
        let rows_updated = try_join_all(notifiers)
            .await
            .map_err(|_| {
                RwError::from(ErrorCode::InternalError(
                    "failed to wait chunks to be written".to_owned(),
                ))
            })?
            .into_iter()
            .sum::<usize>()
            / 2;

        // Create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1)?;
            array_builder.append(Some(rows_updated as i64))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder().columns(vec![array.into()]).build();

            yield ret_chunk
        }
    }
}

impl BoxedExecutor2Builder for UpdateExecutor {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        let update_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Update
        )?;

        let table_id = TableId::from(&update_node.table_source_ref_id);

        let exprs = update_node
            .get_exprs()
            .iter()
            .map(build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build2()?;

        assert_eq!(
            child.schema().data_types(),
            exprs.iter().map(|e| e.return_type()).collect_vec(),
            "bad update schema"
        );

        Ok(Box::new(Self::new(
            table_id,
            source.global_batch_env().source_manager_ref(),
            child,
            exprs,
        )))
    }
}
