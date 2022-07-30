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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Vis};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct ExpandExecutor {
    column_subsets: Vec<Vec<usize>>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
}

impl Executor for ExpandExecutor {
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

impl ExpandExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut data_chunk_builder = DataChunkBuilder::with_default_size(self.schema.data_types());

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk: DataChunk = data_chunk?.compact()?;
            assert!(
                data_chunk.dimension() > 0,
                "The input data chunk of expand can't be dummy chunk."
            );
            let cardinality = data_chunk.cardinality();
            let (columns, vis) = data_chunk.into_parts();
            assert_eq!(vis, Vis::Compact(cardinality));

            #[for_await]
            for new_columns in
                Column::expand_columns(cardinality, columns, self.column_subsets.to_owned())
            {
                #[for_await]
                for data_chunk in
                    data_chunk_builder.trunc_data_chunk(DataChunk::new(new_columns?, vis.clone()))
                {
                    yield data_chunk?;
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all()? {
            yield chunk;
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for ExpandExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.len() == 1);
        let expand_node = source.plan_node().get_node_body()?.as_expand().unwrap();

        let column_subsets = expand_node
            .column_subsets
            .iter()
            .map(|subset| {
                subset
                    .column_indices
                    .iter()
                    .map(|idx| *idx as usize)
                    .collect_vec()
            })
            .collect_vec();

        let child = inputs.remove(0);

        let mut schema = child.schema().clone();
        schema
            .fields
            .push(Field::with_name(DataType::Int64, "flag"));

        Ok(Box::new(Self {
            column_subsets,
            child,
            schema,
            identity: "ExpandExecutor".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::ExpandExecutor;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::Executor;

    #[tokio::test]
    async fn test_expand_executor() {
        let mock_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let expand_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int64),
            ],
        };
        let mut mock_executor = MockExecutor::new(mock_schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i i
             1 2 3
             2 3 4",
        ));
        let column_subsets = vec![vec![0, 1], vec![1, 2]];
        let expand_executor = Box::new(ExpandExecutor {
            column_subsets,
            child: Box::new(mock_executor),
            schema: expand_schema,
            identity: "ExpandExecutor".to_string(),
        });
        let mut stream = expand_executor.execute();
        let res = stream.next().await.unwrap().unwrap();
        let expected_chunk = DataChunk::from_pretty(
            "i i i I
             1 2 . 0
             2 3 . 0
             . 2 3 1
             . 3 4 1",
        );
        assert_eq!(res, expected_chunk);
    }
}
