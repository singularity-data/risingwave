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

use anyhow::anyhow;
use futures::future::try_join_all;
use futures_async_stream::try_stream;
use risingwave_common::array::{ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use crate::error::BatchError;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

/// [`DeleteExecutor`] implements table deletion with values from its child executor.
// TODO: concurrent `DELETE` may cause problems. A scheduler might be required.
pub struct DeleteExecutor {
    /// Target table id.
    table_id: TableId,
    source_manager: SourceManagerRef,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
}

impl DeleteExecutor {
    pub fn new(table_id: TableId, source_manager: SourceManagerRef, child: BoxedExecutor) -> Self {
        Self {
            table_id,
            source_manager,
            child,
            // TODO: support `RETURNING`
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            identity: "DeleteExecutor".to_string(),
        }
    }
}

impl Executor for DeleteExecutor {
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

impl DeleteExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let source_desc = self.source_manager.get_source(&self.table_id)?;
        let source = source_desc.source.as_table_v2().expect("not table source");

        let mut notifiers = Vec::new();

        #[for_await]
        for data_chunk in self.child.execute() {
            let data_chunk = data_chunk?;
            let len = data_chunk.cardinality();
            assert!(data_chunk.visibility().is_none());

            let chunk = StreamChunk::from_parts(vec![Op::Delete; len], data_chunk);

            let notifier = source.write_chunk(chunk)?;
            notifiers.push(notifier);
        }

        // Wait for all chunks to be taken / written.
        let rows_deleted = try_join_all(notifiers)
            .await
            .map_err(|_| BatchError::Internal(anyhow!("failed to wait chunks to be written")))?
            .into_iter()
            .sum::<usize>();

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1);
            array_builder.append(Some(rows_deleted as i64))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::new(vec![array.into()], 1);

            yield ret_chunk
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for DeleteExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.len() == 1, "Delete executor should have 1 child!");
        let delete_node = source.plan_node().get_node_body()?.as_delete().unwrap();

        let table_id = TableId::new(delete_node.table_source_id);

        Ok(Box::new(Self::new(
            table_id,
            source
                .context()
                .source_manager_ref()
                .ok_or_else(|| BatchError::Internal(anyhow!("Source manager not found")))?,
            inputs.remove(0),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array::Array;
    use risingwave_common::catalog::{schema_test_utils, ColumnDesc, ColumnId};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_source::{MemSourceManager, SourceManager, StreamSourceReader};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_delete_executor() -> Result<()> {
        let source_manager = Arc::new(MemSourceManager::default());

        // Schema for mock executor.
        let schema = schema_test_utils::ii();
        let mut mock_executor = MockExecutor::new(schema.clone());

        // Schema of the table
        let schema = schema_test_utils::ii();

        let table_columns: Vec<_> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnDesc {
                data_type: f.data_type.clone(),
                column_id: ColumnId::from(i as i32), // use column index as column id
                name: f.name.clone(),
                field_descs: vec![],
                type_name: "".to_string(),
            })
            .collect();

        mock_executor.add(DataChunk::from_pretty(
            "i  i
             1  2
             3  4
             5  6
             7  8
             9 10",
        ));

        // Create the table.
        let table_id = TableId::new(0);
        source_manager.create_table_source(&table_id, table_columns.to_vec())?;

        // Create reader
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.source.as_table_v2().unwrap();
        let mut reader = source.stream_reader(vec![0.into(), 1.into()]).await?;

        // Delete
        let delete_executor = Box::new(DeleteExecutor::new(
            table_id,
            source_manager.clone(),
            Box::new(mock_executor),
        ));

        let handle = tokio::spawn(async move {
            let fields = &delete_executor.schema().fields;
            assert_eq!(fields[0].data_type, DataType::Int64);

            let mut stream = delete_executor.execute();
            let result = stream.next().await.unwrap().unwrap();

            assert_eq!(
                result
                    .column_at(0)
                    .array()
                    .as_int64()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(5)] // deleted rows
            );
        });

        // Read
        let chunk = reader.next().await?;

        assert_eq!(chunk.chunk.ops().to_vec(), vec![Op::Delete; 5]);

        assert_eq!(
            chunk.chunk.columns()[0]
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
        );

        assert_eq!(
            chunk.chunk.columns()[1]
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(4), Some(6), Some(8), Some(10)]
        );

        handle.await.unwrap();

        Ok(())
    }
}
