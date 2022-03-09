use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::Op::*;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use super::state::ManagedMViewState;
use crate::executor::{
    Barrier, Executor, ExecutorBuilder, Message, PkIndicesRef, Result, SimpleExecutor, StreamChunk,
};
use crate::task::{ExecutorParams, StreamManagerCore};

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore> {
    input: Box<dyn Executor>,

    local_state: ManagedMViewState<S>,

    /// Columns of primary keys
    pk_columns: Vec<usize>,

    /// Columns of group keys
    ///
    /// Group columns will only be used to create a shared arrangement. For normal materialized
    /// view created from source, there should be no group columns.
    group_columns: Vec<usize>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
}

pub struct MaterializeExecutorBuilder {}

impl ExecutorBuilder for MaterializeExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MaterializeNode)?;

        let table_id = TableId::from(&node.table_ref_id);
        let keys = node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        let column_ids = node
            .column_ids
            .iter()
            .map(|id| ColumnId::from(*id))
            .collect();

        let keyspace = Keyspace::table_root(store, &table_id);

        Ok(Box::new(MaterializeExecutor::new(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            params.op_info,
        )))
    }
}

impl<S: StateStore> MaterializeExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        keyspace: Keyspace<S>,
        keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        Self::new_grouped(
            input,
            keyspace,
            vec![],
            keys,
            column_ids,
            executor_id,
            op_info,
        )
    }

    pub fn new_grouped(
        input: Box<dyn Executor>,
        keyspace: Keyspace<S>,
        group_keys: Vec<OrderPair>,
        primary_keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        let pk_columns = primary_keys.iter().map(|k| k.column_idx).collect();
        let pk_order_types = primary_keys.iter().map(|k| k.order_type).collect();

        let group_columns = group_keys.iter().map(|k| k.column_idx).collect();
        let group_order_types = group_keys.iter().map(|k| k.order_type).collect();

        Self {
            input,
            local_state: ManagedMViewState::new_grouped(
                keyspace,
                column_ids,
                pk_order_types,
                group_order_types,
            ),
            pk_columns,
            identity: format!("GroupMaterializeExecutor {:X}", executor_id),
            group_columns,
            op_info,
        }
    }

    async fn flush(&mut self, barrier: Barrier) -> Result<Message> {
        self.local_state.flush(barrier.epoch.prev).await?;
        Ok(Message::Barrier(barrier))
    }
}

#[async_trait]
impl<S: StateStore> Executor for MaterializeExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        match self.input().next().await {
            Ok(message) => match message {
                Message::Chunk(chunk) => self.consume_chunk(chunk),
                Message::Barrier(b) => self.flush(b).await,
            },
            Err(e) => Err(e),
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_columns
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }

    fn reset(&mut self, _epoch: u64) {
        self.local_state.clear_cache();
    }
}

impl<S: StateStore> std::fmt::Debug for MaterializeExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("input", &self.input)
            .field("pk_columns", &self.pk_columns)
            .finish()
    }
}

impl<S: StateStore> SimpleExecutor for MaterializeExecutor<S> {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        for (idx, op) in chunk.ops().iter().enumerate() {
            // check visibility
            let visible = chunk
                .visibility()
                .as_ref()
                .map(|x| x.is_set(idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            // group key first, then primary key
            let encode_key_iter = self.group_columns.iter().chain(self.pk_columns.iter());

            // assemble arrange key row
            let arrange_row = Row(encode_key_iter
                .map(|col_idx| chunk.column_at(*col_idx).array_ref().datum_at(idx))
                .collect_vec());

            // assemble row
            let row = Row(chunk
                .columns()
                .iter()
                .map(|x| x.array_ref().datum_at(idx))
                .collect_vec());

            match op {
                Insert | UpdateInsert => {
                    self.local_state.put(arrange_row, row);
                }
                Delete | UpdateDelete => {
                    self.local_state.delete(arrange_row);
                }
            }
        }

        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_common::array::{I32Array, Op};
    use risingwave_common::catalog::{ColumnId, Schema, TableId};
    use risingwave_common::column_nonnull;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::plan::ColumnDesc;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();

        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let columns = vec![
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                name: "v1".to_string(),
                column_id: 0,
            },
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                name: "v2".to_string(),
                column_id: 1,
            },
        ];
        let column_ids = columns
            .iter()
            .map(|c| ColumnId::from(c.column_id))
            .collect_vec();

        // Prepare source chunks.
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I32Array, [1, 2, 3] },
                column_nonnull! { I32Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I32Array, [7, 3] },
                column_nonnull! { I32Array, [8, 6] },
            ],
            None,
        );

        // Prepare stream executors.
        let schema = Schema::try_from(&columns).unwrap();
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::default()),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::default()),
            ],
        );

        let keyspace = Keyspace::table_root(memory_state_store, &table_id);

        let mut materialize_executor = Box::new(MaterializeExecutor::new(
            Box::new(source),
            keyspace,
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids,
            1,
            "MaterializeExecutor".to_string(),
        ));

        materialize_executor.next().await.unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                // FIXME: restore this test by using new `RowTable` interface
                // let datum = table
                //     .get(Row(vec![Some(3_i32.into())]), 1, u64::MAX)
                //     .await
                //     .unwrap()
                //     .unwrap();
                // assert_eq!(*datum.unwrap().as_int32(), 6_i32);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                // FIXME: restore this test by using new `RowTable` interface
                // let datum = table
                //     .get(Row(vec![Some(7_i32.into())]), 1, u64::MAX)
                //     .await
                //     .unwrap()
                //     .unwrap();
                // assert_eq!(*datum.unwrap().as_int32(), 8);
            }
            _ => unreachable!(),
        }
    }
}
