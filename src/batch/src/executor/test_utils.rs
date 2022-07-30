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

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;

use assert_matches::assert_matches;
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, DataChunkTestExt};
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::field_generator::FieldGeneratorImpl;
use risingwave_common::types::{DataType, Datum, ToOwnedDatum};
use risingwave_expr::expr::BoxedExpression;
use risingwave_pb::batch_plan::ExchangeSource as ProstExchangeSource;

use crate::exchange_source::{ExchangeSource, ExchangeSourceImpl};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, CreateSource, Executor, ProbeSideSourceBuilder,
};
use crate::task::BatchTaskContext;

const SEED: u64 = 0xFF67FEABBAEF76FF;

/// Generate `batch_num` data chunks with type `Int64`, each data chunk has cardinality of
/// `batch_size`.
pub fn gen_data(batch_size: usize, batch_num: usize) -> Vec<DataChunk> {
    let mut data_gen =
        FieldGeneratorImpl::with_random(DataType::Int64, None, None, None, None, SEED).unwrap();
    let mut ret = Vec::<DataChunk>::with_capacity(batch_num);

    for i in 0..batch_num {
        let mut array_builder = DataType::Int64.create_array_builder(batch_size);

        for j in 0..batch_size {
            array_builder
                .append_datum(&data_gen.generate_datum(((i + 1) * (j + 1)) as u64))
                .unwrap();
        }

        let array = array_builder.finish().unwrap();
        ret.push(DataChunk::new(
            vec![Column::new(Arc::new(array))],
            batch_size,
        ));
    }

    ret
}

/// Generate `batch_num` sorted data chunks with type `Int64`, each data chunk has cardinality of
/// `batch_size`.
pub fn gen_sorted_data(
    batch_size: usize,
    batch_num: usize,
    start: String,
    step: u64,
) -> Vec<DataChunk> {
    let mut data_gen = FieldGeneratorImpl::with_sequence(
        DataType::Int64,
        Some(start),
        Some(i64::MAX.to_string()),
        0,
        step,
    )
    .unwrap();
    let mut ret = Vec::<DataChunk>::with_capacity(batch_num);

    for _ in 0..batch_num {
        let mut array_builder = DataType::Int64.create_array_builder(batch_size);

        for _ in 0..batch_size {
            array_builder
                .append_datum(&data_gen.generate_datum(0))
                .unwrap();
        }

        let array = array_builder.finish().unwrap();
        ret.push(DataChunk::new(
            vec![Column::new(Arc::new(array))],
            batch_size,
        ));
    }

    ret
}

/// Generate `batch_num` data chunks with type `Int64`, each data chunk has cardinality of
/// `batch_size`. Then project each data chunk with `expr`.
///
/// NOTE: For convenience, here we only use data type `Int64`.
pub fn gen_projected_data(
    batch_size: usize,
    batch_num: usize,
    expr: BoxedExpression,
) -> Vec<DataChunk> {
    let mut data_gen =
        FieldGeneratorImpl::with_random(DataType::Int64, None, None, None, None, SEED).unwrap();
    let mut ret = Vec::<DataChunk>::with_capacity(batch_num);

    for i in 0..batch_num {
        let mut array_builder = DataType::Int64.create_array_builder(batch_size);

        for j in 0..batch_size {
            array_builder
                .append_datum(&data_gen.generate_datum(((i + 1) * (j + 1)) as u64))
                .unwrap();
        }

        let array = array_builder.finish().unwrap();
        let chunk = DataChunk::new(vec![Column::new(Arc::new(array))], batch_size);

        let array = expr.eval(&chunk).unwrap();
        let chunk = DataChunk::new(vec![Column::new(array)], batch_size);
        ret.push(chunk);
    }

    ret
}

/// Mock the input of executor.
/// You can bind one or more `MockExecutor` as the children of the executor to test,
/// (`HashAgg`, e.g), so that allow testing without instantiating real `SeqScan`s and real storage.
pub struct MockExecutor {
    chunks: VecDeque<DataChunk>,
    schema: Schema,
    identity: String,
}

impl MockExecutor {
    pub fn new(schema: Schema) -> Self {
        Self {
            chunks: VecDeque::new(),
            schema,
            identity: "MockExecutor".to_string(),
        }
    }

    pub fn with_chunk(chunk: DataChunk, schema: Schema) -> Self {
        let mut ret = Self::new(schema);
        ret.add(chunk);
        ret
    }

    pub fn add(&mut self, chunk: DataChunk) {
        self.chunks.push_back(chunk);
    }
}

impl Executor for MockExecutor {
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

impl MockExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        for data_chunk in self.chunks {
            yield data_chunk;
        }
    }
}

/// if the input from two child executor is same(considering order),
/// it will also check the columns structure of chunks from child executor
/// use for executor unit test.
///
/// if want diff ignoring order, add a `order_by` executor in manual currently, when the `schema`
/// method of `executor` is ready, an order-ignored version will be added.
pub async fn diff_executor_output(actual: BoxedExecutor, expect: BoxedExecutor) {
    let mut expect_cardinality = 0;
    let mut actual_cardinality = 0;
    let mut expects = vec![];
    let mut actuals = vec![];

    #[for_await]
    for chunk in expect.execute() {
        assert_matches!(chunk, Ok(_));
        let chunk = chunk.unwrap().compact().unwrap();
        expect_cardinality += chunk.cardinality();
        expects.push(chunk);
    }

    #[for_await]
    for chunk in actual.execute() {
        assert_matches!(chunk, Ok(_));
        let chunk = chunk.unwrap().compact().unwrap();
        actual_cardinality += chunk.cardinality();
        actuals.push(chunk);
    }

    assert_eq!(actual_cardinality, expect_cardinality);
    if actual_cardinality == 0 {
        return;
    }
    let expect = DataChunk::rechunk(expects.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let actual = DataChunk::rechunk(actuals.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let col_num = expect.columns().len();
    assert_eq!(col_num, actual.columns().len());
    expect
        .columns()
        .iter()
        .zip_eq(actual.columns().iter())
        .for_each(|(c1, c2)| assert_eq!(c1.array().to_protobuf(), c2.array().to_protobuf()));

    is_data_chunk_eq(&expect, &actual)
}

fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) {
    assert!(left.visibility().is_none());
    assert!(right.visibility().is_none());

    assert_eq!(
        left.cardinality(),
        right.cardinality(),
        "two chunks cardinality is different"
    );

    left.rows()
        .zip_eq(right.rows())
        .for_each(|(row1, row2)| assert_eq!(row1, row2));
}

#[derive(Debug, Clone)]
pub struct FakeExchangeSource {
    chunks: Vec<Option<DataChunk>>,
}

impl FakeExchangeSource {
    pub fn new(chunks: Vec<Option<DataChunk>>) -> Self {
        Self { chunks }
    }
}

impl ExchangeSource for FakeExchangeSource {
    type TakeDataFuture<'a> = impl Future<Output = Result<Option<DataChunk>>>;

    fn take_data(&mut self) -> Self::TakeDataFuture<'_> {
        async {
            if let Some(chunk) = self.chunks.pop() {
                Ok(chunk)
            } else {
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct FakeCreateSource {
    fake_exchange_source: FakeExchangeSource,
}

impl FakeCreateSource {
    pub fn new(fake_exchange_source: FakeExchangeSource) -> Self {
        Self {
            fake_exchange_source,
        }
    }
}

#[async_trait::async_trait]
impl CreateSource for FakeCreateSource {
    async fn create_source(
        &self,
        _: impl BatchTaskContext,
        _: &ProstExchangeSource,
    ) -> Result<ExchangeSourceImpl> {
        Ok(ExchangeSourceImpl::Fake(self.fake_exchange_source.clone()))
    }
}

pub struct FakeProbeSideSourceBuilder {
    schema: Schema,
    datums: Vec<Vec<Datum>>,
}

impl FakeProbeSideSourceBuilder {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            datums: vec![],
        }
    }
}

#[async_trait::async_trait]
impl ProbeSideSourceBuilder for FakeProbeSideSourceBuilder {
    async fn build_source(&self) -> Result<BoxedExecutor> {
        let mut mock_executor = MockExecutor::new(self.schema.clone());

        let base_data_chunk = DataChunk::from_pretty(
            "i f
             1 9.2
             2 4.4
             2 5.5
             4 6.8
             5 3.7
             5 2.3
             . .",
        );

        for idx in 0..base_data_chunk.capacity() {
            let probe_row = base_data_chunk.row_at_unchecked_vis(idx);
            for datum in &self.datums {
                if datum[0] == probe_row.value_at(0).to_owned_datum() {
                    let owned_row = probe_row.to_owned_row();
                    let chunk =
                        DataChunk::from_rows(&[owned_row], &[DataType::Int32, DataType::Float32])?;
                    mock_executor.add(chunk);
                    break;
                }
            }
        }

        Ok(Box::new(mock_executor))
    }

    fn add_scan_range(&mut self, key_datums: &[Datum]) -> Result<()> {
        self.datums.push(key_datums.iter().cloned().collect_vec());
        Ok(())
    }

    fn reset(&mut self) {
        self.datums = vec![];
    }
}
