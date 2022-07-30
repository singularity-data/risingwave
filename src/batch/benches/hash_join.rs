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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::StreamExt;
use risingwave_batch::executor::hash_join::HashJoinExecutor;
use risingwave_batch::executor::test_utils::{gen_projected_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, JoinType};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::hash;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type::{
    ConstantValue as TConstValue, GreaterThan, InputRef, Modulus,
};
use risingwave_pb::expr::{ConstantValue, ExprNode, FunctionCall, InputRefExpr};
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_hash_join_executor(
    join_type: JoinType,
    with_cond: bool,
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    let left_mod123 = {
        let input_ref = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };
        let literal123 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(123).to_protobuf(),
            })),
        };
        ExprNode {
            expr_type: Modulus as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![input_ref, literal123],
            })),
        }
    };
    let right_mod456 = {
        let input_ref = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };
        let literal456 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(456).to_protobuf(),
            })),
        };
        ExprNode {
            expr_type: Modulus as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![input_ref, literal456],
            })),
        }
    };
    let left_input = gen_projected_data(
        left_chunk_size,
        left_chunk_num,
        build_from_prost(&left_mod123).unwrap(),
    );
    let right_input = gen_projected_data(
        right_chunk_size,
        right_chunk_num,
        build_from_prost(&right_mod456).unwrap(),
    );

    let mut left_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    left_input.into_iter().for_each(|c| left_child.add(c));

    let mut right_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    right_input.into_iter().for_each(|c| right_child.add(c));

    let output_indices = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => vec![0],
        JoinType::RightSemi | JoinType::RightAnti => vec![0],
        _ => vec![0, 1],
    };

    let cond = if with_cond {
        let left_input_ref = ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: 0 })),
        };
        let literal100 = ExprNode {
            expr_type: TConstValue as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::Constant(ConstantValue {
                body: ScalarImpl::Int64(100).to_protobuf(),
            })),
        };
        Some(ExprNode {
            expr_type: GreaterThan as i32,
            return_type: Some(risingwave_pb::data::DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![left_input_ref, literal100],
            })),
        })
    } else {
        None
    }
    .map(|expr| build_from_prost(&expr).unwrap());

    Box::new(HashJoinExecutor::<hash::Key64>::new(
        join_type,
        output_indices,
        left_child,
        right_child,
        vec![0],
        vec![0],
        false,
        cond,
        "HashJoinExecutor".into(),
    ))
}

async fn execute_hash_join_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_hash_join(c: &mut Criterion) {
    const LEFT_SIZE: usize = 2 * 1024;
    const RIGHT_SIZE: usize = 2 * 1024;
    let rt = Runtime::new().unwrap();
    for with_cond in [false, true] {
        for join_type in &[
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightOuter,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            for chunk_size in &[32, 128, 512, 1024] {
                c.bench_with_input(
                    BenchmarkId::new(
                        "HashJoinExecutor",
                        format!(
                            "{}({:?})(non_equi_join: {})",
                            chunk_size, join_type, with_cond
                        ),
                    ),
                    chunk_size,
                    |b, &chunk_size| {
                        let left_chunk_num = LEFT_SIZE / chunk_size;
                        let right_chunk_num = RIGHT_SIZE / chunk_size;
                        b.to_async(&rt).iter_batched(
                            || {
                                create_hash_join_executor(
                                    *join_type,
                                    with_cond,
                                    chunk_size,
                                    left_chunk_num,
                                    chunk_size,
                                    right_chunk_num,
                                )
                            },
                            |e| execute_hash_join_executor(e),
                            BatchSize::SmallInput,
                        );
                    },
                );
            }
        }
    }
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);
