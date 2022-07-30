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
use risingwave_common::catalog::{ColumnDesc, ColumnId, DatabaseId, Field, SchemaId};
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::catalog::Table;
use risingwave_pb::plan_common::{ColumnOrder, Field as ProstField, OrderType as ProstOrderType};
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    ArrangeNode, ArrangementInfo, DeltaIndexJoinNode, DispatchStrategy, DispatcherType,
    ExchangeNode, LookupNode, LookupUnionNode, StreamNode,
};

use super::super::{BuildFragmentGraphState, StreamFragment, StreamFragmentEdge, StreamFragmenter};
use crate::catalog::TableCatalog;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;

impl StreamFragmenter {
    /// All exchanges inside delta join is one-to-one exchange.
    fn build_exchange_for_delta_join(
        &self,
        state: &mut BuildFragmentGraphState,
        upstream: &StreamNode,
    ) -> StreamNode {
        StreamNode {
            operator_id: state.gen_operator_id() as u64,
            identity: "Exchange (Lookup and Merge)".into(),
            fields: upstream.fields.clone(),
            pk_indices: upstream.pk_indices.clone(),
            node_body: Some(NodeBody::Exchange(ExchangeNode {
                strategy: Some(Self::dispatch_no_shuffle()),
            })),
            input: vec![],
            append_only: upstream.append_only,
        }
    }

    /// The ultimate exchange between delta join fragments and the upstream fragments.
    ///
    /// We should shuffle upstream data to make sure it meets the distribution requirement of hash
    /// join.
    fn build_input_with_exchange(
        &self,
        state: &mut BuildFragmentGraphState,
        mut upstream: StreamNode,
    ) -> Result<(StreamFragmentEdge, StreamFragment, StreamNode)> {
        match &upstream.node_body {
            // If the upstream contains a exchange, we should follow that distribution.
            Some(NodeBody::Exchange(exchange_node)) => {
                let exchange_node = exchange_node.clone();
                assert_eq!(upstream.input.len(), 1);
                let child_node = upstream.input.remove(0);
                let child_fragment = self.build_and_add_fragment(state, child_node)?;
                Ok((
                    StreamFragmentEdge {
                        dispatch_strategy: exchange_node.get_strategy()?.clone(),
                        same_worker_node: false,
                        link_id: upstream.operator_id,
                    },
                    child_fragment,
                    StreamNode {
                        input: vec![],
                        ..upstream.clone()
                    },
                ))
            }
            // Otherwise, use 1-to-1 exchange
            _ => {
                let strategy = Self::dispatch_no_shuffle();
                let operator_id = state.gen_operator_id() as u64;
                let node = StreamNode {
                    input: vec![],
                    identity: "Exchange (Arrange)".into(),
                    fields: upstream.fields.clone(),
                    pk_indices: upstream.pk_indices.clone(),
                    node_body: Some(NodeBody::Exchange(ExchangeNode {
                        strategy: Some(strategy.clone()),
                    })),
                    operator_id,
                    append_only: upstream.append_only,
                };

                let child_fragment = self.build_and_add_fragment(state, upstream)?;

                Ok((
                    StreamFragmentEdge {
                        dispatch_strategy: strategy,
                        same_worker_node: false,
                        link_id: operator_id,
                    },
                    child_fragment,
                    node,
                ))
            }
        }
    }

    fn build_arrange_for_delta_join(
        &self,
        state: &mut BuildFragmentGraphState,
        exchange_node: &StreamNode,
        arrange_key_indexes: Vec<i32>,
        table_catalog: &Table,
    ) -> (ArrangementInfo, StreamNode) {
        // Set materialize keys as arrange key + pk
        let arrange_key_orders = arrange_key_indexes
            .iter()
            .map(|x| OrderPair::new(*x as usize, OrderType::Ascending))
            .chain(
                exchange_node
                    .pk_indices
                    .iter()
                    .map(|x| OrderPair::new(*x as usize, OrderType::Ascending)),
            )
            .map(|x| ColumnOrder {
                order_type: x.order_type.to_prost() as i32,
                index: x.column_idx as u32,
            })
            .collect();

        // Simply generate column id 0..schema_len
        let column_descs = exchange_node
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let field = Field::from(field);
                let mut desc = ColumnDesc::from_field_without_column_id(&field);
                desc.column_id = ColumnId::new(idx as i32);
                desc.to_protobuf()
            })
            .collect();

        let arrangement_info = ArrangementInfo {
            arrange_key_orders,
            column_descs,
        };
        (
            arrangement_info.clone(),
            StreamNode {
                operator_id: state.gen_operator_id() as u64,
                identity: "Arrange".into(),
                fields: exchange_node.fields.clone(),
                pk_indices: exchange_node.pk_indices.clone(),
                node_body: Some(NodeBody::Arrange(ArrangeNode {
                    table_info: Some(arrangement_info),
                    // Requires arrange key at the first few columns. This is always true for delta
                    // join.
                    distribution_key: arrange_key_indexes.iter().map(|x| *x as _).collect(),
                    table: Some(table_catalog.clone()),
                })),
                input: vec![exchange_node.clone()],
                append_only: exchange_node.append_only,
            },
        )
    }

    fn dispatch_no_shuffle() -> DispatchStrategy {
        DispatchStrategy {
            r#type: DispatcherType::NoShuffle.into(),
            column_indices: vec![],
        }
    }

    fn build_lookup_for_delta_join(
        &self,
        state: &mut BuildFragmentGraphState,
        (exchange_node_arrangement, exchange_node_stream): (&StreamNode, &StreamNode),
        (output_fields, output_pk_indices): (Vec<ProstField>, Vec<u32>),
        lookup_node: LookupNode,
    ) -> StreamNode {
        StreamNode {
            operator_id: state.gen_operator_id() as u64,
            identity: "Lookup".into(),
            fields: output_fields,
            pk_indices: output_pk_indices,
            node_body: Some(NodeBody::Lookup(lookup_node)),
            input: vec![
                exchange_node_arrangement.clone(),
                exchange_node_stream.clone(),
            ],
            append_only: exchange_node_stream.append_only,
        }
    }

    fn build_delta_join_inner(
        &self,
        state: &mut BuildFragmentGraphState,
        current_fragment: &mut StreamFragment,
        arrange_0_frag: StreamFragment,
        arrange_1_frag: StreamFragment,
        node: &StreamNode,
        is_local_table_id: bool,
    ) -> Result<StreamNode> {
        let delta_join_node = match &node.node_body {
            Some(NodeBody::DeltaIndexJoin(node)) => node,
            _ => unreachable!(),
        };
        let output_indices = &delta_join_node.output_indices;

        let arrange_0 = arrange_0_frag.node.as_ref().unwrap();
        let arrange_1 = arrange_1_frag.node.as_ref().unwrap();
        let exchange_a0l0 = self.build_exchange_for_delta_join(state, arrange_0);
        let exchange_a0l1 = self.build_exchange_for_delta_join(state, arrange_0);
        let exchange_a1l0 = self.build_exchange_for_delta_join(state, arrange_1);
        let exchange_a1l1 = self.build_exchange_for_delta_join(state, arrange_1);

        let i0_length = arrange_0.fields.len();
        let i1_length = arrange_1.fields.len();

        let lookup_0_column_reordering = {
            let tmp: Vec<i32> = (i1_length..i1_length + i0_length)
                .chain(0..i1_length)
                .map(|x| x as _)
                .collect_vec();
            output_indices
                .iter()
                .map(|&x| tmp[x as usize])
                .collect_vec()
        };
        // lookup left table by right stream
        let lookup_0 = self.build_lookup_for_delta_join(
            state,
            (&exchange_a1l0, &exchange_a0l0),
            (node.fields.clone(), node.pk_indices.clone()),
            LookupNode {
                stream_key: delta_join_node.right_key.clone(),
                arrange_key: delta_join_node.left_key.clone(),
                use_current_epoch: false,
                // will be updated later to a global id
                arrangement_table_id: if is_local_table_id {
                    Some(ArrangementTableId::TableId(delta_join_node.left_table_id))
                } else {
                    Some(ArrangementTableId::IndexId(delta_join_node.left_table_id))
                },
                column_mapping: lookup_0_column_reordering,
                arrangement_table_info: delta_join_node.left_info.clone(),
                arrangement_table: Some(
                    Self::infer_internal_table_catalog(
                        delta_join_node.left_info.as_ref(),
                        // Use Arrange node's dist key.
                        try_match_expand!(arrange_0.get_node_body().unwrap(), NodeBody::Arrange)?
                            .distribution_key
                            .clone()
                            .iter()
                            .map(|x| *x as usize)
                            .collect(),
                        exchange_a0l0.append_only,
                    )
                    .to_prost(
                        SchemaId::placeholder() as u32,
                        DatabaseId::placeholder() as u32,
                    ),
                ),
            },
        );
        let lookup_1_column_reordering = {
            let tmp: Vec<i32> = (0..i0_length + i1_length)
                .chain(0..i1_length)
                .map(|x| x as _)
                .collect_vec();
            output_indices
                .iter()
                .map(|&x| tmp[x as usize])
                .collect_vec()
        };
        // lookup right table by left stream
        let lookup_1 = self.build_lookup_for_delta_join(
            state,
            (&exchange_a0l1, &exchange_a1l1),
            (node.fields.clone(), node.pk_indices.clone()),
            LookupNode {
                stream_key: delta_join_node.left_key.clone(),
                arrange_key: delta_join_node.right_key.clone(),
                use_current_epoch: true,
                // will be updated later to a global id
                arrangement_table_id: if is_local_table_id {
                    Some(ArrangementTableId::TableId(delta_join_node.right_table_id))
                } else {
                    Some(ArrangementTableId::IndexId(delta_join_node.right_table_id))
                },
                column_mapping: lookup_1_column_reordering,
                arrangement_table_info: delta_join_node.right_info.clone(),
                arrangement_table: Some(
                    Self::infer_internal_table_catalog(
                        delta_join_node.right_info.as_ref(),
                        // Use Arrange node's dist key.
                        try_match_expand!(arrange_1.get_node_body().unwrap(), NodeBody::Arrange)?
                            .distribution_key
                            .clone()
                            .iter()
                            .map(|x| *x as usize)
                            .collect(),
                        exchange_a1l1.append_only,
                    )
                    .to_prost(
                        SchemaId::placeholder() as u32,
                        DatabaseId::placeholder() as u32,
                    ),
                ),
            },
        );

        let lookup_0_frag = self.build_and_add_fragment(state, lookup_0)?;
        let lookup_1_frag = self.build_and_add_fragment(state, lookup_1)?;

        state.fragment_graph.add_edge(
            arrange_0_frag.fragment_id,
            lookup_0_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: true,
                link_id: exchange_a0l0.operator_id,
            },
        );

        state.fragment_graph.add_edge(
            arrange_0_frag.fragment_id,
            lookup_1_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                // stream input doesn't need to be on the same worker node as lookup
                same_worker_node: false,
                link_id: exchange_a0l1.operator_id,
            },
        );

        state.fragment_graph.add_edge(
            arrange_1_frag.fragment_id,
            lookup_0_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                // stream input doesn't need to be on the same worker node as lookup
                same_worker_node: false,
                link_id: exchange_a1l0.operator_id,
            },
        );

        state.fragment_graph.add_edge(
            arrange_1_frag.fragment_id,
            lookup_1_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: true,
                link_id: exchange_a1l1.operator_id,
            },
        );

        let exchange_l0m = self.build_exchange_for_delta_join(state, node);
        let exchange_l1m = self.build_exchange_for_delta_join(state, node);

        let union = StreamNode {
            operator_id: state.gen_operator_id() as u64,
            identity: "Union".into(),
            fields: node.fields.clone(),
            pk_indices: node.pk_indices.clone(),
            node_body: Some(NodeBody::LookupUnion(LookupUnionNode { order: vec![1, 0] })),
            input: vec![exchange_l0m.clone(), exchange_l1m.clone()],
            append_only: node.append_only,
        };

        state.fragment_graph.add_edge(
            lookup_0_frag.fragment_id,
            current_fragment.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_l0m.operator_id,
            },
        );

        state.fragment_graph.add_edge(
            lookup_1_frag.fragment_id,
            current_fragment.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_l1m.operator_id,
            },
        );

        Ok(union)
    }

    pub(in super::super) fn build_delta_join_without_arrange(
        &self,
        state: &mut BuildFragmentGraphState,
        current_fragment: &mut StreamFragment,
        mut node: StreamNode,
    ) -> Result<StreamNode> {
        match &node.node_body {
            Some(NodeBody::DeltaIndexJoin(node)) => node,
            _ => unreachable!(),
        };

        let arrange_1 = node.input.remove(1);
        let arrange_0 = node.input.remove(0);

        // TODO: when distribution key is added to catalog, chain and delta join won't have any
        // exchange in-between. Then we can safely remove this function.
        fn pass_through_exchange(mut node: StreamNode) -> StreamNode {
            if let Some(NodeBody::Exchange(exchange)) = node.node_body {
                if let DispatcherType::NoShuffle =
                    exchange.strategy.as_ref().unwrap().get_type().unwrap()
                {
                    return node.input.remove(0);
                }
                panic!("exchange other than no_shuffle not allowed between delta join and arrange");
            } else {
                unimplemented!()
            }
        }

        let arrange_0 = pass_through_exchange(arrange_0);
        let arrange_1 = pass_through_exchange(arrange_1);

        let arrange_0_frag = self.build_and_add_fragment(state, arrange_0)?;
        let arrange_1_frag = self.build_and_add_fragment(state, arrange_1)?;

        let union = self.build_delta_join_inner(
            state,
            current_fragment,
            arrange_0_frag,
            arrange_1_frag,
            &node,
            false,
        )?;

        Ok(union)
    }

    pub(in super::super) fn build_delta_join(
        &self,
        state: &mut BuildFragmentGraphState,
        current_fragment: &mut StreamFragment,
        mut node: StreamNode,
    ) -> Result<StreamNode> {
        let hash_join_node = match &node.node_body {
            Some(NodeBody::HashJoin(node)) => node,
            _ => unreachable!(),
        };

        let mut table_l = hash_join_node.get_left_table()?.clone();
        let mut table_r = hash_join_node.get_right_table()?.clone();

        // Note: the last column of hash_join table catalog is degree, which is not used by Arrange
        // Node.
        table_l.columns.pop();
        table_r.columns.pop();
        assert_eq!(node.input.len(), 2);

        // Previous plan:
        //
        // ```
        // input_1 --\
        //            -- HashJoin --- output
        // input_2 --/
        // ```
        //
        // Rewritten plan:
        //
        // ```
        // input_1 --- Arrange --- Lookup ---\
        //                      X            --- Merge --- output
        // input_2 --- Arrange --- Lookup ---/
        // ```
        //
        // Within the whole process, the distribution doesn't get changed, so we always use
        // NoShuffle as exchange type.
        //
        // TODO: support multi-way join.

        let (link_i1a1, input_1_frag, exchange_i1a1) =
            self.build_input_with_exchange(state, node.input.remove(1))?;

        let (link_i0a0, input_0_frag, exchange_i0a0) =
            self.build_input_with_exchange(state, node.input.remove(0))?;

        let (arrange_0_info, arrange_0) = self.build_arrange_for_delta_join(
            state,
            &exchange_i0a0,
            hash_join_node.left_key.clone(),
            &table_l,
        );
        let (arrange_1_info, arrange_1) = self.build_arrange_for_delta_join(
            state,
            &exchange_i1a1,
            hash_join_node.right_key.clone(),
            &table_r,
        );

        let arrange_0_frag = self.build_and_add_fragment(state, arrange_0)?;
        let arrange_1_frag = self.build_and_add_fragment(state, arrange_1)?;

        state.fragment_graph.add_edge(
            input_0_frag.fragment_id,
            arrange_0_frag.fragment_id,
            link_i0a0,
        );

        state.fragment_graph.add_edge(
            input_1_frag.fragment_id,
            arrange_1_frag.fragment_id,
            link_i1a1,
        );

        let delta_join_node = StreamNode {
            node_body: Some(NodeBody::DeltaIndexJoin(DeltaIndexJoinNode {
                join_type: hash_join_node.join_type,
                left_key: hash_join_node.left_key.clone(),
                right_key: hash_join_node.right_key.clone(),
                left_table_id: hash_join_node.left_table.as_ref().unwrap().id,
                right_table_id: hash_join_node.right_table.as_ref().unwrap().id,
                condition: hash_join_node.condition.clone(),
                left_info: Some(arrange_0_info),
                right_info: Some(arrange_1_info),
                output_indices: hash_join_node.output_indices.clone(),
            })),
            ..node
        };

        let union = self.build_delta_join_inner(
            state,
            current_fragment,
            arrange_0_frag,
            arrange_1_frag,
            &delta_join_node,
            true,
        )?;

        Ok(union)
    }

    fn infer_internal_table_catalog(
        arrangement_info: Option<&ArrangementInfo>,
        distribution_key: Vec<usize>,
        append_only: bool,
    ) -> TableCatalog {
        let arrangement_info = arrangement_info.unwrap();
        let mut internal_table_catalog_builder = TableCatalogBuilder::new();
        for column_desc in &arrangement_info.column_descs {
            internal_table_catalog_builder.add_column(&Field::from(&ColumnDesc::from(column_desc)));
        }

        for order in &arrangement_info.arrange_key_orders {
            internal_table_catalog_builder.add_order_column(
                order.index as usize,
                OrderType::from_prost(&ProstOrderType::from_i32(order.order_type).unwrap()),
            );
        }

        internal_table_catalog_builder.build(distribution_key, append_only)
    }
}
