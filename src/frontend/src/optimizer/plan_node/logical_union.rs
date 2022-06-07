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

use std::fmt;

use itertools::Itertools;
use risingwave_common::error::Result;

use super::{ColPrunable, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream};
use crate::optimizer::plan_node::PlanTreeNode;
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, Copy)]
pub enum UnionMode {
    /// Pick any input that is available
    StreamPickAvailable,
    /// Within one epoch, pick from the last input to the first input
    StreamLastToFirst,
}

#[derive(Debug, Clone)]
pub struct LogicalUnion {
    pub base: PlanBase,
    pub mode: UnionMode,
    inputs: Vec<PlanRef>,
}

impl fmt::Display for LogicalUnion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogicalUnion {{ mode: {:?} }}", self.mode)
    }
}

impl LogicalUnion {
    pub(crate) fn new(inputs: Vec<PlanRef>, mode: UnionMode) -> Self {
        assert!(!inputs.is_empty());
        let mut expected_pk_indices = inputs[0].plan_base().pk_indices.clone();
        expected_pk_indices.sort();

        for input in &inputs {
            // We don't care about the order of pk indices
            let mut pk_indices = input.plan_base().pk_indices.clone();
            pk_indices.sort();
            assert_eq!(pk_indices, expected_pk_indices);

            assert_eq!(
                input.plan_base().append_only,
                inputs[0].plan_base().append_only
            );

            assert_eq!(input.plan_base().dist, inputs[0].plan_base().dist);
            assert_eq!(input.plan_base().schema, inputs[0].plan_base().schema);
            // TODO: check other properties
        }
        let ctx = inputs[0].ctx();
        let base = PlanBase::new_logical(
            ctx,
            inputs[0].schema().clone(),
            inputs[0].pk_indices().to_vec(),
        );
        Self { base, mode, inputs }
    }
}

impl PlanTreeNode for LogicalUnion {
    fn inputs(&self) -> smallvec::SmallVec<[PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.inputs.clone().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        Self::new(inputs.to_vec(), self.mode).into()
    }
}

impl ColPrunable for LogicalUnion {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        self.clone_with_inputs(
            &self
                .inputs
                .iter()
                .map(|input| input.prune_col(required_cols))
                .collect_vec(),
        )
    }
}

impl PredicatePushdown for LogicalUnion {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.predicate_pushdown(predicate.clone()))
            .collect_vec();
        self.clone_with_inputs(&inputs)
    }
}

impl ToBatch for LogicalUnion {
    fn to_batch(&self) -> Result<PlanRef> {
        todo!()
    }
}

impl ToStream for LogicalUnion {
    fn to_stream(&self) -> Result<PlanRef> {
        todo!()
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let mut new_inputs = vec![];
        let mut out_col_change = None;
        for input in &self.inputs {
            let (input, input_col_change) = input.logical_rewrite_for_stream()?;
            new_inputs.push(input);
            if let Some(ref out_col_change) = out_col_change {
                assert_eq!(out_col_change, &input_col_change);
            } else {
                out_col_change = Some(input_col_change);
            }
        }
        Ok((
            Self::new(new_inputs, self.mode).into(),
            out_col_change.unwrap(),
        ))
    }
}
