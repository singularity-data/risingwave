use paste::paste;
use risingwave_pb::plan::exchange_info::{
    BroadcastInfo, Distribution as DistributionProst, DistributionMode, HashInfo,
};
use risingwave_pb::plan::ExchangeInfo;

use super::super::plan_node::*;
use crate::optimizer::property::{Convention, Order};
use crate::optimizer::PlanRef;
use crate::{for_batch_plan_nodes, for_logical_plan_nodes, for_stream_plan_nodes};

#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    Any,
    Single,
    Broadcast,
    AnyShard,
    HashShard(Vec<usize>),
}

static ANY_DISTRIBUTION: Distribution = Distribution::Any;

#[allow(dead_code)]
impl Distribution {
    pub fn to_prost(&self, output_count: u32) -> ExchangeInfo {
        ExchangeInfo {
            mode: match self {
                Distribution::Single => DistributionMode::Single,
                Distribution::Broadcast => DistributionMode::Broadcast,
                Distribution::HashShard(_keys) => DistributionMode::Hash,
                // TODO: Should panic if AnyShard or Any
                _ => DistributionMode::Hash,
            } as i32,
            distribution: match self {
                Distribution::Single => None,
                Distribution::Broadcast => Some(DistributionProst::BroadcastInfo(BroadcastInfo {
                    count: output_count,
                })),
                Distribution::HashShard(keys) => Some(DistributionProst::HashInfo(HashInfo {
                    output_count,
                    keys: keys.iter().map(|num| *num as u32).collect(),
                })),
                // TODO: Should panic if AnyShard or Any
                Distribution::AnyShard | Distribution::Any => None,
            },
        }
    }
    pub fn enforce_if_not_satisfies(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        if !plan.distribution().satisfies(self) {
            self.enforce(plan, required_order)
        } else {
            plan
        }
    }
    fn enforce(&self, plan: PlanRef, required_order: &Order) -> PlanRef {
        match plan.convention() {
            Convention::Batch => {
                BatchExchange::new(plan, required_order.clone(), self.clone()).into()
            }
            Convention::Stream => StreamExchange::new(plan, self.clone()).into(),
            _ => unreachable!(),
        }
    }
    // "A -> B" represent A satisfies B
    //                   +---+
    //                   |Any|
    //                   +---+
    //                     ^
    //         +-----------------------+
    //         |           |           |
    //     +---+----+   +--+---+  +----+----+
    //     |Anyshard|   |single|  |broadcast|
    //     +---+----+   +------+  +---------+
    //         ^
    //  +------+------+
    //  |hash_shard(a)|
    //  +------+------+
    //         ^
    // +-------+-------+
    // |hash_shard(a,b)|
    // +---------------+
    fn satisfies(&self, other: &Distribution) -> bool {
        match self {
            Distribution::Any => matches!(other, Distribution::Any),
            Distribution::Single => matches!(other, Distribution::Any | Distribution::Single),
            Distribution::Broadcast => matches!(other, Distribution::Any | Distribution::Broadcast),
            Distribution::AnyShard => matches!(other, Distribution::Any | Distribution::AnyShard),
            Distribution::HashShard(keys) => match other {
                Distribution::Any => true,
                Distribution::AnyShard => true,
                Distribution::HashShard(other_keys) => other_keys
                    .iter()
                    .all(|other_key| keys.iter().any(|key| key == other_key)),
                _ => false,
            },
        }
    }
    pub fn any() -> &'static Self {
        &ANY_DISTRIBUTION
    }
    pub fn is_any(&self) -> bool {
        matches!(self, Distribution::Any)
    }
}

pub trait WithDistribution {
    /// the distribution property of the PlanNode's output
    fn distribution(&self) -> &Distribution;
}

macro_rules! impl_with_dist_base {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithDistribution for [<$convention $name>] {
                fn distribution(&self) -> &Distribution {
                    &self.base.dist
                }
            }
        })*
    }
}
for_batch_plan_nodes! { impl_with_dist_base }
for_stream_plan_nodes! { impl_with_dist_base }
macro_rules! impl_with_dist_any {
    ([], $( { $convention:ident, $name:ident }),*) => {
        $(paste! {
            impl WithDistribution for [<$convention $name>] {
                fn distribution(&self) -> &Distribution {
                   Distribution::any()
                }
            }
        })*
    }
}
for_logical_plan_nodes! { impl_with_dist_any }

#[cfg(test)]
mod tests {
    use super::Distribution;

    fn test_hash_shard_subset(uni: &Distribution, sub: &Distribution) {
        assert!(uni.satisfies(sub));
        assert!(!sub.satisfies(uni));
    }

    fn test_hash_shard_false(d1: &Distribution, d2: &Distribution) {
        assert!(!d1.satisfies(d2));
        assert!(!d2.satisfies(d1));
    }

    #[test]
    fn hash_shard_satisfy() {
        let d1 = Distribution::HashShard(vec![0, 2, 4, 6, 8]);
        let d2 = Distribution::HashShard(vec![2, 4]);
        let d3 = Distribution::HashShard(vec![4, 6]);
        let d4 = Distribution::HashShard(vec![6, 8]);
        test_hash_shard_subset(&d1, &d2);
        test_hash_shard_subset(&d1, &d3);
        test_hash_shard_subset(&d1, &d4);
        test_hash_shard_subset(&d1, &d2);
        test_hash_shard_false(&d2, &d3);
        test_hash_shard_false(&d2, &d4);
        test_hash_shard_false(&d3, &d4);
    }
}
