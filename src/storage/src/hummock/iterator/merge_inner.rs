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

use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, LinkedList};
use std::future::Future;
use std::sync::Arc;

use risingwave_hummock_sdk::VersionedComparator;

use crate::hummock::iterator::{DirectionEnum, HummockIterator, HummockIteratorDirection};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

pub trait NodeExtraOrderInfo: Eq + Ord + Send + Sync {}

/// For unordered merge iterator, no extra order info is needed.
type UnorderedNodeExtra = ();
/// Store the order index for the order aware merge iterator
type OrderedNodeExtra = usize;
impl NodeExtraOrderInfo for UnorderedNodeExtra {}
impl NodeExtraOrderInfo for OrderedNodeExtra {}

pub struct Node<I: HummockIterator, T: NodeExtraOrderInfo> {
    iter: I,
    extra_order_info: T,
}

impl<I: HummockIterator, T: NodeExtraOrderInfo> Eq for Node<I, T> where Self: PartialEq {}
impl<I: HummockIterator, T: NodeExtraOrderInfo> Ord for Node<I, T>
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Implement `PartialOrd` for unordered iter node. Only compare the key.
impl<I: HummockIterator> PartialOrd for Node<I, UnorderedNodeExtra> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        // order should be reversed.

        Some(match I::Direction::direction() {
            DirectionEnum::Forward => {
                VersionedComparator::compare_key(other.iter.key(), self.iter.key())
            }
            DirectionEnum::Backward => {
                VersionedComparator::compare_key(self.iter.key(), other.iter.key())
            }
        })
    }
}

/// Implement `PartialOrd` for ordered iter node. Compare key and use order index as tie breaker.
impl<I: HummockIterator> PartialOrd for Node<I, OrderedNodeExtra> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The `extra_info` is used as a tie-breaker when the keys are equal.
        Some(match I::Direction::direction() {
            DirectionEnum::Forward => {
                VersionedComparator::compare_key(other.iter.key(), self.iter.key())
                    .then_with(|| other.extra_order_info.cmp(&self.extra_order_info))
            }
            DirectionEnum::Backward => {
                VersionedComparator::compare_key(self.iter.key(), other.iter.key())
                    .then_with(|| self.extra_order_info.cmp(&other.extra_order_info))
            }
        })
    }
}

impl<I: HummockIterator> PartialEq for Node<I, UnorderedNodeExtra> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl<I: HummockIterator> PartialEq for Node<I, OrderedNodeExtra> {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key() && self.extra_order_info.eq(&other.extra_order_info)
    }
}

/// Iterates on multiple iterators, a.k.a. `MergeIterator`.
pub struct MergeIteratorInner<I: HummockIterator, NE: NodeExtraOrderInfo> {
    /// Invalid or non-initialized iterators.
    unused_iters: LinkedList<Node<I, NE>>,

    /// The heap for merge sort.
    heap: BinaryHeap<Node<I, NE>>,

    /// Statistics.
    stats: Arc<StateStoreMetrics>,
}

/// An order aware merge iterator.
#[allow(type_alias_bounds)]
pub type OrderedMergeIteratorInner<I: HummockIterator> = MergeIteratorInner<I, OrderedNodeExtra>;

impl<I: HummockIterator> OrderedMergeIteratorInner<I> {
    pub fn new(iterators: impl IntoIterator<Item = I>, stats: Arc<StateStoreMetrics>) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .enumerate()
                .map(|(i, iter)| Node {
                    iter,
                    extra_order_info: i,
                })
                .collect(),
            heap: BinaryHeap::new(),
            stats,
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> MergeIteratorInner<I, NE> {
    fn collect_local_statistic_impl(&self, stats: &mut StoreLocalStatistic) {
        for node in &self.heap {
            node.iter.collect_local_statistic(stats);
        }
        for node in &self.unused_iters {
            node.iter.collect_local_statistic(stats);
        }
    }
}

#[allow(type_alias_bounds)]
pub type UnorderedMergeIteratorInner<I: HummockIterator> =
    MergeIteratorInner<I, UnorderedNodeExtra>;

impl<I: HummockIterator> UnorderedMergeIteratorInner<I> {
    pub fn new(iterators: impl IntoIterator<Item = I>, stats: Arc<StateStoreMetrics>) -> Self {
        Self {
            unused_iters: iterators
                .into_iter()
                .map(|iter| Node {
                    iter,
                    extra_order_info: (),
                })
                .collect(),
            heap: BinaryHeap::new(),
            stats,
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> MergeIteratorInner<I, NE>
where
    Node<I, NE>: Ord,
{
    /// Moves all iterators from the `heap` to the linked list.
    fn reset_heap(&mut self) {
        self.unused_iters.extend(self.heap.drain());
    }

    /// After some iterators in `unused_iterators` are sought or rewound, calls this function
    /// to construct a new heap using the valid ones.
    fn build_heap(&mut self) {
        assert!(self.heap.is_empty());

        self.heap = self
            .unused_iters
            .drain_filter(|i| i.iter.is_valid())
            .collect();
    }
}

/// The behaviour of `next` of order aware merge iterator is different from the normal one, so we
/// extract this trait.
pub trait MergeIteratorNext {
    type HummockResultFuture<'a>: Future<Output = HummockResult<()>> + Send + 'a
    where
        Self: 'a;
    fn next_inner(&mut self) -> Self::HummockResultFuture<'_>;
}

impl<I: HummockIterator> MergeIteratorNext for OrderedMergeIteratorInner<I> {
    type HummockResultFuture<'a> = impl Future<Output = HummockResult<()>>;

    fn next_inner(&mut self) -> Self::HummockResultFuture<'_> {
        async {
            let top_node = self.heap.pop().expect("no inner iter");
            let mut popped_nodes = vec![];

            // Take all nodes with the same current key as the top_node out of the heap.
            while let Some(next_node) = self.heap.peek_mut() {
                match VersionedComparator::compare_key(top_node.iter.key(), next_node.iter.key()) {
                    Ordering::Equal => {
                        popped_nodes.push(PeekMut::pop(next_node));
                    }
                    _ => break,
                }
            }

            popped_nodes.push(top_node);

            // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
            // return. Once the iterator enters an invalid state, we should remove it from heap
            // before returning.

            // Put the popped nodes back to the heap if valid or unused_iters if invalid.
            for mut node in popped_nodes {
                match node.iter.next().await {
                    Ok(_) => {}
                    Err(e) => {
                        // If the iterator returns error, we should clear the heap, so that this
                        // iterator becomes invalid.
                        self.heap.clear();
                        return Err(e);
                    }
                }

                if !node.iter.is_valid() {
                    self.unused_iters.push_back(node);
                } else {
                    self.heap.push(node);
                }
            }

            Ok(())
        }
    }
}

impl<I: HummockIterator> MergeIteratorNext for UnorderedMergeIteratorInner<I> {
    type HummockResultFuture<'a> = impl Future<Output = HummockResult<()>>;

    fn next_inner(&mut self) -> Self::HummockResultFuture<'_> {
        async {
            let mut node = self.heap.peek_mut().expect("no inner iter");

            // WARNING: within scope of BinaryHeap::PeekMut, we must carefully handle all places of
            // return. Once the iterator enters an invalid state, we should remove it from heap
            // before returning.

            match node.iter.next().await {
                Ok(_) => {}
                Err(e) => {
                    // If the iterator returns error, we should clear the heap, so that this
                    // iterator becomes invalid.
                    PeekMut::pop(node);
                    self.heap.clear();
                    return Err(e);
                }
            }

            if !node.iter.is_valid() {
                // Put back to `unused_iters`
                let node = PeekMut::pop(node);
                self.unused_iters.push_back(node);
            } else {
                // This will update the heap top.
                drop(node);
            }

            Ok(())
        }
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> HummockIterator for MergeIteratorInner<I, NE>
where
    Self: MergeIteratorNext + 'static,
    Node<I, NE>: Ord,
{
    type Direction = I::Direction;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move { self.next_inner().await }
    }

    fn key(&self) -> &[u8] {
        self.heap.peek().expect("no inner iter").iter.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.heap.peek().expect("no inner iter").iter.value()
    }

    fn is_valid(&self) -> bool {
        self.heap.peek().map_or(false, |n| n.iter.is_valid())
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.reset_heap();
            futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.rewind()))
                .await?;
            self.build_heap();
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async move {
            self.reset_heap();
            futures::future::try_join_all(self.unused_iters.iter_mut().map(|x| x.iter.seek(key)))
                .await?;
            self.build_heap();
            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.collect_local_statistic_impl(stats);
    }
}

impl<I: HummockIterator, NE: NodeExtraOrderInfo> Drop for MergeIteratorInner<I, NE> {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic_impl(&mut stats);
        stats.report(self.stats.as_ref());
    }
}
