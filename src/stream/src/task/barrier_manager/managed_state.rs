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

use std::collections::{HashMap, HashSet};
use std::iter::once;

use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use tokio::sync::oneshot;

use super::progress::ChainState;
use super::CollectResult;
use crate::executor::Barrier;
use crate::task::ActorId;

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Barriers from some actors have been collected and stashed, however no `send_barrier`
    /// request from the meta service is issued.
    Stashed {
        /// Actor ids we've collected and stashed.
        collected_actors: HashSet<ActorId>,
    },

    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued {
        /// Actor ids remaining to be collected.
        remaining_actors: HashSet<ActorId>,

        /// Notify that the collection is finished.
        collect_notifier: oneshot::Sender<CollectResult>,
    },
}

#[derive(Debug)]
pub(super) struct ManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    epoch_barrier_state_map: HashMap<u64, ManagedBarrierStateInner>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, ChainState>>,
}

impl ManagedBarrierState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new() -> Self {
        Self {
            epoch_barrier_state_map: HashMap::new(),
            create_mview_progress: Default::default(),
        }
    }

    /// Notify if we have collected barriers from all actor ids. The state must be `Issued`.
    fn may_notify(&mut self, curr_epoch: u64) {
        let to_notify = match self.epoch_barrier_state_map.get(&curr_epoch) {
            Some(ManagedBarrierStateInner::Issued {
                remaining_actors, ..
            }) => (remaining_actors.is_empty()),
            _ => unreachable!(),
        };

        if to_notify {
            let inner = self.epoch_barrier_state_map.remove(&curr_epoch).unwrap();

            let create_mview_progress = self
                .create_mview_progress
                .remove(&curr_epoch)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| CreateMviewProgress {
                    chain_actor_id: actor,
                    done: matches!(state, ChainState::Done),
                    consumed_epoch: match state {
                        ChainState::ConsumingUpstream(consumed_epoch) => consumed_epoch,
                        ChainState::Done => curr_epoch,
                    },
                })
                .collect();

            match inner {
                ManagedBarrierStateInner::Issued {
                    collect_notifier, ..
                } => {
                    // Notify about barrier finishing.
                    let result = CollectResult {
                        create_mview_progress,
                    };
                    if collect_notifier.send(result).is_err() {
                        warn!(
                            "failed to notify barrier collection with epoch {}",
                            curr_epoch
                        )
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Remove stop barrier (epoch < `curr_epoch`), and send err.
    pub(crate) fn remove_stop_barrier(&mut self, curr_epoch: u64) {
        self.epoch_barrier_state_map
            .drain_filter(|k, _| k < &curr_epoch);
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::trace!(
            target: "events::stream::barrier::collect_barrier",
            "collect_barrier: epoch = {}, actor_id = {}, state = {:#?}",
            barrier.epoch.curr,
            actor_id,
            self
        );

        match self.epoch_barrier_state_map.get_mut(&barrier.epoch.curr) {
            Some(ManagedBarrierStateInner::Stashed { collected_actors }) => {
                let new = collected_actors.insert(actor_id);
                assert!(new);
            }
            Some(ManagedBarrierStateInner::Issued {
                remaining_actors, ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                // fix me
                if exist {
                    assert!(
                        exist,
                        "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                        actor_id, barrier.epoch.curr
                    );
                }
                self.may_notify(barrier.epoch.curr);
            }
            None => {
                self.epoch_barrier_state_map.insert(
                    barrier.epoch.curr,
                    ManagedBarrierStateInner::Stashed {
                        collected_actors: once(actor_id).collect(),
                    },
                );
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        collect_notifier: oneshot::Sender<CollectResult>,
    ) {
        let inner = match self.epoch_barrier_state_map.get(&barrier.epoch.curr) {
            Some(ManagedBarrierStateInner::Stashed { collected_actors }) => {
                let remaining_actors = actor_ids_to_collect
                    .into_iter()
                    .filter(|a| !collected_actors.contains(a))
                    .collect();
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
                    collect_notifier,
                }
            }
            Some(ManagedBarrierStateInner::Issued { .. }) => {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`",
                    barrier.epoch
                );
            }
            None => {
                let remaining_actors = actor_ids_to_collect.into_iter().collect();
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
                    collect_notifier,
                }
            }
        };
        self.epoch_barrier_state_map
            .insert(barrier.epoch.curr, inner);
        self.may_notify(barrier.epoch.curr);
    }
}
