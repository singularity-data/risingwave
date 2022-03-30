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

use std::collections::HashMap;
use std::sync::Arc;

use futures::channel::mpsc::{Receiver, Sender};
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::addr::HostAddr;

use crate::executor::Message;

mod barrier_manager;
mod compute_client_pool;
mod env;
mod stream_manager;

pub use barrier_manager::*;
pub use compute_client_pool::*;
pub use env::*;
pub use stream_manager::*;

#[cfg(test)]
mod tests;

/// Default capacity of channel if two actors are on the same node
pub const LOCAL_OUTPUT_CHANNEL_SIZE: usize = 16;

pub type ConsumableChannelPair = (Option<Sender<Message>>, Option<Receiver<Message>>);
pub type ConsumableChannelVecPair = (Vec<Sender<Message>>, Vec<Receiver<Message>>);
pub type ActorId = u32;
pub type UpDownActorIds = (ActorId, ActorId);

/// Stores the information which may be modified from the data plane.
pub struct SharedContext {
    /// Stores the senders and receivers for later `Processor`'s usage.
    ///
    /// Each actor has several senders and several receivers. Senders and receivers are created
    /// during `update_actors` and stored in a channel map. Upon `build_actors`, all these channels
    /// will be taken out and built into the executors and outputs.
    /// One sender or one receiver can be uniquely determined by the upstream and downstream actor
    /// id.
    ///
    /// There are three cases when we need local channels to pass around messages:
    /// 1. pass `Message` between two local actors
    /// 2. The RPC client at the downstream actor forwards received `Message` to one channel in
    /// `ReceiverExecutor` or `MergerExecutor`.
    /// 3. The RPC `Output` at the upstream actor forwards received `Message` to
    /// `ExchangeServiceImpl`.
    ///
    /// The channel serves as a buffer because `ExchangeServiceImpl`
    /// is on the server-side and we will also introduce backpressure.
    pub(crate) channel_map: Mutex<HashMap<UpDownActorIds, ConsumableChannelPair>>,

    /// Stores the local address.
    ///
    /// It is used to test whether an actor is local or not,
    /// thus determining whether we should setup local channel only or remote rpc connection
    /// between two actors/actors.
    pub(crate) addr: HostAddr,

    pub(crate) barrier_manager: Arc<Mutex<LocalBarrierManager>>,
}

impl SharedContext {
    pub fn new(addr: HostAddr) -> Self {
        Self {
            channel_map: Mutex::new(HashMap::new()),
            addr,
            barrier_manager: Arc::new(Mutex::new(LocalBarrierManager::new())),
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            channel_map: Mutex::new(HashMap::new()),
            addr: LOCAL_TEST_ADDR.clone(),
            barrier_manager: Arc::new(Mutex::new(LocalBarrierManager::for_test())),
        }
    }

    #[inline]
    fn lock_channel_map(&self) -> MutexGuard<HashMap<UpDownActorIds, ConsumableChannelPair>> {
        self.channel_map.lock()
    }

    /// Create a notifier for Create MV DDL finish. When an executor/actor (essentially a
    /// [`ChainExecutor`]) finishes its DDL job, it can report that using this notifier.
    /// Note that a DDL of MV always corresponds to an epoch in our system.
    ///
    /// Creation of an MV may last for several epochs to finish.
    /// Therefore, when the [`ChainExecutor`] finds that the creation is finished, it will send the
    /// DDL epoch using this notifier, which can be collected by the barrier manager and reported to
    /// the meta service soon.
    pub fn register_finish_create_mview_notifier(
        &self,
        actor_id: ActorId,
    ) -> FinishCreateMviewNotifier {
        debug!("register finish create mview notifier: {}", actor_id);

        let barrier_manager = self.barrier_manager.clone();
        FinishCreateMviewNotifier {
            barrier_manager,
            actor_id,
        }
    }

    pub fn lock_barrier_manager(&self) -> MutexGuard<LocalBarrierManager> {
        self.barrier_manager.lock()
    }

    #[inline]
    pub fn take_sender(&self, ids: &UpDownActorIds) -> Result<Sender<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "channel between {} and {} does not exist",
                    ids.0, ids.1
                )))
            })?
            .0
            .take()
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "sender from {} to {} does no exist",
                    ids.0, ids.1
                )))
            })
    }

    #[inline]
    pub fn take_receiver(&self, ids: &UpDownActorIds) -> Result<Receiver<Message>> {
        self.lock_channel_map()
            .get_mut(ids)
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "channel between {} and {} does not exist",
                    ids.0, ids.1
                )))
            })?
            .1
            .take()
            .ok_or_else(|| {
                RwError::from(ErrorCode::InternalError(format!(
                    "receiver from {} to {} does no exist",
                    ids.0, ids.1
                )))
            })
    }

    #[inline]
    pub fn add_channel_pairs(&self, ids: UpDownActorIds, channels: ConsumableChannelPair) {
        self.lock_channel_map().insert(ids, channels);
    }

    pub fn retain<F>(&self, mut f: F)
    where
        F: FnMut(&(u32, u32)) -> bool,
    {
        self.lock_channel_map()
            .retain(|up_down_ids, _| f(up_down_ids));
    }

    #[cfg(test)]
    pub fn get_channel_pair_number(&self) -> u32 {
        self.lock_channel_map().len() as u32
    }
}
