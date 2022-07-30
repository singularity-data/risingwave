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

use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::stream::select_with_strategy;
use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use tokio::sync::{Mutex, Semaphore};

use super::error::StreamExecutorError;
use super::{
    expect_first_barrier, Barrier, BoxedExecutor, Executor, ExecutorInfo, Message, MessageStream,
};
use crate::task::{ActorId, CreateMviewProgress};

/// `ChainExecutor` is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, `ChainExecutor` is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
///
/// [`RearrangedChainExecutor`] resolves the latency problem when creating MV with a huge amount of
/// existing data, by rearranging the barrier from the upstream. Check the design doc for details.
pub struct RearrangedChainExecutor {
    snapshot: BoxedExecutor,

    upstream: BoxedExecutor,

    upstream_indices: Arc<[usize]>,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], msg: Message) -> Message {
    match msg {
        Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            let mapped_columns = upstream_indices
                .iter()
                .map(|&i| columns[i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(ops, mapped_columns, visibility))
        }
        _ => msg,
    }
}

#[derive(Debug)]
enum RearrangedMessage {
    RearrangedBarrier(Barrier),
    PhantomBarrier(Barrier),
    Chunk(StreamChunk),
}

impl RearrangedMessage {
    fn phantom_into(self) -> Option<Message> {
        match self {
            RearrangedMessage::RearrangedBarrier(_) => None,
            RearrangedMessage::PhantomBarrier(barrier) => Message::Barrier(barrier).into(),
            RearrangedMessage::Chunk(chunk) => Message::Chunk(chunk).into(),
        }
    }
}

impl RearrangedMessage {
    fn rearranged_from(msg: Message) -> Self {
        match msg {
            Message::Chunk(chunk) => RearrangedMessage::Chunk(chunk),
            Message::Barrier(barrier) => RearrangedMessage::RearrangedBarrier(barrier),
        }
    }

    fn phantom_from(msg: Message) -> Self {
        match msg {
            Message::Chunk(chunk) => RearrangedMessage::Chunk(chunk),
            Message::Barrier(barrier) => RearrangedMessage::PhantomBarrier(barrier),
        }
    }
}

impl RearrangedChainExecutor {
    pub fn new(
        snapshot: BoxedExecutor,
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices: upstream.pk_indices().to_owned(),
                identity: "RearrangedChain".to_owned(),
            },
            snapshot,
            upstream,
            upstream_indices: upstream_indices.into(),
            actor_id: progress.actor_id(),
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // 0. Project the upstream with `upstream_indices`.
        let upstream_indices = self.upstream_indices.clone();
        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices, msg)));

        // 1. Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let create_epoch = first_barrier.epoch;

        // If the barrier is a conf change of creating this mview, init snapshot from its epoch
        // and begin to consume the snapshot.
        // Otherwise, it means we've recovered and the snapshot is already consumed.
        let to_consume_snapshot = first_barrier.is_add_dispatcher(self.actor_id);

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier.clone());

        if to_consume_snapshot {
            // If we need to consume the snapshot ...
            // We will spawn a background task to poll the upstream actively, in order to get the
            // barrier as soon as possible and then to rearrange(steal) it.

            // The upstream after transforming the barriers to phantom barriers.
            let (upstream_tx, upstream_rx) = mpsc::unbounded();
            // When we catch-up the progress, notify the task to stop.
            let (stop_rearrange_tx, stop_rearrange_rx) = oneshot::channel();

            // 2. Actually, the `first_msg` is rearranged too. So we need to put a phantom barrier.
            upstream_tx
                .unbounded_send(RearrangedMessage::PhantomBarrier(first_barrier))
                .unwrap();

            // 3. Rearrange stream, will yield the barriers polled from upstream to rearrange.
            let upstream = Arc::new(Mutex::new(upstream));
            let tokens = Arc::new(Semaphore::new(0));
            let rearranged_barrier = Box::pin(
                Self::rearrange_barrier(
                    upstream.clone(),
                    upstream_tx,
                    stop_rearrange_rx,
                    tokens.clone(),
                )
                .map(|result| result.map(RearrangedMessage::RearrangedBarrier)),
            );

            // 4. Init the snapshot with reading epoch.
            let snapshot = self.snapshot.execute_with_epoch(create_epoch.prev);

            // Chain the `snapshot` and `upstream_rx` to get a unified `rearranged_chunks` stream.
            let rearranged_chunks = snapshot
                .map(|result| result.map(RearrangedMessage::rearranged_from))
                .chain(upstream_rx.map(Ok));

            // 5. Merge the rearranged barriers with chunks, with the priority of barrier.
            let mut rearranged =
                select_with_strategy(rearranged_barrier, rearranged_chunks, |_: &mut ()| {
                    stream::PollNext::Left
                });

            // Record the epoch of the last rearranged barrier we received.
            let mut last_rearranged_epoch = create_epoch;
            let mut stop_rearrange_tx = Some(stop_rearrange_tx);

            // 6. Consume the merged `rearranged` stream.
            #[for_await]
            for rearranged_msg in &mut rearranged {
                match rearranged_msg? {
                    // If we received a phantom barrier, update the progress and check whether we
                    // catches up with the progress of upstream MV.
                    //
                    // Note that there's no phantom barrier in the snapshot. So we must have already
                    // consumed the whole snapshot and be on the upstream now.
                    RearrangedMessage::PhantomBarrier(barrier) => {
                        // Update the progress since we've consumed all chunks before this phantom.
                        self.progress
                            .update(last_rearranged_epoch.curr, barrier.epoch.curr);

                        if barrier.epoch.curr >= last_rearranged_epoch.curr {
                            // Stop the background rearrangement task.
                            stop_rearrange_tx.take().unwrap().send(()).map_err(|_| {
                                StreamExecutorError::channel_closed("stop rearrange")
                            })?;
                            break;
                        }
                    }

                    // If we received a message, yield it.
                    RearrangedMessage::RearrangedBarrier(barrier) => {
                        // Reset tokens for new epoch.
                        tokens
                            .try_acquire_many(tokens.available_permits() as _)
                            .unwrap()
                            .forget();
                        assert_eq!(tokens.available_permits(), 0);

                        last_rearranged_epoch = barrier.epoch;
                        yield Message::Barrier(barrier);
                    }
                    RearrangedMessage::Chunk(chunk) => {
                        // Add tokens since we've consumed a chunk.
                        tokens.add_permits(chunk.cardinality());
                        yield Message::Chunk(chunk);
                    }
                }
            }

            // 7. Rearranged task finished.
            // The reason for finish must be that we told it to stop.
            tracing::trace!(actor = self.actor_id, "rearranged task finished");
            if stop_rearrange_tx.is_some() {
                tracing::error!(actor = self.actor_id, "rearrangement finished passively");
            }

            // 8. Consume remainings.
            let mut finish_on_barrier = |msg: &Message| {
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch.curr);
                }
            };

            // Note that there may still be some messages in `rearranged`. However the rearranged
            // barriers must be ignored, we should take the phantoms.
            #[for_await]
            for msg in rearranged {
                let msg: RearrangedMessage = msg?;
                let Some(msg) = msg.phantom_into() else { continue };
                finish_on_barrier(&msg);
                yield msg;
            }

            // Now we take back the remaining upstream. There should be no contention since
            // `rearranged` stream is already dropped.
            let mut remaining_upstream = upstream.try_lock().unwrap();

            // Consume remaining upstream.
            tracing::trace!(actor = self.actor_id, "begin to consume remaining upstream");

            #[for_await]
            for msg in &mut *remaining_upstream {
                let msg: Message = msg?;
                finish_on_barrier(&msg);
                yield msg;
            }
        } else {
            // If there's no need to consume the snapshot ...
            // We directly forward the messages from the upstream.

            #[for_await]
            for msg in upstream {
                yield msg?;
            }
        }
    }

    /// Rearrangement stream. The `upstream: U` will be taken out from the mutex, then put back
    /// after stopped.
    ///
    /// Check `execute_inner` for more details.
    #[try_stream(ok = Barrier, error = StreamExecutorError)]
    async fn rearrange_barrier<U>(
        upstream: Arc<Mutex<U>>,
        upstream_tx: mpsc::UnboundedSender<RearrangedMessage>,
        mut stop_rearrange_rx: oneshot::Receiver<()>,
        tokens: Arc<Semaphore>,
    ) where
        U: MessageStream + std::marker::Unpin,
    {
        // There should be no contention since `upstream` is used only after this stream finishes.
        let mut upstream = upstream.try_lock().unwrap();
        let mut acquire_tokens = 0;

        loop {
            use futures::future::{select, Either};

            // Stop when `stop_rearrange_rx` is received.
            match select(&mut stop_rearrange_rx, upstream.next()).await {
                Either::Left((Ok(_), _)) => break,
                Either::Left((Err(_e), _)) => {
                    return Err(StreamExecutorError::channel_closed("stop rearrange"))
                }

                Either::Right((Some(msg), _)) => {
                    let msg = msg?;

                    // If we polled a barrier, rearrange it by yielding and leave a phantom barrier
                    // with `RearrangedMessage::phantom_from` in-place.
                    // If we polled a chunk, simply put it to the `upstream_tx`.
                    if let Some(barrier) = msg.as_barrier().cloned() {
                        let count = std::mem::take(&mut acquire_tokens);
                        tokens.acquire_many(count as _).await.unwrap().forget();
                        yield barrier;
                    }
                    upstream_tx
                        .unbounded_send(RearrangedMessage::phantom_from(msg))
                        .map_err(|_| StreamExecutorError::channel_closed("rearranged upstream"))?;
                }
                Either::Right((None, _)) => {
                    Err(StreamExecutorError::channel_closed("upstream"))?;
                }
            }
        }
    }
}

impl Executor for RearrangedChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

// TODO: add new unit tests for rearranged chain
