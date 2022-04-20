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

use futures::pin_mut;
use futures_async_stream::try_stream;
use tokio_stream::StreamExt;

use crate::executor::Message;
use crate::executor_v2::error::TracedStreamExecutorError;
use crate::executor_v2::{ExecutorInfo, MessageStream};

/// Streams wrapped by `epoch_check` will check whether the first message received is a barrier, and
/// the epoch in the barriers are monotonically increasing.
#[try_stream(ok = Message, error = TracedStreamExecutorError)]
pub async fn epoch_check(info: Arc<ExecutorInfo>, input: impl MessageStream) {
    pin_mut!(input);

    let first_msg = input.next().await.unwrap()?;
    let barrier = first_msg.as_barrier().unwrap_or_else(|| {
        panic!(
            "epoch check failed on {}: the first message must be a barrier",
            info.identity
        )
    });

    // Epoch number recorded from last barrier message.
    let mut last_epoch = barrier.epoch.curr;

    #[for_await]
    for message in input {
        let message = message?;

        if let Message::Barrier(b) = &message {
            let new_epoch = b.epoch.curr;

            if last_epoch > new_epoch {
                panic!(
                    "epoch check failed on {}: last epoch is {}, while the epoch of incoming barrier is {}.\nstale barrier: {:?}",
                    info.identity,
                    last_epoch,
                    new_epoch,
                    b
                );
            }
            last_epoch = new_epoch;
        }

        yield message;
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::{pin_mut, StreamExt};
    use risingwave_common::array::StreamChunk;

    use super::*;
    use crate::executor_v2::test_utils::MockSource;
    use crate::executor_v2::Executor;

    #[tokio::test]
    async fn test_epoch_ok() {
        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_barrier(100, false);
        source.push_chunks([StreamChunk::default()].into_iter());
        source.push_barrier(114, false);
        source.push_barrier(114, false);
        source.push_barrier(514, false);

        let checked = epoch_check(Arc::new(ExecutorInfo::default()), source.boxed().execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == 114);
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == 114);
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == 514);
    }

    #[should_panic]
    #[tokio::test]
    async fn test_epoch_bad() {
        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_barrier(100, false);
        source.push_chunks([StreamChunk::default()].into_iter());
        source.push_barrier(514, false);
        source.push_barrier(514, false);
        source.push_barrier(114, false);

        let checked = epoch_check(Arc::new(ExecutorInfo::default()), source.boxed().execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == 514);
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == 514);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_epoch_first_not_barrier() {
        let mut source = MockSource::new(Default::default(), vec![]);
        source.push_chunks([StreamChunk::default()].into_iter());
        source.push_barrier(114, false);

        let checked = epoch_check(Arc::new(ExecutorInfo::default()), source.boxed().execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }
}
