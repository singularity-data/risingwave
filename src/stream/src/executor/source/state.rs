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

use std::fmt::Debug;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::error;
use risingwave_connector::{SplitImpl, SplitMetaData};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::WriteOptions;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::error::{StreamExecutorError, StreamExecutorResult};

/// `SourceState` Represents an abstraction of state,
/// e.g. if the Kafka Source state consists of `topic` `partition_id` and `offset`.
pub trait SourceState: Debug + Clone {
    /// A value that can uniquely represent a state trait object
    fn identifier(&self) -> String;
    /// The binary code for the state. In the concrete implementation it is recommended to
    /// `serialize` only the required fields. And the other fields can be `transient`
    fn encode(&self) -> Bytes;
    fn decode(&self, values: Bytes) -> Self;
}

#[derive(Clone)]
pub struct SourceStateHandler<S: StateStore> {
    keyspace: Keyspace<S>,
}

impl<S: StateStore> Debug for SourceStateHandler<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceStateHandler").finish()
    }
}

impl<S: StateStore> SourceStateHandler<S> {
    pub fn new(keyspace: Keyspace<S>) -> Self {
        Self { keyspace }
    }

    /// This function provides the ability to persist the source state
    /// and needs to be invoked by the ``SourceReader`` to call it,
    /// and will return the error when the dependent ``StateStore`` handles the error.
    /// The caller should ensure that the passed parameters are not empty.
    pub async fn take_snapshot<SS>(&self, states: Vec<SS>, epoch: u64) -> Result<()>
    where
        SS: SplitMetaData,
    {
        if states.is_empty() {
            // TODO should be a clear Error Code
            Err(anyhow!("states require not null"))
        } else {
            let mut write_batch = self.keyspace.state_store().start_write_batch(WriteOptions {
                epoch,
                table_id: self.keyspace.table_id(),
            });
            let mut local_batch = write_batch.prefixify(&self.keyspace);
            states.iter().for_each(|state| {
                let value = state.encode_to_bytes();
                // TODO(Yuanxin): Implement value meta
                local_batch.put(state.id(), StorageValue::new_default_put(value));
            });
            // If an error is returned, the underlying state should be rollback
            let ingest_rs = write_batch.ingest().await;
            match ingest_rs {
                Err(e) => {
                    error!(
                        "SourceStateHandler take_snapshot() batch.ingest Error,cause by {:?}",
                        e
                    );
                    Err(anyhow!(e))
                }
                _ => Ok(()),
            }
        }
    }

    /// Retrieves the state of the specified ``state_identifier``
    /// Returns None if it does not exist (e.g., the first accessible source).
    ///
    /// The function returns the serialized state.
    pub async fn restore_states(
        &self,
        state_identifier: String,
        epoch: u64,
    ) -> StreamExecutorResult<Option<Bytes>> {
        self.keyspace
            .get(state_identifier, epoch)
            .await
            .map_err(StreamExecutorError::storage)
    }

    pub async fn try_recover_from_state_store(
        &self,
        stream_source_split: &SplitImpl,
        epoch: u64,
    ) -> StreamExecutorResult<Option<SplitImpl>> {
        // let connector_type = stream_source_split.get_type();
        match self.restore_states(stream_source_split.id(), epoch).await {
            Ok(Some(s)) => Ok(Some(SplitImpl::restore_from_bytes(&s)?)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_storage::memory::MemoryStateStore;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestSourceState {
        partition: String,
        offset: i64,
    }

    impl TestSourceState {
        fn new(partition: String, offset: i64) -> Self {
            Self { partition, offset }
        }
    }

    impl SplitMetaData for TestSourceState {
        fn id(&self) -> String {
            self.partition.clone()
        }

        fn encode_to_bytes(&self) -> Bytes {
            Bytes::from(serde_json::to_string(self).unwrap())
        }

        fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
            serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
        }
    }

    #[test]
    fn test_state_encode() -> Result<()> {
        let offset = 100_i64;
        let partition = String::from("p0");
        let state_instance = TestSourceState::new(partition.clone(), offset);
        assert_eq!(offset, state_instance.offset);
        assert_eq!(partition, state_instance.partition);
        println!("TestSourceState = {:?}", state_instance);
        let encode_value = state_instance.encode_to_bytes();
        let decode_value = TestSourceState::restore_from_bytes(&encode_value)?;
        println!("decode from Bytes instance = {:?}", decode_value);
        assert_eq!(offset, decode_value.offset);
        assert_eq!(partition, decode_value.partition);

        Ok(())
    }

    fn new_test_keyspace() -> Keyspace<MemoryStateStore> {
        let test_mem_state_store = MemoryStateStore::new();
        Keyspace::table_root(test_mem_state_store, &TableId::from(1))
    }

    fn test_state_store_vec() -> Vec<TestSourceState> {
        (0..2)
            .into_iter()
            .map(|i| TestSourceState::new(["p".to_string(), i.to_string()].concat(), i))
            .collect_vec()
    }

    async fn take_snapshot_and_get_states(
        state_handler: SourceStateHandler<MemoryStateStore>,
        current_epoch: u64,
    ) -> (Vec<TestSourceState>, Result<()>) {
        let states = test_state_store_vec();
        println!("Vec<TestSourceStat>={:?}", states.clone());
        let rs = state_handler
            .take_snapshot(states.clone(), current_epoch)
            .await;
        (states, rs)
    }

    #[tokio::test]
    async fn test_take_snapshot() {
        let current_epoch = 1000;
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());
        let _rs = take_snapshot_and_get_states(state_store_handler.clone(), current_epoch).await;
        let stored_states = state_store_handler
            .keyspace
            .scan(None, current_epoch)
            .await
            .unwrap();
        assert_ne!(0, stored_states.len());
    }

    #[tokio::test]
    async fn test_empty_state_restore() {
        let partition = "p01".to_string();
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());
        let list_states = state_store_handler
            .restore_states(partition, u64::MAX)
            .await
            .unwrap();
        println!("current list_states={:?}", list_states);
        assert!(list_states.is_none())
    }

    #[tokio::test]
    async fn test_state_restore() {
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());
        let current_epoch = 1000;
        let saved_states = take_snapshot_and_get_states(state_store_handler.clone(), current_epoch)
            .await
            .0;

        for state in saved_states {
            let identifier = state.id();
            let state_pair = state_store_handler
                .restore_states(identifier, current_epoch)
                .await
                .unwrap();
            println!("source_state_handler restore state_pair={:?}", state_pair);
            state_pair.into_iter().for_each(|s| {
                assert_eq!(
                    state.offset,
                    TestSourceState::restore_from_bytes(&s).unwrap().offset
                );
                assert_eq!(
                    state.partition,
                    TestSourceState::restore_from_bytes(&s).unwrap().partition
                );
            });
        }
    }
}
