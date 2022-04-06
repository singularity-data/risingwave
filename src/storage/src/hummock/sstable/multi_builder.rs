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

use bytes::Bytes;
use futures::Future;
use risingwave_common::storage::key::{Epoch, FullKey};

use super::SstableMeta;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SSTableBuilder};

struct SSTableBuilderWrapper {
    id: u64,
    builder: SSTableBuilder,
    sealed: bool,
}

/// A wrapper for [`SSTableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<B> {
    /// When creating a new [`SSTableBuilder`], caller use this closure to specify the id and
    /// options.
    get_id_and_builder: B,

    /// Wrapped [`SSTableBuilder`]s. The last one is what we are operating on.
    builders: Vec<SSTableBuilderWrapper>,
}

impl<B, F> CapacitySplitTableBuilder<B>
where
    B: FnMut() -> F,
    F: Future<Output = HummockResult<(u64, SSTableBuilder)>>,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    pub fn new(get_id_and_builder: B) -> Self {
        Self {
            get_id_and_builder,
            builders: Vec::new(),
        }
    }

    /// Returns the number of [`SSTableBuilder`]s.
    pub fn len(&self) -> usize {
        self.builders.len()
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.builders.is_empty()
    }

    /// Adds a user key-value pair to the underlying builders, with given `epoch`.
    ///
    /// If the current builder reaches its capacity, this function will create a new one with the
    /// configuration generated by the closure provided earlier.
    pub async fn add_user_key(
        &mut self,
        user_key: Vec<u8>,
        value: HummockValue<&[u8]>,
        epoch: Epoch,
    ) -> HummockResult<()> {
        assert!(!user_key.is_empty());
        let full_key = FullKey::from_user_key(user_key, epoch);
        self.add_full_key(full_key.as_slice(), value, true).await?;
        Ok(())
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        let last_is_full = self
            .builders
            .last()
            .map(|b| b.builder.reach_capacity() || b.sealed)
            .unwrap_or(true);
        let new_builder_required = self.builders.is_empty() || (allow_split && last_is_full);

        if new_builder_required {
            let (id, builder) = (self.get_id_and_builder)().await?;
            self.builders.push(SSTableBuilderWrapper {
                id,
                builder,
                sealed: false,
            });
        }

        let builder = &mut self.builders.last_mut().unwrap().builder;
        builder.add(full_key.into_inner(), value);
        Ok(())
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub fn seal_current(&mut self) {
        if let Some(b) = self.builders.last_mut() {
            b.sealed = true;
        }
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub fn finish(self) -> Vec<(u64, Bytes, SstableMeta)> {
        self.builders
            .into_iter()
            .map(|b| {
                let (data, meta) = b.builder.finish();
                (b.id, data, meta)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use itertools::Itertools;

    use super::*;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::test_utils::default_builder_opt_for_test;
    use crate::hummock::{SSTableBuilderOptions, DEFAULT_RESTART_INTERVAL};

    #[tokio::test]
    async fn test_empty() {
        let next_id = AtomicU64::new(1001);
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = || async {
            Ok((
                next_id.fetch_add(1, SeqCst),
                SSTableBuilder::new(SSTableBuilderOptions {
                    capacity: table_capacity,
                    block_capacity: block_size,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                    bloom_false_positive: 0.1,
                    compression_algorithm: CompressionAlgorithm::None,
                }),
            ))
        };
        let builder = CapacitySplitTableBuilder::new(get_id_and_builder);
        let results = builder.finish();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let next_id = AtomicU64::new(1001);

        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = || async {
            Ok((
                next_id.fetch_add(1, SeqCst),
                SSTableBuilder::new(SSTableBuilderOptions {
                    capacity: table_capacity,
                    block_capacity: block_size,
                    restart_interval: DEFAULT_RESTART_INTERVAL,
                    bloom_false_positive: 0.1,
                    compression_algorithm: CompressionAlgorithm::None,
                }),
            ))
        };
        let mut builder = CapacitySplitTableBuilder::new(get_id_and_builder);

        for i in 0..table_capacity {
            builder
                .add_user_key(
                    b"key".to_vec(),
                    HummockValue::put(b"value"),
                    (table_capacity - i) as u64,
                )
                .await
                .unwrap();
        }

        let results = builder.finish();
        assert!(results.len() > 1);
        assert_eq!(results.iter().map(|p| p.0).duplicates().count(), 0);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let next_id = AtomicU64::new(1001);
        let mut builder = CapacitySplitTableBuilder::new(|| async {
            Ok((
                next_id.fetch_add(1, SeqCst),
                SSTableBuilder::new(default_builder_opt_for_test()),
            ))
        });
        let mut epoch = 100;

        macro_rules! add {
            () => {
                epoch -= 1;
                builder
                    .add_user_key(b"k".to_vec(), HummockValue::put(b"v"), epoch)
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current();
        assert_eq!(builder.len(), 2);
        builder.seal_current();
        assert_eq!(builder.len(), 2);

        let results = builder.finish();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let next_id = AtomicU64::new(1001);
        let mut builder = CapacitySplitTableBuilder::new(|| async {
            Ok((
                next_id.fetch_add(1, SeqCst),
                SSTableBuilder::new(default_builder_opt_for_test()),
            ))
        });

        builder
            .add_full_key(
                FullKey::from_user_key_slice(b"k", 233).as_slice(),
                HummockValue::put(b"v"),
                false,
            )
            .await
            .unwrap();
    }
}
