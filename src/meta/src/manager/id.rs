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

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use risingwave_common::catalog::{NON_RESERVED_PG_CATALOG_TABLE_ID, NON_RESERVED_USER_ID};
use risingwave_common::error::Result;
use tokio::sync::RwLock;

use crate::cluster::META_NODE_ID;
use crate::storage::{self, MetaStore, DEFAULT_COLUMN_FAMILY};

pub const ID_PREALLOCATE_INTERVAL: i32 = 1000;

pub type Id = i32;

// TODO: remove unnecessary async trait.
#[async_trait::async_trait]
pub trait IdGenerator: Sync + Send + 'static {
    /// Generate a batch of identities.
    /// The valid id range will be [result_id, result_id + interval)
    async fn generate_interval(&self, interval: i32) -> Result<Id>;

    /// Generate an identity.
    async fn generate(&self) -> Result<Id> {
        self.generate_interval(1).await
    }
}

/// [`StoredIdGenerator`] implements id generator using metastore.
pub struct StoredIdGenerator<S> {
    meta_store: Arc<S>,
    category_gen_key: String,
    current_id: AtomicI32,
    next_allocate_id: RwLock<Id>,
}

impl<S> StoredIdGenerator<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>, category: &str, start: Option<Id>) -> Self {
        let category_gen_key = format!("{}_id_next_generator", category);
        let res = meta_store
            .get_cf(DEFAULT_COLUMN_FAMILY, category_gen_key.as_bytes())
            .await;
        let current_id = match res {
            Ok(value) => i32::from_be_bytes(value.as_slice().try_into().unwrap()),
            Err(storage::Error::ItemNotFound(_)) => start.unwrap_or(0),
            Err(e) => panic!("{:?}", e),
        };

        let next_allocate_id = current_id + ID_PREALLOCATE_INTERVAL;
        if let Err(err) = meta_store
            .put_cf(
                DEFAULT_COLUMN_FAMILY,
                category_gen_key.clone().into_bytes(),
                next_allocate_id.to_be_bytes().to_vec(),
            )
            .await
        {
            panic!("{:?}", err)
        }

        StoredIdGenerator {
            meta_store,
            category_gen_key,
            current_id: AtomicI32::new(current_id),
            next_allocate_id: RwLock::new(next_allocate_id),
        }
    }
}

#[async_trait::async_trait]
impl<S> IdGenerator for StoredIdGenerator<S>
where
    S: MetaStore,
{
    async fn generate_interval(&self, interval: i32) -> Result<Id> {
        let id = self.current_id.fetch_add(interval, Ordering::Relaxed);
        let next_allocate_id = { *self.next_allocate_id.read().await };
        if id + interval > next_allocate_id {
            let mut next = self.next_allocate_id.write().await;
            if id + interval > *next {
                let weight = num_integer::Integer::div_ceil(
                    &(id + interval - *next),
                    &ID_PREALLOCATE_INTERVAL,
                );
                let next_allocate_id = *next + ID_PREALLOCATE_INTERVAL * weight;
                self.meta_store
                    .put_cf(
                        DEFAULT_COLUMN_FAMILY,
                        self.category_gen_key.clone().into_bytes(),
                        next_allocate_id.to_be_bytes().to_vec(),
                    )
                    .await?;
                *next = next_allocate_id;
            }
        }

        Ok(id)
    }
}

type IdCategoryType = u8;

// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/95174) get completed.
#[expect(non_snake_case, non_upper_case_globals)]
pub mod IdCategory {
    use super::IdCategoryType;

    #[cfg(test)]
    pub const Test: IdCategoryType = 0;
    pub const Database: IdCategoryType = 1;
    pub const Schema: IdCategoryType = 2;
    pub const Table: IdCategoryType = 3;
    pub const Worker: IdCategoryType = 4;
    pub const Fragment: IdCategoryType = 5;
    pub const Actor: IdCategoryType = 6;
    pub const HummockSnapshot: IdCategoryType = 7;
    pub const HummockSstableId: IdCategoryType = 8;
    pub const ParallelUnit: IdCategoryType = 9;
    pub const Source: IdCategoryType = 10;
    pub const HummockCompactionTask: IdCategoryType = 11;
    pub const User: IdCategoryType = 12;
}

pub type IdGeneratorManagerRef<S> = Arc<IdGeneratorManager<S>>;

/// [`IdGeneratorManager`] manages id generators in all categories,
/// which defined as [`IdCategory`] in [`meta.proto`].
pub struct IdGeneratorManager<S> {
    #[cfg(test)]
    test: Arc<StoredIdGenerator<S>>,
    database: Arc<StoredIdGenerator<S>>,
    schema: Arc<StoredIdGenerator<S>>,
    table: Arc<StoredIdGenerator<S>>,
    worker: Arc<StoredIdGenerator<S>>,
    fragment: Arc<StoredIdGenerator<S>>,
    actor: Arc<StoredIdGenerator<S>>,
    user: Arc<StoredIdGenerator<S>>,
    hummock_snapshot: Arc<StoredIdGenerator<S>>,
    hummock_ss_table_id: Arc<StoredIdGenerator<S>>,
    hummock_compaction_task: Arc<StoredIdGenerator<S>>,
    parallel_unit: Arc<StoredIdGenerator<S>>,
}

impl<S> IdGeneratorManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>) -> Self {
        Self {
            #[cfg(test)]
            test: Arc::new(StoredIdGenerator::new(meta_store.clone(), "test", None).await),
            database: Arc::new(StoredIdGenerator::new(meta_store.clone(), "database", None).await),
            schema: Arc::new(StoredIdGenerator::new(meta_store.clone(), "schema", None).await),
            table: Arc::new(
                StoredIdGenerator::new(
                    meta_store.clone(),
                    "table",
                    Some(NON_RESERVED_PG_CATALOG_TABLE_ID),
                )
                .await,
            ),
            worker: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "worker", Some(META_NODE_ID as i32 + 1))
                    .await,
            ),
            fragment: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "fragment", Some(1)).await,
            ),
            actor: Arc::new(StoredIdGenerator::new(meta_store.clone(), "actor", Some(1)).await),
            user: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "user", Some(NON_RESERVED_USER_ID))
                    .await,
            ),
            hummock_snapshot: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "hummock_snapshot", Some(1)).await,
            ),
            hummock_ss_table_id: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "hummock_ss_table_id", Some(1)).await,
            ),
            hummock_compaction_task: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "hummock_compaction_task", Some(1))
                    .await,
            ),
            parallel_unit: Arc::new(
                StoredIdGenerator::new(meta_store.clone(), "parallel_unit", None).await,
            ),
        }
    }

    const fn get<const C: IdCategoryType>(&self) -> &Arc<StoredIdGenerator<S>> {
        match C {
            #[cfg(test)]
            IdCategory::Test => &self.test,
            IdCategory::Database => &self.database,
            IdCategory::Schema => &self.schema,
            IdCategory::Table => &self.table,
            IdCategory::Fragment => &self.fragment,
            IdCategory::Actor => &self.actor,
            IdCategory::User => &self.user,
            IdCategory::HummockSnapshot => &self.hummock_snapshot,
            IdCategory::Worker => &self.worker,
            IdCategory::HummockSstableId => &self.hummock_ss_table_id,
            IdCategory::ParallelUnit => &self.parallel_unit,
            IdCategory::HummockCompactionTask => &self.hummock_compaction_task,
            _ => unreachable!(),
        }
    }

    /// [`Self::generate`] function generates id as `current_id`.
    pub async fn generate<const C: IdCategoryType>(&self) -> Result<Id> {
        self.get::<C>().generate().await
    }

    /// [`Self::generate_interval`] function generates ids as [`current_id`, `current_id` +
    /// interval), the next id will be `current_id` + interval.
    pub async fn generate_interval<const C: IdCategoryType>(&self, interval: i32) -> Result<Id> {
        self.get::<C>().generate_interval(interval).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future;
    use itertools::Itertools;

    use super::*;
    use crate::storage::MemStore;

    async fn join_generate(generator: &dyn IdGenerator, cnt: usize) -> Result<Vec<Id>> {
        future::join_all((0..cnt).map(|_| {
            let id_generator = generator;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }

    async fn join_generate_interval(
        generator: &dyn IdGenerator,
        cnt: usize,
        interval: i32,
    ) -> Result<Vec<Id>> {
        future::join_all((0..cnt).map(|_| {
            let id_generator = generator;
            async move { id_generator.generate_interval(interval).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }

    #[tokio::test]
    async fn test_id_generator() -> Result<()> {
        let meta_store = Arc::new(MemStore::default());
        let test_id_generator = StoredIdGenerator::new(meta_store.clone(), "test", None).await;
        let ids = join_generate(&test_id_generator, 10000).await?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let test_id_generator_2 = StoredIdGenerator::new(meta_store.clone(), "test", None).await;
        let ids = join_generate(&test_id_generator_2, 10000).await?;
        assert_eq!(ids, (10000..20000).collect::<Vec<_>>());

        let actor_id_generator = StoredIdGenerator::new(meta_store.clone(), "actor", Some(1)).await;
        let ids = join_generate_interval(&actor_id_generator, 100, 100).await?;
        let vec_expect = (0..100).map(|e| e * 100 + 1).collect_vec();
        assert_eq!(ids, vec_expect);

        let actor_id_generator_two = StoredIdGenerator::new(meta_store, "actor", None).await;
        let ids = join_generate_interval(&actor_id_generator_two, 100, 10).await?;

        let vec_expect = (0..100).map(|e| 10001 + e * 10).collect_vec();
        assert_eq!(ids, vec_expect);

        Ok(())
    }

    #[tokio::test]
    async fn test_id_generator_manager() -> Result<()> {
        let meta_store = Arc::new(MemStore::default());
        let manager = IdGeneratorManager::new(meta_store.clone()).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let manager = &manager;
            async move { manager.generate::<{ IdCategory::Test }>().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect_vec());

        let ids = future::join_all((0..100).map(|_i| {
            let manager = &manager;
            async move {
                manager
                    .generate_interval::<{ IdCategory::Actor }>(9999)
                    .await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        let vec_expect = (0..100).map(|e| e * 9999 + 1).collect_vec();
        assert_eq!(ids, vec_expect);

        let manager = IdGeneratorManager::new(meta_store).await;
        let id = manager
            .generate_interval::<{ IdCategory::Actor }>(10)
            .await?;
        assert_eq!(id, 1000001);

        Ok(())
    }
}
