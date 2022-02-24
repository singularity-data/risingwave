#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::{
    CompactTask, HummockContextPinnedSnapshot, HummockContextPinnedVersion, HummockContextRefId,
    HummockSnapshot, HummockTablesToDelete, HummockVersion, Level, LevelType, SstableInfo,
    UncommittedEpoch,
};
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockSSTableId, HummockVersionId, INVALID_EPOCH,
};
use tokio::sync::{Mutex, RwLock};

use crate::hummock::compaction::CompactStatus;
use crate::hummock::level_handler::{LevelHandler, SSTableStat};
use crate::hummock::model::{
    CurrentHummockVersionId, HummockContextPinnedSnapshotExt, HummockContextPinnedVersionExt,
};
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{MetadataModel, MetadataUserCfModel, TransactionalUserCf};
use crate::storage::{MetaStore, Transaction, DEFAULT_COLUMN_FAMILY_ID};

pub struct HummockManager<S: MetaStore> {
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    // 1. When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    // 2. The two meta_store_ref from versioning and compaction point to the same meta store
    // instance. By convention we always use the one from compaction to get and commit meta store
    // transaction when both are available.
    // 3. TODO: Currently we don't prevent a meta store
    // transaction from living longer than the compaction guard or versioning guard it was
    // requested from. We need to fix it.
    cf_map: RwLock<BTreeMap<String, Arc<ColumnFamilyInner<S>>>>,
}

struct Compaction<S> {
    meta_store_ref: Arc<S>,
}

struct Versioning<S> {
    meta_store_ref: Arc<S>,
}

struct ColumnFamilyInner<S>
where
    S: MetaStore,
{
    compaction: Mutex<Compaction<S>>,
    versioning: RwLock<Versioning<S>>,
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> Result<HummockManager<S>> {
        let mut cf_map_inner: BTreeMap<String, _> = BTreeMap::new();
        cf_map_inner.insert(
            String::from(DEFAULT_COLUMN_FAMILY_ID),
            Arc::new(ColumnFamilyInner {
                versioning: RwLock::new(Versioning {
                    meta_store_ref: env.meta_store_ref(),
                }),
                compaction: Mutex::new(Compaction {
                    meta_store_ref: env.meta_store_ref(),
                }),
            }),
        );
        let instance = HummockManager {
            id_gen_manager_ref: env.id_gen_manager_ref(),
            cf_map: RwLock::new(cf_map_inner),
        };

        instance.initialize_meta().await?;

        Ok(instance)
    }

    /// Restore the table related data in meta store to a consistent state.
    async fn initialize_meta(&self) -> Result<()> {
        let mut ret = Ok(());
        let cf_map_guard = self.cf_map.write().await;
        for (cf_ident, cf_inner) in cf_map_guard.iter() {
            let res = self.initialize_cf_meta(cf_ident, cf_inner).await;
            if ret.is_ok() {
                ret = res;
            }
        }
        ret
    }

    async fn initialize_cf_meta(
        &self,
        cf_ident: &str,
        column_family_inner: &ColumnFamilyInner<S>,
    ) -> Result<()> {
        let compaction_guard = column_family_inner.compaction.lock().await;
        let versioning_guard = column_family_inner.versioning.write().await;
        // initialize metadata only if CurrentHummockVersionId is not found in metastore
        match CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident).await
        {
            Ok(_) => return Ok(()),
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                true
            }
        };

        let mut transaction = compaction_guard.meta_store_ref.get_transaction();

        let compact_status = CompactStatus::new(cf_ident);
        compact_status.update_in_transaction(&mut transaction);

        let init_version_id = CurrentHummockVersionId::new(cf_ident);
        init_version_id.update_in_transaction(&mut transaction);

        let init_version = &HummockVersion {
            id: init_version_id.id(),
            levels: vec![
                Level {
                    level_type: LevelType::Overlapping as i32,
                    table_ids: vec![],
                },
                Level {
                    level_type: LevelType::Nonoverlapping as i32,
                    table_ids: vec![],
                },
            ],
            uncommitted_epochs: vec![],
            max_committed_epoch: INVALID_EPOCH,
        };
        init_version.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;

        self.commit_trx(compaction_guard.meta_store_ref.as_ref(), transaction, None)
            .await
    }

    async fn commit_trx(
        &self,
        meta_store_ref: &S,
        mut trx: Transaction,
        context_id: Option<HummockContextId>,
    ) -> Result<()> {
        if let Some(_context_id) = context_id {
            // TODO check context validity
            trx.add_preconditions(vec![]);
        }
        meta_store_ref.txn(trx).await.map_err(Into::into)
    }

    pub async fn pin_version(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
    ) -> Result<HummockVersion> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;
        let version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                .await?
                .id();
        let hummock_version = HummockVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockVersionRefId { id: version_id },
        )
        .await?
        .unwrap();
        // pin the version
        let mut context_pinned_version = HummockContextPinnedVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?
        .unwrap_or(HummockContextPinnedVersion {
            context_id,
            version_id: vec![],
        });
        context_pinned_version.pin_version(version_id);
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        context_pinned_version.update_in_transaction(cf_ident, &mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;

        Ok(hummock_version)
    }

    pub async fn unpin_version(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
        pinned_version_id: HummockVersionId,
    ) -> Result<()> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        let mut context_pinned_version = match HummockContextPinnedVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?
        {
            None => {
                return Ok(());
            }
            Some(context_pinned_version) => context_pinned_version,
        };
        context_pinned_version.unpin_version(pinned_version_id);
        context_pinned_version.update_in_transaction(cf_ident, &mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await
    }

    pub async fn add_tables(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
        tables: Vec<SstableInfo>,
        epoch: HummockEpoch,
    ) -> Result<HummockVersion> {
        let stats = tables.iter().map(SSTableStat::from).collect_vec();

        // Hold the compact status lock so that no one else could add/drop SST or search compaction
        // plan.
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let compaction_guard = cf_inner.compaction.lock().await;
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref(), cf_ident).await?;
        match compact_status.level_handlers.first_mut().unwrap() {
            LevelHandler::Overlapping(vec_tier, _) => {
                for stat in stats {
                    let insert_point = vec_tier.partition_point(
                        |SSTableStat {
                             key_range: other_key_range,
                             ..
                         }| { other_key_range <= &stat.key_range },
                    );
                    vec_tier.insert(insert_point, stat);
                }
            }
            LevelHandler::Nonoverlapping(_, _) => {
                panic!("L0 must be Tiering.");
            }
        }
        let mut transaction = compaction_guard.meta_store_ref.get_transaction();
        // update compact_status
        compact_status.update_in_transaction(&mut transaction);

        let versioning_guard = cf_inner.versioning.write().await;
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                .await?;
        let old_version_id = current_version_id.id();
        let mut hummock_version = HummockVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();

        let current_tables = SstableInfo::list(&*versioning_guard.meta_store_ref).await?;
        if tables
            .iter()
            .any(|t| current_tables.iter().any(|ct| ct.id == t.id))
        {
            // Retry an add_tables request is OK if the original request has completed successfully.
            return Ok(hummock_version);
        }

        // check whether the epoch is valid
        // TODO: return error instead of panic
        // TODO: the validation is temporarily disabled until
        // the new barrier manager design is integrated
        // if epoch <= hummock_version.max_committed_epoch {
        //   panic!(
        //     "Epoch {} <= max_committed_epoch {}",
        //     epoch, hummock_version.max_committed_epoch
        //   );
        // }

        // add tables
        for table in &tables {
            table.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?
        }

        // create new_version by adding tables in UncommittedEpoch
        match hummock_version
            .uncommitted_epochs
            .iter_mut()
            .find(|e| e.epoch == epoch)
        {
            Some(uncommitted_epoch) => {
                tables
                    .iter()
                    .for_each(|t| uncommitted_epoch.table_ids.push(t.id));
            }
            None => hummock_version.uncommitted_epochs.push(UncommittedEpoch {
                epoch,
                table_ids: tables.iter().map(|t| t.id).collect(),
            }),
        };
        current_version_id.increase();
        current_version_id.update_in_transaction(&mut transaction);
        hummock_version.id = current_version_id.id();
        hummock_version.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;

        // the trx contain update for both tables and compact_status
        self.commit_trx(
            compaction_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;

        Ok(hummock_version)
    }

    /// TODO: if we need global max committed epoch, we shall traverse each cf and synthesize result
    pub async fn pin_snapshot(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
    ) -> Result<HummockSnapshot> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;

        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                .await?
                .id();
        let version = HummockVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockVersionRefId { id: version_id },
        )
        .await?
        .unwrap();
        let max_committed_epoch = version.max_committed_epoch;
        let mut context_pinned_snapshot = HummockContextPinnedSnapshot::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?
        .unwrap_or(HummockContextPinnedSnapshot {
            context_id,
            snapshot_id: vec![],
        });
        context_pinned_snapshot.pin_snapshot(max_committed_epoch);
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        context_pinned_snapshot.update_in_transaction(cf_ident, &mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;
        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    pub async fn unpin_snapshot(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;

        let mut context_pinned_snapshot = match HummockContextPinnedSnapshot::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?
        {
            None => {
                return Ok(());
            }
            Some(context_pinned_snapshot) => context_pinned_snapshot,
        };
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        context_pinned_snapshot.unpin_snapshot(hummock_snapshot.epoch);
        context_pinned_snapshot.update_in_transaction(cf_ident, &mut transaction)?;
        self.commit_trx(
            versioning_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await
    }

    pub async fn get_compact_task(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
    ) -> Result<Option<CompactTask>> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let watermark = {
            let versioning_guard = cf_inner.versioning.read().await;
            let current_version_id =
                CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                    .await?
                    .id();
            let version_data = HummockVersion::select_with_cf_suffix(
                &*versioning_guard.meta_store_ref,
                cf_ident,
                &HummockVersionRefId {
                    id: current_version_id,
                },
            )
            .await?
            .unwrap();
            HummockContextPinnedSnapshot::list_with_cf_suffix(
                &*versioning_guard.meta_store_ref,
                cf_ident,
            )
            .await?
            .iter()
            .flat_map(|v| v.snapshot_id.clone())
            .fold(version_data.max_committed_epoch, std::cmp::min)
        };
        let compaction_guard = cf_inner.compaction.lock().await;
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref(), cf_ident).await?;
        let compact_task = compact_status.get_compact_task();
        match compact_task {
            None => Ok(None),
            Some(mut compact_task) => {
                let mut transaction = compaction_guard.meta_store_ref.get_transaction();
                compact_status.update_in_transaction(&mut transaction);
                self.commit_trx(
                    compaction_guard.meta_store_ref.as_ref(),
                    transaction,
                    Some(context_id),
                )
                .await?;
                compact_task.watermark = watermark;
                Ok(Some(compact_task))
            }
        }
    }

    pub async fn report_compact_task(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
        compact_task: CompactTask,
        task_result: bool,
    ) -> Result<()> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let output_table_compact_entries: Vec<_> = compact_task
            .sorted_output_ssts
            .iter()
            .map(SSTableStat::from)
            .collect();
        let compaction_guard = cf_inner.compaction.lock().await;
        let mut transaction = compaction_guard.meta_store_ref.get_transaction();
        let mut compact_status =
            CompactStatus::get(compaction_guard.meta_store_ref.as_ref(), cf_ident).await?;
        let (sorted_output_ssts, delete_table_ids) = compact_status.report_compact_task(
            output_table_compact_entries,
            compact_task,
            task_result,
        );
        compact_status.update_in_transaction(&mut transaction);
        let versioning_guard = cf_inner.versioning.write().await;
        if task_result {
            let mut current_version_id =
                CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                    .await?;
            let old_version_id = current_version_id.increase();
            let new_version_id = current_version_id.id();
            current_version_id.update_in_transaction(&mut transaction);
            let old_version = HummockVersion::select_with_cf_suffix(
                &*versioning_guard.meta_store_ref,
                cf_ident,
                &HummockVersionRefId { id: old_version_id },
            )
            .await?
            .unwrap();
            let mut version = HummockVersion {
                id: new_version_id,
                levels: compact_status
                    .level_handlers
                    .iter()
                    .map(|level_handler| match level_handler {
                        LevelHandler::Overlapping(l_n, _) => Level {
                            level_type: LevelType::Overlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                        LevelHandler::Nonoverlapping(l_n, _) => Level {
                            level_type: LevelType::Nonoverlapping as i32,
                            table_ids: l_n
                                .iter()
                                .map(|SSTableStat { table_id, .. }| *table_id)
                                .collect(),
                        },
                    })
                    .collect(),
                uncommitted_epochs: old_version.uncommitted_epochs,
                max_committed_epoch: old_version.max_committed_epoch,
            };

            for table in sorted_output_ssts {
                table.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?
            }

            version.id = new_version_id;
            version.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;
            let mut tables_to_delete = HummockTablesToDelete::select_with_cf_suffix(
                &*versioning_guard.meta_store_ref,
                cf_ident,
                &HummockVersionRefId { id: new_version_id },
            )
            .await?
            .unwrap_or(HummockTablesToDelete {
                version_id: new_version_id,
                id: vec![],
            });
            tables_to_delete.id.extend(delete_table_ids);
            if tables_to_delete.id.is_empty() {
                tables_to_delete.delete_in_transaction_with_cf(cf_ident, &mut transaction)?;
            } else {
                tables_to_delete.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;
            }
        }
        self.commit_trx(
            compaction_guard.meta_store_ref.as_ref(),
            transaction,
            Some(context_id),
        )
        .await?;
        Ok(())
    }

    pub async fn commit_epoch(&self, cf_ident: &str, epoch: HummockEpoch) -> Result<()> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                .await?;
        let old_version_id = current_version_id.increase();
        let new_version_id = current_version_id.id();
        current_version_id.update_in_transaction(&mut transaction);
        let mut hummock_version = HummockVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();
        // TODO: return error instead of panic
        if epoch <= hummock_version.max_committed_epoch {
            panic!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, hummock_version.max_committed_epoch
            );
        }

        // TODO #447: the epoch should fail and rollback if any precedent epoch is uncommitted.
        // get tables in the committing epoch
        if let Some(idx) = hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];

            // commit tables by moving them into level0
            let version_first_level = hummock_version.levels.first_mut().unwrap();
            match version_first_level.get_level_type()? {
                LevelType::Overlapping => {
                    uncommitted_epoch
                        .table_ids
                        .iter()
                        .for_each(|id| version_first_level.table_ids.push(*id));
                }
                LevelType::Nonoverlapping => {
                    unimplemented!()
                }
            };

            // remove the epoch from uncommitted_epochs and update max_committed_epoch
            hummock_version.uncommitted_epochs.swap_remove(idx);
        }
        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        hummock_version.max_committed_epoch = epoch;
        hummock_version.id = new_version_id;
        hummock_version.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;
        self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
            .await?;
        tracing::trace!("new committed epoch {}", epoch);
        Ok(())
    }

    pub async fn abort_epoch(&self, cf_ident: &str, epoch: HummockEpoch) -> Result<()> {
        let cf_inner = {
            let cf_map_rd_guard = self.cf_map.read().await;
            cf_map_rd_guard.get(cf_ident).unwrap().clone()
        };
        let versioning_guard = cf_inner.versioning.write().await;
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        let mut current_version_id =
            CurrentHummockVersionId::get(versioning_guard.meta_store_ref.as_ref(), cf_ident)
                .await?;
        let old_version_id = current_version_id.increase();
        let new_version_id = current_version_id.id();
        current_version_id.update_in_transaction(&mut transaction);
        let mut hummock_version = HummockVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockVersionRefId { id: old_version_id },
        )
        .await?
        .unwrap();

        // get tables in the committing epoch
        match hummock_version
            .uncommitted_epochs
            .iter()
            .position(|e| e.epoch == epoch)
        {
            Some(idx) => {
                let uncommitted_epoch = &hummock_version.uncommitted_epochs[idx];

                // remove tables of the aborting epoch
                for table_id in &uncommitted_epoch.table_ids {
                    SstableInfo {
                        id: *table_id,
                        key_range: None,
                    }
                    .delete_in_transaction_with_cf(cf_ident, &mut transaction)?
                }
                hummock_version.uncommitted_epochs.swap_remove(idx);

                // create new_version
                hummock_version.id = new_version_id;
                hummock_version.upsert_in_transaction_with_cf(cf_ident, &mut transaction)?;

                self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
                    .await
            }
            None => Ok(()),
        }
    }

    pub async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        // TODO id_gen_manager generates u32, we need u64
        self.id_gen_manager_ref
            .generate::<{ IdCategory::HummockSSTableId }>()
            .await
            .map(|id| id as HummockSSTableId)
    }

    pub async fn release_all_context_resource(&self, context_id: HummockContextId) -> Result<()> {
        let mut ret = Ok(());
        let cf_map_guard = self.cf_map.read().await;
        for (cf_ident, cf_inner) in cf_map_guard.iter() {
            let res = self
                .release_cf_context_resource(context_id, cf_ident, cf_inner)
                .await;
            if ret.is_ok() {
                ret = res;
            }
        }
        ret
    }

    async fn release_cf_context_resource(
        &self,
        context_id: HummockContextId,
        cf_ident: &str,
        column_family_inner: &ColumnFamilyInner<S>,
    ) -> Result<()> {
        let versioning_guard = column_family_inner.versioning.write().await;
        let mut transaction = versioning_guard.meta_store_ref.get_transaction();
        let pinned_version = HummockContextPinnedVersion::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?;
        let mut to_commit = false;
        if let Some(pinned_version) = pinned_version {
            pinned_version.delete_in_transaction_with_cf(cf_ident, &mut transaction)?;
            to_commit = true;
        }
        let pinned_snapshot = HummockContextPinnedSnapshot::select_with_cf_suffix(
            &*versioning_guard.meta_store_ref,
            cf_ident,
            &HummockContextRefId { id: context_id },
        )
        .await?;
        if let Some(pinned_snapshot) = pinned_snapshot {
            pinned_snapshot.delete_in_transaction_with_cf(cf_ident, &mut transaction)?;
            to_commit = true;
        }
        if !to_commit {
            return Ok(());
        }
        self.commit_trx(versioning_guard.meta_store_ref.as_ref(), transaction, None)
            .await
    }
}
