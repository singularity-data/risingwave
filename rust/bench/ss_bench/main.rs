use std::sync::Arc;

mod operations;
mod utils;

use clap::Parser;
use moka::future::Cache;
use operations::*;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::hummock_meta_client::RPCHummockMetaClient;
use risingwave_storage::hummock::local_version_manager::LocalVersionManager;
use risingwave_storage::hummock::version_manager::VersionManager;
use risingwave_storage::hummock::{HummockOptions, HummockStateStore, HummockStorage};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::object::{ConnectionInfo, S3ObjectStore};
use risingwave_storage::rocksdb_local::RocksDBStateStore;
use risingwave_storage::tikv::TikvStateStore;
use risingwave_storage::StateStore;

#[allow(dead_code)]
enum WorkloadType {
    WriteBatch = 0,
    GetRandom = 1,
    GetSeq = 2,
    PrefixScanRandom = 3,
    DeleteRandom = 4,
    DeleteSeq = 5,
}

#[derive(Parser, Debug)]
pub(crate) struct Opts {
    // ----- backend type  -----
    #[clap(long, default_value = "in-memory")]
    store: String,

    // ----- Hummock -----
    #[clap(long, default_value_t = 256)]
    table_size_mb: u32,

    #[clap(long, default_value_t = 64)]
    block_size_kb: u32,

    #[clap(long, default_value_t = 0.1)]
    bloom_false_positive: f64,

    #[clap(long, default_value = "crc32c")]
    checksum_algo: String,

    // ----- benchmarks -----
    #[clap(long)]
    benchmarks: String,

    #[clap(long, default_value_t = 1)]
    concurrency_num: u32,

    // ----- operation number -----
    #[clap(long, default_value_t = 1000000)]
    num: i64,

    #[clap(long, default_value_t = -1)]
    deletes: i64,

    #[clap(long, default_value_t = -1)]
    reads: i64,

    #[clap(long, default_value_t = 100)]
    write_batches: u64,

    // ----- single batch -----
    #[clap(long, default_value_t = 100)]
    batch_size: u32,

    #[clap(long, default_value_t = 16)]
    key_size: u32,

    #[clap(long, default_value_t = 5)]
    key_prefix_size: u32,

    #[clap(long, default_value_t = 10)]
    keys_per_prefix: u32,

    #[clap(long, default_value_t = 100)]
    value_size: u32,
}

fn get_checksum_algo(algo: &str) -> ChecksumAlg {
    match algo {
        "crc32c" => ChecksumAlg::Crc32c,
        "xxhash64" => ChecksumAlg::XxHash64,
        other => unimplemented!("checksum algorithm \"{}\" is not supported", other),
    }
}

#[derive(Clone)]
pub(crate) enum StateStoreImpl {
    Hummock(HummockStateStore),
    Memory(MemoryStateStore),
    RocksDB(RocksDBStateStore),
    Tikv(TikvStateStore),
}

async fn get_state_store_impl(opts: &Opts) -> Result<StateStoreImpl> {
    let meta_address = "127.0.0.1:5691";

    let instance = match opts.store.as_ref() {
        "in-memory" | "in_memory" => StateStoreImpl::Memory(MemoryStateStore::new()),
        tikv if tikv.starts_with("tikv") => StateStoreImpl::Tikv(TikvStateStore::new(vec![tikv
            .strip_prefix("tikv://")
            .unwrap()
            .to_string()])),
        minio if minio.starts_with("hummock+minio://") => {
            let object_client = Arc::new(S3ObjectStore::new_with_minio(
                minio.strip_prefix("hummock+").unwrap(),
            ));
            let remote_dir = "hummock_001";
            let hummock_meta_client = Arc::new(RPCHummockMetaClient::new(
                MetaClient::new(meta_address).await?,
            ));
            StateStoreImpl::Hummock(HummockStateStore::new(
                HummockStorage::new(
                    object_client.clone(),
                    HummockOptions {
                        sstable_size: opts.table_size_mb * (1 << 20),
                        block_size: opts.block_size_kb * (1 << 10),
                        bloom_false_positive: opts.bloom_false_positive,
                        remote_dir: remote_dir.to_string(),
                        checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    },
                    Arc::new(VersionManager::new()),
                    Arc::new(LocalVersionManager::new(
                        object_client,
                        remote_dir,
                        Some(Arc::new(Cache::new(65536))),
                    )),
                    hummock_meta_client,
                )
                .await
                .map_err(RwError::from)?,
            ))
        }
        s3 if s3.starts_with("hummock+s3://") => {
            let s3_test_conn_info = ConnectionInfo::new();
            let s3_store = Arc::new(S3ObjectStore::new(
                s3_test_conn_info,
                s3.strip_prefix("hummock+s3://").unwrap().to_string(),
            ));
            let remote_dir = "hummock_001";
            let hummock_meta_client = Arc::new(RPCHummockMetaClient::new(
                MetaClient::new(meta_address).await?,
            ));
            StateStoreImpl::Hummock(HummockStateStore::new(
                HummockStorage::new(
                    s3_store.clone(),
                    HummockOptions {
                        sstable_size: opts.table_size_mb * (1 << 20),
                        block_size: opts.block_size_kb * (1 << 10),
                        bloom_false_positive: opts.bloom_false_positive,
                        remote_dir: remote_dir.to_string(),
                        checksum_algo: get_checksum_algo(opts.checksum_algo.as_ref()),
                    },
                    Arc::new(VersionManager::new()),
                    Arc::new(LocalVersionManager::new(
                        s3_store,
                        remote_dir,
                        Some(Arc::new(Cache::new(65536))),
                    )),
                    hummock_meta_client,
                )
                .await
                .map_err(RwError::from)?,
            ))
        }
        rocksdb if rocksdb.starts_with("rocksdb_local://") => StateStoreImpl::RocksDB(
            RocksDBStateStore::new(rocksdb.strip_prefix("rocksdb_local://").unwrap()),
        ),
        other => unimplemented!("state store \"{}\" is not supported", other),
    };
    Ok(instance)
}

async fn run_operations(store: impl StateStore, opts: &Opts) {
    for operation in opts.benchmarks.split(',') {
        match operation {
            "writebatch" => write_batch::run(&store, opts).await,
            "getrandom" => get_random::run(&store, opts).await,
            "getseq" => get_seq::run(&store, opts).await,
            "prefixscanrandom" => prefix_scan_random::run(&store, opts).await,
            other => unimplemented!("operation \"{}\" is not supported.", other),
        }
    }
}

fn preprocess_options(opts: &mut Opts) {
    if opts.reads < 0 {
        opts.reads = opts.num;
    }
    if opts.deletes < 0 {
        opts.deletes = opts.num;
    }

    // check illegal configurations
    for operation in opts.benchmarks.split(',') {
        if operation == "getseq" {
            // TODO(sun ting): eliminate this limitation
            if opts.batch_size < opts.reads as u32 {
                panic!(
                    "In sequential mode, `batch_size` should be greater than or equal to `reads`"
                );
            }
        }
    }
}

/// This is used to benchmark the state store performance.
/// For usage, see: https://github.com/singularity-data/risingwave-dev/blob/main/docs/developer/benchmark_tool/state_store.md
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let mut opts = Opts::parse();

    println!("Configurations before preprocess:\n {:?}", &opts);
    preprocess_options(&mut opts);
    println!("Configurations after preprocess:\n {:?}", &opts);

    let state_store = match get_state_store_impl(&opts).await {
        Ok(state_store_impl) => state_store_impl,
        Err(_) => {
            eprintln!("Failed to get state_store");
            return;
        }
    };

    match state_store {
        StateStoreImpl::Hummock(store) => run_operations(store, &opts).await,
        StateStoreImpl::Memory(store) => run_operations(store, &opts).await,
        StateStoreImpl::RocksDB(store) => run_operations(store, &opts).await,
        StateStoreImpl::Tikv(store) => run_operations(store, &opts).await,
    };
}
