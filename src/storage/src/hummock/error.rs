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

use std::backtrace::Backtrace;

use risingwave_object_store::object::ObjectError;
use thiserror::Error;

#[derive(Error, Debug)]
enum HummockErrorInner {
    #[error("Magic number mismatch: expected {expected}, found: {found}.")]
    MagicMismatch { expected: u32, found: u32 },
    #[error("Invalid format version: {0}.")]
    InvalidFormatVersion(u32),
    #[error("Checksum mismatch: expected {expected}, found: {found}.")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Invalid block.")]
    InvalidBlock,
    #[error("Encode error {0}.")]
    EncodeError(String),
    #[error("Decode error {0}.")]
    DecodeError(String),
    #[expect(dead_code)]
    #[error("Mock error {0}.")]
    MockError(String),
    #[error("ObjectStore failed with IO error {0}.")]
    ObjectIoError(ObjectError),
    #[error("Meta error {0}.")]
    MetaError(String),
    #[error("Invalid WriteBatch.")]
    InvalidWriteBatch,
    #[error("SharedBuffer error {0}.")]
    SharedBufferError(String),
    #[error("Wait epoch error {0}.")]
    WaitEpoch(String),
    #[error("Expired Epoch: watermark {safe_epoch}, epoch {epoch}.")]
    ExpiredEpoch { safe_epoch: u64, epoch: u64 },
    #[error("CompactionExecutor error {0}.")]
    CompactionExecutor(String),
    #[error("TieredCache error {0}.")]
    TieredCache(String),
    #[error("Other error {0}.")]
    Other(String),
}

#[derive(Error)]
#[error("{inner}")]
pub struct HummockError {
    #[from]
    inner: HummockErrorInner,
    backtrace: Backtrace,
}

impl HummockError {
    pub fn object_io_error(error: ObjectError) -> HummockError {
        HummockErrorInner::ObjectIoError(error).into()
    }

    pub fn invalid_format_version(v: u32) -> HummockError {
        HummockErrorInner::InvalidFormatVersion(v).into()
    }

    pub fn invalid_block() -> HummockError {
        HummockErrorInner::InvalidBlock.into()
    }

    pub fn encode_error(error: impl ToString) -> HummockError {
        HummockErrorInner::EncodeError(error.to_string()).into()
    }

    pub fn decode_error(error: impl ToString) -> HummockError {
        HummockErrorInner::DecodeError(error.to_string()).into()
    }

    pub fn magic_mismatch(expected: u32, found: u32) -> HummockError {
        HummockErrorInner::MagicMismatch { expected, found }.into()
    }

    pub fn checksum_mismatch(expected: u64, found: u64) -> HummockError {
        HummockErrorInner::ChecksumMismatch { expected, found }.into()
    }

    pub fn meta_error(error: impl ToString) -> HummockError {
        HummockErrorInner::MetaError(error.to_string()).into()
    }

    pub fn invalid_write_batch() -> HummockError {
        HummockErrorInner::InvalidWriteBatch.into()
    }

    pub fn shared_buffer_error(error: impl ToString) -> HummockError {
        HummockErrorInner::SharedBufferError(error.to_string()).into()
    }

    pub fn wait_epoch(error: impl ToString) -> HummockError {
        HummockErrorInner::WaitEpoch(error.to_string()).into()
    }

    pub fn expired_epoch(safe_epoch: u64, epoch: u64) -> HummockError {
        HummockErrorInner::ExpiredEpoch { safe_epoch, epoch }.into()
    }

    pub fn compaction_executor(error: impl ToString) -> HummockError {
        HummockErrorInner::CompactionExecutor(error.to_string()).into()
    }

    pub fn tiered_cache(error: impl ToString) -> HummockError {
        HummockErrorInner::TieredCache(error.to_string()).into()
    }

    pub fn other(error: impl ToString) -> HummockError {
        HummockErrorInner::Other(error.to_string()).into()
    }
}

impl From<prost::DecodeError> for HummockError {
    fn from(error: prost::DecodeError) -> Self {
        HummockErrorInner::DecodeError(error.to_string()).into()
    }
}

impl std::fmt::Debug for HummockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `TracedHummockError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

pub type HummockResult<T> = std::result::Result<T, HummockError>;
