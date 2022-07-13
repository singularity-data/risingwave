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

use risingwave_common::array::ArrayError;
use risingwave_common::error::{BoxedError, Error, ErrorCode, RwError, TrackingIssue};
use risingwave_expr::ExprError;
use risingwave_storage::error::StorageError;

use super::Barrier;

#[derive(thiserror::Error, Debug)]
enum StreamExecutorErrorInner {
    #[error("Storage error: {0}")]
    Storage(
        #[backtrace]
        #[source]
        StorageError,
    ),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Chunk operation error: {0}")]
    EvalError(BoxedError),

    // TODO: remove this after state table is fully used
    #[error("Serialize/deserialize error: {0}")]
    SerdeError(BoxedError),

    #[error("Sink error: {0}")]
    SinkError(RwError),

    #[error("Channel `{0}` closed")]
    ChannelClosed(String),

    #[error("Failed to align barrier: expected {0:?} but got {1:?}")]
    AlignBarrier(Box<Barrier>, Box<Barrier>),

    #[error("Feature is not yet implemented: {0}, {1}")]
    NotImplemented(String, TrackingIssue),

    #[error(transparent)]
    Internal(anyhow::Error),
}

impl StreamExecutorError {
    pub fn storage(error: impl Into<StorageError>) -> Self {
        StreamExecutorErrorInner::Storage(error.into()).into()
    }

    pub fn eval_error(error: impl Error) -> Self {
        StreamExecutorErrorInner::EvalError(error.into()).into()
    }

    pub fn serde_error(error: impl Error) -> Self {
        StreamExecutorErrorInner::SerdeError(error.into()).into()
    }

    pub fn sink_error(error: impl Into<RwError>) -> Self {
        StreamExecutorErrorInner::SinkError(error.into()).into()
    }

    pub fn channel_closed(name: impl Into<String>) -> Self {
        StreamExecutorErrorInner::ChannelClosed(name.into()).into()
    }

    pub fn align_barrier(expected: Barrier, received: Barrier) -> Self {
        StreamExecutorErrorInner::AlignBarrier(expected.into(), received.into()).into()
    }

    pub fn invalid_argument(error: impl Into<String>) -> Self {
        StreamExecutorErrorInner::InvalidArgument(error.into()).into()
    }

    pub fn not_implemented(error: impl Into<String>, issue: impl Into<TrackingIssue>) -> Self {
        StreamExecutorErrorInner::NotImplemented(error.into(), issue.into()).into()
    }
}

#[derive(thiserror::Error)]
#[error("{inner}")]
pub struct StreamExecutorError {
    #[from]
    inner: StreamExecutorErrorInner,
    backtrace: Backtrace,
}

impl std::fmt::Debug for StreamExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::error::Error;

        write!(f, "{}", self.inner)?;
        writeln!(f)?;
        if let Some(backtrace) = self.inner.backtrace() {
            write!(f, "  backtrace of inner error:\n{}", backtrace)?;
        } else {
            write!(
                f,
                "  backtrace of `StreamExecutorError`:\n{}",
                self.backtrace
            )?;
        }
        Ok(())
    }
}

/// Storage error.
impl From<StorageError> for StreamExecutorError {
    fn from(s: StorageError) -> Self {
        Self::storage(s)
    }
}

// Chunk operation error.
impl From<ArrayError> for StreamExecutorError {
    fn from(e: ArrayError) -> Self {
        Self::eval_error(e)
    }
}
impl From<ExprError> for StreamExecutorError {
    fn from(e: ExprError) -> Self {
        Self::eval_error(e)
    }
}

/// Internal error.
impl From<anyhow::Error> for StreamExecutorError {
    fn from(a: anyhow::Error) -> Self {
        StreamExecutorErrorInner::Internal(a).into()
    }
}

/// Serialize/deserialize error.
impl From<memcomparable::Error> for StreamExecutorError {
    fn from(m: memcomparable::Error) -> Self {
        Self::serde_error(m)
    }
}

/// Always convert [`StreamExecutorError`] to stream error variant of [`RwError`].
impl From<StreamExecutorError> for RwError {
    fn from(h: StreamExecutorError) -> Self {
        ErrorCode::StreamError(h.into()).into()
    }
}

pub type StreamExecutorResult<T> = std::result::Result<T, StreamExecutorError>;

#[cfg(test)]
mod tests {
    use risingwave_common::bail;

    use super::*;

    fn func_return_error() -> StreamExecutorResult<()> {
        bail!("test_error")
    }

    #[test]
    #[should_panic]
    #[ignore]
    fn executor_error_ui_test_1() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().unwrap();
    }

    #[test]
    #[ignore]
    fn executor_error_ui_test_2() {
        // For this test, ensure that we have only one backtrace from error when panic.
        func_return_error().map_err(|e| println!("{:?}", e)).ok();
    }
}
