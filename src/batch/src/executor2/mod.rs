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

pub mod executor_wrapper;

mod delete;
mod filter;
mod generate_series;
mod generic_exchange;
mod hash_agg;
mod insert;
mod join;
mod limit;
pub mod monitor;
mod project;
mod row_seq_scan;
mod sort_agg;
mod stream_scan;
mod top_n;
mod trace;
mod values;

pub use delete::*;
pub use filter::*;
use futures::stream::BoxStream;
pub use generate_series::*;
pub use generic_exchange::*;
pub use hash_agg::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use monitor::*;
pub use project::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
pub use row_seq_scan::*;
pub use sort_agg::*;
pub use stream_scan::*;
pub use top_n::*;
pub use trace::*;
pub use values::*;

use crate::executor::executor2_wrapper::Executor2Wrapper;
use crate::executor::{BoxedExecutor, ExecutorBuilder};

pub type BoxedExecutor2 = Box<dyn Executor2>;
pub type BoxedDataChunkStream = BoxStream<'static, Result<DataChunk>>;

pub struct ExecutorInfo {
    pub schema: Schema,
    pub id: String,
}

/// Refactoring of `Executor` using `Stream`.
pub trait Executor2: Send + 'static {
    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Executes to return the data chunk stream.
    ///
    /// The implementation should guaranteed that each `DataChunk`'s cardinality is not zero.
    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor2`
/// from proto and global environment.
pub trait BoxedExecutor2Builder {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2>;

    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        Ok(Box::new(Executor2Wrapper::from(Self::new_boxed_executor2(
            source,
        )?)))
    }
}
