use std::mem::swap;

use futures::stream::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;

use crate::executor::executor2_wrapper::WrapperState::{Closed, Created, Opened};
use crate::executor::Executor;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, ExecutorInfo};

enum WrapperState {
    Created { executor: BoxedExecutor2 },
    Opened { stream: BoxedDataChunkStream },
    Closed,
}

/// Wrap an `Executor2` instance to convert it into an `Executor`.
pub struct Executor2Wrapper {
    info: ExecutorInfo,
    state: WrapperState,
}

#[async_trait::async_trait]
impl Executor for Executor2Wrapper {
    async fn open(&mut self) -> Result<()> {
        let mut tmp = WrapperState::Closed;
        swap(&mut tmp, &mut self.state);
        match tmp {
            Created { executor } => {
                self.state = Opened {
                    stream: executor.execute(),
                };
                Ok(())
            }
            _ => Err(InternalError("Executor already opened!".to_string()).into()),
        }
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        match &mut self.state {
            Opened { ref mut stream } => match stream.next().await {
                Some(r) => Ok(Some(r?)),
                None => Ok(None),
            },
            _ => Err(InternalError("Executor not in open state!".to_string()).into()),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.state = Closed;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn identity(&self) -> &str {
        self.info.id.as_str()
    }
}

impl From<BoxedExecutor2> for Executor2Wrapper {
    fn from(executor2: BoxedExecutor2) -> Self {
        let executor_info = ExecutorInfo {
            schema: executor2.schema().to_owned(),
            id: executor2.identity().to_string(),
        };

        Self {
            info: executor_info,
            state: WrapperState::Created {
                executor: executor2,
            },
        }
    }
}
