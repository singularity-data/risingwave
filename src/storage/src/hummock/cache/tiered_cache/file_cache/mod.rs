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

pub mod alloc;
pub mod buffer;
pub mod cache;
pub mod coding;
pub mod error;
pub mod file;
pub mod meta;
pub mod store;
pub mod utils;

#[cfg(test)]
pub mod test_utils;

async fn asyncify<F, T>(f: F) -> error::Result<T>
where
    F: FnOnce() -> error::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(error::Error::Other("background task failed".to_string())),
    }
}

/// The logical block size of the underlying storage (typically 512 bytes).
///
/// Can be determined using `ioctl(2)` `BLKSSZGET` operation or from the sheel using the command:
///
/// ```bash
///     blockdev --getss
/// ```
///
/// For more details, see man open(2) NOTES section.
const LOGICAL_BLOCK_SIZE: usize = 512;
/// Size of `st_blocks` with `fstat(2)`.
const ST_BLOCK_SIZE: usize = 512;

const LRU_SHARD_BITS: usize = 5;

type DioBuffer = Vec<u8, &'static alloc::AlignedAllocator<LOGICAL_BLOCK_SIZE>>;

static DIO_BUFFER_ALLOCATOR: alloc::AlignedAllocator<LOGICAL_BLOCK_SIZE> =
    alloc::AlignedAllocator::<LOGICAL_BLOCK_SIZE>;
