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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("nix error: {0}")]
    NixError(#[from] nix::errno::Errno),
    #[error("unsupported file system, super block magic: {0}")]
    UnsupportedFilesystem(i64),
    #[error("invalid slot: {0}")]
    InvalidSlot(usize),
    #[error("other error: {0}")]
    Other(String),
}

pub type Result<T> = core::result::Result<T, Error>;
