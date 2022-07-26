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

#![feature(lint_reasons)]

mod version_cmp;

use risingwave_pb::hummock::{SstableId, SstableInfo};
pub use version_cmp::*;

use crate::compaction_group::hummock_version_ext::SstableIdExt;

pub mod compact;
pub mod compaction_group;
pub mod key;
pub mod key_range;
pub mod prost_key_range;
pub mod slice_transform;

pub type HummockSstableId = u128;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub type HummockCompactionTaskId = u64;
pub type CompactionGroupId = u64;
pub const INVALID_VERSION_ID: HummockVersionId = 0;
pub const FIRST_VERSION_ID: HummockVersionId = 1;

pub type LocalSstableInfo = (CompactionGroupId, SstableInfo);

const INVALID_NODE_ID: u64 = u64::MAX;
pub fn get_local_sst_id(seq_id: u64) -> HummockSstableId {
    SstableId {
        node_id: INVALID_NODE_ID,
        seq_id,
    }
    .as_int()
}

pub fn is_remote_sst_id(id: HummockSstableId) -> bool {
    SstableId::from_int(id).node_id != INVALID_NODE_ID
}

pub fn get_sst_id_hash(id: HummockSstableId) -> u64 {
    (id % u64::MAX as HummockSstableId) as u64
}
