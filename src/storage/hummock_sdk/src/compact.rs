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

use risingwave_pb::hummock::{CompactTask, SstableInfo};

pub fn compact_task_to_string(compact_task: &CompactTask) -> String {
    use std::fmt::Write;

    let mut s = String::new();
    writeln!(
        s,
        "Compaction task id: {:?}, target level: {:?}",
        compact_task.task_id, compact_task.target_level
    )
    .unwrap();
    writeln!(s, "Compaction watermark: {:?} ", compact_task.watermark).unwrap();
    writeln!(
        s,
        "Compaction target_file_size: {:?} ",
        compact_task.target_file_size
    )
    .unwrap();
    writeln!(s, "Compaction # splits: {:?} ", compact_task.splits.len()).unwrap();
    writeln!(s, "Compaction task status: {:?} ", compact_task.task_status).unwrap();
    s.push_str("Compaction Sstables structure: \n");
    for level_entry in &compact_task.input_ssts {
        let tables: Vec<String> = level_entry
            .table_infos
            .iter()
            .map(|table| format!("[id: {}, {}KB]", table.id, table.file_size / 1024))
            .collect();
        writeln!(s, "Level {:?} {:?} ", level_entry.level_idx, tables).unwrap();
    }
    s.push_str("Compaction task output: \n");
    for sst in &compact_task.sorted_output_ssts {
        append_sstable_info_to_string(&mut s, sst);
    }
    s
}

pub fn append_sstable_info_to_string(s: &mut String, sstable_info: &SstableInfo) {
    use std::fmt::Write;

    let key_range = sstable_info.key_range.as_ref().unwrap();
    let key_range_str = if key_range.inf {
        "(-inf, +inf)".to_owned()
    } else {
        format!(
            "[{}, {}]",
            hex::encode(key_range.left.as_slice()),
            hex::encode(key_range.right.as_slice())
        )
    };
    writeln!(
        s,
        "SstableInfo: id={:?}, KeyRange={:?}, size={:?}",
        sstable_info.id, key_range_str, sstable_info.file_size
    )
    .unwrap();
}
