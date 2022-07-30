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

use std::collections::{BTreeMap, HashMap};

use comfy_table::{Attribute, Cell, Row, Table};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::meta::GetClusterInfoResponse;

use crate::common::MetaServiceOpts;

pub async fn cluster_info() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments,
        actor_splits: _,
        stream_source_infos: _,
    } = meta_client.get_cluster_info().await?;

    // Fragment ID -> [Parallel Unit ID -> (Parallel Unit, Actor)]
    let mut fragments = BTreeMap::new();

    for table_fragment in &table_fragments {
        for (&id, fragment) in &table_fragment.fragments {
            for actor in &fragment.actors {
                let parallel_unit = table_fragment
                    .actor_status
                    .get(&actor.actor_id)
                    .unwrap()
                    .get_parallel_unit()
                    .unwrap();
                fragments
                    .entry(id)
                    .or_insert_with(HashMap::new)
                    .insert(parallel_unit.id, (parallel_unit, actor));
            }
        }
    }

    // Parallel Unit ID -> Worker Node
    let all_parallel_units: BTreeMap<_, _> = worker_nodes
        .iter()
        .flat_map(|worker_node| {
            worker_node
                .parallel_units
                .iter()
                .map(|parallel_unit| (parallel_unit.id, worker_node.clone()))
        })
        .collect();

    let mut table = Table::new();

    // Compute Node, Parallel Unit, Frag 1, Frag 2, ..., Frag N
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Compute Node".into());
        row.add_cell("Parallel Unit".into());
        for f in fragments.keys() {
            row.add_cell(format!("Frag {f}").into());
        }
        row
    });

    let mut last_worker_id = None;
    for (pu, worker) in all_parallel_units {
        // Compute Node, Parallel Unit, Actor 1, Actor 11, -, ..., Actor N
        let mut row = Row::new();
        row.add_cell(if last_worker_id == Some(worker.id) {
            "".into()
        } else {
            last_worker_id = Some(worker.id);
            Cell::new(format!(
                "{}@{}",
                worker.id,
                HostAddr::from(worker.get_host().unwrap())
            ))
            .add_attribute(Attribute::Bold)
        });
        row.add_cell(pu.into());
        for f in fragments.values() {
            let cell = if let Some((_pu, actor)) = f.get(&pu) {
                actor.actor_id.into()
            } else {
                "-".into()
            };
            row.add_cell(cell);
        }
        table.add_row(row);
    }

    println!("{table}");

    Ok(())
}
