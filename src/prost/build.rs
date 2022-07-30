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

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    println!("cargo:rerun-if-changed={}", proto_dir);

    let proto_files = vec![
        "catalog",
        "common",
        "data",
        "ddl_service",
        "expr",
        "plan_common",
        "meta",
        "batch_plan",
        "task_service",
        "stream_plan",
        "stream_service",
        "hummock",
        "user",
        "source",
    ];
    let protos: Vec<String> = proto_files
        .iter()
        .map(|f| format!("{}/{}.proto", proto_dir, f))
        .collect();

    // Build protobuf structs.
    let out_dir = PathBuf::from("./src");
    let file_descriptor_set_path: PathBuf = out_dir.join("file_descriptor_set.bin");
    tonic_build::configure()
        .file_descriptor_set_path(file_descriptor_set_path.as_path())
        .compile_well_known_types(true)
        .type_attribute(".", "#[derive(prost_helpers::AnyPB)]")
        .type_attribute("node_body", "#[derive(enum_as_inner::EnumAsInner)]")
        .out_dir(out_dir.as_path())
        .compile(&protos, &[proto_dir.to_string()])
        .expect("Failed to compile grpc!");

    // Implement `serde::Serialize` on those structs.
    let descriptor_set = std::fs::read(file_descriptor_set_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .out_dir(out_dir.as_path())
        .build(&["."])
        .expect("Failed to compile serde");

    // Tweak the serde files so that they can be compiled in our project.
    // By adding a `use crate::module::*`
    let rewrite_files = proto_files;
    for serde_proto_file in &rewrite_files {
        let out_file = out_dir.join(format!("{}.serde.rs", serde_proto_file));
        let file_content = String::from_utf8(std::fs::read(&out_file)?)?;
        let module_path_id = serde_proto_file.replace('.', "::");
        std::fs::write(
            &out_file,
            format!("use crate::{}::*;\n{}", module_path_id, file_content),
        )?;
    }
    Ok(())
}
