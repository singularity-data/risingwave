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

//! Generate docker compose yaml files for risedev components.

use std::collections::{BTreeMap, HashMap};
use std::process::Command;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::{
    CompactorConfig, CompactorService, ComputeNodeConfig, ComputeNodeService, FrontendConfig,
    FrontendService, MetaNodeConfig, MetaNodeService, MinioConfig, MinioService,
    PrometheusConfig, PrometheusGen, PrometheusService, RedPandaConfig,
};

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Default)]
pub struct ComposeService {
    pub image: String,
    pub command: Vec<String>,
    pub expose: Vec<String>,
    pub ports: Vec<String>,
    pub depends_on: Vec<String>,
    pub volumes: Vec<String>,
    pub entrypoint: Option<String>,
    pub environment: HashMap<String, String>,
    pub user: Option<String>,
    pub container_name: String,
    pub network_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComposeFile {
    pub version: String,
    pub services: BTreeMap<String, ComposeService>,
    pub volumes: BTreeMap<String, ComposeVolume>,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct DockerImageConfig {
    pub risingwave: String,
    pub compute_node: String,
    pub meta_node: String,
    pub compactor_node: String,
    pub frontend_node: String,
    pub prometheus: String,
    pub grafana: String,
    pub minio: String,
    pub redpanda: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ComposeVolume {
    pub external: bool,
}

/// Generate compose yaml files for a component.
pub trait Compose {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService>;
}

fn get_cmd_args(cmd: &Command, with_argv_0: bool) -> Result<Vec<String>> {
    let mut result = vec![];
    if with_argv_0 {
        result.push(
            cmd.get_program()
                .to_str()
                .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                .to_string(),
        );
    }
    for arg in cmd.get_args() {
        result.push(
            arg.to_str()
                .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                .to_string(),
        );
    }
    Ok(result)
}

fn get_cmd_envs(cmd: &Command) -> Result<HashMap<String, String>> {
    let mut result = HashMap::new();
    for (k, v) in cmd.get_envs() {
        let k = k
            .to_str()
            .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
            .to_string();
        let v = if let Some(v) = v {
            Some(
                v.to_str()
                    .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                    .to_string(),
            )
        } else {
            None
        };
        result.insert(k, v.unwrap_or_default());
    }
    Ok(result)
}

impl Compose for ComputeNodeConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("compute-node");
        ComputeNodeService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();
        let provide_minio = self.provide_minio.as_ref().unwrap();

        Ok(ComposeService {
            image: image.risingwave.clone(),
            command,
            expose: vec![self.port.to_string(), self.exporter_port.to_string()],
            depends_on: provide_meta_node
                .iter()
                .map(|x| x.id.clone())
                .chain(provide_minio.iter().map(|x| x.id.clone()))
                .collect(),
            ..Default::default()
        })
    }
}

impl Compose for MetaNodeConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("meta-node");
        MetaNodeService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        Ok(ComposeService {
            image: image.risingwave.clone(),
            command,
            expose: vec![
                self.port.to_string(),
                self.exporter_port.to_string(),
                self.dashboard_port.to_string(),
            ],
            ..Default::default()
        })
    }
}

impl Compose for FrontendConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("frontend-node");
        FrontendService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();

        Ok(ComposeService {
            image: image.risingwave.clone(),
            command,
            ports: vec![format!("{}:{}", self.port, self.port)],
            expose: vec![self.port.to_string()],
            depends_on: provide_meta_node.iter().map(|x| x.id.clone()).collect(),
            ..Default::default()
        })
    }
}

impl Compose for CompactorConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("compactor-node");
        CompactorService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();
        let provide_minio = self.provide_minio.as_ref().unwrap();

        Ok(ComposeService {
            image: image.risingwave.clone(),
            command,
            expose: vec![self.port.to_string(), self.exporter_port.to_string()],
            depends_on: provide_meta_node
                .iter()
                .map(|x| x.id.clone())
                .chain(provide_minio.iter().map(|x| x.id.clone()))
                .collect(),
            ..Default::default()
        })
    }
}

impl Compose for MinioConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("minio");
        MinioService::apply_command_args(&mut command, self)?;
        command.arg("/data");

        let env = get_cmd_envs(&command)?;
        let command = get_cmd_args(&command, false)?;
        let bucket_name = &self.hummock_bucket;

        let entrypoint = format!(
            r#"
/bin/sh -c '
set -e
mkdir -p "/data/{bucket_name}"
/usr/bin/docker-entrypoint.sh "$$0" "$$@"
'"#
        );

        Ok(ComposeService {
            image: image.minio.clone(),
            command,
            environment: env,
            entrypoint: Some(entrypoint),
            ports: vec![
                format!("{}:{}", self.port, self.port),
                format!("{}:{}", self.console_port, self.console_port),
            ],
            volumes: vec![format!("{}:/data", self.id)],
            expose: vec![self.port.to_string(), self.console_port.to_string()],
            ..Default::default()
        })
    }
}

impl Compose for RedPandaConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("redpanda");

        command.args(vec![
            "start",
            "--smp",
            "1",
            "--reserve-memory",
            "0M",
            "--overprovisioned",
            "--node-id",
            "0",
        ]);

        command.arg("--kafka-addr").arg(format!(
            "PLAINTEXT://0.0.0.0:{},OUTSIDE://0.0.0.0:{}",
            self.internal_port, self.outside_port
        ));

        command.arg("--advertise-kafka-addr").arg(format!(
            "PLAINTEXT://{}:{},OUTSIDE://localhost:{}",
            self.address, self.internal_port, self.outside_port
        ));

        let command = get_cmd_args(&command, true)?;

        Ok(ComposeService {
            image: image.redpanda.clone(),
            command,
            expose: vec![
                self.internal_port.to_string(),
                self.outside_port.to_string(),
            ],
            ports: vec![format!("{}:{}", self.outside_port, self.outside_port)],
            ..Default::default()
        })
    }
}

impl Compose for PrometheusConfig {
    fn compose(&self, image: &DockerImageConfig) -> Result<ComposeService> {
        let mut command = Command::new("prometheus");
        command
            .arg("--config.file=/etc/prometheus/prometheus.yml")
            .arg("--storage.tsdb.path=/prometheus")
            .arg("--web.console.libraries=/usr/share/prometheus/console_libraries")
            .arg("--web.console.templates=/usr/share/prometheus/consoles");
        PrometheusService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, false)?;

        let config = PrometheusGen.gen_prometheus_yml(self);

        let entrypoint = r#"
/bin/sh -c '
set -e
echo "$$PROMETHEUS_CONFIG" > /etc/prometheus/prometheus.yml
/bin/prometheus "$$0" "$$@"
'"#
        .to_string();

        Ok(ComposeService {
            image: image.prometheus.clone(),
            command,
            expose: vec![self.port.to_string()],
            ports: vec![format!("{}:{}", self.port, self.port)],
            volumes: vec![format!("{}:/prometheus", self.id)],
            environment: vec![("PROMETHEUS_CONFIG".to_string(), config)]
                .into_iter()
                .collect(),
            entrypoint: Some(entrypoint),
            ..Default::default()
        })
    }
}
