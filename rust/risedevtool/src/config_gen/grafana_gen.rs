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
//
use std::path::Path;
use std::{env, fs};

use anyhow::{anyhow, Result};

use crate::GrafanaConfig;
pub struct GrafanaGen;

impl GrafanaGen {
    pub fn gen_custom_ini(&self, config: &GrafanaConfig) -> String {
        let grafana_host = &config.address;
        let grafana_port = config.port;

        format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
[server]
http_addr = {grafana_host}
http_port = {grafana_port}

[users]
default_theme = light

[auth.anonymous]
enabled = true
org_role = Admin
    "#
        )
    }

    pub fn gen_datasource_yml(&self, config: &GrafanaConfig) -> Result<String> {
        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        if provide_prometheus.len() != 1 {
            return Err(anyhow!(
                "expect 1 prometheus nodes, found {}",
                provide_prometheus.len()
            ));
        }
        let prometheus = &provide_prometheus[0];
        let prometheus_host = &prometheus.address;
        let prometheus_port = prometheus.port;

        let yml = format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
apiVersion: 1
deleteDatasources:
  - name: risedev-prometheus
datasources:
  - name: risedev-prometheus
    type: prometheus
    access: proxy
    url: http://{prometheus_host}:{prometheus_port}
    withCredentials: false
    isDefault: false
    tlsAuth: false
    tlsAuthWithCACert: false
    version: 1
    editable: true
    isDefault: true
    "#,
        );
        Ok(yml)
    }

    pub fn gen_dashboard_yml(&self, config: &GrafanaConfig) -> Result<String> {
        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        if provide_prometheus.len() != 1 {
            return Err(anyhow!(
                "expect 1 prometheus nodes, found {}",
                provide_prometheus.len()
            ));
        };

        let prefix_bin = env::var("PREFIX_CONFIG")?;
        let dashboard_path = Path::new(&prefix_bin)
            .join("grafana_dashboard.json")
            .to_string_lossy()
            .to_string();

        let json_context = fs::read_to_string(&dashboard_path)?;
        let json_context = json_context.replace("${DS_RISEDEV-PROMETHEUS}", "risedev-prometheus");
        fs::write(&dashboard_path, json_context)?;
        let yml = format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
apiVersion: 1

providers:
  - name: 'risingwave-grafana'
    orgId: 1
    folder: ''
    folderUid: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 1
    allowUiUpdates: true
    options:
      path: {dashboard_path}
      foldersFromFilesStructure: false
    "#,
        );
        Ok(yml)
    }

    pub fn gen_s3_dashboard_yml(&self, config: &GrafanaConfig) -> Result<String> {
        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        if provide_prometheus.len() != 1 {
            return Err(anyhow!(
                "expect 1 prometheus nodes, found {}",
                provide_prometheus.len()
            ));
        };

        let prefix_bin = env::var("PREFIX_CONFIG")?;
        let s3_dashboard_path = Path::new(&prefix_bin)
            .join("aws-s3.json")
            .to_string_lossy()
            .to_string();

        let json_context = fs::read_to_string(&s3_dashboard_path)?;
        fs::write(&s3_dashboard_path, json_context)?;
        let yml = format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
apiVersion: 1

providers:
  - name: 's3-grafana'
    orgId: 1
    folder: ''
    folderUid: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 1
    allowUiUpdates: true
    options:
      path: {s3_dashboard_path}
      foldersFromFilesStructure: false
    "#,
        );
        Ok(yml)
    }
}
