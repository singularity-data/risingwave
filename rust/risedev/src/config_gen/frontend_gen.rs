use itertools::Itertools;

use crate::FrontendConfig;

pub struct FrontendGen;

impl FrontendGen {
    pub fn gen_server_properties(&self, config: &FrontendConfig) -> String {
        let frontend_host = &config.address;
        let frontend_port = config.port;
        let meta_node_hosts = config
            .provide_meta_node
            .as_ref()
            .unwrap()
            .iter()
            .map(|node| format!("{}:{}", node.address, node.port))
            .join(",");

        format!(
            r#"# --- THIS FILE IS AUTO GENERATED BY RISEDEV ---
risingwave.pgserver.ip={frontend_host}
risingwave.pgserver.port={frontend_port}
risingwave.leader.clustermode=Distributed

risingwave.catalog.mode=Remote
risingwave.meta.node={meta_node_hosts}
"#
        )
    }
}
