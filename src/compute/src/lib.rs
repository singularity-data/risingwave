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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::unused_async)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![deny(unused_must_use)]
#![deny(rustdoc::broken_intra_doc_links)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(generic_associated_types)]
#![feature(let_else)]
#![cfg_attr(coverage, feature(no_coverage))]

#[macro_use]
extern crate log;

pub mod compute_observer;
pub mod rpc;
pub mod server;

use clap::Parser;

/// Command-line arguments for compute-node.
#[derive(Parser, Debug)]
pub struct ComputeNodeOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, default_value = "127.0.0.1:5688")]
    pub host: String,

    // Optional, we will use listen_address if not specified.
    #[clap(long)]
    pub client_address: Option<String>,

    // TODO: This is currently unused.
    #[clap(long)]
    pub port: Option<u16>,

    #[clap(long, default_value = "hummock+memory")]
    pub state_store: String,

    #[clap(long, default_value = "127.0.0.1:1222")]
    pub prometheus_listener_addr: String,

    #[clap(long, default_value = "0")]
    pub metrics_level: u32,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,

    /// URI for tiered cache.
    /// "none://" and "file://<path>" are supported.
    /// Note: Currently, "file://<path>" is only supported on linux.
    /// Setting "file://<path>" on other targets will be ignored.
    #[clap(long, default_value = "none://")]
    pub tiered_cache_uri: String,

    /// Enable reporting tracing information to jaeger
    #[clap(long)]
    pub enable_jaeger_tracing: bool,
}

use std::future::Future;
use std::pin::Pin;

use crate::server::compute_node_serve;

/// Start compute node
pub fn start(opts: ComputeNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("meta address: {}", opts.meta_address.clone());

        let listen_address = opts.host.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_address);

        let client_address = opts
            .client_address
            .as_ref()
            .unwrap_or(&opts.host)
            .parse()
            .unwrap();
        tracing::info!("Client address is {}", client_address);

        let (join_handle_vec, _shutdown_send) =
            compute_node_serve(listen_address, client_address, opts).await;

        for join_handle in join_handle_vec {
            join_handle.await.unwrap();
        }
    })
}
