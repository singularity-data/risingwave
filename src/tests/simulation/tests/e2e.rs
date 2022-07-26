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

#![cfg(madsim)]

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

#[madsim::test]
async fn basic() {
    let handle = madsim::runtime::Handle::current();

    // meta node
    handle
        .create_node()
        .name("meta")
        .ip("192.168.1.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_meta::MetaNodeOpts::parse_from([
                "meta-node",
                "--listen-addr",
                "0.0.0.0:5690",
            ]);
            risingwave_meta::start(opts).await
        })
        .build();
    // wait for the service to be ready
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // frontend node
    handle
        .create_node()
        .name("frontend")
        .ip("192.168.2.1".parse().unwrap())
        .init(|| async {
            let opts = risingwave_frontend::FrontendOpts::parse_from([
                "frontend-node",
                "--host",
                "0.0.0.0:4566",
                "--client-address",
                "192.168.2.1:4566",
                "--meta-addr",
                "192.168.1.1:5690",
            ]);
            risingwave_frontend::start(opts).await
        })
        .build();

    // compute node
    for i in 1..=3 {
        handle
            .create_node()
            .name(format!("compute-{i}"))
            .ip([192, 168, 3, i].into())
            .init(move || async move {
                let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                    "compute-node",
                    "--host",
                    "0.0.0.0:5688",
                    "--client-address",
                    &format!("192.168.3.{i}:5688"),
                    "--meta-address",
                    "192.168.1.1:5690",
                ]);
                risingwave_compute::start(opts).await
            })
            .build();
    }
    // wait for the service to be ready
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // client
    handle
        .create_node()
        .name("client")
        .ip("192.168.100.1".parse().unwrap())
        .build()
        .spawn(async {
            let (client, connection) = tokio_postgres::Config::new()
                .host("192.168.2.1")
                .port(4566)
                .dbname("dev")
                .user("root")
                .connect_timeout(Duration::from_secs(5))
                .connect(tokio_postgres::NoTls)
                .await
                .expect("Failed to connect to database");
            tokio::spawn(async move {
                connection.await.expect("Postgres connection error");
            });
            let mut tester = sqllogictest::Runner::new(Postgres {
                client: Arc::new(client),
            });
            // run the following e2e tests
            // for dir in ["ddl", "batch", "streaming", "streaming_delta_join"] {
            //     let files = glob::glob(&format!("../../../e2e_test/{dir}/**/*.slt"))
            //         .expect("failed to read glob pattern");
            //     for file in files {
            //         tester
            //             .run_file_async(file.unwrap().as_path())
            //             .await
            //             .unwrap();
            //     }
            // }
            tester
                .run_file_async("../../../e2e_test/batch/local_mode.slt")
                .await
                .unwrap();
        })
        .await
        .unwrap();
}

#[derive(Clone)]
struct Postgres {
    client: Arc<tokio_postgres::Client>,
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Postgres {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        use std::fmt::Write;

        let mut output = String::new();
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) if v.is_empty() => write!(output, "(empty)").unwrap(),
                            Some(v) => write!(output, "{}", v).unwrap(),
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    fn engine_name(&self) -> &str {
        "risingwave"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
