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

use std::collections::HashMap;
use std::env;
use std::fmt::Write;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use console::style;
use indicatif::{MultiProgress, ProgressBar};
use risedev::util::{complete_spin, fail_spin};
use risedev::{
    compute_risectl_env, preflight_check, AwsS3Config, CompactorService, ComputeNodeService,
    ConfigExpander, ConfigureTmuxTask, EnsureStopService, ExecuteContext, FrontendService,
    GrafanaService, JaegerService, KafkaService, MetaNodeService, MinioService, PrometheusService,
    RedisService, ServiceConfig, Task, ZooKeeperService, RISEDEV_SESSION_NAME,
};
use tempfile::tempdir;
use yaml_rust::YamlEmitter;

#[derive(Default)]
pub struct ProgressManager {
    mp: Arc<MultiProgress>,
    pa: Vec<ProgressBar>,
    insert: Option<usize>,
}

impl ProgressManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new progress bar from task
    pub fn new_progress(&mut self) -> ProgressBar {
        let pb = risedev::util::new_spinner();
        if let Some(ref mut insert) = self.insert {
            self.mp.insert(*insert, pb.clone());
            *insert += 1;
        } else {
            self.mp.add(pb.clone());
            self.insert = Some(0);
        }
        self.pa.push(pb.clone());
        pb.enable_steady_tick(100);
        pb
    }

    /// Finish all progress bars.
    pub fn finish_all(&self) {
        for p in &self.pa {
            p.finish();
        }
    }

    pub fn spawn(&self) -> JoinHandle<anyhow::Result<()>> {
        let mp = self.mp.clone();
        std::thread::spawn(move || mp.join().map_err(|err| err.into()))
    }
}

fn task_main(
    manager: &mut ProgressManager,
    steps: &[String],
    services: &HashMap<String, ServiceConfig>,
) -> Result<(Vec<(String, Duration)>, String)> {
    let log_path = env::var("PREFIX_LOG")?;

    let mut logger = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(Path::new(&log_path).join("risedev.log"))?;

    let status_dir = Arc::new(tempdir()?);

    let mut log_buffer = String::new();

    // Start Tmux and kill previous services
    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = ConfigureTmuxTask::new()?;
        service.execute(&mut ctx)?;

        writeln!(
            log_buffer,
            "* Run {} to attach to the tmux console.",
            style(format!("tmux a -t {}", RISEDEV_SESSION_NAME))
                .blue()
                .bold()
        )?;
    }

    // Firstly, ensure that all ports needed is not occupied by previous runs.
    let mut ports = vec![];

    for step in steps {
        let service = services.get(step).unwrap();
        let listen_info = match service {
            ServiceConfig::Minio(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Etcd(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Prometheus(c) => Some((c.port, c.id.clone())),
            ServiceConfig::ComputeNode(c) => Some((c.port, c.id.clone())),
            ServiceConfig::MetaNode(c) => Some((c.port, c.id.clone())),
            ServiceConfig::FrontendV2(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Compactor(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Grafana(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Jaeger(c) => Some((c.dashboard_port, c.id.clone())),
            ServiceConfig::Kafka(c) => Some((c.port, c.id.clone())),
            ServiceConfig::Redis(c) => Some((c.port, c.id.clone())),
            ServiceConfig::ZooKeeper(c) => Some((c.port, c.id.clone())),
            ServiceConfig::AwsS3(_) => None,
            ServiceConfig::RedPanda(_) => None,
        };

        if let Some(x) = listen_info {
            ports.push(x);
        }
    }

    {
        let mut ctx = ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
        let mut service = EnsureStopService::new(ports)?;
        service.execute(&mut ctx)?;
    }

    // Then, start services one by one

    let mut stat = vec![];

    for step in steps {
        let service = services.get(step).unwrap();
        let start_time = Instant::now();

        match service {
            ServiceConfig::Minio(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MinioService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = risedev::ConfigureMinioTask::new(c.clone())?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::Etcd(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = risedev::EtcdService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task = risedev::EtcdReadyCheckTask::new(c.clone())?;
                task.execute(&mut ctx)?;
            }
            ServiceConfig::Prometheus(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = PrometheusService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api http://{}:{}/", c.address, c.port));
            }
            ServiceConfig::ComputeNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = ComputeNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;

                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api grpc://{}:{}/", c.address, c.port));
            }
            ServiceConfig::MetaNode(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = MetaNodeService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb.set_message(format!(
                    "api grpc://{}:{}/, dashboard http://{}:{}/",
                    c.address, c.port, c.address, c.dashboard_port
                ));
            }
            ServiceConfig::FrontendV2(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = FrontendService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("api postgres://{}:{}/", c.address, c.port));

                writeln!(
                    log_buffer,
                    "* Run {} to start Postgres interactive shell.",
                    style(format!("psql -h localhost -p {} -d dev -U root", c.port))
                        .blue()
                        .bold()
                )?;
            }
            ServiceConfig::Compactor(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = CompactorService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, c.user_managed)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("compactor {}:{}", c.address, c.port));
            }
            ServiceConfig::Grafana(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = GrafanaService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("dashboard http://{}:{}/", c.address, c.port));
            }
            ServiceConfig::Jaeger(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = JaegerService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::ConfigureGrpcNodeTask::new(
                    c.dashboard_address.clone(),
                    c.dashboard_port,
                    false,
                )?;
                task.execute(&mut ctx)?;
                ctx.pb.set_message(format!(
                    "dashboard http://{}:{}/",
                    c.dashboard_address, c.dashboard_port
                ));
            }
            ServiceConfig::AwsS3(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());

                struct AwsService(AwsS3Config);
                impl Task for AwsService {
                    fn execute(
                        &mut self,
                        _ctx: &mut ExecuteContext<impl std::io::Write>,
                    ) -> anyhow::Result<()> {
                        Ok(())
                    }

                    fn id(&self) -> String {
                        self.0.id.clone()
                    }
                }

                ctx.service(&AwsService(c.clone()));
                ctx.complete_spin();
                ctx.pb
                    .set_message(format!("using AWS s3 bucket {}", c.bucket));
            }
            ServiceConfig::ZooKeeper(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = ZooKeeperService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task =
                    risedev::ConfigureGrpcNodeTask::new(c.address.clone(), c.port, false)?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("zookeeper {}:{}", c.address, c.port));
            }
            ServiceConfig::Kafka(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = KafkaService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::KafkaReadyCheckTask::new(c.clone())?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("kafka {}:{}", c.address, c.port));
            }
            ServiceConfig::RedPanda(_) => {
                return Err(anyhow!("redpanda is only supported in RiseDev compose."));
            }
            ServiceConfig::Redis(c) => {
                let mut ctx =
                    ExecuteContext::new(&mut logger, manager.new_progress(), status_dir.clone());
                let mut service = RedisService::new(c.clone())?;
                service.execute(&mut ctx)?;
                let mut task = risedev::RedisReadyCheckTask::new(c.clone())?;
                task.execute(&mut ctx)?;
                ctx.pb
                    .set_message(format!("redis {}:{}", c.address, c.port));
            }
        }

        let service_id = service.id().to_string();
        let duration = Instant::now() - start_time;
        stat.push((service_id, duration));
    }

    Ok((stat, log_buffer))
}

fn main() -> Result<()> {
    let risedev_config = {
        let mut content = String::new();
        File::open("risedev.yml")?.read_to_string(&mut content)?;
        content
    };

    let task_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "playground".to_string());

    let risedev_config = ConfigExpander::expand(&risedev_config, &task_name)?;
    {
        let mut out_str = String::new();
        let mut emitter = YamlEmitter::new(&mut out_str);
        emitter.dump(&risedev_config)?;
        std::fs::write(
            Path::new(&env::var("PREFIX_CONFIG")?).join("risedev-expanded.yml"),
            &out_str,
        )?;
    }

    preflight_check()?;

    let (steps, services) = ConfigExpander::select(&risedev_config, &task_name)?;

    let mut manager = ProgressManager::new();
    // Always create a progress before calling `task_main`. Otherwise the progress bar won't be
    // shown.
    let p = manager.new_progress();
    p.set_prefix("playground");
    p.set_message(format!(
        "starting {} services for {}...",
        steps.len(),
        task_name
    ));
    let join_handle = manager.spawn();
    let task_result = task_main(&mut manager, &steps, &services);

    match task_result {
        Ok(_) => {
            p.set_message(format!(
                "done bootstrapping with config {}",
                style(task_name).bold()
            ));
            complete_spin(&p);
        }
        Err(_) => {
            p.set_message(format!(
                "failed to bootstrap with config {}",
                style(task_name).bold()
            ));
            fail_spin(&p);
        }
    }
    manager.finish_all();
    join_handle.join().unwrap()?;

    match task_result {
        Ok((stat, log_buffer)) => {
            println!("---- summary of startup time ----");
            for (task_name, duration) in stat {
                println!("{}: {:.2}s", task_name, duration.as_secs_f64());
            }
            println!("-------------------------------");
            println!();

            let risectl_env = match compute_risectl_env(&services) {
                Ok(x) => x,
                Err(_) => "".into(),
            };

            std::fs::write(
                Path::new(&env::var("PREFIX_CONFIG")?).join("risectl-env"),
                &risectl_env,
            )?;

            println!("All services started successfully.");

            print!("{}", log_buffer);

            println!(
                "* You may find logs using {} command",
                style("./risedev l").blue().bold()
            );

            println!(
                "* Run {} to kill cluster.",
                style("./risedev k").blue().bold()
            );

            println!(
                "* Run {} to run `risedev` anywhere!",
                style("./risedev install").blue().bold()
            );

            Ok(())
        }
        Err(err) => {
            println!(
                "{} - Failed to start: {}\nCaused by:\n\t{}",
                style("ERROR").red().bold(),
                err,
                err.root_cause().to_string().trim(),
            );
            println!(
                "* Use `{}` to enable new compoenents, if they are missing.",
                style("./risedev configure").blue().bold(),
            );
            println!(
                "* Use `{}` to view logs, or visit `{}`",
                style("./risedev l").blue().bold(),
                env::var("PREFIX_LOG")?
            );
            println!(
                "* Run `{}` to clean up cluster.",
                style("./risedev k").blue().bold()
            );
            println!(
                "* Run `{}` to clean data, which might potentially fix the issue.",
                style("./risedev clean-data").blue().bold()
            );
            println!("---");
            println!();
            println!();

            Err(err)
        }
    }
}
