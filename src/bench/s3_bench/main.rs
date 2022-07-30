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

#![feature(type_ascription)]
use std::collections::hash_map::{Entry, HashMap};
use std::ops::Div;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

extern crate bytesize;

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::output::UploadPartOutput;
use aws_sdk_s3::Client;
use aws_smithy_http::body::SdkBody;
use bytesize::ByteSize;
use clap::Parser;
use futures::stream::{self, StreamExt};
use futures::{future, Future, FutureExt};
use itertools::Itertools;
use log::debug;
use rand::{Rng, SeedableRng};
use risingwave_common::error::RwError;
use tokio::join;
use tokio::sync::RwLock;

// Avoid regenerate objs in the same size with `rand`.
#[derive(Default)]
struct ObjPool(HashMap<(ByteSize, String), Vec<u8>>);

#[derive(Default)]
struct Cost {
    /// total bytes
    bytes: usize,
    /// time to first byte
    ttfb: Duration,
    /// time to last byte
    ttlb: Duration,
    /// time to live
    ttl: Duration,
    /// time to per part
    part_ttls: Vec<Duration>,
}
#[derive(Default)]
struct Durations {
    min: Duration,
    max: Duration,
    avg: Duration,
    p50: Duration,
    p90: Duration,
    p99: Duration,
}
impl Durations {
    fn gen(mut durations: Vec<Duration>) -> Durations {
        durations.sort();
        Durations {
            min: durations.first().unwrap().to_owned(),
            max: durations.last().unwrap().to_owned(),
            avg: durations
                .iter()
                .sum::<Duration>()
                .div(durations.len() as u32),
            p50: durations
                .get((durations.len() as i32 / 2 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
            p90: durations
                .get((durations.len() as i32 / 10 * 9 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
            p99: durations
                .get((durations.len() as i32 / 100 * 99 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
        }
    }
}

impl ObjPool {
    fn obj(&mut self, key: (ByteSize, String)) -> Vec<u8> {
        match self.0.entry(key.clone()) {
            Entry::Occupied(o) => o.get().to_owned(),
            Entry::Vacant(v) => v.insert(gen_obj(key.0)).to_owned(),
        }
    }
}
fn gen_obj(size: ByteSize) -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let mut buffer: Vec<u8> = vec![0; size.as_u64() as usize];
    rng.fill(&mut buffer[..]);
    buffer
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type", content = "args")]
enum Case {
    Put {
        name: String,
        obj: String,
        #[serde(deserialize_with = "de_size_str")]
        size: ByteSize,
    },
    MultiPartUpload {
        name: String,
        obj: String,
        #[serde(deserialize_with = "de_size_str")]
        size: ByteSize,
        #[serde(deserialize_with = "de_size_str")]
        part: ByteSize,
    },
    Get {
        name: String,
        obj: String,
    },
    MultiPartGet {
        name: String,
        obj: String,
        part: Vec<i32>,
    },
    ByteRangeGet {
        name: String,
        obj: String,
        start: u64,
        end: u64,
    },
}

#[derive(Parser, Debug)]
pub struct Config {
    /// AWS S3 Bucket, either <BUCKET> should be given.
    #[clap(short, long)]
    bucket: String,

    /// AWS S3 Endpoint
    #[clap(short, long)]
    endpoint: Option<String>,

    /// Iter times per case.
    #[clap(short, long)]
    iter: usize,

    /// Path to cases toml file.
    #[clap(short, long)]
    path: String,

    /// Print tabular format.
    #[clap(short, long)]
    format: bool,

    /// Concurrent get and put
    #[clap(short, long)]
    multithread: bool,
}

fn read_cases(cfg: Arc<Config>) -> Vec<Case> {
    let data = std::fs::read_to_string(&cfg.path).unwrap();
    toml::from_str::<HashMap<String, Vec<Case>>>(&data)
        .unwrap()
        .remove("case")
        .unwrap()
}

async fn put(cfg: Arc<Config>, client: Arc<Client>, name: String, obj: Vec<u8>) -> Cost {
    let t = Instant::now();
    let bytes = obj.len();
    let ttfb = t.elapsed();
    client
        .put_object()
        .bucket(&cfg.bucket)
        .body(SdkBody::from(obj).into())
        .key(name)
        .send()
        .await
        .unwrap();
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn multi_part_upload(
    cfg: Arc<Config>,
    client: Arc<Client>,
    name: String,
    obj: Vec<u8>,
    part_size: ByteSize,
) -> Cost {
    let t = Instant::now();
    let bytes = obj.len();
    let bucket = cfg.bucket.clone();
    let rsp = client
        .create_multipart_upload()
        .bucket(bucket.clone())
        .key(name.clone())
        .send()
        .await
        .unwrap();
    let upload_id = rsp.upload_id.unwrap();
    let upload_id_clone = upload_id.clone();
    let name_clone = name.clone();
    let client_clone = client.clone();
    let upload_part_handle = move |part: Vec<u8>, part_number: i32| {
        client_clone
            .upload_part()
            .bucket(bucket.clone())
            .key(name_clone.clone())
            .upload_id(upload_id_clone.clone())
            .part_number(part_number)
            .body(SdkBody::from(part).into())
    };
    let chunks = obj
        .chunks(part_size.as_u64() as usize)
        .into_iter()
        .map(|chunk| chunk.to_owned())
        .collect_vec();

    let futures = chunks
        .into_iter()
        .enumerate()
        .map(|(i, part)| upload_part_handle(part, (i + 1) as i32))
        .map(|a| async move {
            let part_t = Instant::now();
            let result = a.send().await.unwrap();
            let part_ttl = part_t.elapsed();
            Ok((result, part_ttl)): Result<(UploadPartOutput, Duration), RwError>
        })
        .collect_vec();
    let ttfb = t.elapsed();
    let mut part_ttls = vec![];
    let completed_parts = future::try_join_all(futures)
        .await
        .unwrap()
        .into_iter()
        .map(|(result, part_ttl)| {
            part_ttls.push(part_ttl);
            result
        })
        .enumerate()
        .map(|(i, rsp)| {
            CompletedPart::builder()
                .set_e_tag(rsp.e_tag)
                .set_part_number(Some((i + 1) as i32))
                .build()
        })
        .collect_vec();
    let ttlb = t.elapsed();

    client
        .complete_multipart_upload()
        .bucket(cfg.bucket.clone())
        .key(name.clone())
        .upload_id(upload_id.clone())
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .unwrap();

    Cost {
        bytes,
        ttfb,
        ttlb,
        part_ttls,
        ..Default::default()
    }
}

async fn get(cfg: Arc<Config>, client: Arc<Client>, name: String) -> Cost {
    let t = Instant::now();
    let resp = client
        .get_object()
        .bucket(&cfg.bucket)
        .key(name)
        .send()
        .await
        .unwrap();
    let ttfb = t.elapsed();
    let bytes = resp.body.collect().await.unwrap().into_bytes().len();
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn multi_part_get(
    cfg: Arc<Config>,
    client: Arc<Client>,
    name: String,
    part_numbers: Vec<i32>,
) -> Cost {
    let t = Instant::now();
    let create_part_get = move |part_number| {
        client
            .get_object()
            .bucket(cfg.bucket.clone())
            .key(name.clone())
            .part_number(part_number)
            .send()
    };
    let futures = part_numbers
        .into_iter()
        .map(create_part_get)
        .map(|resp| async move {
            let result: Result<(usize, Duration), RwError> = Ok((
                resp.await
                    .unwrap()
                    .body
                    .collect()
                    .await
                    .unwrap()
                    .into_bytes()
                    .len(),
                t.elapsed(),
            ));
            result
        })
        .collect_vec();
    let (bytes, ttfb) = future::try_join_all(futures)
        .await
        .unwrap()
        .iter()
        .fold((0, Duration::MAX), |(total_bytes, ttfb), (bytes, time)| {
            (total_bytes + bytes, ttfb.min(*time))
        });
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn byte_range_get(
    cfg: Arc<Config>,
    client: Arc<Client>,
    name: String,
    start: u64,
    end: u64,
) -> Cost {
    let t = Instant::now();
    let resp = client
        .get_object()
        .bucket(cfg.bucket.clone())
        .key(name.clone())
        .range(format!("bytes={}-{}", start, end))
        .send()
        .await
        .unwrap();
    let ttfb = t.elapsed();
    let bytes = resp.body.collect().await.unwrap().into_bytes().len();
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn run_case(
    index: usize,
    case: Case,
    cfg: Arc<Config>,
    client: Arc<Client>,
    objs: Arc<RwLock<ObjPool>>,
) -> Result<(), RwError> {
    let (name, analysis) = match case.clone() {
        Case::Put {
            name,
            obj: obj_name,
            size: obj_size,
        } => {
            let obj = objs.write().await.obj((obj_size, obj_name.clone()));
            (
                name.to_owned(),
                iter_exec(|| put(cfg.clone(), client, obj_name, obj), cfg.iter).await,
            )
        }
        Case::MultiPartUpload {
            name,
            obj: obj_name,
            size: obj_size,
            part: part_size,
        } => {
            let obj = objs.write().await.obj((obj_size, obj_name.clone()));
            (
                name.to_owned(),
                iter_exec(
                    || multi_part_upload(cfg.clone(), client, obj_name, obj, part_size),
                    cfg.iter,
                )
                .await,
            )
        }
        Case::Get {
            name,
            obj: obj_name,
        } => (
            name.to_owned(),
            iter_exec(|| get(cfg.clone(), client, obj_name), cfg.iter).await,
        ),
        Case::MultiPartGet {
            name,
            obj: obj_name,
            part: part_numbers,
        } => (
            name.to_owned(),
            iter_exec(
                || multi_part_get(cfg.clone(), client, obj_name, part_numbers),
                cfg.iter,
            )
            .await,
        ),
        Case::ByteRangeGet {
            name,
            obj: obj_name,
            start,
            end,
        } => (
            name.to_owned(),
            iter_exec(
                || byte_range_get(cfg.clone(), client, obj_name, start, end),
                cfg.iter,
            )
            .await,
        ),
    };
    let d2s = |d: &Duration| match d.as_nanos() {
        0 => "-".to_owned(),
        _ => format!("{:.3?}", d),
    };
    let d2s_with_case = |case: &Case, d: &Duration| match case {
        Case::Put { .. } | Case::MultiPartGet { .. } => "-".to_owned(),
        _ => d2s(d),
    };
    let d2s_with_case_part = |case: &Case, d: &Duration| match case {
        Case::MultiPartUpload { .. } => d2s(d),
        _ => "-".to_owned(),
    };
    let data_with_name = [
        (
            "name".to_owned(),
            format!("{} ({} iters)", name, cfg.iter.clone()),
        ),
        ("bytes".to_owned(), analysis.bytes.to_string_as(true)),
        (
            "bandwidth".to_owned(),
            analysis.bandwidth.to_string_as(true),
        ),
        ("rtt-avg".to_owned(), d2s(&analysis.rtts.avg)),
        ("rtt-min".to_owned(), d2s(&analysis.rtts.min)),
        ("rtt-max".to_owned(), d2s(&analysis.rtts.max)),
        ("rtt-p50".to_owned(), d2s(&analysis.rtts.p50)),
        ("rtt-p90".to_owned(), d2s(&analysis.rtts.p90)),
        ("rtt-p99".to_owned(), d2s(&analysis.rtts.p99)),
        (
            "ttfb-avg".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.avg),
        ),
        (
            "ttfb-min".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.min),
        ),
        (
            "ttfb-max".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.max),
        ),
        (
            "ttfb-p50".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p50),
        ),
        (
            "ttfb-p90".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p90),
        ),
        (
            "ttfb-p99".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p99),
        ),
        (
            "rtt_part-avg".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.avg),
        ),
        (
            "rtt_part-min".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.min),
        ),
        (
            "rtt_part-max".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.max),
        ),
        (
            "rtt_part-p50".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.p50),
        ),
        (
            "rtt_part-p90".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.p90),
        ),
        (
            "rtt_part-p99".to_owned(),
            d2s_with_case_part(&case, &analysis.part_rtts.p99),
        ),
    ];
    if cfg.format {
        if index == 0 {
            let names = data_with_name
                .iter()
                .map(|(name, _)| name.to_owned())
                .collect_vec();
            println!("{}", names.join(","));
        }
        let data = data_with_name
            .iter()
            .map(|(_, item)| item.to_owned())
            .collect_vec();
        println!("{}", data.join(","));
        return Ok(());
    }
    for (name, item) in data_with_name {
        println!("{}: {}", name, item);
    }
    println!();
    Ok(())
}
async fn exec<F, FB>(fb: FB) -> Cost
where
    F: Future<Output = Cost>,
    FB: FnOnce() -> F + Clone,
{
    let start = Instant::now();
    let mut cost = fb().await;
    cost.ttl = start.elapsed();
    cost
}

fn de_size_str<'de, D>(deserializer: D) -> Result<ByteSize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringVisitor;

    impl<'de> serde::de::Visitor<'de> for StringVisitor {
        type Value = ByteSize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("ByteSize")
        }

        fn visit_str<E>(self, value: &str) -> Result<ByteSize, E>
        where
            E: serde::de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }
    }

    deserializer.deserialize_any(StringVisitor)
}

async fn iter_exec<F, FB>(fb: FB, iter_size: usize) -> Analysis
where
    F: Future<Output = Cost>,
    FB: FnOnce() -> F + Clone,
{
    let costs = stream::iter(0..iter_size)
        .flat_map(|_| exec(fb.clone()).into_stream())
        .collect::<Vec<_>>()
        .await;
    let bytes = ByteSize::b(costs[0].bytes as u64);
    let ttls = Durations::gen(costs.iter().map(|cost| cost.ttl).collect_vec());
    let ttfbs = Durations::gen(costs.iter().map(|cost| cost.ttfb).collect_vec());
    let part_rtts = if !costs[0].part_ttls.is_empty() {
        Durations::gen(
            costs
                .iter()
                .flat_map(|cost| cost.part_ttls.clone())
                .collect_vec(),
        )
    } else {
        Durations::default()
    };
    let data_transfer_secs = costs
        .iter()
        .map(|cost| (cost.ttlb - cost.ttfb).as_secs_f64())
        .collect_vec();
    let data_transfer_sec_avg =
        data_transfer_secs.iter().sum::<f64>() / data_transfer_secs.len() as f64;
    let bandwidth = ByteSize::b(((costs[0].bytes as f64) / data_transfer_sec_avg) as u64);
    Analysis {
        bytes,
        bandwidth,
        rtts: ttls,
        ttfbs,
        part_rtts,
    }
}

struct Analysis {
    bytes: ByteSize,
    bandwidth: ByteSize,
    rtts: Durations,
    ttfbs: Durations,
    part_rtts: Durations,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cfg = Arc::new(Config::parse());
    let shared_config = aws_config::load_from_env().await;
    let client = Arc::new(Client::new(&shared_config));

    let objs = Arc::new(RwLock::new(ObjPool::default()));

    let mut cases = read_cases(cfg.clone());

    if cfg.multithread {
        let mut features_put = vec![];
        let mut features_get = vec![];
        for (i, case) in cases.drain(..).enumerate() {
            debug!("running case: {:?}", case);
            let objs = objs.clone();
            if matches!(case, Case::Put { .. }) || matches!(case, Case::MultiPartUpload { .. }) {
                features_put.push(run_case(i, case, cfg.clone(), client.clone(), objs));
            } else {
                features_get.push(run_case(i, case, cfg.clone(), client.clone(), objs));
            }
        }
        let mut stream_put = tokio_stream::iter(features_put);
        let mut stream_get = tokio_stream::iter(features_get);
        let put_handle = tokio::spawn(async move {
            while let Some(a) = stream_put.next().await {
                a.await.unwrap();
            }
        });
        let get_handle = tokio::spawn(async move {
            while let Some(a) = stream_get.next().await {
                a.await.unwrap();
            }
        });
        let _r = join!(put_handle, get_handle);
    } else {
        for (i, case) in cases.drain(..).enumerate() {
            debug!("running case: {:?}", case);
            run_case(i, case, cfg.clone(), client.clone(), objs.clone())
                .await
                .unwrap();
        }
    }
}
