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

use std::hash::{BuildHasher, Hasher};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use tokio::sync::{mpsc, Mutex};

use super::cache::FlushBufferHook;
use super::error::Result;
use crate::hummock::TieredCacheKey;

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct TestCacheKey(pub u64);

impl TieredCacheKey for TestCacheKey {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
    }

    fn decode(mut buf: &[u8]) -> Self {
        Self(buf.get_u64())
    }
}

pub fn key(v: u64) -> TestCacheKey {
    TestCacheKey(v)
}

#[derive(Clone)]
pub struct FlushHolder {
    pre_sender: mpsc::UnboundedSender<()>,
    pre_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,

    post_sender: mpsc::UnboundedSender<()>,
    post_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,
}

impl Default for FlushHolder {
    fn default() -> Self {
        let (tx0, rx0) = mpsc::unbounded_channel();
        let (tx1, rx1) = mpsc::unbounded_channel();
        Self {
            pre_sender: tx0,
            pre_receiver: Arc::new(Mutex::new(rx0)),

            post_sender: tx1,
            post_receiver: Arc::new(Mutex::new(rx1)),
        }
    }
}

impl FlushHolder {
    pub fn trigger(&self) {
        self.pre_sender.send(()).unwrap();
    }

    pub async fn wait(&self) {
        self.post_receiver.lock().await.recv().await.unwrap();
    }
}

#[async_trait]
impl FlushBufferHook for FlushHolder {
    async fn pre_flush(&self) -> Result<()> {
        self.pre_receiver.lock().await.recv().await.unwrap();
        Ok(())
    }

    async fn post_flush(&self, _bytes: usize) -> Result<()> {
        self.post_sender.send(()).unwrap();
        Ok(())
    }
}

pub struct ModuloHasher<const M: u8>(u8);

impl<const M: u8> Default for ModuloHasher<M> {
    fn default() -> Self {
        Self(0)
    }
}

impl<const M: u8> ModuloHasher<M> {
    fn hash(&mut self, v: u8) {
        self.0 = ((((self.0 as u16) << 8) + v as u16) % M as u16) as u8;
    }
}

impl<const M: u8> Hasher for ModuloHasher<M> {
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        if cfg!(target_endian = "big") {
            for v in bytes {
                self.hash(*v);
            }
        } else {
            for v in bytes.iter().rev() {
                self.hash(*v);
            }
        }
    }
}

#[derive(Clone)]
pub struct ModuloHasherBuilder<const M: u8>;

impl<const M: u8> Default for ModuloHasherBuilder<M> {
    fn default() -> Self {
        Self
    }
}

impl<const M: u8> BuildHasher for ModuloHasherBuilder<M> {
    type Hasher = ModuloHasher<M>;

    fn build_hasher(&self) -> ModuloHasher<M> {
        ModuloHasher::<M>::default()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;

    #[test]
    fn test_modulo_hasher() {
        const M: u8 = 100;
        for i in 0..1_000_000u64 {
            let mut hasher = ModuloHasher::<M>::default();
            i.hash(&mut hasher);
            let hash = hasher.finish();
            assert_eq!(
                hash,
                i % M as u64,
                "i: {}, hash: {}, i % m: {}",
                i,
                hash,
                i % M as u64
            );
        }
    }
}

/// `datasize()` returns the actual data size of a file.
///
/// File systems like ext4 takes metadata blocks into account in `stat.st_blocks` of `fstat(2)`.
/// So it'not accurate if you really want to know the data size of sparse file with `fstat`.
///
/// `datasize` is implementated by iterates the `fiemap` of the file.
pub fn datasize(path: impl AsRef<Path>) -> Result<usize> {
    let mut size = 0;

    let fm = fiemap::fiemap(path)?;
    for fe in fm {
        let fe = fe.unwrap();
        size += fe.fe_length as usize;
    }

    Ok(size)
}
