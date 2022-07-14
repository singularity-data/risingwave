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

use prometheus::HistogramVec;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct MonitoredRwLock<T> {
    metrics: HistogramVec,
    inner: RwLock<T>,
}

impl<T> MonitoredRwLock<T> {
    pub fn new(metrics: HistogramVec, val: T) -> Self {
        Self {
            metrics,
            inner: RwLock::new(val),
        }
    }

    pub async fn read<'a>(&self, label_values: &[&'a str]) -> RwLockReadGuard<'_, T> {
        let _timer = self.metrics.with_label_values(label_values).start_timer();
        self.inner.read().await
    }

    pub async fn write<'a>(&self, label_values: &[&'a str]) -> RwLockWriteGuard<'_, T> {
        let _timer = self.metrics.with_label_values(label_values).start_timer();
        self.inner.write().await
    }
}
