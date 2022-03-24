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

use serde::{Deserialize, Serialize};

use super::*;

/// Every interval can be represented by a `IntervalUnit`.
/// Note that the difference between Interval and Instant.
/// For example, `5 yrs 1 month 25 days 23:22:57` is a interval (Can be interpreted by Interval Unit
/// with month = 61, days = 25, seconds = (57 + 23 * 3600 + 22 * 60) * 1000),
/// `1970-01-01 04:05:06` is a Instant or Timestamp
/// One month may contain 28/31 days. One day may contain 23/25 hours.
/// This internals is learned from PG:
/// <https://www.postgresql.org/docs/9.1/datatype-datetime.html#:~:text=field%20is%20negative.-,Internally,-interval%20values%20are>
///
/// FIXME: if this derives `PartialEq` and `PartialOrd`, caller must guarantee the fields are valid.
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct IntervalUnit {
    months: i32,
    days: i32,
    ms: i64,
}

impl IntervalUnit {
    pub fn new(months: i32, days: i32, ms: i64) -> Self {
        IntervalUnit { months, days, ms }
    }

    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_years(&self) -> i32 {
        self.months / 12
    }

    pub fn get_ms(&self) -> i64 {
        self.ms
    }

    #[must_use]
    pub fn negative(&self) -> Self {
        IntervalUnit {
            months: -self.months,
            days: -self.days,
            ms: -self.ms,
        }
    }

    #[must_use]
    pub fn from_ymd(year: i32, month: i32, days: i32) -> Self {
        let months = year * 12 + month;
        let days = days;
        let ms = 0;
        IntervalUnit { months, days, ms }
    }

    #[must_use]
    pub fn from_month(months: i32) -> Self {
        IntervalUnit {
            months,
            days: 0,
            ms: 0,
        }
    }

    #[must_use]
    pub fn from_millis(ms: i64) -> Self {
        IntervalUnit {
            months: 0,
            days: 0,
            ms,
        }
    }
}

impl ToString for IntervalUnit {
    fn to_string(&self) -> String {
        todo!()
    }
}
