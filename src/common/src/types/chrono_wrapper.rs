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

use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Write;

use bytes::{BufMut, BytesMut};
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use super::{CheckedAdd, IntervalUnit};
use crate::array::ArrayResult;
use crate::error::Result;
use crate::util::value_encoding::error::ValueEncodingError;
/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;
const LEAP_DAYS: &[i32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const NORMAL_DAYS: &[i32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

macro_rules! impl_chrono_wrapper {
    ($({ $variant_name:ident, $chrono:ty, $_array:ident, $_builder:ident }),*) => {
        $(
            #[derive(Clone, Copy, Debug, Eq, PartialOrd, Ord)]
            #[repr(transparent)]
            pub struct $variant_name(pub $chrono);

            impl $variant_name {
                pub fn new(data: $chrono) -> Self {
                    $variant_name(data)
                }
            }

            impl Hash for $variant_name {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    self.0.hash(state);
                }
            }

            impl PartialEq for $variant_name {
                fn eq(&self, other: &Self) -> bool {
                    self.0 == other.0
                }
            }

            impl Display for $variant_name {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    Display::fmt(&self.0, f)
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! for_all_chrono_variants {
    ($macro:ident) => {
        $macro! {
            { NaiveDateWrapper, NaiveDate, NaiveDateArray, NaiveDateArrayBuilder },
            { NaiveDateTimeWrapper, NaiveDateTime, NaiveDateTimeArray, NaiveDateTimeArrayBuilder },
            { NaiveTimeWrapper, NaiveTime, NaiveTimeArray, NaiveTimeArrayBuilder }
        }
    };
}

for_all_chrono_variants! { impl_chrono_wrapper }

impl Default for NaiveDateWrapper {
    fn default() -> Self {
        NaiveDateWrapper(NaiveDate::from_ymd(1970, 1, 1))
    }
}

impl Default for NaiveTimeWrapper {
    fn default() -> Self {
        NaiveTimeWrapper(NaiveTime::from_hms(0, 0, 0))
    }
}

impl Default for NaiveDateTimeWrapper {
    fn default() -> Self {
        NaiveDateTimeWrapper(NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0))
    }
}

impl NaiveDateWrapper {
    pub fn with_days(days: i32) -> memcomparable::Result<Self> {
        Ok(NaiveDateWrapper::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or(memcomparable::Error::InvalidNaiveDateEncoding(days))?,
        ))
    }

    pub fn new_with_days_value_encoding(days: i32) -> Result<Self> {
        Ok(NaiveDateWrapper::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or(ValueEncodingError::InvalidNaiveDateEncoding(days))?,
        ))
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&(self.0.num_days_from_ce()).to_be_bytes())
            .map_err(Into::into)
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        self.0.num_days_from_ce().to_be_bytes().to_vec()
    }

    pub fn from_protobuf(days: i32) -> ArrayResult<Self> {
        Self::with_days(days).map_err(Into::into)
    }

    pub fn from_protobuf_bytes(b: &[u8]) -> ArrayResult<Self> {
        let days = i32::from_be_bytes(
            b.try_into()
                .map_err(|e| anyhow::anyhow!("Failed to deserialize date, reason: {:?}", e))?,
        );
        Self::from_protobuf(days)
    }
}

impl NaiveTimeWrapper {
    pub fn with_secs_nano(secs: u32, nano: u32) -> memcomparable::Result<Self> {
        Ok(NaiveTimeWrapper::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or(memcomparable::Error::InvalidNaiveTimeEncoding(secs, nano))?,
        ))
    }

    pub fn new_with_secs_nano_value_encoding(secs: u32, nano: u32) -> Result<Self> {
        Ok(NaiveTimeWrapper::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or(ValueEncodingError::InvalidNaiveTimeEncoding(secs, nano))?,
        ))
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        let buf = BytesMut::with_capacity(8);
        let mut writer = buf.writer();
        self.to_protobuf(&mut writer).unwrap();
        writer.into_inner().to_vec()
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(
                &(self.0.num_seconds_from_midnight() as u64 * 1_000_000_000
                    + self.0.nanosecond() as u64)
                    .to_be_bytes(),
            )
            .map_err(Into::into)
    }

    pub fn from_protobuf(nano: u64) -> ArrayResult<Self> {
        let secs = (nano / 1_000_000_000) as u32;
        let nano = (nano % 1_000_000_000) as u32;
        Self::with_secs_nano(secs, nano).map_err(Into::into)
    }

    pub fn from_protobuf_bytes(b: &[u8]) -> ArrayResult<Self> {
        let nanos = u64::from_be_bytes(
            b.try_into()
                .map_err(|e| anyhow::anyhow!("Failed to deserialize time, reason: {:?}", e))?,
        );
        Self::from_protobuf(nanos)
    }
}

impl NaiveDateTimeWrapper {
    pub fn with_secs_nsecs(secs: i64, nsecs: u32) -> memcomparable::Result<Self> {
        Ok(NaiveDateTimeWrapper::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs).ok_or(
                memcomparable::Error::InvalidNaiveDateTimeEncoding(secs, nsecs),
            )?
        }))
    }

    pub fn new_with_secs_nsecs_value_encoding(secs: i64, nsecs: u32) -> Result<Self> {
        Ok(NaiveDateTimeWrapper::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs).ok_or(
                ValueEncodingError::InvalidNaiveDateTimeEncoding(secs, nsecs),
            )?
        }))
    }

    /// Although `NaiveDateTime` takes 12 bytes, we drop 4 bytes in protobuf encoding.
    /// TODO: Consider another way to save. Nanosecond timestamp can only represent about 584 years.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&(self.0.timestamp_nanos()).to_be_bytes())
            .map_err(Into::into)
    }

    pub fn to_protobuf_owned(&self) -> Vec<u8> {
        self.0.timestamp_nanos().to_be_bytes().to_vec()
    }

    pub fn from_protobuf(timestamp_nanos: i64) -> ArrayResult<Self> {
        let secs = timestamp_nanos / 1_000_000_000;
        let nsecs = (timestamp_nanos % 1_000_000_000) as u32;
        Self::with_secs_nsecs(secs, nsecs).map_err(Into::into)
    }

    pub fn from_protobuf_bytes(b: &[u8]) -> ArrayResult<Self> {
        let nanos =
            i64::from_be_bytes(b.try_into().map_err(|e| {
                anyhow::anyhow!("Failed to deserialize date time, reason: {:?}", e)
            })?);
        Self::from_protobuf(nanos)
    }
}

impl From<NaiveDateWrapper> for NaiveDateTimeWrapper {
    fn from(date: NaiveDateWrapper) -> Self {
        NaiveDateTimeWrapper::new(date.0.and_hms(0, 0, 0))
    }
}

/// return the days of the `year-month`
fn get_mouth_days(year: i32, month: usize) -> i32 {
    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

impl CheckedAdd<IntervalUnit> for NaiveDateTimeWrapper {
    type Output = NaiveDateTimeWrapper;

    fn checked_add(self, rhs: IntervalUnit) -> Option<NaiveDateTimeWrapper> {
        let mut date = self.0.date();
        if rhs.get_months() != 0 {
            // NaiveDate don't support add months. We need calculate manually
            let mut day = date.day() as i32;
            let mut month = date.month() as i32;
            let mut year = date.year();
            // Calculate the number of year in this interval
            let interval_months = rhs.get_months();
            let year_diff = interval_months / 12;
            year += year_diff;

            // Calculate the number of month in this interval except the added year
            // The range of month_diff is (-12, 12) (The month is negative when the interval is
            // negative)
            let month_diff = interval_months - year_diff * 12;
            // The range of new month is (-12, 24) ( original month:[1, 12] + month_diff:(-12, 12) )
            month += month_diff;
            // Process the overflow months
            if month > 12 {
                year += 1;
                month -= 12;
            } else if month <= 0 {
                year -= 1;
                month += 12;
            }

            // Fix the days after changing date.
            // For example, 1970.1.31 + 1 month = 1970.2.28
            day = min(day, get_mouth_days(year, month as usize));
            date = NaiveDate::from_ymd(year, month as u32, day as u32);
        }
        let mut datetime = NaiveDateTime::new(date, self.0.time());
        datetime = datetime.checked_add_signed(Duration::days(rhs.get_days().into()))?;
        datetime = datetime.checked_add_signed(Duration::milliseconds(rhs.get_ms()))?;

        Some(NaiveDateTimeWrapper::new(datetime))
    }
}
