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

use std::assert_matches::assert_matches;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};
use risingwave_common::types::{
    Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, OrderedF32, OrderedF64,
};

use crate::vector_op::arithmetic_op::*;
use crate::vector_op::bitwise_op::*;
use crate::vector_op::cast::general_cast;
use crate::vector_op::cmp::*;
use crate::vector_op::conjunction::*;
use crate::ExprError;

#[test]
fn test_arithmetic() {
    assert_eq!(
        general_add::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        general_sub::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("-1.0").unwrap()
    );
    assert_eq!(
        general_mul::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap(),
        Decimal::from_str("2.0").unwrap()
    );
    assert_eq!(
        general_div::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("1.0").unwrap()
    );
    assert_eq!(
        general_mod::<Decimal, i32, Decimal>(Decimal::from_str("2.0").unwrap(), 2).unwrap(),
        Decimal::from_str("0").unwrap()
    );
    assert_eq!(
        general_neg::<Decimal>(Decimal::from_str("1.0").unwrap()).unwrap(),
        Decimal::from_str("-1.0").unwrap()
    );
    assert_eq!(general_add::<i16, i32, i32>(1i16, 1i32).unwrap(), 2i32);
    assert_eq!(general_sub::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_mul::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_div::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
    assert_eq!(general_mod::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
    assert_eq!(general_neg::<i16>(1i16).unwrap(), -1i16);

    assert_eq!(
        general_add::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), -1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_sub::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_div::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_mul::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert_eq!(
        general_mod::<Decimal, f32, Decimal>(Decimal::from_str("0.0").unwrap(), 1f32).unwrap(),
        Decimal::from_str("0.0").unwrap()
    );
    assert!(
        general_add::<i32, OrderedF32, OrderedF64>(-1i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_sub::<i32, OrderedF32, OrderedF64>(1i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_mul::<i32, OrderedF32, OrderedF64>(0i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert!(
        general_div::<i32, OrderedF32, OrderedF64>(0i32, 1f32.into())
            .unwrap()
            .abs()
            < f64::EPSILON
    );
    assert_eq!(
        general_neg::<OrderedF32>(1f32.into()).unwrap(),
        OrderedF32::from(-1f32)
    );
    assert_eq!(
        date_interval_add::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1)),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        interval_date_add::<IntervalUnit, NaiveDateWrapper, NaiveDateTimeWrapper>(
            IntervalUnit::from_month(12),
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1))
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
    assert_eq!(
        date_interval_sub::<NaiveDateWrapper, IntervalUnit, NaiveDateTimeWrapper>(
            NaiveDateWrapper::new(NaiveDate::from_ymd(1994, 1, 1)),
            IntervalUnit::from_month(12)
        )
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1993-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_bitwise() {
    // check the boundary
    assert_eq!(general_shl::<i32, i32>(1i32, 0i32).unwrap(), 1i32);
    assert_eq!(general_shl::<i64, i32>(1i64, 31i32).unwrap(), 2147483648i64);
    assert_matches!(
        general_shl::<i32, i32>(1i32, 32i32).unwrap_err(),
        ExprError::NumericOutOfRange,
    );
    assert_eq!(
        general_shr::<i64, i32>(-2147483648i64, 31i32).unwrap(),
        -1i64
    );
    assert_eq!(general_shr::<i64, i32>(1i64, 0i32).unwrap(), 1i64);
    // truth table
    assert_eq!(
        general_bitand::<u32, u32, u64>(0b0011u32, 0b0101u32).unwrap(),
        0b1u64
    );
    assert_eq!(
        general_bitor::<u32, u32, u64>(0b0011u32, 0b0101u32).unwrap(),
        0b0111u64
    );
    assert_eq!(
        general_bitxor::<u32, u32, u64>(0b0011u32, 0b0101u32).unwrap(),
        0b0110u64
    );
    assert_eq!(general_bitnot::<i32>(0b01i32).unwrap(), -2i32);
}

#[test]
fn test_comparison() {
    assert!(general_eq::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(general_eq::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!general_ne::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 1).unwrap());
    assert!(!general_ne::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 1.0).unwrap());
    assert!(!general_gt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!general_gt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.0).unwrap());
    assert!(general_le::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(general_le::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(!general_ge::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(!general_ge::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(general_lt::<Decimal, i32, Decimal>(Decimal::from_str("1.0").unwrap(), 2).unwrap());
    assert!(general_lt::<Decimal, f32, Decimal>(Decimal::from_str("1.0").unwrap(), 2.1).unwrap());
    assert!(general_is_distinct_from::<Decimal, i32, Decimal>(
        Some(Decimal::from_str("1.0").unwrap()),
        Some(2)
    )
    .unwrap()
    .unwrap());
    assert!(general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(Decimal::from_str("1.0").unwrap()),
        Some(2.0)
    )
    .unwrap()
    .unwrap());
    assert!(general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(Decimal::from_str("1.0").unwrap()),
        None
    )
    .unwrap()
    .unwrap());
    assert!(
        general_is_distinct_from::<Decimal, i32, Decimal>(None, Some(1))
            .unwrap()
            .unwrap()
    );
    assert!(!general_is_distinct_from::<Decimal, i32, Decimal>(
        Some(Decimal::from_str("1.0").unwrap()),
        Some(1)
    )
    .unwrap()
    .unwrap());
    assert!(!general_is_distinct_from::<Decimal, f32, Decimal>(
        Some(Decimal::from_str("1.0").unwrap()),
        Some(1.0)
    )
    .unwrap()
    .unwrap());
    assert!(
        !general_is_distinct_from::<Decimal, f32, Decimal>(None, None)
            .unwrap()
            .unwrap()
    );
    assert!(general_eq::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_ne::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_lt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(general_le::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(!general_gt::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(general_ge::<OrderedF32, i32, OrderedF64>(1.0.into(), 1).unwrap());
    assert!(
        !general_is_distinct_from::<OrderedF32, i32, OrderedF64>(Some(1.0.into()), Some(1))
            .unwrap()
            .unwrap()
    );
    assert!(general_eq::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_ne::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_lt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(general_le::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(!general_gt::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(general_ge::<i64, i32, i64>(1i64, 1).unwrap());
    assert!(
        !general_is_distinct_from::<i64, i32, i64>(Some(1i64), Some(1))
            .unwrap()
            .unwrap()
    );
}

#[test]
fn test_conjunction() {
    assert!(not(Some(false)).unwrap().unwrap());
    assert!(!and(Some(true), Some(false)).unwrap().unwrap());
    assert!(or(Some(true), Some(false)).unwrap().unwrap());
}
#[test]
fn test_cast() {
    assert_eq!(
        general_cast::<_, NaiveDateTimeWrapper>(NaiveDateWrapper::new(NaiveDate::from_ymd(
            1994, 1, 1
        )))
        .unwrap(),
        NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
        )
    )
}
