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

use std::any::type_name;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Sub;

use chrono::{Duration, NaiveDateTime};
use num_traits::{CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Signed, Zero};
use risingwave_common::types::{
    CheckedAdd, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
    OrderedF64,
};

use crate::{ExprError, Result};

#[inline(always)]
pub fn general_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedAdd<Output = T3>,
{
    general_atm(l, r, |a, b| {
        a.checked_add(b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedSub,
{
    general_atm(l, r, |a, b| {
        a.checked_sub(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedMul,
{
    general_atm(l, r, |a, b| {
        a.checked_mul(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedDiv + Zero,
{
    general_atm(l, r, |a, b| {
        a.checked_div(&b).ok_or_else(|| {
            if b.is_zero() {
                ExprError::DivisionByZero
            } else {
                ExprError::NumericOutOfRange
            }
        })
    })
}

#[inline(always)]
pub fn general_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    T3: CheckedRem,
{
    general_atm(l, r, |a, b| {
        a.checked_rem(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[inline(always)]
pub fn general_neg<T1: CheckedNeg>(expr: T1) -> Result<T1> {
    expr.checked_neg().ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn general_abs<T1: Signed + CheckedNeg>(expr: T1) -> Result<T1> {
    if expr.is_negative() {
        general_neg(expr)
    } else {
        Ok(expr)
    }
}

pub fn decimal_abs(decimal: Decimal) -> Result<Decimal> {
    Ok(Decimal::abs(&decimal).unwrap())
}

#[inline(always)]
pub fn general_atm<T1, T2, T3, F>(l: T1, r: T2, atm: F) -> Result<T3>
where
    T1: TryInto<T3> + Debug,
    T2: TryInto<T3> + Debug,
    F: FnOnce(T3, T3) -> Result<T3>,
{
    // TODO: We need to improve the error message
    let l: T3 = l
        .try_into()
        .map_err(|_| ExprError::Cast(type_name::<T1>(), type_name::<T3>()))?;
    let r: T3 = r
        .try_into()
        .map_err(|_| ExprError::Cast(type_name::<T2>(), type_name::<T3>()))?;
    atm(l, r)
}

#[inline(always)]
pub fn timestamp_timestamp_sub<T1, T2, T3>(
    l: NaiveDateTimeWrapper,
    r: NaiveDateTimeWrapper,
) -> Result<IntervalUnit> {
    let tmp = l.0 - r.0;
    let days = tmp.num_days();
    let ms = tmp.sub(Duration::days(tmp.num_days())).num_milliseconds();
    Ok(IntervalUnit::new(0, days as i32, ms))
}

#[inline(always)]
pub fn date_date_sub<T1, T2, T3>(l: NaiveDateWrapper, r: NaiveDateWrapper) -> Result<i32> {
    Ok((l.0 - r.0).num_days() as i32)
}

#[inline(always)]
pub fn interval_timestamp_add<T1, T2, T3>(
    l: IntervalUnit,
    r: NaiveDateTimeWrapper,
) -> Result<NaiveDateTimeWrapper> {
    r.checked_add(l).ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn interval_date_add<T1, T2, T3>(
    l: IntervalUnit,
    r: NaiveDateWrapper,
) -> Result<NaiveDateTimeWrapper> {
    interval_timestamp_add::<T1, T2, T3>(l, r.into())
}

#[inline(always)]
pub fn interval_time_add<T1, T2, T3>(
    l: IntervalUnit,
    r: NaiveTimeWrapper,
) -> Result<NaiveTimeWrapper> {
    time_interval_add::<T2, T1, T3>(r, l)
}

#[inline(always)]
pub fn date_interval_add<T2, T1, T3>(
    l: NaiveDateWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_date_add::<T1, T2, T3>(r, l)
}

#[inline(always)]
pub fn date_interval_sub<T2, T1, T3>(
    l: NaiveDateWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_date_add::<T1, T2, T3>(r.negative(), l)
}

#[inline(always)]
pub fn date_int_add<T1, T2, T3>(l: NaiveDateWrapper, r: i32) -> Result<NaiveDateWrapper> {
    let date = l.0;
    let date_wrapper = date
        .checked_add_signed(chrono::Duration::days(r as i64))
        .map(NaiveDateWrapper::new);

    date_wrapper.ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn int_date_add<T1, T2, T3>(l: i32, r: NaiveDateWrapper) -> Result<NaiveDateWrapper> {
    date_int_add::<T2, T1, T3>(r, l)
}

#[inline(always)]
pub fn date_int_sub<T1, T2, T3>(l: NaiveDateWrapper, r: i32) -> Result<NaiveDateWrapper> {
    date_int_add::<T1, T2, T3>(l, -r)
}

#[inline(always)]
pub fn timestamp_interval_add<T1, T2, T3>(
    l: NaiveDateTimeWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_timestamp_add::<T1, T2, T3>(r, l)
}

#[inline(always)]
pub fn timestamp_interval_sub<T1, T2, T3>(
    l: NaiveDateTimeWrapper,
    r: IntervalUnit,
) -> Result<NaiveDateTimeWrapper> {
    interval_timestamp_add::<T1, T2, T3>(r.negative(), l)
}

#[inline(always)]
pub fn interval_int_mul<T1, T2, T3>(l: IntervalUnit, r: T2) -> Result<IntervalUnit>
where
    T2: TryInto<i32> + Debug,
{
    l.checked_mul_int(r).ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn int_interval_mul<T1, T2, T3>(l: T1, r: IntervalUnit) -> Result<IntervalUnit>
where
    T1: TryInto<i32> + Debug,
{
    interval_int_mul::<T2, T1, T3>(r, l)
}

#[inline(always)]
pub fn date_time_add<T1, T2, T3>(
    l: NaiveDateWrapper,
    r: NaiveTimeWrapper,
) -> Result<NaiveDateTimeWrapper> {
    let date_time = NaiveDateTime::new(l.0, r.0);
    Ok(NaiveDateTimeWrapper::new(date_time))
}

#[inline(always)]
pub fn time_date_add<T1, T2, T3>(
    l: NaiveTimeWrapper,
    r: NaiveDateWrapper,
) -> Result<NaiveDateTimeWrapper> {
    date_time_add::<T2, T1, T3>(r, l)
}

#[inline(always)]
pub fn time_time_sub<T1, T2, T3>(l: NaiveTimeWrapper, r: NaiveTimeWrapper) -> Result<IntervalUnit> {
    let tmp = l.0 - r.0;
    let ms = tmp.num_milliseconds();
    Ok(IntervalUnit::new(0, 0, ms))
}

#[inline(always)]
pub fn time_interval_sub<T1, T2, T3>(
    l: NaiveTimeWrapper,
    r: IntervalUnit,
) -> Result<NaiveTimeWrapper> {
    time_interval_add::<T1, T2, T3>(l, r.negative())
}

#[inline(always)]
pub fn time_interval_add<T1, T2, T3>(
    l: NaiveTimeWrapper,
    r: IntervalUnit,
) -> Result<NaiveTimeWrapper> {
    let time = l.0;
    let new_time = time + Duration::milliseconds(r.get_ms());
    Ok(NaiveTimeWrapper::new(new_time))
}

#[inline(always)]
pub fn interval_float_div<T1, T2, T3>(l: IntervalUnit, r: T2) -> Result<IntervalUnit>
where
    T2: TryInto<OrderedF64> + Debug,
{
    l.div_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn interval_float_mul<T1, T2, T3>(l: IntervalUnit, r: T2) -> Result<IntervalUnit>
where
    T2: TryInto<OrderedF64> + Debug,
{
    l.mul_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn float_interval_mul<T1, T2, T3>(l: T1, r: IntervalUnit) -> Result<IntervalUnit>
where
    T1: TryInto<OrderedF64> + Debug,
{
    r.mul_float(l).ok_or(ExprError::NumericOutOfRange)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::types::Decimal;

    use crate::vector_op::arithmetic_op::general_add;

    #[test]
    fn test() {
        assert_eq!(
            general_add::<_, _, Decimal>(Decimal::from_str("1").unwrap(), 1i32).unwrap(),
            Decimal::from_str("2").unwrap()
        );
    }
}
