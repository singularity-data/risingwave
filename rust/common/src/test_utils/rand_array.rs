//! Contains helper methods for generating random arrays in tests.
//!
//! Use [`seed_rand_array`] to generate an random array.

use std::sync::Arc;

use chrono::{Datelike, NaiveTime};
use num_traits::FromPrimitive;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::array::{Array, ArrayBuilder, ArrayRef, StructValue};
use crate::types::{
    Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, NativeType,
    Scalar,
};

pub trait RandValue {
    fn rand_value<R: Rng>(rand: &mut R) -> Self;
}

impl<T> RandValue for T
where
    T: NativeType,
    Standard: Distribution<T>,
{
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen()
    }
}

impl RandValue for String {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let len = rand.gen_range(1..=10);
        // `rand.gen:::<char>` create a random Unicode scalar value.
        (0..len).map(|_| rand.gen::<char>()).collect()
    }
}

impl RandValue for Decimal {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        Decimal::from_f64((rand.gen::<u32>() as f64) + 0.1f64).unwrap()
    }
}

impl RandValue for IntervalUnit {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let months = rand.gen_range(0..100);
        let days = rand.gen_range(0..200);
        let ms = rand.gen_range(0..100_000);
        IntervalUnit::new(months, days, ms)
    }
}

impl RandValue for NaiveDateWrapper {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let max_day = chrono::MAX_DATE.num_days_from_ce();
        let min_day = chrono::MIN_DATE.num_days_from_ce();
        let days = rand.gen_range(min_day..=max_day);
        NaiveDateWrapper::new_with_days(days).unwrap()
    }
}

impl RandValue for NaiveTimeWrapper {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let hour = rand.gen_range(0..24);
        let min = rand.gen_range(0..60);
        let sec = rand.gen_range(0..60);
        let nano = rand.gen_range(0..1_000_000_000);
        NaiveTimeWrapper::new(NaiveTime::from_hms_nano(hour, min, sec, nano))
    }
}

impl RandValue for NaiveDateTimeWrapper {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        NaiveDateTimeWrapper::new(
            NaiveDateWrapper::rand_value(rand)
                .0
                .and_time(NaiveTimeWrapper::rand_value(rand).0),
        )
    }
}

impl RandValue for bool {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen::<bool>()
    }
}

impl RandValue for StructValue {
    fn rand_value<R: rand::Rng>(_rand: &mut R) -> Self {
        StructValue::new(vec![])
    }
}

pub fn rand_array<A, R>(rand: &mut R, size: usize) -> A
where
    A: Array,
    R: Rng,
    A::OwnedItem: RandValue,
{
    let mut builder = A::Builder::new(size).unwrap();
    for _ in 0..size {
        let is_null = rand.gen::<bool>();
        if is_null {
            builder.append_null().unwrap();
        } else {
            let value = A::OwnedItem::rand_value(rand);
            builder.append(Some(value.as_scalar_ref())).unwrap();
        }
    }

    builder.finish().unwrap()
}

pub fn seed_rand_array<A>(size: usize, seed: u64) -> A
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let mut rand = SmallRng::seed_from_u64(seed);
    rand_array(&mut rand, size)
}

pub fn seed_rand_array_ref<A>(size: usize, seed: u64) -> ArrayRef
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let array: A = seed_rand_array(size, seed);
    Arc::new(array.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::interval_array::IntervalArray;
    use crate::array::*;
    use crate::for_all_variants;

    #[test]
    fn test_create_array() {
        macro_rules! gen_rand_array {
      ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        $(
          {
            let array = seed_rand_array::<$array>(10, 1024);
            assert_eq!(10, array.len());

          }
        )*
      };
    }

        for_all_variants! { gen_rand_array }
    }
}
