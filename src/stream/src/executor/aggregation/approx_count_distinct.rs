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

//! This module implements `StreamingApproxCountDistinct`.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{Datum, DatumRef, Scalar, ScalarImpl};

use super::StreamingAggStateImpl;
use crate::executor::error::StreamExecutorResult;

const INDEX_BITS: u8 = 16; // number of bits used for finding the index of each 64-bit hash
const NUM_OF_REGISTERS: usize = 1 << INDEX_BITS; // number of registers available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash

// Approximation for bias correction for 16384 registers. See "HyperLogLog: the analysis of a
// near-optimal cardinality estimation algorithm" by Philippe Flajolet et al.
const BIAS_CORRECTION: f64 = 0.72125;

pub(crate) const DENSE_BITS_DEFAULT: usize = 16; // number of bits in the dense repr of the `RegisterBucket`

#[derive(Clone, Debug)]
struct SparseCount {
    inner: Vec<(u8, u64)>,
}

impl SparseCount {
    fn new() -> Self {
        Self {
            inner: Vec::default(),
        }
    }

    fn get(&self, k: u8) -> u64 {
        for (key, count) in &self.inner {
            if *key == k {
                return *count;
            }
            if *key > k {
                break;
            }
        }
        0
    }

    fn add(&mut self, k: u8) -> bool {
        let mut last = 0;
        for (key, count) in self.inner.iter_mut() {
            if *key == k {
                *count += 1;
                return true;
            }
            if *key > k {
                break;
            }
            last += 1;
        }
        self.inner.insert(last, (k, 1));
        false
    }

    fn subtract(&mut self, k: u8) -> bool {
        for (i, (key, count)) in self.inner.iter_mut().enumerate() {
            if *key == k {
                *count -= 1;
                if *count == 0 {
                    // delete the count
                    self.inner.remove(i);
                }
                return true;
            }
            if *key > k {
                break;
            }
        }
        false
    }

    fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    fn last_key(&self) -> u8 {
        assert!(!self.is_empty());
        self.inner.last().unwrap().0
    }
}

#[derive(Clone, Debug)]
struct RegisterBucket<const DENSE_BITS: usize> {
    dense_counts: [u64; DENSE_BITS],
    sparse_counts: Option<SparseCount>,
}

impl<const DENSE_BITS: usize> RegisterBucket<DENSE_BITS> {
    pub fn new() -> Self {
        Self {
            dense_counts: [0u64; DENSE_BITS],
            sparse_counts: None,
        }
    }

    fn get_bucket(&self, index: usize) -> StreamExecutorResult<u64> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        if index >= 17 {
            if let Some(counts) = &self.sparse_counts {
                return Ok(counts.get(index as u8));
            } else {
                return Ok(0);
            }
        }

        Ok(self.dense_counts[index - 1])
    }

    /// Increments or decrements the bucket at `index` depending on the state of `is_insert`.
    /// Returns an Error if `index` is invalid or if inserting will cause an overflow in the bucket.
    fn update_bucket(&mut self, index: usize, is_insert: bool) -> StreamExecutorResult<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        let count = self.get_bucket(index)?;

        if is_insert {
            if index >= 17 {
                if let Some(counts) = &mut self.sparse_counts {
                    counts.add(index as u8);
                } else {
                    let mut counts = SparseCount::new();
                    counts.add(index as u8);
                    self.sparse_counts = Some(counts);
                }
            } else if index >= 1 {
                if count == u64::MAX {
                    bail!(
                        "HyperLogLog: Count exceeds maximum bucket value.\
                        Your data stream may have too many repeated values or too large a\
                        cardinality for approx_count_distinct to handle (max: 2^64 - 1)"
                    );
                }
                self.dense_counts[index - 1] = count + 1;
            }
        } else {
            // We don't have to worry about the user deleting nonexistent elements, so the counts
            // can never go below 0.
            if index >= 17 {
                if let Some(counts) = &mut self.sparse_counts {
                    counts.subtract(index as u8);
                    if counts.is_empty() {
                        self.sparse_counts = None;
                    }
                } else {
                    bail!("HyperLogLog: Deletion of non-existent count");
                }
            } else if index >= 1 {
                self.dense_counts[index - 1] = count - 1;
            }
        }

        Ok(())
    }

    /// Gets the number of the maximum bucket which has a count greater than zero.
    fn get_max(&self) -> StreamExecutorResult<u8> {
        if let Some(counts) = &self.sparse_counts && !counts.is_empty() {
            return Ok(counts.last_key());
        }
        for i in (0..DENSE_BITS).rev() {
            if self.dense_counts[i] > 0 {
                return Ok(i as u8);
            }
        }
        Ok(0)
    }
}

/// `StreamingApproxCountDistinct` approximates the count of non-null rows using a modified version
/// of the `HyperLogLog` algorithm. Each `RegisterBucket` stores a count of how many hash values
/// have x trailing zeroes for all x from 1-64. This allows the algorithm to support insertion and
/// deletion, but uses up more memory and limits the number of rows that can be counted.
///
/// `StreamingApproxCountDistinct` can count up to a total of 2^64 rows.
///
/// The estimation error for `HyperLogLog` is 1.04/sqrt(num of registers). With 2^16 registers this
/// is ~1/256, or about 0.4%. The memory usage for the default choice of parameters is about
/// 1024 bits * 2^16 buckets, which is about 8.4 MB.
#[derive(Clone, Debug)]
pub struct StreamingApproxCountDistinct<const DENSE_BITS: usize> {
    registers: Vec<RegisterBucket<DENSE_BITS>>,
    initial_count: i64,
}

impl<const DENSE_BITS: usize> StreamingApproxCountDistinct<DENSE_BITS> {
    pub fn new() -> Self {
        StreamingApproxCountDistinct::new_with_datum(None)
    }

    pub fn new_with_datum(datum: Datum) -> Self {
        let count = if let Some(c) = datum {
            match c {
                ScalarImpl::Int64(num) => num,
                other => panic!(
                    "type mismatch in streaming aggregator StreamingApproxCountDistinct init: expected i64, get {}",
                    other.get_ident()
                ),
            }
        } else {
            0
        };

        Self {
            registers: vec![RegisterBucket::new(); NUM_OF_REGISTERS],
            initial_count: count,
        }
    }

    /// Adds the count of the datum's hash into the register, if it is greater than the existing
    /// count at the register.
    fn update_registers(
        &mut self,
        datum_ref: DatumRef,
        is_insert: bool,
    ) -> StreamExecutorResult<()> {
        if datum_ref.is_none() {
            return Ok(());
        }

        let scalar_impl = datum_ref.unwrap().into_scalar_impl();
        let hash = self.get_hash(scalar_impl);

        let index = (hash as usize) & (NUM_OF_REGISTERS - 1); // Index is based on last few bits
        let count = self.count_hash(hash) as usize;

        self.registers[index].update_bucket(count, is_insert)?;

        Ok(())
    }

    /// Calculate the hash of the `scalar_impl`.
    fn get_hash(&self, scalar_impl: ScalarImpl) -> u64 {
        let mut hasher = DefaultHasher::new();
        scalar_impl.hash(&mut hasher);
        hasher.finish()
    }

    /// Counts the number of trailing zeroes plus 1 in the non-index bits of the hash.
    fn count_hash(&self, mut hash: u64) -> u8 {
        hash >>= INDEX_BITS; // Ignore bits used as index for the hash
        hash |= 1 << COUNT_BITS; // To allow hash to terminate if it is all 0s

        (hash.trailing_zeros() + 1) as u8
    }
}

impl<const DENSE_BITS: usize> StreamingAggStateImpl for StreamingApproxCountDistinct<DENSE_BITS> {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        match visibility {
            None => {
                for (op, datum) in ops.iter().zip_eq(data[0].iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.update_registers(datum, true)?,
                        Op::Delete | Op::UpdateDelete => self.update_registers(datum, false)?,
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), datum) in
                    visibility.iter().zip_eq(ops.iter()).zip_eq(data[0].iter())
                {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.update_registers(datum, true)?,
                            Op::Delete | Op::UpdateDelete => self.update_registers(datum, false)?,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_output(&self) -> StreamExecutorResult<Datum> {
        let m = NUM_OF_REGISTERS as f64;
        let mut mean = 0.0;

        // Get harmonic mean of all the counts in results
        for register_bucket in &self.registers {
            let count = register_bucket.get_max()?;
            mean += 1.0 / ((1 << count) as f64);
        }

        let raw_estimate = BIAS_CORRECTION * m * m / mean;

        // If raw_estimate is not much bigger than m and some registers have value 0, set answer to
        // m * log(m/V) where V is the number of registers with value 0
        let answer = if raw_estimate <= 2.5 * m {
            let mut zero_registers: f64 = 0.0;
            for i in &self.registers {
                if i.get_max()? == 0 {
                    zero_registers += 1.0;
                }
            }

            if zero_registers == 0.0 {
                raw_estimate
            } else {
                m * (m.log2() - (zero_registers.log2()))
            }
        } else {
            raw_estimate
        };

        Ok(Some((answer as i64 + self.initial_count).to_scalar_value()))
    }

    fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0).unwrap())
    }

    fn reset(&mut self) {
        self.registers = vec![RegisterBucket::new(); NUM_OF_REGISTERS];
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array_nonnull;

    use super::*;

    #[test]
    fn test_insert_and_delete_dense() {
        let mut agg = StreamingApproxCountDistinct::<4>::new();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Insert],
            Some(&(vec![true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![true, true, true, true]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().into_int64(), 0);
    }

    #[test]
    fn test_insert_and_delete_sparse() {
        let mut agg = StreamingApproxCountDistinct::<0>::new();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Insert],
            Some(&(vec![true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![true, true, true, true]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().into_int64(), 0);
    }

    #[test]
    fn test_register_bucket_get_and_update_dense() {
        let mut rb = RegisterBucket::<4>::new();

        for i in 0..20 {
            rb.update_bucket(i % 2 + 1, true).unwrap();
        }
        assert_eq!(rb.get_bucket(1).unwrap(), 10);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(1, false).unwrap();
        assert_eq!(rb.get_bucket(1).unwrap(), 9);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(64, true).unwrap();
        assert_eq!(rb.get_bucket(64).unwrap(), 1);
    }

    #[test]
    fn test_register_bucket_get_and_update_sparse() {
        let mut rb = RegisterBucket::<0>::new();

        for i in 0..20 {
            rb.update_bucket(i % 2 + 1, true).unwrap();
        }
        assert_eq!(rb.get_bucket(1).unwrap(), 10);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(1, false).unwrap();
        assert_eq!(rb.get_bucket(1).unwrap(), 9);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(64, true).unwrap();
        assert_eq!(rb.get_bucket(64).unwrap(), 1);
    }

    #[test]
    fn test_register_bucket_invalid_register() {
        let mut rb = RegisterBucket::<0>::new();

        assert_matches!(rb.get_bucket(0), Err(_));
        assert_matches!(rb.get_bucket(65), Err(_));
        assert_matches!(rb.update_bucket(0, true), Err(_));
        assert_matches!(rb.update_bucket(65, true), Err(_));
    }
}
