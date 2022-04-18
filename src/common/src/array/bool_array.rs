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

use std::hash::{Hash, Hasher};
use std::mem::size_of;

use risingwave_pb::data::buffer::CompressionType;
use risingwave_pb::data::{Array as ProstArray, ArrayType, Buffer};

use super::{Array, ArrayBuilder, ArrayIterator, ArrayMeta, NULL_VAL_FOR_HASH};
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;

#[derive(Debug)]
pub struct BoolArray {
    bitmap: Bitmap,
    data: Bitmap,
}

impl BoolArray {
    pub fn from_slice(data: &[Option<bool>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for BoolArray {
    type Builder = BoolArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;
    type OwnedItem = bool;
    type RefItem<'a> = bool;

    fn value_at(&self, idx: usize) -> Option<bool> {
        if !self.is_null(idx) {
            Some(self.data.is_set(idx).unwrap())
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> ProstArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<bool>());

        for v in self.iter().flatten() {
            let bool_numeric = if v { 1 } else { 0 } as u8;
            output_buffer.push(bool_numeric);
        }

        let values = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf();
        ProstArray {
            null_bitmap: Some(null_bitmap),
            values: vec![values],
            array_type: ArrayType::Bool as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.data.is_set(idx).unwrap().hash(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        let array_builder = BoolArrayBuilder::new(capacity)?;
        Ok(ArrayBuilderImpl::Bool(array_builder))
    }
}

/// `BoolArrayBuilder` constructs a `BoolArray` from `Option<Bool>`.
#[derive(Debug)]
pub struct BoolArrayBuilder {
    bitmap: BitmapBuilder,
    data: BitmapBuilder,
    capacity: usize,
}

impl ArrayBuilder for BoolArrayBuilder {
    type ArrayType = BoolArray;

    fn new_with_meta(capacity: usize, _meta: ArrayMeta) -> Result<Self> {
        Ok(Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: BitmapBuilder::with_capacity(capacity),
            capacity,
        })
    }

    fn append(&mut self, value: Option<bool>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.append(x);
            }
            None => {
                self.bitmap.append(false);
                self.data.append(bool::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &BoolArray) -> Result<()> {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }

        for bit in other.data.iter() {
            self.data.append(bit);
        }

        Ok(())
    }

    fn finish(mut self) -> Result<BoolArray> {
        Ok(BoolArray {
            bitmap: self.bitmap.finish(),
            data: self.data.finish(),
        })
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    fn helper_test_builder(data: Vec<Option<bool>>) -> BoolArray {
        let mut builder = BoolArrayBuilder::new(data.len()).unwrap();
        for d in data {
            builder.append(d).unwrap();
        }
        builder.finish().unwrap()
    }

    #[test]
    fn test_bool_builder() {
        let v = (0..1000)
            .map(|x| {
                if x % 2 == 0 {
                    None
                } else if x % 3 == 0 {
                    Some(true)
                } else {
                    Some(false)
                }
            })
            .collect_vec();
        let array = helper_test_builder(v.clone());
        let res = v.iter().zip_eq(array.iter()).all(|(a, b)| *a == b);
        assert!(res);
    }

    #[test]
    fn test_bool_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

        const ARR_NUM: usize = 2;
        const ARR_LEN: usize = 48;
        let vecs: [Vec<Option<bool>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some(true),
                    1 => Some(false),
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some(true),
                    1 => Some(false),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| helper_test_builder(v.clone()))
            .collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter().zip_eq(&mut states).for_each(|(x, state)| match x {
                Some(inner) => inner.hash(state),
                None => NULL_VAL_FOR_HASH.hash(state),
            })
        });
        let hashes = hash_finish(&mut states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 6);

        test_hash(arrs, hashes, hasher_builder);
    }
}
