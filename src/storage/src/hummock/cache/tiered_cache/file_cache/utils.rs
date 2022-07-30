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

use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, Not, Sub};

pub trait UnsignedTrait = Add<Output = Self>
    + Sub<Output = Self>
    + BitAnd<Output = Self>
    + Not<Output = Self>
    + Sized
    + From<u8>
    + Eq
    + Debug
    + Display
    + Clone
    + Copy;

pub trait Unsigned: UnsignedTrait {}

impl<U: UnsignedTrait> Unsigned for U {}

#[inline(always)]
pub fn is_pow2<U: Unsigned>(v: U) -> bool {
    v & (v - U::from(1)) == U::from(0)
}

#[inline(always)]
pub fn assert_pow2<U: Unsigned>(v: U) {
    assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

#[inline(always)]
pub fn debug_assert_pow2<U: Unsigned>(v: U) {
    debug_assert_eq!(v & (v - U::from(1)), U::from(0), "v: {}", v);
}

#[inline(always)]
pub fn is_aligned<U: Unsigned>(align: U, v: U) -> bool {
    debug_assert_pow2(align);
    v & (align - U::from(1)) == U::from(0)
}

#[inline(always)]
pub fn assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

#[inline(always)]
pub fn debug_assert_aligned<U: Unsigned>(align: U, v: U) {
    debug_assert_pow2(align);
    debug_assert!(is_aligned(align, v), "align: {}, v: {}", align, v);
}

#[inline(always)]
pub fn align_up<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    (v + align - U::from(1)) & !(align - U::from(1))
}

#[inline(always)]
pub fn align_down<U: Unsigned>(align: U, v: U) -> U {
    debug_assert_pow2(align);
    v & !(align - U::from(1))
}
