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

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use postgres_types::{ToSql, Type};
use rust_decimal::Decimal;
use serde::{ser, Serialize};

use crate::error::{Error, Result};

/// A structure for serializing Rust values into a memcomparable bytes.
pub struct Serializer {
    output: BytesMut,
    placeholder: Type,
}

impl Serializer {
    /// Create a new `Serializer`.
    pub fn new() -> Self {
        Self {
            output: BytesMut::new(),
            placeholder: Type::ANY.clone(),
        }
    }

    /// Get the output buffer from the `Serializer`.
    pub fn get_ouput(self) -> Bytes {
        self.output.freeze()
    }
}

impl Default for Serializer {
    fn default() -> Self {
        Self::new()
    }
}

// Format Reference:
// postgres-types:https://docs.rs/postgres-types/0.2.3/postgres_types/trait.FromSql.html
impl<'a> ser::Serializer for &'a mut Serializer {
    type Error = Error;
    type Ok = ();
    type SerializeMap = Self;
    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        unimplemented!("Postgres does not support u8")
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        unimplemented!("Postgres does not support u16")
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<()> {
        unimplemented!("Postgres does not support u64")
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        unimplemented!("Postgres does not support char")
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    fn serialize_none(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit(self) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        unimplemented!()
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        unimplemented!()
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        unimplemented!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        unimplemented!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        unimplemented!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        unimplemented!()
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_element<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_field<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeMap for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_key<T>(&mut self, _key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeStruct for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl<'a> ser::SerializeStructVariant for &'a mut Serializer {
    type Error = Error;
    type Ok = ();

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<()> {
        unimplemented!()
    }
}

impl Serializer {
    /// Serialize a Decimal value.
    ///
    /// v:Option<Decimal>
    /// Some(v) means v is Normalized Decimal.
    /// None means v is NaN.
    pub fn serialize_decimal(&mut self, v: Option<Decimal>) -> Result<()> {
        match v {
            Some(v) => {
                v.to_sql(&self.placeholder, &mut self.output).unwrap();
            }
            None => {
                self.output.reserve(8);
                self.output.put_u16(0);
                self.output.put_i16(0);
                self.output.put_u16(0xC000);
                self.output.put_i16(0);
            }
        }
        Ok(())
    }

    /// Serialize a NaiveDate value.
    pub fn serialize_naivedate(&mut self, v: NaiveDate) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    /// Serialize a NaiveTimeWrapper value.
    pub fn serialize_naivetime(&mut self, v: NaiveTime) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    /// Serialize a NaiveDateTime value.
    pub fn serialize_naivedatetime(&mut self, v: NaiveDateTime) -> Result<()> {
        v.to_sql(&self.placeholder, &mut self.output).unwrap();
        Ok(())
    }

    /// Serialize bytes of ListValue or StructValue.
    pub fn serialize_struct_or_list(&mut self, _bytes: Vec<u8>) -> Result<()> {
        unimplemented!()
    }
}

// #[cfg(test)]
// mod tests {
// use rand::distributions::Alphanumeric;
// use rand::Rng;
// use serde::Serialize;
//
// use super::*;
//
// #[test]
// fn test_unit() {
// assert_eq!(to_vec(&()).unwrap(), []);
//
// #[derive(Serialize)]
// struct UnitStruct;
// assert_eq!(to_vec(&UnitStruct).unwrap(), []);
// }
//
// #[test]
// fn test_option() {
// assert_eq!(to_vec(&(None as Option<u8>)).unwrap(), [0]);
// assert_eq!(to_vec(&Some(0x12u8)).unwrap(), [1, 0x12]);
// }
//
// #[test]
// fn test_tuple() {
// let tuple: (i8, i16, i32, i64) = (0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321);
// assert_eq!(
// to_vec(&tuple).unwrap(),
// [
// 0x92, 0x92, 0x34, 0x92, 0x34, 0x56, 0x78, 0x92, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
// 0x21
// ]
// );
//
// #[derive(Serialize)]
// struct TupleStruct(u8, u16, u32, u64);
// let tuple = TupleStruct(0x12, 0x1234, 0x12345678, 0x1234_5678_8765_4321);
// assert_eq!(
// to_vec(&tuple).unwrap(),
// [
// 0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x87, 0x65, 0x43,
// 0x21
// ]
// );
//
// #[derive(Serialize)]
// struct NewTypeStruct(char);
// let tuple = NewTypeStruct('G');
// assert_eq!(to_vec(&tuple).unwrap(), [0, 0, 0, b'G']);
// }
//
// #[test]
// fn test_vec() {
// let s: &[u8] = &[1, 2, 3];
// assert_eq!(to_vec(&s).unwrap(), [1, 0x01, 1, 0x02, 1, 0x03, 0]);
// }
//
// #[test]
// fn test_enum() {
// #[derive(PartialEq, Eq, PartialOrd, Ord, Serialize)]
// enum TestEnum {
// Unit,
// NewType(u8),
// Tuple(u8, u8),
// Struct { a: u8, b: u8 },
// }
//
// let test = TestEnum::Unit;
// assert_eq!(to_vec(&test).unwrap(), [0]);
//
// let test = TestEnum::NewType(0x12);
// assert_eq!(to_vec(&test).unwrap(), [1, 0x12]);
//
// let test = TestEnum::Tuple(0x12, 0x34);
// assert_eq!(to_vec(&test).unwrap(), [2, 0x12, 0x34]);
//
// let test = TestEnum::Struct { a: 0x12, b: 0x34 };
// assert_eq!(to_vec(&test).unwrap(), [3, 0x12, 0x34]);
// }
//
// #[derive(PartialEq, PartialOrd, Serialize)]
// struct Test {
// a: bool,
// b: f32,
// c: f64,
// }
//
// #[test]
// fn test_struct() {
// let test = Test {
// a: true,
// b: 0.0,
// c: 0.0,
// };
// assert_eq!(
// to_vec(&test).unwrap(),
// [1, 0x80, 0, 0, 0, 0x80, 0, 0, 0, 0, 0, 0, 0]
// );
// }
//
// #[test]
// fn test_struct_order() {
// for _ in 0..1000 {
// let mut rng = rand::thread_rng();
// let a = Test {
// a: rng.gen(),
// b: rng.gen(),
// c: rng.gen(),
// };
// let b = Test {
// a: if rng.gen_bool(0.5) { a.a } else { rng.gen() },
// b: if rng.gen_bool(0.5) {
// a.b
// } else {
// rng.gen_range(-1.0..1.0)
// },
// c: if rng.gen_bool(0.5) {
// a.c
// } else {
// rng.gen_range(-1.0..1.0)
// },
// };
// let ea = to_vec(&a).unwrap();
// let eb = to_vec(&b).unwrap();
// assert_eq!(a.partial_cmp(&b), ea.partial_cmp(&eb));
// }
// }
//
// #[test]
// fn test_string() {
// assert_eq!(to_vec(&"").unwrap(), [0]);
// assert_eq!(
// to_vec(&"123").unwrap(),
// [1, b'1', b'2', b'3', 0, 0, 0, 0, 0, 3]
// );
// assert_eq!(
// to_vec(&"12345678").unwrap(),
// [1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 8]
// );
// assert_eq!(
// to_vec(&"1234567890").unwrap(),
// [
// 1, b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', 9, b'9', b'0', 0, 0, 0, 0, 0, 0,
// 2
// ]
// );
// }
//
// #[test]
// fn test_string_order() {
// fn to_vec_desc(s: &str) -> Vec<u8> {
// let mut ser = Serializer::new(vec![]);
// ser.set_reverse(true);
// s.serialize(&mut ser).unwrap();
// ser.into_inner()
// }
//
// for _ in 0..1000 {
// let s = rand_string(0..16);
// let a = s.clone() + &rand_string(0..16);
// let b = s + &rand_string(0..16);
//
// let ea = to_vec(&a).unwrap();
// let eb = to_vec(&b).unwrap();
// assert_eq!(a.cmp(&b), ea.cmp(&eb));
//
// let ra = to_vec_desc(&a);
// let rb = to_vec_desc(&b);
// assert_eq!(a.cmp(&b), ra.cmp(&rb).reverse());
// }
// }
//
// fn rand_string(len_range: std::ops::Range<usize>) -> String {
// let mut rng = rand::thread_rng();
// let len = rng.gen_range(len_range);
// rng.sample_iter(&Alphanumeric)
// .take(len)
// .map(char::from)
// .collect()
// }
//
// #[test]
// fn test_decimal() {
// let a = serialize_decimal(12_3456_7890_1234, 4);
// let b = serialize_decimal(0, 4);
// let c = serialize_decimal(-12_3456_7890_1234, 4);
// assert!(a > b && b > c);
// }
//
// fn serialize_decimal(mantissa: i128, scale: u8) -> Vec<u8> {
// let mut serializer = Serializer::new(vec![]);
// serializer.serialize_decimal(mantissa, scale).unwrap();
// serializer.into_inner()
// }
//
// #[test]
// fn test_decimal_e_m() {
// from: https://sqlite.org/src4/doc/trunk/www/key_encoding.wiki
// let cases = vec![
// (decimal, exponents, significand)
// ("1.0", 1, "02"),
// ("10.0", 1, "14"),
// ("10", 1, "14"),
// ("99.0", 1, "c6"),
// ("99.01", 1, "c7 02"),
// ("99.0001", 1, "c7 01 02"),
// ("100.0", 2, "02"),
// ("100.01", 2, "03 01 02"),
// ("100.1", 2, "03 01 14"),
// ("111.11", 2, "03 17 16"),
// ("1234", 2, "19 44"),
// ("9999", 2, "c7 c6"),
// ("9999.000001", 2, "c7 c7 01 01 02"),
// ("9999.000009", 2, "c7 c7 01 01 12"),
// ("9999.00001", 2, "c7 c7 01 01 14"),
// ("9999.00009", 2, "c7 c7 01 01 b4"),
// ("9999.000099", 2, "c7 c7 01 01 c6"),
// ("9999.0001", 2, "c7 c7 01 02"),
// ("9999.001", 2, "c7 c7 01 14"),
// ("9999.01", 2, "c7 c7 02"),
// ("9999.1", 2, "c7 c7 14"),
// ("10000", 3, "02"),
// ("10001", 3, "03 01 02"),
// ("12345", 3, "03 2f 5a"),
// ("123450", 3, "19 45 64"),
// ("1234.5", 2, "19 45 64"),
// ("12.345", 1, "19 45 64"),
// ("0.123", 0, "19 3c"),
// ("0.0123", 0, "03 2e"),
// ("0.00123", -1, "19 3c"),
// ("9223372036854775807", 10, "13 2d 43 91 07 89 6d 9b 75 0e"),
// ];
//
// for (decimal, exponents, significand) in cases {
// let d = decimal.parse::<rust_decimal::Decimal>().unwrap();
// let (exp, sig) = Serializer::<Vec<u8>>::decimal_e_m(d.mantissa(), d.scale() as u8);
// assert_eq!(exp, exponents, "wrong exponents for decimal: {decimal}");
// assert_eq!(
// sig.iter()
// .map(|b| format!("{b:02x}"))
// .collect::<Vec<_>>()
// .join(" "),
// significand,
// "wrong significand for decimal: {decimal}"
// );
// }
// }
//
// #[test]
// fn test_naivedate() {
// let a = serialize_naivedate(12_3456);
// let b = serialize_naivedate(0);
// let c = serialize_naivedate(-12_3456);
// assert!(a > b && b > c);
// }
//
// fn serialize_naivedate(days: i32) -> Vec<u8> {
// let mut serializer = Serializer::new(vec![]);
// serializer.serialize_naivedate(days).unwrap();
// serializer.into_inner()
// }
//
// #[test]
// fn test_naivetime() {
// let a = serialize_naivetime(23 * 3600 + 59 * 60 + 59, 1234_5678);
// let b = serialize_naivetime(12 * 3600, 1);
// let c = serialize_naivetime(12 * 3600, 0);
// let d = serialize_naivetime(0, 0);
// assert!(a > b && b > c && c > d);
// }
//
// fn serialize_naivetime(secs: u32, nano: u32) -> Vec<u8> {
// let mut serializer = Serializer::new(vec![]);
// serializer.serialize_naivetime(secs, nano).unwrap();
// serializer.into_inner()
// }
//
// #[test]
// fn test_naivedatetime() {
// let a = serialize_naivedatetime(12_3456_7890_1234, 1234_5678);
// let b = serialize_naivedatetime(0, 0);
// let c = serialize_naivedatetime(-12_3456_7890_1234, 1234_5678);
// assert!(a > b && b > c);
// }
//
// fn serialize_naivedatetime(secs: i64, nsecs: u32) -> Vec<u8> {
// let mut serializer = Serializer::new(vec![]);
// serializer.serialize_naivedatetime(secs, nsecs).unwrap();
// serializer.into_inner()
// }
//
// #[test]
// fn test_reverse_order() {
// Order: (ASC, DESC)
// let v1 = (0u8, 1i32);
// let v2 = (0u8, -1i32);
// let v3 = (1u8, -1i32);
//
// fn serialize(v: (u8, i32)) -> Vec<u8> {
// let mut ser = Serializer::new(vec![]);
// v.0.serialize(&mut ser).unwrap();
// ser.set_reverse(true);
// v.1.serialize(&mut ser).unwrap();
// ser.into_inner()
// }
// assert!(serialize(v1) < serialize(v2));
// assert!(serialize(v2) < serialize(v3));
// }
// }
