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

use std::convert::TryFrom;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut};
use risingwave_pb::data::DataType as ProstDataType;
use serde::{Deserialize, Serialize};

use crate::array::{ArrayError, ArrayResult, NULL_VAL_FOR_HASH};
mod native_type;
mod ops;
mod scalar_impl;

use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::str::FromStr;

pub use native_type::*;
use risingwave_pb::data::data_type::IntervalType::*;
use risingwave_pb::data::data_type::{IntervalType, TypeName};
pub use scalar_impl::*;
pub mod chrono_wrapper;
pub mod decimal;
pub mod interval;

mod ordered_float;

use chrono::{Datelike, Timelike};
pub use chrono_wrapper::{
    NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, UNIX_EPOCH_DAYS,
};
pub use decimal::Decimal;
pub use interval::*;
use itertools::Itertools;
pub use ops::CheckedAdd;
pub use ordered_float::IntoOrdered;
use paste::paste;
use prost::Message;
use risingwave_pb::expr::{ListValue as ProstListValue, StructValue as ProstStructValue};

use crate::array::{
    read_interval_unit, ArrayBuilderImpl, ListRef, ListValue, PrimitiveArrayItemType, StructRef,
    StructValue,
};

/// Parallel unit is the minimal scheduling unit.
pub type ParallelUnitId = u32;

// VirtualNode (a.k.a. VNode) is a minimal partition that a set of keys belong to. It is used for
// consistent hashing.
pub type VirtualNode = u8;
pub const VIRTUAL_NODE_SIZE: usize = std::mem::size_of::<VirtualNode>();
pub const VNODE_BITS: usize = 8;
pub const VIRTUAL_NODE_COUNT: usize = 1 << VNODE_BITS;

pub type OrderedF32 = ordered_float::OrderedFloat<f32>;
pub type OrderedF64 = ordered_float::OrderedFloat<f64>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Date,
    Varchar,
    Time,
    Timestamp,
    Timestampz,
    Interval,
    Struct { fields: Arc<[DataType]> },
    List { datatype: Box<DataType> },
}

pub fn unnested_list_type(datatype: DataType) -> DataType {
    match datatype {
        DataType::List { datatype } => unnested_list_type(*datatype),
        _ => datatype,
    }
}

impl From<&ProstDataType> for DataType {
    #[expect(clippy::needless_borrow)]
    fn from(proto: &ProstDataType) -> DataType {
        match proto.get_type_name().expect("missing type field") {
            TypeName::Int16 => DataType::Int16,
            TypeName::Int32 => DataType::Int32,
            TypeName::Int64 => DataType::Int64,
            TypeName::Float => DataType::Float32,
            TypeName::Double => DataType::Float64,
            TypeName::Boolean => DataType::Boolean,
            TypeName::Varchar => DataType::Varchar,
            TypeName::Date => DataType::Date,
            TypeName::Time => DataType::Time,
            TypeName::Timestamp => DataType::Timestamp,
            TypeName::Timestampz => DataType::Timestampz,
            TypeName::Decimal => DataType::Decimal,
            TypeName::Interval => DataType::Interval,
            TypeName::Struct => {
                let fields: Vec<DataType> = proto.field_type.iter().map(|f| f.into()).collect_vec();
                DataType::Struct {
                    fields: fields.into(),
                }
            }
            TypeName::List => DataType::List {
                // The first (and only) item is the list element type.
                datatype: Box::new((&proto.field_type[0]).into()),
            },
            TypeName::TypeUnspecified => unreachable!(),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => f.write_str("boolean"),
            DataType::Int16 => f.write_str("smallint"),
            DataType::Int32 => f.write_str("integer"),
            DataType::Int64 => f.write_str("bigint"),
            DataType::Float32 => f.write_str("real"),
            DataType::Float64 => f.write_str("double precision"),
            DataType::Decimal => f.write_str("numeric"),
            DataType::Date => f.write_str("date"),
            DataType::Varchar => f.write_str("character varying"),
            DataType::Time => f.write_str("time without time zone"),
            DataType::Timestamp => f.write_str("timestamp without time zone"),
            DataType::Timestampz => f.write_str("timestamp with time zone"),
            DataType::Interval => f.write_str("interval"),
            DataType::Struct { .. } => f.write_str("record"),
            DataType::List { datatype } => write!(f, "{}[]", datatype),
        }
    }
}

impl DataType {
    pub fn create_array_builder(&self, capacity: usize) -> ArrayBuilderImpl {
        use crate::array::*;
        match self {
            DataType::Boolean => BoolArrayBuilder::new(capacity).into(),
            DataType::Int16 => PrimitiveArrayBuilder::<i16>::new(capacity).into(),
            DataType::Int32 => PrimitiveArrayBuilder::<i32>::new(capacity).into(),
            DataType::Int64 => PrimitiveArrayBuilder::<i64>::new(capacity).into(),
            DataType::Float32 => PrimitiveArrayBuilder::<OrderedF32>::new(capacity).into(),
            DataType::Float64 => PrimitiveArrayBuilder::<OrderedF64>::new(capacity).into(),
            DataType::Decimal => DecimalArrayBuilder::new(capacity).into(),
            DataType::Date => NaiveDateArrayBuilder::new(capacity).into(),
            DataType::Varchar => Utf8ArrayBuilder::new(capacity).into(),
            DataType::Time => NaiveTimeArrayBuilder::new(capacity).into(),
            DataType::Timestamp => NaiveDateTimeArrayBuilder::new(capacity).into(),
            DataType::Timestampz => PrimitiveArrayBuilder::<i64>::new(capacity).into(),
            DataType::Interval => IntervalArrayBuilder::new(capacity).into(),
            DataType::Struct { fields } => StructArrayBuilder::with_meta(
                capacity,
                ArrayMeta::Struct {
                    children: fields.clone(),
                },
            )
            .into(),
            DataType::List { datatype } => ListArrayBuilder::with_meta(
                capacity,
                ArrayMeta::List {
                    datatype: datatype.clone(),
                },
            )
            .into(),
        }
    }

    pub fn prost_type_name(&self) -> TypeName {
        match self {
            DataType::Int16 => TypeName::Int16,
            DataType::Int32 => TypeName::Int32,
            DataType::Int64 => TypeName::Int64,
            DataType::Float32 => TypeName::Float,
            DataType::Float64 => TypeName::Double,
            DataType::Boolean => TypeName::Boolean,
            DataType::Varchar => TypeName::Varchar,
            DataType::Date => TypeName::Date,
            DataType::Time => TypeName::Time,
            DataType::Timestamp => TypeName::Timestamp,
            DataType::Timestampz => TypeName::Timestampz,
            DataType::Decimal => TypeName::Decimal,
            DataType::Interval => TypeName::Interval,
            DataType::Struct { .. } => TypeName::Struct,
            DataType::List { .. } => TypeName::List,
        }
    }

    pub fn to_protobuf(&self) -> ProstDataType {
        let field_type = match self {
            DataType::Struct { fields } => fields.iter().map(|f| f.to_protobuf()).collect_vec(),
            DataType::List { datatype } => vec![datatype.to_protobuf()],
            _ => vec![],
        };
        ProstDataType {
            type_name: self.prost_type_name() as i32,
            is_nullable: true,
            field_type,
            ..Default::default()
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal
        )
    }

    /// Checks if memcomparable encoding of datatype is equivalent to its value encoding.
    pub fn mem_cmp_eq_value_enc(&self) -> bool {
        use DataType::*;
        match self {
            Boolean | Int16 | Int32 | Int64 => true,
            Float32 | Float64 | Decimal | Date | Varchar | Time | Timestamp | Timestampz
            | Interval => false,
            Struct { fields } => fields.iter().all(|dt| dt.mem_cmp_eq_value_enc()),
            List { datatype } => datatype.mem_cmp_eq_value_enc(),
        }
    }
}

/// `Scalar` is a trait over all possible owned types in the evaluation
/// framework.
///
/// `Scalar` is reciprocal to `ScalarRef`. Use `as_scalar_ref` to get a
/// reference which has the same lifetime as `self`.
pub trait Scalar:
    std::fmt::Debug
    + Send
    + Sync
    + 'static
    + Clone
    + std::fmt::Debug
    + TryFrom<ScalarImpl, Error = ArrayError>
    + Into<ScalarImpl>
{
    /// Type for reference of `Scalar`
    type ScalarRefType<'a>: ScalarRef<'a, ScalarType = Self> + 'a
    where
        Self: 'a;

    /// Get a reference to current scalar.
    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_>;

    fn to_scalar_value(self) -> ScalarImpl;
}

/// Convert an `Option<Scalar>` to corresponding `Option<ScalarRef>`.
pub fn option_as_scalar_ref<S: Scalar>(scalar: &Option<S>) -> Option<S::ScalarRefType<'_>> {
    scalar.as_ref().map(|x| x.as_scalar_ref())
}

/// Convert an `Option<ScalarRef>` to corresponding `Option<Scalar>`.
pub fn option_to_owned_scalar<S: Scalar>(scalar: &Option<S::ScalarRefType<'_>>) -> Option<S> {
    scalar.map(|x| x.to_owned_scalar())
}

/// `ScalarRef` is a trait over all possible references in the evaluation
/// framework.
///
/// `ScalarRef` is reciprocal to `Scalar`. Use `to_owned_scalar` to get an
/// owned scalar.
pub trait ScalarRef<'a>:
    Copy
    + std::fmt::Debug
    + 'a
    + TryFrom<ScalarRefImpl<'a>, Error = ArrayError>
    + Into<ScalarRefImpl<'a>>
{
    /// `ScalarType` is the owned type of current `ScalarRef`.
    type ScalarType: Scalar<ScalarRefType<'a> = Self>;

    /// Convert `ScalarRef` to an owned scalar.
    fn to_owned_scalar(&self) -> Self::ScalarType;
}

/// `for_all_scalar_variants` includes all variants of our scalar types. If you added a new scalar
/// type inside the project, be sure to add a variant here.
///
/// Every tuple has four elements, where
/// `{ enum variant name, function suffix name, scalar type, scalar ref type }`
#[macro_export]
macro_rules! for_all_scalar_variants {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Int16, int16, i16, i16 },
            { Int32, int32, i32, i32 },
            { Int64, int64, i64, i64 },
            { Float32, float32, OrderedF32, OrderedF32 },
            { Float64, float64, OrderedF64, OrderedF64 },
            { Utf8, utf8, String, &'scalar str },
            { Bool, bool, bool, bool },
            { Decimal, decimal, Decimal, Decimal  },
            { Interval, interval, IntervalUnit, IntervalUnit },
            { NaiveDate, naivedate, NaiveDateWrapper, NaiveDateWrapper },
            { NaiveDateTime, naivedatetime, NaiveDateTimeWrapper, NaiveDateTimeWrapper },
            { NaiveTime, naivetime, NaiveTimeWrapper, NaiveTimeWrapper },
            { Struct, struct, StructValue, StructRef<'scalar> },
            { List, list, ListValue, ListRef<'scalar> }
        }
    };
}

/// Define `ScalarImpl` and `ScalarRefImpl` with macro.
macro_rules! scalar_impl_enum {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        /// `ScalarImpl` embeds all possible scalars in the evaluation framework.
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub enum ScalarImpl {
            $( $variant_name($scalar) ),*
        }

        /// `ScalarRefImpl` embeds all possible scalar references in the evaluation
        /// framework.
        #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub enum ScalarRefImpl<'scalar> {
            $( $variant_name($scalar_ref) ),*
        }
    };
}

for_all_scalar_variants! { scalar_impl_enum }

pub type Datum = Option<ScalarImpl>;
pub type DatumRef<'a> = Option<ScalarRefImpl<'a>>;

/// Convert a [`Datum`] to a [`DatumRef`].
pub fn to_datum_ref(datum: &Datum) -> DatumRef<'_> {
    datum.as_ref().map(|d| d.as_scalar_ref_impl())
}

// TODO: specify `NULL FIRST` or `NULL LAST`.
pub fn serialize_datum_ref_into(
    datum_ref: &DatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    // By default, `null` is treated as largest in PostgreSQL.
    if let Some(datum_ref) = datum_ref {
        0u8.serialize(&mut *serializer)?;
        datum_ref.serialize(serializer)?;
    } else {
        1u8.serialize(serializer)?;
    }
    Ok(())
}

pub fn serialize_datum_ref_not_null_into(
    datum_ref: &DatumRef,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    datum_ref
        .as_ref()
        .expect("datum cannot be null")
        .serialize(serializer)
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
// TODO: specify `NULL FIRST` or `NULL LAST`.
pub fn serialize_datum_into(
    datum: &Datum,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    // By default, `null` is treated as largest in PostgreSQL.
    if let Some(datum) = datum {
        0u8.serialize(&mut *serializer)?;
        datum.serialize(serializer)?;
    } else {
        1u8.serialize(serializer)?;
    }
    Ok(())
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn serialize_datum_not_null_into(
    datum: &Datum,
    serializer: &mut memcomparable::Serializer<impl BufMut>,
) -> memcomparable::Result<()> {
    datum
        .as_ref()
        .expect("datum cannot be null")
        .serialize(serializer)
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn deserialize_datum_from(
    ty: &DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    let null_tag = u8::deserialize(&mut *deserializer)?;
    match null_tag {
        1 => Ok(None),
        0 => Ok(Some(ScalarImpl::deserialize(ty.clone(), deserializer)?)),
        _ => Err(memcomparable::Error::InvalidTagEncoding(null_tag as _)),
    }
}

// TODO(MrCroxx): turn Datum into a struct, and impl ser/de as its member functions.
pub fn deserialize_datum_not_null_from(
    ty: DataType,
    deserializer: &mut memcomparable::Deserializer<impl Buf>,
) -> memcomparable::Result<Datum> {
    Ok(Some(ScalarImpl::deserialize(ty, deserializer)?))
}

/// This trait is to implement `to_owned_datum` for `Option<ScalarImpl>`
pub trait ToOwnedDatum {
    /// implement `to_owned_datum` for `DatumRef` to convert to `Datum`
    fn to_owned_datum(self) -> Datum;
}

impl ToOwnedDatum for DatumRef<'_> {
    fn to_owned_datum(self) -> Datum {
        self.map(ScalarRefImpl::into_scalar_impl)
    }
}

/// `for_all_native_types` includes all native variants of our scalar types.
///
/// Specifically, it doesn't support u8/u16/u32/u64.
#[macro_export]
macro_rules! for_all_native_types {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { $crate::types::OrderedF32, Float32 },
            { $crate::types::OrderedF64, Float64 }
        }
    };
}

/// `impl_convert` implements several conversions for `Scalar`.
/// * `Scalar <-> ScalarImpl` with `From` and `TryFrom` trait.
/// * `ScalarRef <-> ScalarRefImpl` with `From` and `TryFrom` trait.
/// * `&ScalarImpl -> &Scalar` with `impl.as_int16()`.
/// * `ScalarImpl -> Scalar` with `impl.into_int16()`.
macro_rules! impl_convert {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        $(
            impl From<$scalar> for ScalarImpl {
                fn from(val: $scalar) -> Self {
                    ScalarImpl::$variant_name(val)
                }
            }

            impl TryFrom<ScalarImpl> for $scalar {
                type Error = ArrayError;

                fn try_from(val: ScalarImpl) -> ArrayResult<Self> {
                    match val {
                        ScalarImpl::$variant_name(scalar) => Ok(scalar),
                        other_scalar => bail!("cannot convert ScalarImpl::{} to concrete type", other_scalar.get_ident()),
                    }
                }
            }

            impl <'scalar> From<$scalar_ref> for ScalarRefImpl<'scalar> {
                fn from(val: $scalar_ref) -> Self {
                    ScalarRefImpl::$variant_name(val)
                }
            }

            impl <'scalar> TryFrom<ScalarRefImpl<'scalar>> for $scalar_ref {
                type Error = ArrayError;

                fn try_from(val: ScalarRefImpl<'scalar>) -> ArrayResult<Self> {
                    match val {
                        ScalarRefImpl::$variant_name(scalar_ref) => Ok(scalar_ref),
                        other_scalar => bail!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name)),
                    }
                }
            }

            paste! {
                impl ScalarImpl {
                    pub fn [<as_ $suffix_name>](&self) -> &$scalar {
                        match self {
                        Self::$variant_name(ref scalar) => scalar,
                        other_scalar => panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }

                    pub fn [<into_ $suffix_name>](self) -> $scalar {
                        match self {
                            Self::$variant_name(scalar) => scalar,
                            other_scalar =>  panic!("cannot convert ScalarImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }

                impl <'scalar> ScalarRefImpl<'scalar> {
                    // Note that this conversion consume self.
                    pub fn [<into_ $suffix_name>](self) -> $scalar_ref {
                        match self {
                            Self::$variant_name(inner) => inner,
                            other_scalar => panic!("cannot convert ScalarRefImpl::{} to concrete type {}", other_scalar.get_ident(), stringify!($variant_name))
                        }
                    }
                }
            }
        )*
    };
}

for_all_scalar_variants! { impl_convert }

// Implement `From<raw float>` for `ScalarImpl` manually
impl From<f32> for ScalarImpl {
    fn from(f: f32) -> Self {
        Self::Float32(f.into())
    }
}

impl From<f64> for ScalarImpl {
    fn from(f: f64) -> Self {
        Self::Float64(f.into())
    }
}

macro_rules! impl_scalar_impl_ref_conversion {
    ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
        impl ScalarImpl {
            /// Converts [`ScalarImpl`] to [`ScalarRefImpl`]
            pub fn as_scalar_ref_impl(&self) -> ScalarRefImpl<'_> {
                match self {
                    $(
                        Self::$variant_name(inner) => ScalarRefImpl::<'_>::$variant_name(inner.as_scalar_ref())
                    ), *
                }
            }
        }

        impl<'a> ScalarRefImpl<'a> {
            /// Converts [`ScalarRefImpl`] to [`ScalarImpl`]
            pub fn into_scalar_impl(self) -> ScalarImpl {
                match self {
                    $(
                        Self::$variant_name(inner) => ScalarImpl::$variant_name(inner.to_owned_scalar())
                    ), *
                }
            }
        }
    };
}

for_all_scalar_variants! { impl_scalar_impl_ref_conversion }

/// Should behave the same as [`crate::array::Array::hash_at`] for non-null items.
#[expect(clippy::derive_hash_xor_eq)]
impl Hash for ScalarImpl {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        macro_rules! impl_all_hash {
            ([$self:ident], $({ $variant_type:ty, $scalar_type:ident } ),*) => {
                match $self {
                    // Primitive types
                    $( Self::$scalar_type(inner) => {
                        NativeType::hash_wrapper(inner, state);
                    }, )*
                    Self::Interval(interval) => interval.hash(state),
                    Self::NaiveDate(naivedate) => naivedate.hash(state),
                    Self::NaiveTime(naivetime) => naivetime.hash(state),
                    Self::NaiveDateTime(naivedatetime) => naivedatetime.hash(state),

                    // Manually implemented
                    Self::Bool(b) => b.hash(state),
                    Self::Utf8(s) => state.write(s.as_bytes()),
                    Self::Decimal(decimal) => decimal.normalize().hash(state),
                    Self::Struct(v) => v.hash(state), // TODO: check if this is consistent with `StructArray::hash_at`
                    Self::List(v) => v.hash(state),   // TODO: check if this is consistent with `ListArray::hash_at`
                }
            };
        }
        for_all_native_types! { impl_all_hash, self }
    }
}

/// Feeds the raw scalar of `datum` to the given `state`, which should behave the same as
/// [`crate::array::Array::hash_at`]. NULL value will be carefully handled.
///
/// Caveats: this relies on the above implementation of [`Hash`].
pub fn hash_datum(datum: &Datum, state: &mut impl std::hash::Hasher) {
    match datum {
        Some(scalar) => scalar.hash(state),
        None => NULL_VAL_FOR_HASH.hash(state),
    }
}

impl Display for ScalarImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        macro_rules! impl_display_fmt {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(ref inner) => {
                        Display::fmt(inner, f)
                    }, )*
                }
            }
        }

        for_all_scalar_variants! { impl_display_fmt }
    }
}

impl Display for ScalarRefImpl<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        macro_rules! impl_display_fmt {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match self {
                    $( Self::$variant_name(inner) => {
                        Display::fmt(inner, f)
                    }, )*
                }
            }
        }
        for_all_scalar_variants! { impl_display_fmt }
    }
}

pub fn display_datum_ref(d: &DatumRef<'_>) -> String {
    match d {
        Some(s) => format!("{}", s),
        None => "NULL".to_string(),
    }
}

impl ScalarRefImpl<'_> {
    /// Serializes the scalar to binary format.
    pub fn binary_serialize(
        &self,
        ser: &mut pgtype_binary::Serializer,
    ) -> pgtype_binary::Result<()> {
        match self {
            Self::Int64(v) => v.serialize(ser)?,
            Self::Float32(v) => v.0.serialize(ser)?,
            Self::Float64(v) => v.0.serialize(ser)?,
            Self::Utf8(v) => v.serialize(ser)?,
            Self::Bool(v) => v.serialize(ser)?,
            Self::Int16(v) => v.serialize(ser)?,
            Self::Int32(v) => v.serialize(ser)?,
            Self::Decimal(v) => match v {
                Decimal::Normalized(v) => ser.serialize_decimal(Some(*v))?,
                Decimal::NaN | Decimal::PositiveINF | Decimal::NegativeINF => {
                    ser.serialize_decimal(None)?
                }
            },
            Self::NaiveDate(v) => ser.serialize_naivedate(v.0)?,
            Self::NaiveDateTime(v) => ser.serialize_naivedatetime(v.0)?,
            Self::NaiveTime(v) => ser.serialize_naivetime(v.0)?,
            Self::Struct(_) => {
                todo!("Don't support struct serialization yet")
            }
            Self::List(_) => {
                todo!("Don't support list serialization yet")
            }
            Self::Interval(_) => {
                todo!("Don't support interval serialization yet")
            }
        };
        Ok(())
    }

    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        match self {
            &Self::Int16(v) => v.serialize(ser)?,
            &Self::Int32(v) => v.serialize(ser)?,
            &Self::Int64(v) => v.serialize(ser)?,
            &Self::Float32(v) => v.serialize(ser)?,
            &Self::Float64(v) => v.serialize(ser)?,
            &Self::Utf8(v) => v.serialize(ser)?,
            &Self::Bool(v) => v.serialize(ser)?,
            &Self::Decimal(v) => {
                let (mantissa, scale) = v.mantissa_scale_for_serialization();
                ser.serialize_decimal(mantissa, scale)?;
            }
            Self::Interval(v) => v.serialize(ser)?,
            &Self::NaiveDate(v) => ser.serialize_naivedate(v.0.num_days_from_ce())?,
            &Self::NaiveDateTime(v) => {
                ser.serialize_naivedatetime(v.0.timestamp(), v.0.timestamp_subsec_nanos())?
            }
            &Self::NaiveTime(v) => {
                ser.serialize_naivetime(v.0.num_seconds_from_midnight(), v.0.nanosecond())?
            }
            &Self::Struct(StructRef::ValueRef { val }) => {
                ser.serialize_struct_or_list(val.to_protobuf_owned())?
            }
            &Self::List(ListRef::ValueRef { val }) => {
                ser.serialize_struct_or_list(val.to_protobuf_owned())?
            }
            _ => {
                panic!("Type is unable to be serialized.")
            }
        };
        Ok(())
    }
}

impl ScalarImpl {
    /// Serialize the scalar.
    pub fn serialize(
        &self,
        ser: &mut memcomparable::Serializer<impl BufMut>,
    ) -> memcomparable::Result<()> {
        self.as_scalar_ref_impl().serialize(ser)
    }

    /// Deserialize the scalar.
    pub fn deserialize(
        ty: DataType,
        de: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        use DataType as Ty;
        Ok(match ty {
            Ty::Int16 => Self::Int16(i16::deserialize(de)?),
            Ty::Int32 => Self::Int32(i32::deserialize(de)?),
            Ty::Int64 => Self::Int64(i64::deserialize(de)?),
            Ty::Float32 => Self::Float32(f32::deserialize(de)?.into()),
            Ty::Float64 => Self::Float64(f64::deserialize(de)?.into()),
            Ty::Varchar => Self::Utf8(String::deserialize(de)?),
            Ty::Boolean => Self::Bool(bool::deserialize(de)?),
            Ty::Decimal => Self::Decimal({
                let (mantissa, scale) = de.deserialize_decimal()?;
                match scale {
                    29 => Decimal::NegativeINF,
                    30 => Decimal::PositiveINF,
                    31 => Decimal::NaN,
                    _ => Decimal::from_i128_with_scale(mantissa, scale as u32),
                }
            }),
            Ty::Interval => Self::Interval(IntervalUnit::deserialize(de)?),
            Ty::Time => Self::NaiveTime({
                let (secs, nano) = de.deserialize_naivetime()?;
                NaiveTimeWrapper::with_secs_nano(secs, nano)?
            }),
            Ty::Timestamp => Self::NaiveDateTime({
                let (secs, nsecs) = de.deserialize_naivedatetime()?;
                NaiveDateTimeWrapper::with_secs_nsecs(secs, nsecs)?
            }),
            Ty::Timestampz => Self::Int64(i64::deserialize(de)?),
            Ty::Date => Self::NaiveDate({
                let days = de.deserialize_naivedate()?;
                NaiveDateWrapper::with_days(days)?
            }),
            Ty::Struct { fields: _ } => {
                let bytes = de.deserialize_struct_or_list()?;
                ScalarImpl::bytes_to_scalar(&bytes, &ty.to_protobuf()).unwrap()
            }
            Ty::List { datatype: _ } => {
                let bytes = de.deserialize_struct_or_list()?;
                ScalarImpl::bytes_to_scalar(&bytes, &ty.to_protobuf()).unwrap()
            }
        })
    }

    /// Deserialize the `data_size` of `input_data_type` in `storage_encoding`. This function will
    /// consume the offset of deserializer then return the length (without memcopy, only length
    /// calculation). The difference between `encoding_data_size` and `ScalarImpl::data_size` is
    /// that `ScalarImpl::data_size` calculates the `memory_length` of type instead of
    /// `storage_encoding`
    pub fn encoding_data_size(
        data_type: &DataType,
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<usize> {
        let base_position = deserializer.position();
        let null_tag = u8::deserialize(&mut *deserializer)?;
        match null_tag {
            1 => {}
            0 => {
                use std::mem::size_of;
                let len = match data_type {
                    DataType::Int16 => size_of::<i16>(),
                    DataType::Int32 => size_of::<i32>(),
                    DataType::Int64 => size_of::<i64>(),
                    DataType::Float32 => size_of::<OrderedF32>(),
                    DataType::Float64 => size_of::<OrderedF64>(),
                    DataType::Date => size_of::<NaiveDateWrapper>(),
                    DataType::Time => size_of::<NaiveTimeWrapper>(),
                    DataType::Timestamp => size_of::<NaiveDateTimeWrapper>(),
                    DataType::Timestampz => size_of::<i64>(),
                    DataType::Boolean => size_of::<u8>(),
                    DataType::Interval => size_of::<IntervalUnit>(),

                    DataType::Decimal => deserializer.read_decimal_len()?,
                    DataType::List { .. } | DataType::Struct { .. } => {
                        // these two types is var-length and should only be determine at runtime.
                        // TODO: need some test for this case (e.g. e2e test)
                        deserializer.read_struct_and_list_len()?
                    }
                    DataType::Varchar => deserializer.read_bytes_len()?,
                };

                // consume offset of fixed_type
                if deserializer.position() == base_position + 1 {
                    // fixed type
                    deserializer.advance(len);
                }
            }
            _ => return Err(memcomparable::Error::InvalidTagEncoding(null_tag as _)),
        }

        Ok(deserializer.position() - base_position)
    }

    pub fn to_protobuf(&self) -> Vec<u8> {
        let body = match self {
            ScalarImpl::Int16(v) => v.to_be_bytes().to_vec(),
            ScalarImpl::Int32(v) => v.to_be_bytes().to_vec(),
            ScalarImpl::Int64(v) => v.to_be_bytes().to_vec(),
            ScalarImpl::Float32(v) => v.to_be_bytes().to_vec(),
            ScalarImpl::Float64(v) => v.to_be_bytes().to_vec(),
            ScalarImpl::Utf8(s) => s.as_bytes().to_vec(),
            ScalarImpl::Bool(v) => (*v as i8).to_be_bytes().to_vec(),
            ScalarImpl::Decimal(v) => v.to_string().as_bytes().to_vec(),
            ScalarImpl::Interval(v) => v.to_protobuf_owned(),
            ScalarImpl::NaiveDate(v) => v.to_protobuf_owned(),
            ScalarImpl::NaiveDateTime(v) => v.to_protobuf_owned(),
            ScalarImpl::NaiveTime(v) => v.to_protobuf_owned(),
            ScalarImpl::Struct(v) => v.to_protobuf_owned(),
            ScalarImpl::List(v) => v.to_protobuf_owned(),
        };
        body
    }

    pub fn bytes_to_scalar(b: &Vec<u8>, data_type: &ProstDataType) -> ArrayResult<Self> {
        let value = match data_type.get_type_name()? {
            TypeName::Boolean => ScalarImpl::Bool(
                i8::from_be_bytes(
                    b.as_slice()
                        .try_into()
                        .map_err(|e| anyhow!("Failed to deserialize bool, reason: {:?}", e))?,
                ) == 1,
            ),
            TypeName::Int16 => ScalarImpl::Int16(i16::from_be_bytes(
                b.as_slice()
                    .try_into()
                    .map_err(|e| anyhow!("Failed to deserialize i16, reason: {:?}", e))?,
            )),
            TypeName::Int32 => ScalarImpl::Int32(i32::from_be_bytes(
                b.as_slice()
                    .try_into()
                    .map_err(|e| anyhow!("Failed to deserialize i32, reason: {:?}", e))?,
            )),
            TypeName::Int64 => ScalarImpl::Int64(i64::from_be_bytes(
                b.as_slice()
                    .try_into()
                    .map_err(|e| anyhow!("Failed to deserialize i64, reason: {:?}", e))?,
            )),
            TypeName::Float => ScalarImpl::Float32(
                f32::from_be_bytes(
                    b.as_slice()
                        .try_into()
                        .map_err(|e| anyhow!("Failed to deserialize f32, reason: {:?}", e))?,
                )
                .into(),
            ),
            TypeName::Double => ScalarImpl::Float64(
                f64::from_be_bytes(
                    b.as_slice()
                        .try_into()
                        .map_err(|e| anyhow!("Failed to deserialize f64, reason: {:?}", e))?,
                )
                .into(),
            ),
            TypeName::Varchar => ScalarImpl::Utf8(
                std::str::from_utf8(b)
                    .map_err(|e| anyhow!("Failed to deserialize varchar, reason: {:?}", e))?
                    .to_string(),
            ),
            TypeName::Decimal => ScalarImpl::Decimal(
                Decimal::from_str(std::str::from_utf8(b).unwrap())
                    .map_err(|e| anyhow!("Failed to deserialize decimal, reason: {:?}", e))?,
            ),
            TypeName::Interval => ScalarImpl::Interval(IntervalUnit::from_protobuf_bytes(
                b,
                data_type.get_interval_type().unwrap_or(Unspecified),
            )?),
            TypeName::Timestamp => {
                ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::from_protobuf_bytes(b)?)
            }
            TypeName::Time => ScalarImpl::NaiveTime(NaiveTimeWrapper::from_protobuf_bytes(b)?),
            TypeName::Date => ScalarImpl::NaiveDate(NaiveDateWrapper::from_protobuf_bytes(b)?),
            TypeName::Struct => {
                let struct_value: ProstStructValue = Message::decode(b.as_slice())?;
                let fields: Vec<Datum> = struct_value
                    .fields
                    .iter()
                    .zip_eq(data_type.field_type.iter())
                    .map(|(b, d)| {
                        if b.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(ScalarImpl::bytes_to_scalar(b, d)?))
                        }
                    })
                    .collect::<ArrayResult<Vec<Datum>>>()?;
                ScalarImpl::Struct(StructValue::new(fields))
            }
            TypeName::List => {
                let list_value: ProstListValue = Message::decode(b.as_slice())?;
                let d = &data_type.field_type[0];
                let fields: Vec<Datum> = list_value
                    .fields
                    .iter()
                    .map(|b| {
                        if b.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some(ScalarImpl::bytes_to_scalar(b, d)?))
                        }
                    })
                    .collect::<ArrayResult<Vec<Datum>>>()?;
                ScalarImpl::List(ListValue::new(fields))
            }
            _ => bail!("Unrecognized type name: {:?}", data_type.get_type_name()),
        };
        Ok(value)
    }
}

pub fn literal_type_match(data_type: &DataType, literal: Option<&ScalarImpl>) -> bool {
    match literal {
        Some(datum) => {
            matches!(
                (data_type, datum),
                (DataType::Boolean, ScalarImpl::Bool(_))
                    | (DataType::Int16, ScalarImpl::Int16(_))
                    | (DataType::Int32, ScalarImpl::Int32(_))
                    | (DataType::Int64, ScalarImpl::Int64(_))
                    | (DataType::Float32, ScalarImpl::Float32(_))
                    | (DataType::Float64, ScalarImpl::Float64(_))
                    | (DataType::Varchar, ScalarImpl::Utf8(_))
                    | (DataType::Date, ScalarImpl::NaiveDate(_))
                    | (DataType::Time, ScalarImpl::NaiveTime(_))
                    | (DataType::Timestamp, ScalarImpl::NaiveDateTime(_))
                    | (DataType::Timestampz, ScalarImpl::Int64(_))
                    | (DataType::Decimal, ScalarImpl::Decimal(_))
                    | (DataType::Interval, ScalarImpl::Interval(_))
                    | (DataType::Struct { .. }, ScalarImpl::Struct(_))
                    | (DataType::List { .. }, ScalarImpl::List(_))
            )
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Neg;

    use itertools::Itertools;
    use rand::thread_rng;

    use super::*;

    fn serialize_datum_not_null_into_vec(data: i64) -> Vec<u8> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serialize_datum_not_null_into(&Some(ScalarImpl::Int64(data)), &mut serializer).unwrap();
        serializer.into_inner()
    }

    #[test]
    fn test_memcomparable() {
        let memcmp_minus_1 = serialize_datum_not_null_into_vec(-1);
        let memcmp_3874 = serialize_datum_not_null_into_vec(3874);
        let memcmp_45745 = serialize_datum_not_null_into_vec(45745);
        let memcmp_21234 = serialize_datum_not_null_into_vec(21234);
        assert!(memcmp_3874 < memcmp_45745);
        assert!(memcmp_3874 < memcmp_21234);
        assert!(memcmp_21234 < memcmp_45745);

        assert!(memcmp_minus_1 < memcmp_3874);
        assert!(memcmp_minus_1 < memcmp_21234);
        assert!(memcmp_minus_1 < memcmp_45745);
    }

    #[test]
    fn test_issue_2057_ordered_float_memcomparable() {
        use num_traits::*;
        use rand::seq::SliceRandom;

        fn serialize(f: OrderedF32) -> Vec<u8> {
            let mut serializer = memcomparable::Serializer::new(vec![]);
            serialize_datum_not_null_into(&Some(f.into()), &mut serializer).unwrap();
            serializer.into_inner()
        }

        fn deserialize(data: Vec<u8>) -> OrderedF32 {
            let mut deserializer = memcomparable::Deserializer::new(data.as_slice());
            let datum =
                deserialize_datum_not_null_from(DataType::Float32, &mut deserializer).unwrap();
            datum.unwrap().try_into().unwrap()
        }

        let floats = vec![
            // -inf
            OrderedF32::neg_infinity(),
            // -1
            OrderedF32::one().neg(),
            // 0, -0 should be treated the same
            OrderedF32::zero(),
            OrderedF32::neg_zero(),
            OrderedF32::zero(),
            // 1
            OrderedF32::one(),
            // inf
            OrderedF32::infinity(),
            // nan, -nan should be treated the same
            OrderedF32::nan(),
            OrderedF32::nan().neg(),
            OrderedF32::nan(),
        ];
        assert!(floats.is_sorted());

        let mut floats_clone = floats.clone();
        floats_clone.shuffle(&mut thread_rng());
        floats_clone.sort();
        assert_eq!(floats, floats_clone);

        let memcomparables = floats.clone().into_iter().map(serialize).collect_vec();
        assert!(memcomparables.is_sorted());

        let decoded_floats = memcomparables.into_iter().map(deserialize).collect_vec();
        assert!(decoded_floats.is_sorted());
        assert_eq!(floats, decoded_floats);
    }

    #[test]
    fn test_size() {
        assert_eq!(std::mem::size_of::<StructValue>(), 16);
        assert_eq!(std::mem::size_of::<ListValue>(), 16);
        // TODO: try to reduce the memory usage of `Decimal`, `ScalarImpl` and `Datum`.
        assert_eq!(std::mem::size_of::<Decimal>(), 20);
        assert_eq!(std::mem::size_of::<ScalarImpl>(), 32);
        assert_eq!(std::mem::size_of::<Datum>(), 32);
    }

    #[test]
    fn test_protobuf_conversion() {
        let v = ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::default());
        let actual =
            ScalarImpl::bytes_to_scalar(&v.to_protobuf(), &DataType::Timestamp.to_protobuf())
                .unwrap();
        assert_eq!(v, actual);

        let v = ScalarImpl::NaiveDate(NaiveDateWrapper::default());
        let actual =
            ScalarImpl::bytes_to_scalar(&v.to_protobuf(), &DataType::Date.to_protobuf()).unwrap();
        assert_eq!(v, actual);

        let v = ScalarImpl::NaiveTime(NaiveTimeWrapper::default());
        let actual =
            ScalarImpl::bytes_to_scalar(&v.to_protobuf(), &DataType::Time.to_protobuf()).unwrap();
        assert_eq!(v, actual);
    }
}
