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

//! Template macro to generate code for unary/binary/ternary expression.

use std::fmt;
use std::sync::Arc;

use itertools::{multizip, Itertools};
use paste::paste;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayRef, BytesGuard, BytesWriter, DataChunk,
    Row, Utf8Array,
};
use risingwave_common::for_all_variants;
use risingwave_common::types::{option_as_scalar_ref, DataType, Datum, Scalar, ScalarImpl};

use crate::expr::{BoxedExpression, Expression};

macro_rules! array_impl_add_datum {
    ([$arr_builder: ident, $datum: ident], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match ($arr_builder, $datum) {
            $(
                (ArrayBuilderImpl::$variant_name(inner), Some(ScalarImpl::$variant_name(v))) => {
                    inner.append(Some(v.as_scalar_ref()))?;
                }
                (ArrayBuilderImpl::$variant_name(inner), None) => {
                    inner.append(None)?;
                }
            )*
            (_, _) => $crate::bail!(
                "Do not support values in insert values executor".to_string(),
            ),
        }
    };
}

macro_rules! gen_eval {
    { $macro:ident, $ty_name:ident, $OA:ty, $($arg:ident,)* } => {
        fn eval(&self, data_chunk: &DataChunk) -> $crate::Result<ArrayRef> {
            paste! {
                $(
                    let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval_checked(data_chunk)?;
                    let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
                )*

                let bitmap = data_chunk.get_visibility_ref();
                let mut output_array = <$OA as Array>::Builder::new(data_chunk.capacity());
                Ok(Arc::new(match bitmap {
                    Some(bitmap) => {
                        for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip_eq(bitmap.iter()) {
                            if !visible {
                                output_array.append_null()?;
                                continue;
                            }
                            $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                        }
                        output_array.finish()?.into()
                    }
                    None => {
                        for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                            $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                        }
                        output_array.finish()?.into()
                    }
                }))
            }
        }

        /// Currently, `eval_row()` first calls `eval_row()` on the inner expressions and the
        /// resulting datums are placed in their own arrays. The arrays are then handled in the same
        /// way as in `eval()`. This could be optimized to work on the datums directly
        /// instead of placing them in arrays.
        fn eval_row(&self, row: &Row) -> $crate::Result<Datum> {
            paste! {
                $(
                    let [<datum_ $arg:lower>] = self.[<expr_ $arg:lower>].eval_row(row)?;

                    let mut [<builder_ $arg:lower>] = self.[<expr_ $arg:lower>].return_type().create_array_builder(1);
                    let [<ref_ $arg:lower>] = &mut [<builder_ $arg:lower>];

                    for_all_variants! {array_impl_add_datum, [<ref_ $arg:lower>], [<datum_ $arg:lower>]}

                    let [<arr_ $arg:lower>] = [<builder_ $arg:lower>].finish().map(Arc::new)?;
                    let [<arr_ $arg:lower>]: &$arg = [<arr_ $arg:lower>].as_ref().into();
                )*

                let mut output_array = <$OA as Array>::Builder::new(1);
                for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                    $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                }
                let output_arrayimpl: ArrayImpl = output_array.finish()?.into();

                Ok(output_arrayimpl.to_datum())
            }
        }
    }
}

macro_rules! eval_normal {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let ret = ($self.func)($($arg, )*)?;
            let output = Some(ret.as_scalar_ref());
            $output_array.append(output)?;
        } else {
            $output_array.append(None)?;
        }
    }
}

macro_rules! gen_expr_normal {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> $crate::Result<OA::OwnedItem>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> $crate::Result<OA::OwnedItem> + Sized + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* OA, F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> $crate::Result<OA::OwnedItem> + Sized + Sync + Send,
            > Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
                for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_normal, $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> $crate::Result<OA::OwnedItem> + Sized + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
                #[allow(dead_code)]
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom : std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

macro_rules! eval_bytes {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let writer = $output_array.writer();
            let guard = ($self.func)($($arg, )* writer)?;
            $output_array = guard.into_inner();
        } else {
            $output_array.append(None)?;
        }
    }
}

macro_rules! gen_expr_bytes {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> $crate::Result<BytesGuard>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )*)>,
            }

            impl<$($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> $crate::Result<BytesGuard> + Sized + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            impl<$($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> $crate::Result<BytesGuard> + Sized + Sync + Send,
            > Expression for $ty_name<$($arg, )* F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_bytes, $ty_name, Utf8Array, $($arg, )* }
            }

            impl<$($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> $crate::Result<BytesGuard> + Sized + Sync + Send,
            > $ty_name<$($arg, )* F> {
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

macro_rules! eval_nullable {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        {
            let ret = ($self.func)($($arg,)*)?;
            $output_array.append(option_as_scalar_ref(&ret))?;
        }
    }
}

macro_rules! gen_expr_nullable {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> $crate::Result<Option<OA::OwnedItem>>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* OA, F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
            > Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
                for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_nullable, $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
                // Compile failed due to some GAT lifetime issues so make this field private.
                // Check issues #742.
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

gen_expr_normal!(UnaryExpression, { IA1 }, { 'ia1 });
gen_expr_normal!(BinaryExpression, { IA1, IA2 }, { 'ia1, 'ia2 });
gen_expr_normal!(TernaryExpression, { IA1, IA2, IA3 }, { 'ia1, 'ia2, 'ia3 });

gen_expr_bytes!(UnaryBytesExpression, { IA1 }, { 'ia1 });
gen_expr_bytes!(BinaryBytesExpression, { IA1, IA2 }, { 'ia1, 'ia2 });
gen_expr_bytes!(TernaryBytesExpression, { IA1, IA2, IA3 }, { 'ia1, 'ia2, 'ia3 });
gen_expr_bytes!(QuaternaryBytesExpression, { IA1, IA2, IA3, IA4 }, { 'ia1, 'ia2, 'ia3, 'ia4 });

gen_expr_nullable!(UnaryNullableExpression, { IA1 }, { 'ia1 });
gen_expr_nullable!(BinaryNullableExpression, { IA1, IA2 }, { 'ia1, 'ia2 });

/// `for_all_cmp_types` helps in matching and casting types when building comparison expressions
///  such as <= or IS DISTINCT FROM.
#[macro_export]
macro_rules! for_all_cmp_variants {
    ($macro:ident, $l:expr, $r:expr, $ret:expr, $general_f:ident) => {
        $macro! {
            [$l, $r, $ret],
            { int16, int16, int16, $general_f },
            { int16, int32, int32, $general_f },
            { int16, int64, int64, $general_f },
            { int16, float32, float64, $general_f },
            { int16, float64, float64, $general_f },
            { int32, int16, int32, $general_f },
            { int32, int32, int32, $general_f },
            { int32, int64, int64, $general_f },
            { int32, float32, float64, $general_f },
            { int32, float64, float64, $general_f },
            { int64, int16,int64, $general_f },
            { int64, int32,int64, $general_f },
            { int64, int64, int64, $general_f },
            { int64, float32, float64 , $general_f},
            { int64, float64, float64, $general_f },
            { float32, int16, float64, $general_f },
            { float32, int32, float64, $general_f },
            { float32, int64, float64 , $general_f},
            { float32, float32, float32, $general_f },
            { float32, float64, float64, $general_f },
            { float64, int16, float64, $general_f },
            { float64, int32, float64, $general_f },
            { float64, int64, float64, $general_f },
            { float64, float32, float64, $general_f },
            { float64, float64, float64, $general_f },
            { decimal, int16, decimal, $general_f },
            { decimal, int32, decimal, $general_f },
            { decimal, int64, decimal, $general_f },
            { decimal, float32, float64, $general_f },
            { decimal, float64, float64, $general_f },
            { int16, decimal, decimal, $general_f },
            { int32, decimal, decimal, $general_f },
            { int64, decimal, decimal, $general_f },
            { decimal, decimal, decimal, $general_f },
            { float32, decimal, float64, $general_f },
            { float64, decimal, float64, $general_f },
            { timestamp, timestamp, timestamp, $general_f },
            { interval, interval, interval, $general_f },
            { time, time, time, $general_f },
            { date, date, date, $general_f },
            { boolean, boolean, boolean, $general_f },
            { timestamp, date, timestamp, $general_f },
            { date, timestamp, timestamp, $general_f },
            { interval, time, interval, $general_f },
            { time, interval, interval, $general_f }
        }
    };
}
