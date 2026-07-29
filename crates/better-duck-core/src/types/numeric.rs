// The `AppendAble` and `DuckDialect` implementations accept raw FFI pointer parameters
// by design. Implementations are responsible for passing valid pointers.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use super::{value::DuckValue, DuckDBConversionError, DuckDialect, DuckLogicalType};
use crate::{
    error::Result,
    ffi::{
        duckdb_create_double, duckdb_create_float, duckdb_create_hugeint, duckdb_create_int16,
        duckdb_create_int32, duckdb_create_int64, duckdb_create_int8, duckdb_create_logical_type,
        duckdb_create_uhugeint, duckdb_create_uint16, duckdb_create_uint32, duckdb_create_uint64,
        duckdb_create_uint8, duckdb_get_double, duckdb_get_float, duckdb_get_int16,
        duckdb_get_int32, duckdb_get_int64, duckdb_get_int8, duckdb_get_uint16, duckdb_get_uint32,
        duckdb_get_uint64, duckdb_get_uint8, duckdb_hugeint, duckdb_logical_type, duckdb_uhugeint,
        duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
        DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    },
    types::appendable::AppendAble,
};

#[cfg(feature = "decimal")]
use crate::error::Error;
use crate::ffi::{
    duckdb_append_double, duckdb_append_float, duckdb_append_hugeint, duckdb_append_int16,
    duckdb_append_int32, duckdb_append_int64, duckdb_append_int8, duckdb_append_uhugeint,
    duckdb_append_uint16, duckdb_append_uint32, duckdb_append_uint64, duckdb_append_uint8,
    duckdb_bind_double, duckdb_bind_float, duckdb_bind_hugeint, duckdb_bind_int16,
    duckdb_bind_int32, duckdb_bind_int64, duckdb_bind_int8, duckdb_bind_uhugeint,
    duckdb_bind_uint16, duckdb_bind_uint32, duckdb_bind_uint64, duckdb_bind_uint8,
};
#[cfg(feature = "decimal")]
use crate::ffi::{
    duckdb_create_decimal, duckdb_decimal, duckdb_get_decimal, DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

// Macro to implement DuckDialect for primitive numeric types.
macro_rules! impl_duck_dialect {
    ($rust_type:ty, $duck_type:expr, $to_duck_fn:expr, $from_duck_fn:expr) => {
        impl DuckDialect for $rust_type {
            fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
                // SAFETY: `value` is a valid duckdb_value of the matching DuckDB type.
                // The caller is responsible for passing the correct type.
                Ok(unsafe { $from_duck_fn(value) })
            }

            fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
                // SAFETY: The value passed is a copy of a valid Rust primitive.
                // `duckdb_create_*` functions accept any value of the matching type.
                Ok(unsafe { $to_duck_fn(*self) })
            }
        }
    };
}

// Macro to implement AppendAble for primitive numeric types.
//
// `idx` is always the **1-based** parameter index supplied by `Statement::bind`.
macro_rules! impl_duck_append_able {
    ($rust_type:ty, $duck_append_fn:expr, $duck_bind_fn:expr) => {
        impl AppendAble for $rust_type {
            fn appender_append(
                &mut self,
                appender: crate::ffi::duckdb_appender,
            ) -> Result<()> {
                // SAFETY: `appender` is a valid duckdb_appender. The value is a copy of
                // a valid Rust primitive compatible with the DuckDB column type.
                unsafe { $duck_append_fn(appender, *self) };
                Ok(())
            }
            fn stmt_append(
                &mut self,
                idx: u64,
                stmt: crate::ffi::duckdb_prepared_statement,
            ) -> Result<()> {
                // SAFETY: `stmt` is a valid duckdb_prepared_statement. `idx` is a 1-based
                // parameter index within the statement's parameter count, as required by
                // the DuckDB C API. The value is a copy of a valid Rust primitive.
                unsafe { $duck_bind_fn(stmt, idx, *self) };
                Ok(())
            }
        }
    };
}

impl_duck_dialect!(i8, DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, duckdb_create_int8, duckdb_get_int8);
impl_duck_append_able!(i8, duckdb_append_int8, duckdb_bind_int8);
impl_duck_dialect!(u8, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, duckdb_create_uint8, duckdb_get_uint8);
impl_duck_append_able!(u8, duckdb_append_uint8, duckdb_bind_uint8);
impl_duck_dialect!(i16, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, duckdb_create_int16, duckdb_get_int16);
impl_duck_append_able!(i16, duckdb_append_int16, duckdb_bind_int16);
impl_duck_dialect!(u16, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, duckdb_create_uint16, duckdb_get_uint16);
impl_duck_append_able!(u16, duckdb_append_uint16, duckdb_bind_uint16);
impl_duck_dialect!(i32, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, duckdb_create_int32, duckdb_get_int32);
impl_duck_append_able!(i32, duckdb_append_int32, duckdb_bind_int32);
impl_duck_dialect!(u32, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, duckdb_create_uint32, duckdb_get_uint32);
impl_duck_append_able!(u32, duckdb_append_uint32, duckdb_bind_uint32);
impl_duck_dialect!(i64, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, duckdb_create_int64, duckdb_get_int64);
impl_duck_append_able!(i64, duckdb_append_int64, duckdb_bind_int64);
impl_duck_dialect!(u64, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT, duckdb_create_uint64, duckdb_get_uint64);
impl_duck_append_able!(u64, duckdb_append_uint64, duckdb_bind_uint64);
impl_duck_dialect!(f32, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, duckdb_create_float, duckdb_get_float);
impl_duck_append_able!(f32, duckdb_append_float, duckdb_bind_float);
impl_duck_dialect!(f64, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, duckdb_create_double, duckdb_get_double);
impl_duck_append_able!(f64, duckdb_append_double, duckdb_bind_double);

// DuckLogicalType + From<T> for DuckValue — one fixed DuckDB type per Rust numeric type.
macro_rules! impl_duck_logical_type_and_from {
    ($rust_type:ty, $duck_type:expr, $variant:ident) => {
        impl DuckLogicalType for $rust_type {
            fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
                // SAFETY: `$duck_type` is always a valid duckdb_type constant.
                Ok(unsafe { duckdb_create_logical_type($duck_type) })
            }
        }
        impl From<$rust_type> for DuckValue {
            fn from(v: $rust_type) -> Self {
                DuckValue::$variant(v)
            }
        }
    };
}

impl_duck_logical_type_and_from!(i8, DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, TinyInt);
impl_duck_logical_type_and_from!(i16, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, SmallInt);
impl_duck_logical_type_and_from!(i32, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, Int);
impl_duck_logical_type_and_from!(i64, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, BigInt);
impl_duck_logical_type_and_from!(i128, DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, HugeInt);
impl_duck_logical_type_and_from!(u8, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, UTinyInt);
impl_duck_logical_type_and_from!(u16, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, USmallInt);
impl_duck_logical_type_and_from!(u32, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, UInt);
impl_duck_logical_type_and_from!(u64, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT, UBigInt);
impl_duck_logical_type_and_from!(f32, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, Float);
impl_duck_logical_type_and_from!(f64, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, Double);

impl DuckLogicalType for u128 {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT) })
    }
}
impl From<u128> for DuckValue {
    fn from(v: u128) -> Self {
        DuckValue::UHugeInt(v)
    }
}

#[cfg(feature = "decimal")]
impl DuckLogicalType for Decimal {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL) })
    }
}
#[cfg(feature = "decimal")]
impl From<Decimal> for DuckValue {
    fn from(v: Decimal) -> Self {
        DuckValue::Decimal(v)
    }
}

/// Decode a DuckDB HUGEINT (two's-complement 128-bit) into an [`i128`].
///
/// DuckDB stores HUGEINT as a signed `i64` upper half (bits 127–64) and an
/// unsigned `u64` lower half (bits 63–0): `value = upper * 2^64 + lower`.
/// Shifting `upper` left by 64 and OR-ing the zero-extended `lower` half
/// reconstructs the full two's-complement value for the entire `i128` range.
pub(crate) fn i128_from_hugeint(hugeint: duckdb_hugeint) -> i128 {
    (hugeint.upper as i128) << 64 | (hugeint.lower as i128)
}

/// Encode an [`i128`] as a DuckDB HUGEINT.
///
/// Truncating `as u64` extracts the low 64 bits; an arithmetic right-shift of 64
/// sign-extends the high bits into an `i64`.  The full `i128` range is supported.
pub(crate) fn hugeint_from_i128(value: i128) -> duckdb_hugeint {
    duckdb_hugeint { upper: (value >> 64) as i64, lower: value as u64 }
}

/// Decode a DuckDB UHUGEINT into a [`u128`], mirroring [`i128_from_hugeint`]
/// for the unsigned case: `value = upper * 2^64 + lower`.
pub(crate) fn u128_from_uhugeint(uhugeint: duckdb_uhugeint) -> u128 {
    (uhugeint.upper as u128) << 64 | (uhugeint.lower as u128)
}

/// Encode a [`u128`] as a DuckDB UHUGEINT, mirroring [`hugeint_from_i128`].
pub(crate) fn uhugeint_from_u128(value: u128) -> duckdb_uhugeint {
    duckdb_uhugeint { upper: (value >> 64) as u64, lower: value as u64 }
}

impl DuckDialect<duckdb_hugeint> for i128 {
    fn from_duck(hugeint: duckdb_hugeint) -> Result<Self, DuckDBConversionError> {
        Ok(i128_from_hugeint(hugeint))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // SAFETY: `hugeint_from_i128` converts any i128 to the correct duckdb_hugeint
        // two's-complement layout. The full i128 range is supported without panicking.
        Ok(unsafe { duckdb_create_hugeint(hugeint_from_i128(*self)) })
    }
}

impl AppendAble for i128 {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        // SAFETY: `appender` is a valid duckdb_appender. `hugeint_from_i128` converts the
        // value to a valid duckdb_hugeint.
        unsafe { duckdb_append_hugeint(appender, hugeint_from_i128(*self)) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        // SAFETY: `stmt` is a valid prepared statement. `idx` is a 1-based parameter index
        // within the statement's parameter count (as required by the DuckDB C API).
        // `hugeint_from_i128` converts the value to a valid duckdb_hugeint.
        unsafe { duckdb_bind_hugeint(stmt, idx, hugeint_from_i128(*self)) };
        Ok(())
    }
}

impl DuckDialect<duckdb_uhugeint> for u128 {
    fn from_duck(uhugeint: duckdb_uhugeint) -> Result<Self, DuckDBConversionError> {
        Ok(u128_from_uhugeint(uhugeint))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // SAFETY: `uhugeint_from_u128` converts any u128 to the correct duckdb_uhugeint
        // layout. The full u128 range is supported without panicking.
        Ok(unsafe { duckdb_create_uhugeint(uhugeint_from_u128(*self)) })
    }
}

impl AppendAble for u128 {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        // SAFETY: `appender` is a valid duckdb_appender. `uhugeint_from_u128` converts the
        // value to a valid duckdb_uhugeint.
        unsafe { duckdb_append_uhugeint(appender, uhugeint_from_u128(*self)) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        // SAFETY: `stmt` is a valid prepared statement. `idx` is a 1-based parameter index
        // within the statement's parameter count (as required by the DuckDB C API).
        // `uhugeint_from_u128` converts the value to a valid duckdb_uhugeint.
        unsafe { duckdb_bind_uhugeint(stmt, idx, uhugeint_from_u128(*self)) };
        Ok(())
    }
}

#[cfg(feature = "decimal")]
impl DuckDialect for Decimal {
    fn from_duck(value: duckdb_value) -> Result<Self, super::DuckDBConversionError>
    where
        Self: Sized,
    {
        // SAFETY: `value` is a valid duckdb_value of type DECIMAL.
        let decimal_value = unsafe { duckdb_get_decimal(value) };

        let scale = decimal_value.scale;
        // TODO: surface decimal_value.width (precision) for callers that need it

        let decimal =
            Decimal::from_i128_with_scale(i128_from_hugeint(decimal_value.value), scale as u32);
        Ok(decimal)
    }

    fn to_duck(&self) -> Result<duckdb_value, super::DuckDBConversionError> {
        let scale = self.scale();
        if scale > u8::MAX as u32 {
            return Err(super::DuckDBConversionError::PrecisionLoss(
                "Decimal scale exceeds maximum value of u8".to_string(),
            ));
        }
        let scale = scale as u8;
        let value = self.mantissa();

        // Digit count of `value` including a leading `-` for negatives, matching
        // `format!("{value}").len()` but without allocating a `String` on every call
        // (this runs once per appended/bound row).
        let digits = value.unsigned_abs().checked_ilog10().map_or(1, |d| d as usize + 1);
        let mut num_width = if value < 0 { digits + 1 } else { digits };
        if scale as usize >= num_width {
            num_width += scale as usize - num_width + 1;
        }
        if value < 0 {
            num_width -= 1;
        }

        let val = duckdb_decimal { scale, width: num_width as u8, value: hugeint_from_i128(value) };
        // SAFETY: `val` is a fully initialized `duckdb_decimal` with valid scale/width.
        Ok(unsafe { duckdb_create_decimal(val) })
    }
}

#[cfg(feature = "decimal")]
impl AppendAble for Decimal {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        use crate::types::DuckDialect as _;
        let mut dv = self.to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` was just created by `to_duck()`.
        unsafe { crate::ffi::duckdb_append_value(appender, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { crate::ffi::duckdb_destroy_value(&mut dv) };
        Ok(())
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        use crate::types::DuckDialect as _;
        let mut dv = self.to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `stmt` is valid; `dv` was just created by `to_duck()`.
        unsafe { crate::ffi::duckdb_bind_value(stmt, idx, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { crate::ffi::duckdb_destroy_value(&mut dv) };
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod test_numeric_conversion {
    use crate::ffi::{duckdb_destroy_value, duckdb_get_hugeint, duckdb_get_uhugeint};

    /// Regression test: appending a `Decimal` whose dynamically-computed width (from its
    /// own digit count) is narrower than the target column's declared width used to
    /// dereference an invalid pointer on read — `from_duckdb_vec`'s DECIMAL arm cast the
    /// raw scaled-integer payload straight to a `duckdb_value` handle instead of reading
    /// it as the packed integer it actually is. See `value.rs`'s DECIMAL read arm.
    #[cfg(feature = "decimal")]
    #[test]
    fn decimal_appender_narrow_width_into_wide_column_roundtrips() {
        use crate::connection::Connection;
        use crate::types::value::DuckValue;
        use rust_decimal::Decimal;

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (v DECIMAL(18,4))").unwrap();
        let expected: Vec<Decimal> = (0i64..1_000).map(|i| Decimal::new(i * 12345, 4)).collect();
        {
            let mut app = conn.appender("t", "main").unwrap();
            for v in &expected {
                app.append(&mut v.clone()).unwrap();
            }
            app.save().unwrap();
        }
        let result = conn.execute("SELECT v FROM t").unwrap();
        let mut actual: Vec<Decimal> = Vec::with_capacity(expected.len());
        for row in result {
            match row.unwrap().get("v").unwrap() {
                DuckValue::Decimal(got) => actual.push(*got),
                other => panic!("expected Decimal, got {other:?}"),
            }
        }
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        actual.sort();
        assert_eq!(actual, expected_sorted);
    }

    #[test]
    fn test_i8_conversion() {
        use super::*;
        let value: i8 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i8::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        // SAFETY: `duck_value` is a valid duckdb_value created by `to_duck`.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u8_conversion() {
        use super::*;
        let value: u8 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u8::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_i16_conversion() {
        use super::*;
        let value: i16 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i16::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u16_conversion() {
        use super::*;
        let value: u16 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u16::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_i32_conversion() {
        use super::*;
        let value: i32 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i32::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u32_conversion() {
        use super::*;
        let value: u32 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u32::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_i64_conversion() {
        use super::*;
        let value: i64 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i64::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u64_conversion() {
        use super::*;
        let value: u64 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u64::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_f32_conversion() {
        use super::*;
        let value: f32 = 42.0;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = f32::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_f64_conversion() {
        use super::*;
        let value: f64 = 42.0;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = f64::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_i128_conversion() {
        use super::*;

        let value: i128 = 5;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(unsafe { duckdb_get_hugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = 170_141_183_460_469_231_722_463_931_679_029_329_919;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(unsafe { duckdb_get_hugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = -5;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(unsafe { duckdb_get_hugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = -170_141_183_460_469_231_722_463_931_679_029_329_919;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(unsafe { duckdb_get_hugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u128_conversion() {
        use super::*;

        let value: u128 = 5;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u128::from_duck(unsafe { duckdb_get_uhugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: u128 = u128::MAX;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = u128::from_duck(unsafe { duckdb_get_uhugeint(duck_value) }).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[test]
    fn test_u128_appendable_round_trip() {
        use crate::connection::Connection;
        use crate::types::value::DuckValue;

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (v UHUGEINT)").unwrap();
        let expected: Vec<u128> = vec![0, 5, u64::MAX as u128, u128::MAX];
        {
            let mut app = conn.appender("t", "main").unwrap();
            for v in &expected {
                app.append(&mut { *v }).unwrap();
            }
            app.save().unwrap();
        }
        let result = conn.execute("SELECT v FROM t").unwrap();
        let mut actual: Vec<u128> = Vec::with_capacity(expected.len());
        for row in result {
            match row.unwrap().get("v").unwrap() {
                DuckValue::UHugeInt(got) => actual.push(*got),
                other => panic!("expected UHugeInt, got {other:?}"),
            }
        }
        actual.sort_unstable();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort_unstable();
        assert_eq!(actual, expected_sorted);
    }
    #[cfg(feature = "decimal")]
    #[test]
    fn test_decimal_conversion() {
        use super::*;

        let value = Decimal::from_i128_with_scale(-0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF, 0);
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(
            -0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF,
            Decimal::MAX_SCALE,
        );
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(-42, 4);
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(-42, 0);
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(0, 4);
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF, 0);
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(
            0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF,
            Decimal::MAX_SCALE,
        );
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
}
