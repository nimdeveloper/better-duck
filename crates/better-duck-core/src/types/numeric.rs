use super::{DuckDBConversionError, DuckDialect};
use crate::{
    error::Result,
    ffi::{
        duckdb_create_decimal, duckdb_create_double, duckdb_create_float, duckdb_create_hugeint,
        duckdb_create_int16, duckdb_create_int32, duckdb_create_int64, duckdb_create_int8,
        duckdb_create_uint16, duckdb_create_uint32, duckdb_create_uint64, duckdb_create_uint8,
        duckdb_decimal, duckdb_get_decimal, duckdb_get_double, duckdb_get_float,
        duckdb_get_hugeint, duckdb_get_int16, duckdb_get_int32, duckdb_get_int64, duckdb_get_int8,
        duckdb_get_uint16, duckdb_get_uint32, duckdb_get_uint64, duckdb_get_uint8, duckdb_hugeint,
        duckdb_value,
    },
    types::appendable::AppendAble,
};

use libduckdb_sys::{
    duckdb_append_double, duckdb_append_float, duckdb_append_hugeint, duckdb_append_int16,
    duckdb_append_int32, duckdb_append_int64, duckdb_append_int8, duckdb_append_uint16,
    duckdb_append_uint32, duckdb_append_uint64, duckdb_append_uint8, duckdb_bind_double,
    duckdb_bind_float, duckdb_bind_hugeint, duckdb_bind_int16, duckdb_bind_int32,
    duckdb_bind_int64, duckdb_bind_int8, duckdb_bind_uint16, duckdb_bind_uint32,
    duckdb_bind_uint64, duckdb_bind_uint8,
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

const MAX_SUPPORTED_I128: i128 = (i64::MAX as i128 + 1) * (u64::MAX as i128);

// Macro to implement DuckDialect for types
macro_rules! impl_duck_dialect {
    ($rust_type:ty, $duck_type:expr, $to_duck_fn:expr, $from_duck_fn:expr) => {
        impl DuckDialect for $rust_type {
            fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
                // if type_ != $duck_type {
                //     return Err(DuckDBConversionError::TypeMismatch {
                //         expected: $duck_type,
                //         found: type_,
                //     });
                // }
                Ok(unsafe { $from_duck_fn(value) })
            }

            fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
                Ok(unsafe { $to_duck_fn(*self) })
            }
        }
    };
}

macro_rules! impl_duck_append_able {
    ($rust_type:ty, $duck_append_fn:expr, $duck_bind_fn:expr) => {
        impl AppendAble for $rust_type {
            fn appender_append(
                &mut self,
                appender: crate::ffi::duckdb_appender,
            ) -> Result<()> {
                unsafe { $duck_append_fn(appender, *self) };
                Ok(())
            }
            fn stmt_append(
                &mut self,
                idx: u64,
                stmt: crate::ffi::duckdb_prepared_statement,
            ) -> Result<()> {
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

fn i128_from_hugeint(hugeint: duckdb_hugeint) -> i128 {
    let measure = u64::MAX as i128;
    (hugeint.upper as i128) * measure + (hugeint.lower as i128)
}

fn hugeint_from_i128(hugeint: i128) -> duckdb_hugeint {
    #[allow(clippy::manual_range_contains)]
    if hugeint > MAX_SUPPORTED_I128 || hugeint < -MAX_SUPPORTED_I128 {
        panic!("Unsupported! MAX:{}", MAX_SUPPORTED_I128); // TODO: Better error handling
    }
    let negative = hugeint < 0;
    let mut hugeint = hugeint;
    if negative {
        hugeint = -hugeint;
    }
    let measure = u64::MAX as i128;
    let mut value =
        duckdb_hugeint { upper: (hugeint / measure) as i64, lower: (hugeint % measure) as u64 };
    if negative {
        value.lower = u64::MAX - value.lower;
        value.upper = (!value.upper).wrapping_add((value.lower == 0) as i64);
    }
    value
}

impl DuckDialect for i128 {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT {
        //     return Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
        //         found: type_,
        //     });
        // }
        let hugeint: duckdb_hugeint = unsafe { duckdb_get_hugeint(value) };
        Ok(i128_from_hugeint(hugeint))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        Ok(unsafe { duckdb_create_hugeint(hugeint_from_i128(*self)) })
    }
}

impl AppendAble for i128 {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        unsafe { duckdb_append_hugeint(appender, hugeint_from_i128(*self)) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        unsafe { duckdb_bind_hugeint(stmt, idx, hugeint_from_i128(*self)) };
        Ok(())
    }
}

#[cfg(feature = "decimal")]
impl DuckDialect for Decimal {
    fn from_duck(value: duckdb_value) -> Result<Self, super::DuckDBConversionError>
    where
        Self: Sized,
    {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
        //     return Err(super::DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
        //         found: type_,
        //     });
        // }
        let decimal_value = unsafe { duckdb_get_decimal(value) };

        let scale = decimal_value.scale;
        // TODO: Do we need to handle precision?
        // let mut precision = 0;
        // precision = decimal_value.width;

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

        let mut num_width = format!("{}", value).len();
        if scale as usize >= num_width {
            num_width += scale as usize - num_width + 1; // for the decimal point
        }
        if value < 0 {
            num_width -= 1; // for the negative sign
        }

        let val = duckdb_decimal { scale, width: num_width as u8, value: hugeint_from_i128(value) };
        Ok(unsafe { duckdb_create_decimal(val) })
    }
}
#[cfg(feature = "decimal")]
impl AppendAble for Decimal {
    fn appender_append(
        &mut self,
        _appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        panic!("Decimal does not support appender append!");
        // unsafe { duckdb_append_decimal(appender, self.to_duck()?) };
    }
    fn stmt_append(
        &mut self,
        _idx: u64,
        _stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        panic!("Decimal does not support statement append!");
    }
}

#[cfg(test)]
mod test_numeric_conversion {
    use crate::ffi::duckdb_destroy_value;

    #[test]
    fn test_i8_conversion() {
        use super::*;
        let value: i8 = 42;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i8::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
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
        let converted_value = i128::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = 170141183460469231722463931679029329919;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = -5;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value: i128 = -170141183460469231722463931679029329919;
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = i128::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
    #[cfg(feature = "decimal")]
    #[test]
    fn test_decimal_conversion() {
        use super::*;

        let value = Decimal::from_i128_with_scale(-0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF, 0); // -79228162514264337593543950335
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(
            -0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF,
            Decimal::MAX_SCALE,
        ); // -7.9228162514264337593543950335
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(-42, 4); // -0.042
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(-42, 0); // -0.042
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(0, 4); // -0.042
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF, 0); // -79228162514264337593543950335
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };

        let value = Decimal::from_i128_with_scale(
            0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF,
            Decimal::MAX_SCALE,
        ); // 7.9228162514264337593543950335
        let mut duck_value = value.to_duck().unwrap();
        let converted_value = Decimal::from_duck(duck_value).unwrap();
        assert_eq!(value, converted_value);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
}
