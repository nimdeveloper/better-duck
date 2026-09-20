//! Metadata-preserving DECIMAL value: [`DuckDecimal`].
//!
//! DuckDB's `DECIMAL(width, scale)` is a fixed-point number stored as a scaled
//! integer (the *mantissa*) whose declared `width` (total significant digits,
//! 1..=38) and `scale` (fractional digits, `<= width`) live in the column's
//! logical type, not in any per-row payload.
//!
//! [`DuckDecimal`] carries all three — mantissa, width, scale — so the declared
//! precision survives a round trip. This is deliberately available without the
//! `decimal` feature: the feature only adds [`rust_decimal`] interop, not basic
//! representability. Losing the width (as the old `rust_decimal`-only value did)
//! is exactly the defect this type fixes.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::{
    error::{DuckDBConversionError, Result},
    ffi::{
        duckdb_create_decimal, duckdb_create_decimal_type, duckdb_create_logical_type,
        duckdb_decimal, duckdb_decimal_scale, duckdb_decimal_width, duckdb_get_decimal,
        duckdb_logical_type, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    },
    types::{
        appendable::AppendAble,
        numeric::{hugeint_from_i128, i128_from_hugeint},
        DuckDialect, DuckLogicalType,
    },
};

use super::value::DuckValue;

/// The maximum DECIMAL width DuckDB supports (`DECIMAL(38, …)`).
pub const DECIMAL_MAX_WIDTH: u8 = 38;

/// A DuckDB `DECIMAL(width, scale)` value, preserving its declared precision.
///
/// The numeric value is `mantissa * 10^(-scale)`. For example the `DECIMAL(6, 2)`
/// value `1234.56` is `DuckDecimal { value: 123456, width: 6, scale: 2 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DuckDecimal {
    /// The scaled integer value (mantissa): the number with the decimal point removed.
    pub value: i128,
    /// Total number of significant digits, `1..=38`.
    pub width: u8,
    /// Number of fractional digits, `<= width`.
    pub scale: u8,
}

impl DuckDecimal {
    /// Creates a `DuckDecimal`, validating the declared precision.
    ///
    /// # Errors
    ///
    /// Returns [`DuckDBConversionError::ConversionError`] if `width` is not in
    /// `1..=38`, or if `scale > width` — the exact bounds DuckDB enforces on
    /// `DECIMAL(width, scale)`.
    pub fn new(
        value: i128,
        width: u8,
        scale: u8,
    ) -> Result<DuckDecimal, DuckDBConversionError> {
        if width == 0 || width > DECIMAL_MAX_WIDTH {
            return Err(DuckDBConversionError::ConversionError(format!(
                "DECIMAL width must be between 1 and {DECIMAL_MAX_WIDTH}, got {width}"
            )));
        }
        if scale > width {
            return Err(DuckDBConversionError::ConversionError(format!(
                "DECIMAL scale ({scale}) must not exceed width ({width})"
            )));
        }
        Ok(DuckDecimal { value, width, scale })
    }

    /// Builds a `DuckDecimal` from a raw `duckdb_decimal`, validating precision.
    pub(crate) fn from_ffi(raw: duckdb_decimal) -> Result<DuckDecimal, DuckDBConversionError> {
        DuckDecimal::new(i128_from_hugeint(raw.value), raw.width, raw.scale)
    }

    /// Returns the raw `duckdb_decimal` for this value.
    pub(crate) fn to_ffi(self) -> duckdb_decimal {
        duckdb_decimal {
            width: self.width,
            scale: self.scale,
            value: hugeint_from_i128(self.value),
        }
    }

    /// Creates a DuckDB `DECIMAL(width, scale)` logical type for this value's precision.
    ///
    /// The caller must destroy the returned type with `duckdb_destroy_logical_type`.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB rejects the width/scale (it returns null), which
    /// should not happen for a validated `DuckDecimal`.
    pub(crate) fn logical_type(&self) -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: `width`/`scale` are validated on construction to DuckDB's bounds.
        let lt = unsafe { duckdb_create_decimal_type(self.width, self.scale) };
        if lt.is_null() {
            return Err(DuckDBConversionError::ConversionError(format!(
                "DuckDB rejected DECIMAL({}, {})",
                self.width, self.scale
            )));
        }
        Ok(lt)
    }
}

/// Reads the declared width of a DECIMAL logical type.
///
/// # Safety
///
/// `logical_type` must be a valid DECIMAL `duckdb_logical_type`.
#[inline]
pub(crate) unsafe fn decimal_width(logical_type: duckdb_logical_type) -> u8 {
    // SAFETY: caller guarantees a valid DECIMAL logical type.
    unsafe { duckdb_decimal_width(logical_type) }
}

/// Reads the declared scale of a DECIMAL logical type.
///
/// # Safety
///
/// `logical_type` must be a valid DECIMAL `duckdb_logical_type`.
#[inline]
pub(crate) unsafe fn decimal_scale(logical_type: duckdb_logical_type) -> u8 {
    // SAFETY: caller guarantees a valid DECIMAL logical type.
    unsafe { duckdb_decimal_scale(logical_type) }
}

impl DuckDialect for DuckDecimal {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type DECIMAL.
        let raw = unsafe { duckdb_get_decimal(value) };
        DuckDecimal::from_ffi(raw)
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // SAFETY: `to_ffi` yields a fully initialized `duckdb_decimal` with a
        // validated width/scale.
        Ok(unsafe { duckdb_create_decimal(self.to_ffi()) })
    }
}

impl DuckLogicalType for DuckDecimal {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // A bare `DuckDecimal` type has no fixed precision; a specific value's type
        // comes from `DuckDecimal::logical_type`. Fall back to the default DECIMAL
        // for the type-only path (used by empty collections).
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL is a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL) })
    }
}

impl AppendAble for DuckDecimal {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        let dv = self.to_duck().map_err(crate::error::Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` is an owned value appended and destroyed here.
        unsafe { crate::types::appendable::append_owned_value(appender, dv) }
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let dv = self.to_duck().map_err(crate::error::Error::ConversionError)?;
        // SAFETY: `stmt`/`idx` are valid; `dv` is an owned value bound and destroyed here.
        unsafe { crate::types::appendable::bind_owned_value(stmt, idx, dv) }
    }
}

impl From<DuckDecimal> for DuckValue {
    fn from(d: DuckDecimal) -> Self {
        DuckValue::Decimal(d)
    }
}

// rust_decimal interop — checked conversion, not basic representability.
#[cfg(feature = "decimal")]
mod rust_decimal_interop {
    use super::{DuckDBConversionError, DuckDecimal, DECIMAL_MAX_WIDTH};
    use rust_decimal::Decimal;

    impl From<Decimal> for DuckDecimal {
        /// Converts a [`rust_decimal::Decimal`] into a `DuckDecimal`, computing the
        /// minimum width that holds the value at its scale.
        ///
        /// Infallible: `rust_decimal` holds a 96-bit mantissa (at most 29 significant
        /// digits) and a scale of 0..=28, so the required `DECIMAL(width, scale)`
        /// always fits DuckDB's 1..=38 bounds.
        fn from(d: Decimal) -> Self {
            let scale = d.scale() as u8; // <= 28 by rust_decimal's invariant
            let mantissa = d.mantissa();

            // Significant digits of the mantissa (at least 1, e.g. for 0).
            let digits = mantissa.unsigned_abs().checked_ilog10().map_or(1, |d| d as usize + 1);
            // Width must cover both the integer and fractional digits, and stay within
            // DuckDB's maximum. `digits` <= 29 and `scale` <= 28, so this never clamps a
            // real rust_decimal value; the `min` is a defensive cap, not truncation.
            let width = digits.max(scale as usize).max(1).min(DECIMAL_MAX_WIDTH as usize) as u8;

            // Width/scale are within bounds by construction; `new` cannot fail here.
            DuckDecimal::new(mantissa, width, scale).unwrap_or(DuckDecimal {
                value: mantissa,
                width: DECIMAL_MAX_WIDTH,
                scale,
            })
        }
    }

    impl TryFrom<DuckDecimal> for Decimal {
        type Error = DuckDBConversionError;

        /// Converts a `DuckDecimal` into a [`rust_decimal::Decimal`], preserving the
        /// scale. `rust_decimal` supports scales up to 28; a larger DuckDB scale is
        /// reported as precision loss rather than silently truncated.
        fn try_from(d: DuckDecimal) -> Result<Self, Self::Error> {
            if d.scale as u32 > 28 {
                return Err(DuckDBConversionError::PrecisionLoss(format!(
                    "DECIMAL scale {} exceeds rust_decimal's maximum of 28",
                    d.scale
                )));
            }
            Ok(Decimal::from_i128_with_scale(d.value, d.scale as u32))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_validates_width_and_scale() {
        assert!(DuckDecimal::new(0, 0, 0).is_err(), "width 0 rejected");
        assert!(DuckDecimal::new(0, 39, 0).is_err(), "width 39 rejected");
        assert!(DuckDecimal::new(0, 5, 6).is_err(), "scale > width rejected");
        let d = DuckDecimal::new(123456, 6, 2).unwrap();
        assert_eq!((d.value, d.width, d.scale), (123456, 6, 2));
    }

    #[test]
    fn hugeint_round_trips_including_negatives_and_extremes() {
        for v in [0_i128, 1, -1, i128::MAX, i128::MIN, 123456, -987654321] {
            assert_eq!(i128_from_hugeint(hugeint_from_i128(v)), v, "round trip {v}");
        }
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn rust_decimal_round_trip_preserves_scale_and_width() {
        use rust_decimal::Decimal;
        let original = Decimal::from_i128_with_scale(1234567, 3); // 1234.567
        let duck: DuckDecimal = original.into();
        assert_eq!(duck.scale, 3);
        assert!(duck.width >= 7);
        let back: Decimal = duck.try_into().unwrap();
        assert_eq!(back, original);
    }

    #[cfg(feature = "decimal")]
    #[test]
    fn oversized_scale_reports_precision_loss() {
        let duck = DuckDecimal::new(1, 38, 30).unwrap();
        assert!(matches!(
            rust_decimal::Decimal::try_from(duck),
            Err(DuckDBConversionError::PrecisionLoss(_))
        ));
    }
}
