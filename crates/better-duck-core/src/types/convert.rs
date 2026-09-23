//! Numeric and temporal conversion helpers over DuckDB's own routines.
//!
//! These wrap DuckDB's C conversions so callers get the *same* rounding/overflow
//! behaviour DuckDB uses internally, rather than a Rust re-implementation that might
//! diverge at the boundaries:
//!
//! * 128-bit ↔ `f64`: [`hugeint_to_double`](crate::hugeint_to_double)/[`double_to_hugeint`](crate::double_to_hugeint)
//!   and the unsigned [`uhugeint_to_double`](crate::uhugeint_to_double)/[`double_to_uhugeint`](crate::double_to_uhugeint).
//!   DuckDB returns `0` when a double
//!   is too large to fit, and cannot represent a non-finite double, so the
//!   double→int direction here rejects `NaN`/`±∞` and surfaces the overflow sentinel
//!   as an error instead of silently yielding `0`.
//! * `TIMESTAMP` ↔ broken-down parts: [`timestamp_micros_to_parts`](crate::timestamp_micros_to_parts) /
//!   [`parts_to_timestamp_micros`](crate::parts_to_timestamp_micros) via `duckdb_from_timestamp`/`duckdb_to_timestamp`.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::{
    error::{DuckDBConversionError, Result},
    ffi::{
        duckdb_date_struct, duckdb_double_to_hugeint, duckdb_double_to_uhugeint,
        duckdb_from_timestamp, duckdb_hugeint_to_double, duckdb_time_struct, duckdb_timestamp,
        duckdb_timestamp_struct, duckdb_to_timestamp, duckdb_uhugeint_to_double,
    },
    types::numeric::{
        hugeint_from_i128, i128_from_hugeint, u128_from_uhugeint, uhugeint_from_u128,
    },
};

/// Converts a 128-bit signed integer to the nearest `f64`, using DuckDB's own
/// conversion (`duckdb_hugeint_to_double`). Lossy beyond f64's 53-bit significand.
#[must_use]
pub fn hugeint_to_double(value: i128) -> f64 {
    // SAFETY: `hugeint_from_i128` yields a fully initialised `duckdb_hugeint`.
    unsafe { duckdb_hugeint_to_double(hugeint_from_i128(value)) }
}

/// Converts an `f64` to a 128-bit signed integer using DuckDB's own conversion
/// (`duckdb_double_to_hugeint`).
///
/// # Errors
///
/// Returns [`DuckDBConversionError::ConversionError`] if `value` is non-finite, or
/// too large to fit (DuckDB signals this by returning `0`, which this rejects unless
/// `value` actually rounds to zero).
pub fn double_to_hugeint(value: f64) -> Result<i128, DuckDBConversionError> {
    if !value.is_finite() {
        return Err(DuckDBConversionError::ConversionError(format!(
            "cannot convert non-finite value {value} to HUGEINT"
        )));
    }
    // SAFETY: `value` is finite; the call is a pure numeric conversion.
    let raw = unsafe { duckdb_double_to_hugeint(value) };
    let out = i128_from_hugeint(raw);
    // DuckDB returns 0 on overflow; distinguish a genuine zero (|value| < 1).
    if out == 0 && value.abs() >= 1.0 {
        return Err(DuckDBConversionError::ConversionError(format!(
            "{value} does not fit in HUGEINT"
        )));
    }
    Ok(out)
}

/// Converts a 128-bit unsigned integer to the nearest `f64`
/// (`duckdb_uhugeint_to_double`). Lossy beyond f64's 53-bit significand.
#[must_use]
pub fn uhugeint_to_double(value: u128) -> f64 {
    // SAFETY: `uhugeint_from_u128` yields a fully initialised `duckdb_uhugeint`.
    unsafe { duckdb_uhugeint_to_double(uhugeint_from_u128(value)) }
}

/// Converts an `f64` to a 128-bit unsigned integer (`duckdb_double_to_uhugeint`).
///
/// # Errors
///
/// Returns [`DuckDBConversionError::ConversionError`] if `value` is non-finite,
/// negative, or too large to fit (DuckDB signals overflow with `0`).
pub fn double_to_uhugeint(value: f64) -> Result<u128, DuckDBConversionError> {
    if !value.is_finite() {
        return Err(DuckDBConversionError::ConversionError(format!(
            "cannot convert non-finite value {value} to UHUGEINT"
        )));
    }
    if value < 0.0 {
        return Err(DuckDBConversionError::ConversionError(format!(
            "cannot convert negative value {value} to UHUGEINT"
        )));
    }
    // SAFETY: `value` is finite and non-negative; the call is a pure conversion.
    let raw = unsafe { duckdb_double_to_uhugeint(value) };
    let out = u128_from_uhugeint(raw);
    if out == 0 && value >= 1.0 {
        return Err(DuckDBConversionError::ConversionError(format!(
            "{value} does not fit in UHUGEINT"
        )));
    }
    Ok(out)
}

/// A broken-down `TIMESTAMP`: the calendar date and wall-clock time DuckDB derives
/// from a microsecond count (`duckdb_from_timestamp`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampParts {
    /// Year (proleptic Gregorian; may be negative).
    pub year: i32,
    /// Month, 1..=12.
    pub month: i8,
    /// Day of month, 1..=31.
    pub day: i8,
    /// Hour, 0..=23.
    pub hour: i8,
    /// Minute, 0..=59.
    pub min: i8,
    /// Second, 0..=59.
    pub sec: i8,
    /// Sub-second microseconds, 0..=999_999.
    pub micros: i32,
}

/// Decomposes a microsecond `TIMESTAMP` into calendar/clock parts using DuckDB's own
/// normalisation (`duckdb_from_timestamp`).
#[must_use]
pub fn timestamp_micros_to_parts(micros: i64) -> TimestampParts {
    // SAFETY: `duckdb_timestamp { micros }` is a fully initialised value; the call is
    // pure integer arithmetic that always yields normalised parts.
    let s = unsafe { duckdb_from_timestamp(duckdb_timestamp { micros }) };
    TimestampParts {
        year: s.date.year,
        month: s.date.month,
        day: s.date.day,
        hour: s.time.hour,
        min: s.time.min,
        sec: s.time.sec,
        micros: s.time.micros,
    }
}

/// Re-composes a microsecond `TIMESTAMP` from calendar/clock parts using DuckDB's own
/// routine (`duckdb_to_timestamp`). The inverse of [`timestamp_micros_to_parts`].
#[must_use]
pub fn parts_to_timestamp_micros(parts: TimestampParts) -> i64 {
    let s = duckdb_timestamp_struct {
        date: duckdb_date_struct { year: parts.year, month: parts.month, day: parts.day },
        time: duckdb_time_struct {
            hour: parts.hour,
            min: parts.min,
            sec: parts.sec,
            micros: parts.micros,
        },
    };
    // SAFETY: `s` is a fully initialised `duckdb_timestamp_struct`; the call is pure
    // integer arithmetic.
    unsafe { duckdb_to_timestamp(s) }.micros
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hugeint_double_round_trips_small_values_exactly() {
        // Values within f64's 53-bit exact-integer range round-trip exactly.
        for v in [0_i128, 1, -1, 1_000_000, -1_000_000, 1_i128 << 52, -(1_i128 << 52)] {
            assert_eq!(double_to_hugeint(hugeint_to_double(v)).unwrap(), v, "round trip {v}");
        }
    }

    #[test]
    fn double_to_hugeint_rejects_non_finite() {
        assert!(double_to_hugeint(f64::NAN).is_err());
        assert!(double_to_hugeint(f64::INFINITY).is_err());
        // A value far beyond i128 overflows (DuckDB returns 0).
        assert!(double_to_hugeint(1e40).is_err());
    }

    #[test]
    fn uhugeint_double_round_trips_and_rejects_negatives() {
        // Values within f64's 53-bit exact-integer range round-trip exactly.
        for v in [0_u128, 1, 1_000_000, 1_u128 << 52] {
            assert_eq!(double_to_uhugeint(uhugeint_to_double(v)).unwrap(), v, "round trip {v}");
        }
        assert!(double_to_uhugeint(-1.0).is_err(), "negative rejected");
        assert!(double_to_uhugeint(f64::NAN).is_err());
    }

    #[test]
    fn timestamp_parts_round_trip() {
        // 2024-03-15 14:30:45.123456 as microseconds since the epoch.
        let micros = parts_to_timestamp_micros(TimestampParts {
            year: 2024,
            month: 3,
            day: 15,
            hour: 14,
            min: 30,
            sec: 45,
            micros: 123_456,
        });
        let parts = timestamp_micros_to_parts(micros);
        assert_eq!(parts.year, 2024);
        assert_eq!(parts.month, 3);
        assert_eq!(parts.day, 15);
        assert_eq!(parts.hour, 14);
        assert_eq!(parts.min, 30);
        assert_eq!(parts.sec, 45);
        assert_eq!(parts.micros, 123_456);
    }
}
