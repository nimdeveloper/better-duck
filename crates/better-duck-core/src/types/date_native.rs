use super::*;

use crate::ffi::{
    duckdb_create_date, duckdb_create_interval, duckdb_create_time, duckdb_create_timestamp,
    duckdb_date, duckdb_date_struct, duckdb_from_date, duckdb_from_time, duckdb_get_date,
    duckdb_get_interval, duckdb_get_time, duckdb_get_timestamp, duckdb_interval, duckdb_time,
    duckdb_time_struct, duckdb_timestamp, duckdb_to_date, duckdb_to_time,
};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

// ── No-chrono date/time component types ──────────────────────────────────────

/// A calendar date without time-zone awareness, for use without the `chrono` feature.
///
/// Holds the year/month/day components decoded from DuckDB's `DATE` storage
/// (int32 days-since-epoch decoded via `duckdb_from_date`).
///
// TODO: implement Display/arithmetic if needed by callers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckDate {
    /// Calendar year (e.g. 2024).
    pub year: i32,
    /// Month of the year in `[1, 12]`.
    pub month: u8,
    /// Day of the month in `[1, 31]`.
    pub day: u8,
}

/// A time-of-day value without a date or time-zone, for use without the `chrono` feature.
///
/// Stores the hour/minute/second and sub-second microseconds decoded from DuckDB's
/// `TIME` storage (int64 microseconds-since-midnight decoded via `duckdb_from_time`).
///
// TODO: implement Display/arithmetic if needed by callers;
// TODO: nanosecond TIME precision (`duckdb_get_time_ns`) is unavailable in libduckdb-sys 1.3.1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckTime {
    /// Hour in `[0, 23]`.
    pub hour: u8,
    /// Minute in `[0, 59]`.
    pub min: u8,
    /// Second in `[0, 59]`.
    pub sec: u8,
    /// Sub-second part in microseconds `[0, 999_999]`.
    pub micros: u32,
}

impl DuckDialect for DuckDate {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type DATE.
        // `duckdb_get_date` reads the internal days representation; `duckdb_from_date`
        // converts it to y/m/d components using only integer arithmetic.
        let s = unsafe {
            let raw = duckdb_get_date(value);
            duckdb_from_date(raw)
        };
        Ok(DuckDate { year: s.year, month: s.month as u8, day: s.day as u8 })
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let date_struct =
            duckdb_date_struct { year: self.year, month: self.month as i8, day: self.day as i8 };
        // SAFETY: `date_struct` is a fully initialised `duckdb_date_struct`.
        // `duckdb_to_date` converts it to the packed `duckdb_date { days: i32 }` form.
        let raw_date = unsafe { duckdb_to_date(date_struct) };
        // SAFETY: `raw_date` is a valid `duckdb_date` value.
        Ok(unsafe { duckdb_create_date(raw_date) })
    }
}

impl DuckDialect for DuckTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME.
        // `duckdb_get_time` reads the microseconds-since-midnight field;
        // `duckdb_from_time` converts it to h/m/s/micros using only integer arithmetic.
        let s = unsafe {
            let raw = duckdb_get_time(value);
            duckdb_from_time(raw)
        };
        Ok(DuckTime {
            hour: s.hour as u8,
            min: s.min as u8,
            sec: s.sec as u8,
            micros: s.micros as u32,
        })
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let time_struct = duckdb_time_struct {
            hour: self.hour as i8,
            min: self.min as i8,
            sec: self.sec as i8,
            micros: self.micros as i32,
        };
        // SAFETY: `time_struct` is a fully initialised `duckdb_time_struct`.
        // `duckdb_to_time` converts it to the packed `duckdb_time { micros: i64 }` form.
        let raw_time = unsafe { duckdb_to_time(time_struct) };
        // SAFETY: `raw_time` is a valid `duckdb_time` value.
        Ok(unsafe { duckdb_create_time(raw_time) })
    }
}

// StdDuration (Interval)

impl DuckDialect for StdDuration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type INTERVAL.
        // `duckdb_get_interval` reads the months/days/micros fields.
        unsafe {
            let interval = duckdb_get_interval(value);
            // DuckDB interval: months, days, micros
            let total_days = interval.months as u64 * 30 + interval.days as u64;
            let total_micros = total_days * 86_400_000_000 + interval.micros as u64;
            Ok(StdDuration::from_micros(total_micros))
        }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // This is a simplification: only micros, no months/days
        let micros = self.as_micros();
        let interval = duckdb_interval { months: 0, days: 0, micros: micros as i64 };
        // SAFETY: `interval` is a fully initialised `duckdb_interval` value.
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

// ── SystemTime (Timestamp, microsecond precision) ─────────────────────────────

impl DuckDialect for SystemTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP.
        // `duckdb_get_timestamp` reads the microseconds-since-epoch field.
        let raw_ts = unsafe { duckdb_get_timestamp(value) };
        let micros = raw_ts.micros;
        let secs = micros / 1_000_000;
        let sub_micros = (micros % 1_000_000) as u32;
        Ok(UNIX_EPOCH + StdDuration::new(secs as u64, sub_micros * 1000))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let duration = self
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
        let micros = duration.as_secs() as i64 * 1_000_000 + (duration.subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        // SAFETY: `raw_ts` is a fully initialised `duckdb_timestamp` value.
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}
