use super::*;

use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_create_date, duckdb_create_interval, duckdb_create_time, duckdb_create_time_ns,
        duckdb_create_time_tz_value, duckdb_create_timestamp, duckdb_date_struct, duckdb_from_date,
        duckdb_from_time, duckdb_from_time_tz, duckdb_get_date, duckdb_get_interval,
        duckdb_get_time, duckdb_get_time_ns, duckdb_get_time_tz, duckdb_get_timestamp,
        duckdb_interval, duckdb_time_ns, duckdb_time_struct, duckdb_timestamp, duckdb_to_date,
        duckdb_to_time,
    },
    impl_appendable_via_to_duck_native,
};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

/*
* No-chrono date/time component types
*/

/// A calendar date without time-zone awareness, for use without the `chrono` feature.
///
/// Holds the year/month/day components decoded from DuckDB's `DATE` storage
/// (int32 days-since-epoch decoded via `duckdb_from_date`).
///
// TODO: implement Display/arithmetic if needed by callers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DuckDate {
    /// Calendar year (e.g. 2024).
    pub year: i32,
    /// Month of the year in `[1, 12]`.
    pub month: u8,
    /// Day of the month in `[1, 31]`.
    pub day: u8,
}

/// A microsecond-precision time-of-day value, for use without the `chrono` feature.
///
/// Stores the hour/minute/second and sub-second microseconds decoded from DuckDB's
/// `TIME` storage (int64 microseconds-since-midnight decoded via `duckdb_from_time`).
///
// TODO: implement Display/arithmetic if needed by callers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// A nanosecond-precision time-of-day value, for use without the `chrono` feature.
///
/// Decoded from DuckDB's `TIME_NS` storage (int64 nanoseconds-since-midnight via
/// `duckdb_get_time_ns`, available since libduckdb-sys 1.10503.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DuckTimeNs {
    /// Hour in `[0, 23]`.
    pub hour: u8,
    /// Minute in `[0, 59]`.
    pub min: u8,
    /// Second in `[0, 59]`.
    pub sec: u8,
    /// Sub-second part in nanoseconds `[0, 999_999_999]`.
    pub nanos: u32,
}

/// A microsecond-precision time-of-day with UTC offset, for use without the `chrono` feature.
///
/// Decoded from DuckDB's `TIME WITH TIME ZONE` (`TIME_TZ`) storage via `duckdb_get_time_tz`
/// and `duckdb_from_time_tz`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DuckTimeTz {
    /// Hour in `[0, 23]`.
    pub hour: u8,
    /// Minute in `[0, 59]`.
    pub min: u8,
    /// Second in `[0, 59]`.
    pub sec: u8,
    /// Sub-second part in microseconds `[0, 999_999]`.
    pub micros: u32,
    /// UTC offset in seconds (e.g. `3600` = UTC+1).
    pub offset_secs: i32,
}

/*
* DuckDialect implementations
*/

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
        // SAFETY: `date_struct` is a fully initialized `duckdb_date_struct`.
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
        // SAFETY: `time_struct` is a fully initialized `duckdb_time_struct`.
        // `duckdb_to_time` converts it to the packed `duckdb_time { micros: i64 }` form.
        let raw_time = unsafe { duckdb_to_time(time_struct) };
        // SAFETY: `raw_time` is a valid `duckdb_time` value.
        Ok(unsafe { duckdb_create_time(raw_time) })
    }
}

impl DuckDialect for DuckTimeNs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME_NS.
        // `duckdb_get_time_ns` reads the nanoseconds-since-midnight field.
        let raw = unsafe { duckdb_get_time_ns(value) };
        let nanos = raw.nanos;
        let total_secs = (nanos / 1_000_000_000) as u64;
        let sub_nanos = (nanos % 1_000_000_000).unsigned_abs() as u32;
        let hour = (total_secs / 3_600) as u8;
        let min = ((total_secs % 3_600) / 60) as u8;
        let sec = (total_secs % 60) as u8;
        Ok(DuckTimeNs { hour, min, sec, nanos: sub_nanos })
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let total_nanos = (self.hour as i64) * 3_600_000_000_000
            + (self.min as i64) * 60_000_000_000
            + (self.sec as i64) * 1_000_000_000
            + self.nanos as i64;
        let raw = duckdb_time_ns { nanos: total_nanos };
        // SAFETY: `raw` is a fully initialized `duckdb_time_ns` value.
        Ok(unsafe { duckdb_create_time_ns(raw) })
    }
}

impl DuckDialect for DuckTimeTz {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME_TZ.
        // `duckdb_get_time_tz` unpacks the 64-bit packed representation.
        let raw_tz = unsafe { duckdb_get_time_tz(value) };
        // `duckdb_from_time_tz` decomposes into duckdb_time_tz_struct { time, offset }.
        // SAFETY: `raw_tz` is a valid `duckdb_time_tz` obtained from `duckdb_get_time_tz`.
        let parts = unsafe { duckdb_from_time_tz(raw_tz) };
        let ts = &parts.time;
        Ok(DuckTimeTz {
            hour: ts.hour as u8,
            min: ts.min as u8,
            sec: ts.sec as u8,
            micros: ts.micros as u32,
            offset_secs: parts.offset,
        })
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = (self.hour as i64) * 3_600_000_000
            + (self.min as i64) * 60_000_000
            + (self.sec as i64) * 1_000_000
            + self.micros as i64;
        // SAFETY: `duckdb_create_time_tz` packs micros + offset into a `duckdb_time_tz`.
        let raw_tz = unsafe { crate::ffi::duckdb_create_time_tz(micros, self.offset_secs) };
        // SAFETY: `raw_tz` is a valid `duckdb_time_tz` created above.
        Ok(unsafe { duckdb_create_time_tz_value(raw_tz) })
    }
}

// StdDuration (Interval)

impl DuckDialect for StdDuration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type INTERVAL.
        // `duckdb_get_interval` reads the months/days/micros fields.
        unsafe {
            let interval = duckdb_get_interval(value);
            let total_days = interval.months as u64 * 30 + interval.days as u64;
            let total_micros = total_days * 86_400_000_000 + interval.micros as u64;
            Ok(StdDuration::from_micros(total_micros))
        }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.as_micros();
        let interval = duckdb_interval { months: 0, days: 0, micros: micros as i64 };
        // SAFETY: `interval` is a fully initialized `duckdb_interval` value.
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

// SystemTime (Timestamp, microsecond precision)

impl DuckDialect for SystemTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP (or TIMESTAMP_TZ).
        // `duckdb_get_timestamp` reads the microseconds-since-epoch field.
        let raw_ts = unsafe { duckdb_get_timestamp(value) };
        let micros = raw_ts.micros;

        let abs = micros.unsigned_abs();
        Ok(if micros >= 0 {
            UNIX_EPOCH + StdDuration::from_micros(abs)
        } else {
            UNIX_EPOCH - StdDuration::from_micros(abs)
        })
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let duration = self
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
        let micros = duration.as_secs() as i64 * 1_000_000 + (duration.subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        // SAFETY: `raw_ts` is a fully initialized `duckdb_timestamp` value.
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}

impl AppendAble for DuckDate {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let ds =
            duckdb_date_struct { year: self.year, month: self.month as i8, day: self.day as i8 };
        // SAFETY: `duckdb_to_date` is a pure arithmetic conversion on a valid struct.
        let raw = unsafe { duckdb_to_date(ds) };
        // SAFETY: `raw` is a valid duckdb_date; `appender` is a valid duckdb_appender.
        unsafe { crate::ffi::duckdb_append_date(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let ds =
            duckdb_date_struct { year: self.year, month: self.month as i8, day: self.day as i8 };
        // SAFETY: `duckdb_to_date` is a pure arithmetic conversion on a valid struct.
        let raw = unsafe { duckdb_to_date(ds) };
        // SAFETY: `raw` is a valid duckdb_date; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_date(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for DuckTime {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let ts = duckdb_time_struct {
            hour: self.hour as i8,
            min: self.min as i8,
            sec: self.sec as i8,
            micros: self.micros as i32,
        };
        // SAFETY: `duckdb_to_time` is a pure arithmetic conversion on a valid struct.
        let raw = unsafe { duckdb_to_time(ts) };
        // SAFETY: `raw` is a valid duckdb_time; `appender` is valid.
        unsafe { crate::ffi::duckdb_append_time(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let ts = duckdb_time_struct {
            hour: self.hour as i8,
            min: self.min as i8,
            sec: self.sec as i8,
            micros: self.micros as i32,
        };
        // SAFETY: `duckdb_to_time` is a pure arithmetic conversion on a valid struct.
        let raw = unsafe { duckdb_to_time(ts) };
        // SAFETY: `raw` is a valid duckdb_time; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_time(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for StdDuration {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let micros = self.as_micros().min(i64::MAX as u128) as i64;
        let raw = duckdb_interval { months: 0, days: 0, micros };
        // SAFETY: `raw` is a valid duckdb_interval; `appender` is valid.
        unsafe { crate::ffi::duckdb_append_interval(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let micros = self.as_micros().min(i64::MAX as u128) as i64;
        let raw = duckdb_interval { months: 0, days: 0, micros };
        // SAFETY: `raw` is a valid duckdb_interval; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_interval(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for SystemTime {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let dur = self.duration_since(UNIX_EPOCH).unwrap_or_default();
        let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a valid duckdb_timestamp; `appender` is valid.
        unsafe { crate::ffi::duckdb_append_timestamp(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let dur = self.duration_since(UNIX_EPOCH).unwrap_or_default();
        let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a valid duckdb_timestamp; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_timestamp(stmt, idx, raw) };
        Ok(())
    }
}

// `DuckTimeNs` and `DuckTimeTz` have no dedicated append/bind function; use the value path.
impl_appendable_via_to_duck_native!(DuckTimeNs);
impl_appendable_via_to_duck_native!(DuckTimeTz);

/// Hash `SystemTime` by converting to `Duration` since UNIX_EPOCH (platform-stable).
#[cfg(not(feature = "chrono"))]
#[inline]
fn hash_system_time<H: Hasher>(
    st: &SystemTime,
    state: &mut H,
) {
    let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    d.hash(state);
}
