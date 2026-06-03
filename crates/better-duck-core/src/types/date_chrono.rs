//! [`DuckDialect`] implementations for chrono types using the `duckdb_value` API.
//!
//! These conversions operate on `duckdb_value` heap objects created by
//! `duckdb_create_date` / `duckdb_create_time` / etc.  They are **not** used when
//! reading values from data chunk vectors; for that path see
//! [`crate::types::value::DuckValue::from_duckdb_vec`].

use super::*;
use crate::error::DuckDBConversionError;
use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_create_date, duckdb_create_interval, duckdb_create_time, duckdb_create_time_ns,
        duckdb_create_time_tz_value, duckdb_create_timestamp, duckdb_create_timestamp_ms,
        duckdb_create_timestamp_ns, duckdb_create_timestamp_s, duckdb_create_timestamp_tz,
        duckdb_date, duckdb_destroy_value, duckdb_from_date, duckdb_from_time_tz, duckdb_get_date,
        duckdb_get_interval, duckdb_get_time, duckdb_get_time_ns, duckdb_get_time_tz,
        duckdb_get_timestamp, duckdb_get_timestamp_ms, duckdb_get_timestamp_ns,
        duckdb_get_timestamp_s, duckdb_get_timestamp_tz, duckdb_interval, duckdb_time,
        duckdb_time_ns, duckdb_timestamp, duckdb_timestamp_ms, duckdb_timestamp_ns,
        duckdb_timestamp_s, duckdb_value,
    },
    impl_appendable_via_to_duck_native,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

/*
* Simple scalar types
*/

impl DuckDialect for Duration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type INTERVAL.
        // `duckdb_get_interval` reads the months/days/micros fields from the value.
        unsafe {
            let interval = duckdb_get_interval(value);
            let total_days = interval.months as i64 * 30 + interval.days as i64;
            let total_micros = total_days * 86_400_000_000 + interval.micros;
            Ok(Duration::microseconds(total_micros))
        }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.num_microseconds().unwrap_or(0);
        let interval = duckdb_interval { months: 0, days: 0, micros };
        // SAFETY: `interval` is a fully initialized `duckdb_interval` value.
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

impl DuckDialect for NaiveDate {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type DATE. `duckdb_get_date` reads
        // the internal date representation; `duckdb_from_date` converts it to y/m/d.
        unsafe {
            let val = duckdb_get_date(value);
            let val = duckdb_from_date(val);
            NaiveDate::from_ymd_opt(val.year, val.month as u32, val.day as u32)
                .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid date".to_string()))
        }
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let days = self.num_days_from_ce() - 719_163;
        let raw_date = duckdb_date { days };
        // SAFETY: `raw_date` is a fully initialized `duckdb_date` value.
        Ok(unsafe { duckdb_create_date(raw_date) })
    }
}

impl DuckDialect for NaiveTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME.
        // `duckdb_get_time` reads the microseconds-since-midnight field.
        let raw_time = unsafe { duckdb_get_time(value) };
        NaiveTime::from_num_seconds_from_midnight_opt(
            (raw_time.micros / 1_000_000) as u32,
            ((raw_time.micros % 1_000_000) * 1_000) as u32,
        )
        .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time".to_string()))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = (self.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.nanosecond() as i64) / 1_000;
        let raw_time = duckdb_time { micros };
        // SAFETY: `raw_time` is a fully initialized `duckdb_time` value.
        Ok(unsafe { duckdb_create_time(raw_time) })
    }
}

impl DuckDialect for NaiveDateTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP.
        // `duckdb_get_timestamp` reads the microseconds-since-epoch field.
        let micros = unsafe { duckdb_get_timestamp(value) }.micros;
        DateTime::<Utc>::from_timestamp_micros(micros).map(|dt| dt.naive_utc()).ok_or_else(|| {
            DuckDBConversionError::ConversionError(format!("timestamp {micros}µs out of range"))
        })
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.and_utc().timestamp() * 1_000_000
            + (self.and_utc().timestamp_subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        // SAFETY: `raw_ts` is a fully initialized `duckdb_timestamp` value.
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}

// Precision-specific timestamp wrappers

/// A [`NaiveDateTime`] that maps to DuckDB's **second**-precision timestamp (`TIMESTAMP_S`).
///
/// Use `TimestampS::from_duck` / `TimestampS::to_duck` when reading or writing values from
/// a `DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S` column via the `duckdb_value` API.
/// Use `TimestampS::from_raw_secs` when reading from a data chunk vector.
pub struct TimestampS(pub NaiveDateTime);

impl DuckDialect for TimestampS {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_S.
        // `duckdb_get_timestamp_s` reads the seconds-since-epoch field.
        let seconds = unsafe { duckdb_get_timestamp_s(value) }.seconds;
        DateTime::<Utc>::from_timestamp_secs(seconds)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DuckDBConversionError::ConversionError(format!("timestamp {seconds}s out of range"))
            })
            .map(TimestampS)
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let secs = self.0.and_utc().timestamp();
        let raw = duckdb_timestamp_s { seconds: secs };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_s` value.
        Ok(unsafe { duckdb_create_timestamp_s(raw) })
    }
}

impl TimestampS {
    /// Convert a raw seconds-since-epoch value (as stored in a chunk vector) to `TimestampS`.
    ///
    /// Creates a genuine `duckdb_value` internally so the full `duckdb_get_timestamp_s`
    /// conversion path is exercised, then destroys it.
    ///
    /// # Errors
    ///
    /// Returns `DuckDBConversionError` if the timestamp is out of the representable range.
    pub fn from_raw_secs(secs: i64) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_create_timestamp_s` accepts any i64 seconds value.
        let mut dv = unsafe { duckdb_create_timestamp_s(duckdb_timestamp_s { seconds: secs }) };
        let result = Self::from_duck(dv);
        // SAFETY: `dv` was just created above; destroy exactly once here.
        unsafe { duckdb_destroy_value(&mut dv) };
        result
    }
}

/// A [`NaiveDateTime`] that maps to DuckDB's **millisecond**-precision timestamp
/// (`TIMESTAMP_MS`).
pub struct TimestampMs(pub NaiveDateTime);

impl DuckDialect for TimestampMs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_MS.
        // `duckdb_get_timestamp_ms` reads the milliseconds-since-epoch field.
        let millis = unsafe { duckdb_get_timestamp_ms(value) }.millis;
        DateTime::<Utc>::from_timestamp_millis(millis)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DuckDBConversionError::ConversionError(format!("timestamp {millis}ms out of range"))
            })
            .map(TimestampMs)
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let millis = self.0.and_utc().timestamp() * 1_000
            + self.0.and_utc().timestamp_subsec_millis() as i64;
        let raw = duckdb_timestamp_ms { millis };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_ms` value.
        Ok(unsafe { duckdb_create_timestamp_ms(raw) })
    }
}

impl TimestampMs {
    /// Convert a raw milliseconds-since-epoch value (as stored in a chunk vector) to
    /// `TimestampMs`.
    ///
    /// # Errors
    ///
    /// Returns `DuckDBConversionError` if the timestamp is out of the representable range.
    pub fn from_raw_millis(millis: i64) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_create_timestamp_ms` accepts any i64 milliseconds value.
        let mut dv = unsafe { duckdb_create_timestamp_ms(duckdb_timestamp_ms { millis }) };
        let result = Self::from_duck(dv);
        // SAFETY: `dv` was just created above; destroy exactly once here.
        unsafe { duckdb_destroy_value(&mut dv) };
        result
    }
}

/// A [`NaiveDateTime`] that maps to DuckDB's **nanosecond**-precision timestamp
/// (`TIMESTAMP_NS`).
///
/// Unlike the other timestamp wrappers, this type preserves full nanosecond precision because
/// `chrono::NaiveTime` stores sub-second time as nanoseconds internally.
pub struct TimestampNs(pub NaiveDateTime);

impl DuckDialect for TimestampNs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_NS.
        // `duckdb_get_timestamp_ns` reads the nanoseconds-since-epoch field.
        let nanos = unsafe { duckdb_get_timestamp_ns(value) }.nanos;
        // Use the nanos helper (NOT the micros helper) so no precision is lost.
        Ok(TimestampNs(DateTime::<Utc>::from_timestamp_nanos(nanos).naive_utc()))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let nanos = self.0.and_utc().timestamp_nanos_opt().ok_or_else(|| {
            DuckDBConversionError::ConversionError(
                "timestamp out of range for nanosecond i64 representation".to_owned(),
            )
        })?;
        let raw = duckdb_timestamp_ns { nanos };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_ns` value.
        Ok(unsafe { duckdb_create_timestamp_ns(raw) })
    }
}

impl TimestampNs {
    /// Convert a raw nanoseconds-since-epoch value (as stored in a chunk vector) to
    /// `TimestampNs` with **full nanosecond precision**.
    ///
    /// # Errors
    ///
    /// Returns `DuckDBConversionError` if the timestamp is out of the representable range.
    pub fn from_raw_nanos(nanos: i64) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_create_timestamp_ns` accepts any i64 nanoseconds value.
        let mut dv = unsafe { duckdb_create_timestamp_ns(duckdb_timestamp_ns { nanos }) };
        let result = Self::from_duck(dv);
        // SAFETY: `dv` was just created above; destroy exactly once here.
        unsafe { duckdb_destroy_value(&mut dv) };
        result
    }
}

// Timezone-aware timestamp

/// A [`DateTime<Utc>`] that maps to DuckDB's `TIMESTAMP WITH TIME ZONE` (`TIMESTAMP_TZ`).
///
/// DuckDB stores `TIMESTAMP_TZ` as UTC microseconds-since-epoch (identical wire format to
/// `TIMESTAMP`).  This wrapper carries chrono's `Utc` zone marker so callers can
/// distinguish it from naive timestamps at the type level.
pub struct TimestampTz(pub DateTime<Utc>);

impl DuckDialect for TimestampTz {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_TZ.
        // `duckdb_get_timestamp_tz` returns the UTC microseconds-since-epoch.
        let raw = unsafe { duckdb_get_timestamp_tz(value) };
        let micros = raw.micros;
        let secs = micros / 1_000_000;
        let sub_nanos = ((micros % 1_000_000).unsigned_abs() * 1_000) as u32;
        DateTime::<Utc>::from_timestamp(secs, sub_nanos).map(TimestampTz).ok_or_else(|| {
            DuckDBConversionError::ConversionError(format!("timestamp_tz {micros}µs out of range"))
        })
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.0.timestamp_micros();
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp` value.
        Ok(unsafe { duckdb_create_timestamp_tz(raw) })
    }
}

impl TimestampTz {
    /// Convert a raw UTC microseconds-since-epoch value (as stored in a chunk vector) to
    /// `TimestampTz`.
    ///
    /// # Errors
    ///
    /// Returns `DuckDBConversionError` if the timestamp is out of the representable range.
    pub fn from_raw_micros_tz(micros: i64) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_create_timestamp_tz` accepts any i64 microseconds value.
        let mut dv = unsafe { duckdb_create_timestamp_tz(duckdb_timestamp { micros }) };
        let result = Self::from_duck(dv);
        // SAFETY: `dv` was just created above; destroy exactly once here.
        unsafe { duckdb_destroy_value(&mut dv) };
        result
    }
}

// Timezone-aware time

/// A microsecond-precision time-with-timezone value, mapping to DuckDB's `TIME WITH TIME ZONE`
/// (`TIME_TZ`).
///
/// DuckDB encodes `TIME_TZ` as a packed 64-bit integer (40 bits of microseconds-since-midnight,
/// 24 bits of UTC offset in seconds).  This struct preserves both components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeTz {
    /// The time-of-day component (microsecond precision).
    pub time: NaiveTime,
    /// The UTC offset in seconds (e.g. `3600` = UTC+1, `-18000` = UTC-5).
    pub offset_secs: i32,
}

impl DuckDialect for TimeTz {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME_TZ.
        // `duckdb_get_time_tz` unpacks the 64-bit encoding into a `duckdb_time_tz`.
        let raw_tz = unsafe { duckdb_get_time_tz(value) };
        // `duckdb_from_time_tz` decomposes the packed bits into hour/min/sec/micros + offset.
        // SAFETY: `raw_tz` is a valid `duckdb_time_tz` obtained above.
        let parts = unsafe { duckdb_from_time_tz(raw_tz) };
        let ts = &parts.time;
        let naive = NaiveTime::from_hms_micro_opt(
            ts.hour as u32,
            ts.min as u32,
            ts.sec as u32,
            ts.micros as u32,
        )
        .ok_or_else(|| {
            DuckDBConversionError::ConversionError("Invalid time_tz components".to_string())
        })?;
        Ok(TimeTz { time: naive, offset_secs: parts.offset })
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = (self.time.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.time.nanosecond() as i64) / 1_000;
        // `duckdb_create_time_tz` packs micros + offset into a `duckdb_time_tz`.
        // SAFETY: any i64 micros and i32 offset are valid inputs.
        let raw_tz = unsafe { crate::ffi::duckdb_create_time_tz(micros, self.offset_secs) };
        // SAFETY: `raw_tz` is a valid `duckdb_time_tz` created above.
        Ok(unsafe { duckdb_create_time_tz_value(raw_tz) })
    }
}

// Nanosecond-precision time

/// A [`NaiveTime`] with **nanosecond** precision, mapping to DuckDB's `TIME_NS` type.
///
/// `chrono::NaiveTime` stores sub-second time as nanoseconds internally, so no precision
/// is lost in the round-trip.
pub struct TimeNs(pub NaiveTime);

impl DuckDialect for TimeNs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME_NS.
        // `duckdb_get_time_ns` reads the nanoseconds-since-midnight field.
        let raw = unsafe { duckdb_get_time_ns(value) };
        let secs = (raw.nanos / 1_000_000_000) as u32;
        let sub_nanos = (raw.nanos % 1_000_000_000).unsigned_abs() as u32;
        NaiveTime::from_num_seconds_from_midnight_opt(secs, sub_nanos)
            .map(TimeNs)
            .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time_ns".to_string()))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let nanos = (self.0.num_seconds_from_midnight() as i64) * 1_000_000_000
            + self.0.nanosecond() as i64;
        let raw = duckdb_time_ns { nanos };
        // SAFETY: `raw` is a fully initialized `duckdb_time_ns` value.
        Ok(unsafe { duckdb_create_time_ns(raw) })
    }
}

impl TimeNs {
    /// Convert a raw nanoseconds-since-midnight value (as stored in a chunk vector) to `TimeNs`.
    ///
    /// # Errors
    ///
    /// Returns `DuckDBConversionError` if the nanoseconds value is out of range for a valid time.
    pub fn from_raw_ns(nanos: i64) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_create_time_ns` accepts any i64 nanoseconds value.
        let mut dv = unsafe { duckdb_create_time_ns(duckdb_time_ns { nanos }) };
        let result = Self::from_duck(dv);
        // SAFETY: `dv` was just created above; destroy exactly once here.
        unsafe { duckdb_destroy_value(&mut dv) };
        result
    }
}

impl AppendAble for NaiveDate {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let raw = duckdb_date { days: self.num_days_from_ce() - 719_163 };
        // SAFETY: `raw` is a valid duckdb_date; `appender` is a valid duckdb_appender.
        unsafe { crate::ffi::duckdb_append_date(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let raw = duckdb_date { days: self.num_days_from_ce() - 719_163 };
        // SAFETY: `raw` is a valid duckdb_date; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_date(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for NaiveTime {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let micros = (self.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.nanosecond() as i64) / 1_000;
        let raw = duckdb_time { micros };
        // SAFETY: `raw` is a valid duckdb_time; `appender` is valid.
        unsafe { crate::ffi::duckdb_append_time(appender, raw) };
        Ok(())
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let micros = (self.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.nanosecond() as i64) / 1_000;
        let raw = duckdb_time { micros };
        // SAFETY: `raw` is a valid duckdb_time; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_time(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for NaiveDateTime {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let micros = self.and_utc().timestamp() * 1_000_000
            + self.and_utc().timestamp_subsec_micros() as i64;
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
        let micros = self.and_utc().timestamp() * 1_000_000
            + self.and_utc().timestamp_subsec_micros() as i64;
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a valid duckdb_timestamp; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_timestamp(stmt, idx, raw) };
        Ok(())
    }
}

impl AppendAble for Duration {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let micros = self.num_microseconds().unwrap_or(0);
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
        let micros = self.num_microseconds().unwrap_or(0);
        let raw = duckdb_interval { months: 0, days: 0, micros };
        // SAFETY: `raw` is a valid duckdb_interval; `stmt`/`idx` are valid.
        unsafe { crate::ffi::duckdb_bind_interval(stmt, idx, raw) };
        Ok(())
    }
}

/// For `TimestampS`, `TimestampMs`, `TimestampNs`, `TimestampTz`, `TimeTz`, `TimeNs`:
/// no dedicated `duckdb_append_*` / `duckdb_bind_*` function exists, so we go through
/// the `duckdb_value` path via `DuckDialect::to_duck()`.
impl_appendable_via_to_duck_native!(TimestampS);
impl_appendable_via_to_duck_native!(TimestampMs);
impl_appendable_via_to_duck_native!(TimestampNs);
impl_appendable_via_to_duck_native!(TimestampTz);
impl_appendable_via_to_duck_native!(TimeTz);
impl_appendable_via_to_duck_native!(TimeNs);

// Tests

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod test_chrono_conversion {

    #[test]
    fn test_duration_conversion() {
        use super::*;
        let duration = Duration::new(3661, 0).unwrap();
        let mut duck_value = duration.to_duck().unwrap();
        let converted_duration = Duration::from_duck(duck_value).unwrap();
        assert_eq!(duration, converted_duration);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_date_conversion() {
        use super::*;
        let date = NaiveDate::from_ymd_opt(2023, 10, 1).unwrap();
        let mut duck_value = date.to_duck().unwrap();
        let converted_date = NaiveDate::from_duck(duck_value).unwrap();
        assert_eq!(date, converted_date);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_time_conversion() {
        use super::*;
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let mut duck_value = time.to_duck().unwrap();
        let converted_time = NaiveTime::from_duck(duck_value).unwrap();
        assert_eq!(time, converted_time);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_datetime_conversion() {
        use super::*;
        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2023, 10, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
        );
        let mut duck_value = datetime.to_duck().unwrap();
        let converted_datetime = NaiveDateTime::from_duck(duck_value).unwrap();
        assert_eq!(datetime, converted_datetime);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_s_conversion() {
        use super::*;
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        let wrapper = TimestampS(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampS::from_duck(duck_value).unwrap();
        // Seconds precision: sub-second part is truncated.
        assert_eq!(converted.0.date(), dt.date());
        assert_eq!(converted.0.second(), dt.second());
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_s_from_raw() {
        use super::*;
        let secs: i64 = 1_704_067_200; // 2024-01-01 00:00:00 UTC
        let converted = TimestampS::from_raw_secs(secs).unwrap();
        assert_eq!(converted.0.and_utc().timestamp(), secs);
    }

    #[test]
    fn test_timestamp_ms_conversion() {
        use super::*;
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_milli_opt(12, 30, 45, 500).unwrap(),
        );
        let wrapper = TimestampMs(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampMs::from_duck(duck_value).unwrap();
        assert_eq!(converted.0.date(), dt.date());
        assert_eq!(converted.0.second(), dt.second());
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_ms_from_raw() {
        use super::*;
        let millis: i64 = 1_718_451_045_500; // 2024-06-15 12:30:45.500
        let converted = TimestampMs::from_raw_millis(millis).unwrap();
        assert_eq!(converted.0.and_utc().timestamp_millis(), millis);
    }

    #[test]
    fn test_timestamp_ns_conversion_preserves_nanos() {
        use super::*;
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            // 123_456_789 ns = 123_456 µs + 789 ns — nanoseconds must survive the round-trip.
            NaiveTime::from_hms_nano_opt(23, 59, 59, 123_456_789).unwrap(),
        );
        let wrapper = TimestampNs(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampNs::from_duck(duck_value).unwrap();
        // Full nanosecond round-trip.
        assert_eq!(converted.0, dt);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_ns_from_raw_preserves_nanos() {
        use super::*;
        // epoch + 123_456_789 ns = 1970-01-01 00:00:00.123456789 UTC
        let nanos: i64 = 123_456_789;
        let converted = TimestampNs::from_raw_nanos(nanos).unwrap();
        assert_eq!(converted.0.and_utc().timestamp_nanos_opt().unwrap(), nanos);
    }

    #[test]
    fn test_timestamp_tz_conversion() {
        use super::*;
        let dt = DateTime::<Utc>::from_timestamp(1_718_451_045, 500_000_000).unwrap();
        let wrapper = TimestampTz(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampTz::from_duck(duck_value).unwrap();
        // Microsecond round-trip (500ms → 500_000µs is preserved).
        assert_eq!(converted.0.timestamp_micros(), dt.timestamp_micros());
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_tz_from_raw() {
        use super::*;
        let micros: i64 = 1_718_451_045_000_000;
        let converted = TimestampTz::from_raw_micros_tz(micros).unwrap();
        assert_eq!(converted.0.timestamp_micros(), micros);
    }

    #[test]
    fn test_time_tz_conversion() {
        use super::*;
        let tz = TimeTz {
            time: NaiveTime::from_hms_micro_opt(14, 30, 0, 123_456).unwrap(),
            offset_secs: 3_600, // UTC+1
        };
        let mut duck_value = tz.to_duck().unwrap();
        let converted = TimeTz::from_duck(duck_value).unwrap();
        assert_eq!(converted.time, tz.time);
        assert_eq!(converted.offset_secs, tz.offset_secs);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_time_ns_conversion() {
        use super::*;
        // 14:30:00.123456789 — 789 sub-microsecond nanoseconds must survive.
        let t = NaiveTime::from_hms_nano_opt(14, 30, 0, 123_456_789).unwrap();
        let wrapper = TimeNs(t);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimeNs::from_duck(duck_value).unwrap();
        assert_eq!(converted.0, t);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_time_ns_from_raw() {
        use super::*;
        let nanos: i64 = 14 * 3_600_000_000_000 + 30 * 60_000_000_000 + 123_456_789;
        let converted = TimeNs::from_raw_ns(nanos).unwrap();
        let expected = NaiveTime::from_hms_nano_opt(14, 30, 0, 123_456_789).unwrap();
        assert_eq!(converted.0, expected);
    }
}
