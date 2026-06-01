//! [`DuckDialect`] implementations for chrono types using the `duckdb_value` API.
//!
//! These conversions operate on `duckdb_value` heap objects created by
//! `duckdb_create_date` / `duckdb_create_time` / etc. They are **not** used when
//! reading values from data chunk vectors; for that path see
//! [`crate::types::value::DuckValue::from_duckdb_vec`].

// Chrono implementations (feature-gated)
use super::*;
use crate::error::DuckDBConversionError;
use crate::ffi::{
    duckdb_create_date, duckdb_create_interval, duckdb_create_time, duckdb_create_timestamp,
    duckdb_create_timestamp_ms, duckdb_create_timestamp_ns, duckdb_create_timestamp_s, duckdb_date,
    duckdb_from_date, duckdb_get_date, duckdb_get_interval, duckdb_get_time, duckdb_get_timestamp,
    duckdb_get_timestamp_ms, duckdb_get_timestamp_ns, duckdb_get_timestamp_s, duckdb_interval,
    duckdb_time, duckdb_timestamp, duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s,
};
use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

impl DuckDialect for Duration {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type INTERVAL. `duckdb_get_interval`
        // reads the months/days/micros fields from the value.
        unsafe {
            let interval = duckdb_get_interval(value);
            let total_days = interval.months as i64 * 30 + interval.days as i64;
            let total_micros = total_days * 86_400_000_000 + interval.micros as i64;
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
        let days = self.num_days_from_ce() - 719163;
        let raw_date = duckdb_date { days };
        // SAFETY: `raw_date` is a fully initialized `duckdb_date` value.
        Ok(unsafe { duckdb_create_date(raw_date) })
    }
}

impl DuckDialect for NaiveTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIME. `duckdb_get_time` reads
        // the microseconds-since-midnight field.
        let raw_time = unsafe { duckdb_get_time(value) };
        NaiveTime::from_num_seconds_from_midnight_opt(
            (raw_time.micros / 1_000_000) as u32,
            ((raw_time.micros % 1_000_000) * 1000) as u32,
        )
        .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time".to_string()))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = (self.num_seconds_from_midnight() as i64) * 1_000_000
            + (self.nanosecond() as i64) / 1000;
        let raw_time = duckdb_time { micros };
        // SAFETY: `raw_time` is a fully initialized `duckdb_time` value.
        Ok(unsafe { duckdb_create_time(raw_time) })
    }
}

impl DuckDialect for NaiveDateTime {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP. `duckdb_get_timestamp`
        // reads the microseconds-since-epoch field.
        let raw_ts = unsafe { duckdb_get_timestamp(value) };
        let micros = raw_ts.micros;
        let days_since_epoch = micros / 86_400_000_000;
        let date = NaiveDate::from_num_days_from_ce_opt(days_since_epoch as i32 + 719163)
            .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid date".to_string()))?;
        let micros_of_day = micros % 86_400_000_000;
        let secs = (micros_of_day / 1_000_000) as u32;
        let nsecs = ((micros_of_day % 1_000_000) * 1000) as u32;
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsecs)
            .ok_or_else(|| DuckDBConversionError::ConversionError("Invalid time".to_string()))?;
        Ok(NaiveDateTime::new(date, time))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.and_utc().timestamp() * 1_000_000
            + (self.and_utc().timestamp_subsec_micros() as i64);
        let raw_ts = duckdb_timestamp { micros };
        // SAFETY: `raw_ts` is a fully initialized `duckdb_timestamp` value.
        Ok(unsafe { duckdb_create_timestamp(raw_ts) })
    }
}

/*
* Precision-specific timestamp wrappers
*/

/// Private helper: convert microseconds since the Unix epoch to a `NaiveDateTime`.
fn micros_to_naive_datetime_chrono(micros: i64) -> Result<NaiveDateTime, DuckDBConversionError> {
    DateTime::<Utc>::from_timestamp(
        micros / 1_000_000,
        ((micros % 1_000_000).unsigned_abs() * 1_000) as u32,
    )
    .map(|dt| dt.naive_utc())
    .ok_or_else(|| {
        DuckDBConversionError::ConversionError(format!("timestamp {micros}µs out of range"))
    })
}

/// A [`NaiveDateTime`] that maps to DuckDB's **second**-precision timestamp (`TIMESTAMP_S`).
///
/// Use `TimestampS::from_duck` / `TimestampS::to_duck` when reading or writing values from
/// a `DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S` column via the `duckdb_value` API.
pub struct TimestampS(pub NaiveDateTime);

impl DuckDialect for TimestampS {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_S.
        // `duckdb_get_timestamp_s` reads the seconds-since-epoch field.
        let raw = unsafe { duckdb_get_timestamp_s(value) };
        micros_to_naive_datetime_chrono(raw.seconds.saturating_mul(1_000_000)).map(TimestampS)
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let secs = self.0.and_utc().timestamp();
        let raw = duckdb_timestamp_s { seconds: secs };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_s` value.
        Ok(unsafe { duckdb_create_timestamp_s(raw) })
    }
}

/// A [`NaiveDateTime`] that maps to DuckDB's **millisecond**-precision timestamp
/// (`TIMESTAMP_MS`).
pub struct TimestampMs(pub NaiveDateTime);

impl DuckDialect for TimestampMs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_MS.
        // `duckdb_get_timestamp_ms` reads the milliseconds-since-epoch field.
        let raw = unsafe { duckdb_get_timestamp_ms(value) };
        micros_to_naive_datetime_chrono(raw.millis.saturating_mul(1_000)).map(TimestampMs)
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let millis = self.0.and_utc().timestamp() * 1_000
            + self.0.and_utc().timestamp_subsec_millis() as i64;
        let raw = duckdb_timestamp_ms { millis };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_ms` value.
        Ok(unsafe { duckdb_create_timestamp_ms(raw) })
    }
}

/// A [`NaiveDateTime`] that maps to DuckDB's **nanosecond**-precision timestamp
/// (`TIMESTAMP_NS`).
pub struct TimestampNs(pub NaiveDateTime);

impl DuckDialect for TimestampNs {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type TIMESTAMP_NS.
        // `duckdb_get_timestamp_ns` reads the nanoseconds-since-epoch field.
        let raw = unsafe { duckdb_get_timestamp_ns(value) };
        // truncate nanos → micros (chrono NaiveDateTime has microsecond precision)
        micros_to_naive_datetime_chrono(raw.nanos / 1_000).map(TimestampNs)
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.0.and_utc().timestamp() * 1_000_000
            + self.0.and_utc().timestamp_subsec_micros() as i64;
        let nanos = micros.saturating_mul(1_000);
        let raw = duckdb_timestamp_ns { nanos };
        // SAFETY: `raw` is a fully initialized `duckdb_timestamp_ns` value.
        Ok(unsafe { duckdb_create_timestamp_ns(raw) })
    }
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod test_chrono_conversion {
    use crate::ffi::duckdb_destroy_value;

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
    fn test_timestamp_ms_conversion() {
        use super::*;
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            NaiveTime::from_hms_milli_opt(12, 30, 45, 500).unwrap(),
        );
        let wrapper = TimestampMs(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampMs::from_duck(duck_value).unwrap();
        // Millisecond precision: sub-millisecond part may be truncated.
        assert_eq!(converted.0.date(), dt.date());
        assert_eq!(converted.0.second(), dt.second());
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn test_timestamp_ns_conversion() {
        use super::*;
        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            NaiveTime::from_hms_micro_opt(23, 59, 59, 123_456).unwrap(),
        );
        let wrapper = TimestampNs(dt);
        let mut duck_value = wrapper.to_duck().unwrap();
        let converted = TimestampNs::from_duck(duck_value).unwrap();
        // Nanosecond precision: microsecond part is preserved.
        assert_eq!(converted.0, dt);
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
}
