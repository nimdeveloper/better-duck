use super::*;

use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_create_date, duckdb_create_interval, duckdb_create_logical_type, duckdb_create_time,
        duckdb_create_time_ns, duckdb_create_time_tz_value, duckdb_create_timestamp, duckdb_date,
        duckdb_date_struct, duckdb_from_date, duckdb_from_time, duckdb_from_time_tz,
        duckdb_interval, duckdb_logical_type, duckdb_time, duckdb_time_ns, duckdb_time_struct,
        duckdb_time_tz, duckdb_timestamp, duckdb_to_date, duckdb_to_time,
        DUCKDB_TYPE_DUCKDB_TYPE_DATE, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ,
    },
    impl_appendable_via_to_duck_native,
};
use std::hash::{Hash, Hasher};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

/*
* No-chrono date/time component types
*/

/// A calendar date without time-zone awareness, for use without the `chrono` feature.
///
/// Holds the year/month/day components decoded from DuckDB's `DATE` storage
/// (int32 days-since-epoch decoded via `duckdb_from_date`).
// TODO: add Display and date-arithmetic helpers
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

impl DuckDialect<duckdb_date> for DuckDate {
    fn from_duck(value: duckdb_date) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_from_date` decomposes a `duckdb_date` using only integer arithmetic.
        let s = unsafe { duckdb_from_date(value) };
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

impl DuckDialect<duckdb_time> for DuckTime {
    fn from_duck(value: duckdb_time) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_from_time` decomposes a `duckdb_time` using only integer arithmetic.
        let s = unsafe { duckdb_from_time(value) };
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

impl DuckDialect<duckdb_time_ns> for DuckTimeNs {
    fn from_duck(value: duckdb_time_ns) -> Result<Self, DuckDBConversionError> {
        let nanos = value.nanos;
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

impl DuckDialect<duckdb_time_tz> for DuckTimeTz {
    fn from_duck(value: duckdb_time_tz) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `duckdb_from_time_tz` decomposes packed bits into h/m/s/micros + offset.
        let parts = unsafe { duckdb_from_time_tz(value) };
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

impl DuckDialect<duckdb_interval> for StdDuration {
    fn from_duck(value: duckdb_interval) -> Result<Self, DuckDBConversionError> {
        let total_days = value.months as u64 * 30 + value.days as u64;
        let total_micros = total_days * 86_400_000_000 + value.micros as u64;
        Ok(StdDuration::from_micros(total_micros))
    }
    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let micros = self.as_micros();
        let interval = duckdb_interval { months: 0, days: 0, micros: micros as i64 };
        // SAFETY: `interval` is a fully initialized `duckdb_interval` value.
        Ok(unsafe { duckdb_create_interval(interval) })
    }
}

// SystemTime (Timestamp, microsecond precision)

impl DuckDialect<duckdb_timestamp> for SystemTime {
    fn from_duck(value: duckdb_timestamp) -> Result<Self, DuckDBConversionError> {
        let micros = value.micros;
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
        let rc = unsafe { crate::ffi::duckdb_append_date(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { crate::helpers::duck_result::check_append(rc, appender) }
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
        let rc = unsafe { crate::ffi::duckdb_bind_date(stmt, idx, raw) };
        crate::helpers::duck_result::check_state(rc)
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
        let rc = unsafe { crate::ffi::duckdb_append_time(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { crate::helpers::duck_result::check_append(rc, appender) }
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
        let rc = unsafe { crate::ffi::duckdb_bind_time(stmt, idx, raw) };
        crate::helpers::duck_result::check_state(rc)
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
        let rc = unsafe { crate::ffi::duckdb_append_interval(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { crate::helpers::duck_result::check_append(rc, appender) }
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let micros = self.as_micros().min(i64::MAX as u128) as i64;
        let raw = duckdb_interval { months: 0, days: 0, micros };
        // SAFETY: `raw` is a valid duckdb_interval; `stmt`/`idx` are valid.
        let rc = unsafe { crate::ffi::duckdb_bind_interval(stmt, idx, raw) };
        crate::helpers::duck_result::check_state(rc)
    }
}

impl AppendAble for SystemTime {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        use crate::error::Error;
        let dur = self.duration_since(UNIX_EPOCH).map_err(|e| {
            Error::ConversionError(DuckDBConversionError::ConversionError(e.to_string()))
        })?;
        let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a valid duckdb_timestamp; `appender` is valid.
        let rc = unsafe { crate::ffi::duckdb_append_timestamp(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { crate::helpers::duck_result::check_append(rc, appender) }
    }
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        use crate::error::Error;
        let dur = self.duration_since(UNIX_EPOCH).map_err(|e| {
            Error::ConversionError(DuckDBConversionError::ConversionError(e.to_string()))
        })?;
        let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
        let raw = duckdb_timestamp { micros };
        // SAFETY: `raw` is a valid duckdb_timestamp; `stmt`/`idx` are valid.
        let rc = unsafe { crate::ffi::duckdb_bind_timestamp(stmt, idx, raw) };
        crate::helpers::duck_result::check_state(rc)
    }
}

// `DuckTimeNs` and `DuckTimeTz` have no dedicated append/bind function; use the value path.
impl_appendable_via_to_duck_native!(DuckTimeNs);
impl_appendable_via_to_duck_native!(DuckTimeTz);

// DuckLogicalType + From<T> for DuckValue

macro_rules! impl_duck_logical_type {
    ($rust_type:ty, $duck_type:expr) => {
        impl DuckLogicalType for $rust_type {
            fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
                // SAFETY: `$duck_type` is always a valid duckdb_type constant.
                Ok(unsafe { duckdb_create_logical_type($duck_type) })
            }
        }
    };
}

impl_duck_logical_type!(DuckDate, DUCKDB_TYPE_DUCKDB_TYPE_DATE);
impl_duck_logical_type!(DuckTime, DUCKDB_TYPE_DUCKDB_TYPE_TIME);
impl_duck_logical_type!(SystemTime, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP);
impl_duck_logical_type!(StdDuration, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL);
impl_duck_logical_type!(DuckTimeTz, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ);
impl_duck_logical_type!(DuckTimeNs, DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS);

impl From<DuckDate> for value::DuckValue {
    fn from(v: DuckDate) -> Self {
        value::DuckValue::Date(v)
    }
}
impl From<DuckTime> for value::DuckValue {
    fn from(v: DuckTime) -> Self {
        value::DuckValue::Time(v)
    }
}
impl From<SystemTime> for value::DuckValue {
    fn from(v: SystemTime) -> Self {
        value::DuckValue::Timestamp(v)
    }
}
impl From<StdDuration> for value::DuckValue {
    fn from(v: StdDuration) -> Self {
        value::DuckValue::Interval(v)
    }
}
impl From<DuckTimeTz> for value::DuckValue {
    fn from(v: DuckTimeTz) -> Self {
        value::DuckValue::TimeTz(v)
    }
}
impl From<DuckTimeNs> for value::DuckValue {
    fn from(v: DuckTimeNs) -> Self {
        value::DuckValue::TimeNs(v)
    }
}

/// Hash `SystemTime` by converting to `Duration` since UNIX_EPOCH (platform-stable).
#[cfg(not(feature = "chrono"))]
#[inline]
pub(super) fn hash_system_time<H: Hasher>(
    st: &SystemTime,
    state: &mut H,
) {
    let d = st.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    d.hash(state);
}

#[cfg(all(test, not(feature = "chrono")))]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;
    use crate::ffi::{
        duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_date, duckdb_get_interval,
        duckdb_get_time, duckdb_get_time_ns, duckdb_get_time_tz, duckdb_get_timestamp,
        duckdb_get_type_id,
    };
    use std::collections::hash_map::DefaultHasher;

    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    fn round_trip_date(value: DuckDate) -> DuckDate {
        let mut duck_value = value.to_duck().expect("date should convert to duckdb_value");
        let raw = unsafe { duckdb_get_date(duck_value) };
        let converted = DuckDate::from_duck(raw).expect("date should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    fn round_trip_time(value: DuckTime) -> DuckTime {
        let mut duck_value = value.to_duck().expect("time should convert to duckdb_value");
        let raw = unsafe { duckdb_get_time(duck_value) };
        let converted = DuckTime::from_duck(raw).expect("time should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    fn round_trip_time_ns(value: DuckTimeNs) -> DuckTimeNs {
        let mut duck_value = value.to_duck().expect("time_ns should convert to duckdb_value");
        let raw = unsafe { duckdb_get_time_ns(duck_value) };
        let converted = DuckTimeNs::from_duck(raw).expect("time_ns should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    fn round_trip_time_tz(value: DuckTimeTz) -> DuckTimeTz {
        let mut duck_value = value.to_duck().expect("time_tz should convert to duckdb_value");
        let raw = unsafe { duckdb_get_time_tz(duck_value) };
        let converted = DuckTimeTz::from_duck(raw).expect("time_tz should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    fn round_trip_interval(value: StdDuration) -> StdDuration {
        let mut duck_value = value.to_duck().expect("interval should convert to duckdb_value");
        let raw = unsafe { duckdb_get_interval(duck_value) };
        let converted = StdDuration::from_duck(raw).expect("interval should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    fn round_trip_timestamp(value: SystemTime) -> SystemTime {
        let mut duck_value = value.to_duck().expect("timestamp should convert to duckdb_value");
        let raw = unsafe { duckdb_get_timestamp(duck_value) };
        let converted = SystemTime::from_duck(raw).expect("timestamp should convert from DuckDB");
        unsafe { duckdb_destroy_value(&mut duck_value) };
        converted
    }

    #[test]
    fn date_round_trips_epoch_leap_day_and_pre_epoch() {
        for date in [
            DuckDate { year: 1970, month: 1, day: 1 },
            DuckDate { year: 2000, month: 2, day: 29 },
            DuckDate { year: 1969, month: 12, day: 31 },
        ] {
            assert_eq!(round_trip_date(date), date);
        }
    }

    #[test]
    fn time_round_trips_boundaries_and_microseconds() {
        for time in [
            DuckTime { hour: 0, min: 0, sec: 0, micros: 0 },
            DuckTime { hour: 14, min: 30, sec: 45, micros: 123_456 },
            DuckTime { hour: 23, min: 59, sec: 59, micros: 999_999 },
        ] {
            assert_eq!(round_trip_time(time), time);
        }
    }

    #[test]
    fn time_ns_round_trips_boundaries_and_sub_microsecond_precision() {
        for time in [
            DuckTimeNs { hour: 0, min: 0, sec: 0, nanos: 1 },
            DuckTimeNs { hour: 14, min: 30, sec: 45, nanos: 123_456_789 },
            DuckTimeNs { hour: 23, min: 59, sec: 59, nanos: 999_999_999 },
        ] {
            assert_eq!(round_trip_time_ns(time), time);
        }
    }

    #[test]
    fn time_ns_from_duck_decomposes_raw_nanoseconds() {
        let raw = duckdb_time_ns {
            nanos: 14 * 3_600_000_000_000 + 30 * 60_000_000_000 + 45 * 1_000_000_000 + 987_654_321,
        };
        assert_eq!(
            DuckTimeNs::from_duck(raw).unwrap(),
            DuckTimeNs { hour: 14, min: 30, sec: 45, nanos: 987_654_321 }
        );
    }

    #[test]
    fn time_tz_round_trips_offsets_and_microseconds() {
        for time in [
            DuckTimeTz { hour: 0, min: 0, sec: 0, micros: 0, offset_secs: 0 },
            DuckTimeTz {
                hour: 12,
                min: 34,
                sec: 56,
                micros: 789_012,
                offset_secs: 5 * 3_600 + 30 * 60,
            },
            DuckTimeTz {
                hour: 23,
                min: 59,
                sec: 59,
                micros: 999_999,
                offset_secs: -(3 * 3_600 + 30 * 60),
            },
        ] {
            assert_eq!(round_trip_time_tz(time), time);
        }
    }

    #[test]
    fn interval_from_duck_combines_months_days_and_micros() {
        let raw = duckdb_interval { months: 2, days: 3, micros: 4_567_890 };
        let expected_days = 2 * 30 + 3;
        let expected = StdDuration::from_micros(expected_days * 86_400_000_000 + 4_567_890);
        assert_eq!(StdDuration::from_duck(raw).unwrap(), expected);
    }

    #[test]
    fn interval_round_trips_microsecond_precision() {
        for interval in
            [StdDuration::ZERO, StdDuration::from_micros(1), StdDuration::new(86_461, 987_654_000)]
        {
            assert_eq!(round_trip_interval(interval), interval);
        }
    }

    #[test]
    fn interval_to_duck_truncates_sub_microsecond_precision() {
        let interval = StdDuration::new(7, 123_456_789);
        assert_eq!(round_trip_interval(interval), StdDuration::new(7, 123_456_000));
    }

    #[test]
    fn timestamp_from_duck_handles_both_sides_of_epoch() {
        let before = SystemTime::from_duck(duckdb_timestamp { micros: -1_500_001 }).unwrap();
        let after = SystemTime::from_duck(duckdb_timestamp { micros: 1_500_001 }).unwrap();
        assert_eq!(UNIX_EPOCH.duration_since(before).unwrap(), StdDuration::from_micros(1_500_001));
        assert_eq!(after.duration_since(UNIX_EPOCH).unwrap(), StdDuration::from_micros(1_500_001));
    }

    #[test]
    fn timestamp_round_trips_epoch_and_positive_microseconds() {
        for timestamp in [
            UNIX_EPOCH,
            UNIX_EPOCH + StdDuration::from_micros(1),
            UNIX_EPOCH + StdDuration::new(1_700_000_000, 123_456_000),
        ] {
            assert_eq!(round_trip_timestamp(timestamp), timestamp);
        }
    }

    #[test]
    fn timestamp_to_duck_truncates_sub_microsecond_precision() {
        let timestamp = UNIX_EPOCH + StdDuration::new(42, 123_456_789);
        assert_eq!(round_trip_timestamp(timestamp), UNIX_EPOCH + StdDuration::new(42, 123_456_000));
    }

    #[test]
    fn timestamp_to_duck_rejects_pre_epoch_values() {
        let timestamp = UNIX_EPOCH - StdDuration::from_micros(1);
        assert!(matches!(timestamp.to_duck(), Err(DuckDBConversionError::ConversionError(_))));
    }

    #[test]
    fn logical_types_match_native_duckdb_types() {
        macro_rules! assert_logical_type {
            ($rust_type:ty, $duck_type:expr) => {{
                let mut logical_type =
                    <$rust_type as DuckLogicalType>::duck_logical_type().unwrap();
                assert_eq!(unsafe { duckdb_get_type_id(logical_type) }, $duck_type);
                unsafe { duckdb_destroy_logical_type(&mut logical_type) };
            }};
        }

        assert_logical_type!(DuckDate, DUCKDB_TYPE_DUCKDB_TYPE_DATE);
        assert_logical_type!(DuckTime, DUCKDB_TYPE_DUCKDB_TYPE_TIME);
        assert_logical_type!(DuckTimeNs, DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS);
        assert_logical_type!(DuckTimeTz, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ);
        assert_logical_type!(StdDuration, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL);
        assert_logical_type!(SystemTime, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP);
    }

    #[test]
    fn into_duck_value_selects_matching_variants() {
        let date = DuckDate { year: 2024, month: 2, day: 29 };
        let time = DuckTime { hour: 1, min: 2, sec: 3, micros: 4 };
        let time_ns = DuckTimeNs { hour: 5, min: 6, sec: 7, nanos: 8 };
        let time_tz = DuckTimeTz { hour: 9, min: 10, sec: 11, micros: 12, offset_secs: 3_600 };
        let interval = StdDuration::from_secs(13);
        let timestamp = UNIX_EPOCH + StdDuration::from_secs(14);

        assert_eq!(value::DuckValue::from(date), value::DuckValue::Date(date));
        assert_eq!(value::DuckValue::from(time), value::DuckValue::Time(time));
        assert_eq!(value::DuckValue::from(time_ns), value::DuckValue::TimeNs(time_ns));
        assert_eq!(value::DuckValue::from(time_tz), value::DuckValue::TimeTz(time_tz));
        assert_eq!(value::DuckValue::from(interval), value::DuckValue::Interval(interval));
        assert_eq!(value::DuckValue::from(timestamp), value::DuckValue::Timestamp(timestamp));
    }

    #[test]
    fn temporal_types_bind_through_real_prepared_statements() {
        use crate::{connection::Connection, types::value::DuckValue};

        let mut conn = Connection::open_in_memory().unwrap();
        let mut date = DuckDate { year: 2024, month: 2, day: 29 };
        let mut time = DuckTime { hour: 12, min: 30, sec: 45, micros: 123_456 };
        let mut time_ns = DuckTimeNs { hour: 12, min: 30, sec: 45, nanos: 123_456_789 };
        let mut time_tz =
            DuckTimeTz { hour: 12, min: 30, sec: 45, micros: 123_456, offset_secs: 19_800 };
        let mut interval = StdDuration::from_secs(90) + StdDuration::from_micros(7);
        let mut timestamp =
            UNIX_EPOCH + StdDuration::from_secs(1_700_000_000) + StdDuration::from_micros(123_456);

        let mut rows = conn
            .execute_with(
                "SELECT $1::DATE AS d, $2::TIME AS t, $3::TIME_NS AS tn, \
             $4::TIMETZ AS ttz, $5::INTERVAL AS i, $6::TIMESTAMP AS ts",
                &mut [
                    &mut date,
                    &mut time,
                    &mut time_ns,
                    &mut time_tz,
                    &mut interval,
                    &mut timestamp,
                ],
            )
            .unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("d"), Some(&DuckValue::Date(date)));
        assert_eq!(row.get("t"), Some(&DuckValue::Time(time)));
        assert_eq!(row.get("tn"), Some(&DuckValue::TimeNs(time_ns)));
        assert!(matches!(row.get("ttz"), Some(DuckValue::TimeTz(_))));
        assert_eq!(row.get("i"), Some(&DuckValue::Interval(interval)));
        assert_eq!(row.get("ts"), Some(&DuckValue::Timestamp(timestamp)));
    }

    #[test]
    fn temporal_types_append_through_real_appender() {
        use crate::{connection::Connection, types::value::DuckValue, AppendAble};

        struct TemporalRow {
            date: DuckDate,
            time: DuckTime,
            time_ns: DuckTimeNs,
            time_tz: DuckTimeTz,
            interval: StdDuration,
            timestamp: SystemTime,
        }

        impl AppendAble for TemporalRow {
            fn appender_append(
                &mut self,
                appender: crate::ffi::duckdb_appender,
            ) -> crate::error::Result<()> {
                self.date.appender_append(appender)?;
                self.time.appender_append(appender)?;
                self.time_ns.appender_append(appender)?;
                self.time_tz.appender_append(appender)?;
                self.interval.appender_append(appender)?;
                self.timestamp.appender_append(appender)
            }

            fn stmt_append(
                &mut self,
                _idx: u64,
                _stmt: crate::ffi::duckdb_prepared_statement,
            ) -> crate::error::Result<()> {
                unreachable!("TemporalRow is only used by an appender")
            }
        }

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE native_temporal (d DATE, t TIME, tn TIME_NS, ttz TIMETZ, i INTERVAL, ts TIMESTAMP)",
        ).unwrap();
        let mut row = TemporalRow {
            date: DuckDate { year: 2024, month: 2, day: 29 },
            time: DuckTime { hour: 12, min: 30, sec: 45, micros: 123_456 },
            time_ns: DuckTimeNs { hour: 12, min: 30, sec: 45, nanos: 123_456_789 },
            time_tz: DuckTimeTz {
                hour: 12,
                min: 30,
                sec: 45,
                micros: 123_456,
                offset_secs: 19_800,
            },
            interval: StdDuration::from_secs(90) + StdDuration::from_micros(7),
            timestamp: UNIX_EPOCH
                + StdDuration::from_secs(1_700_000_000)
                + StdDuration::from_micros(123_456),
        };
        {
            let mut appender = conn.appender("native_temporal", "main").unwrap();
            appender.append(&mut row).unwrap();
            appender.save().unwrap();
        }

        let result = conn.execute("SELECT d, t, tn, ttz, i, ts FROM native_temporal").unwrap();
        let stored = result.into_iter().next().unwrap().unwrap();
        assert_eq!(stored.get("d"), Some(&DuckValue::Date(row.date)));
        assert_eq!(stored.get("t"), Some(&DuckValue::Time(row.time)));
        assert_eq!(stored.get("tn"), Some(&DuckValue::TimeNs(row.time_ns)));
        assert!(matches!(stored.get("ttz"), Some(DuckValue::TimeTz(_))));
        assert_eq!(stored.get("i"), Some(&DuckValue::Interval(row.interval)));
        assert_eq!(stored.get("ts"), Some(&DuckValue::Timestamp(row.timestamp)));
    }

    #[test]
    fn pre_epoch_system_time_append_and_bind_match_to_duck_error() {
        use crate::connection::Connection;

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE native_pre_epoch (ts TIMESTAMP)").unwrap();
        let value = UNIX_EPOCH - StdDuration::from_micros(1);
        assert!(matches!(value.to_duck(), Err(DuckDBConversionError::ConversionError(_))));

        let mut bound = value;
        let bind_error = match conn.execute_with("SELECT $1::TIMESTAMP", &mut [&mut bound]) {
            Ok(_) => panic!("pre-epoch SystemTime binding should fail"),
            Err(error) => error,
        };
        assert!(matches!(bind_error, crate::error::Error::ConversionError(_)));

        let mut appended = value;
        let mut appender = conn.appender("native_pre_epoch", "main").unwrap();
        let append_error = appender.append(&mut appended).unwrap_err();
        assert!(matches!(append_error, crate::error::Error::ConversionError(_)));
    }
}
