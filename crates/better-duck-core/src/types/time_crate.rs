//! Bind/append [`time`](https://docs.rs/time) crate values as DuckDB temporals
//! (feature `time`).
//!
//! This is the **write path** only — it implements [`AppendAble`] for the common
//! `time` types so they can be used as prepared-statement parameters and appender
//! inputs, going straight through DuckDB's dedicated `duckdb_bind_*` / `duckdb_append_*`
//! FFI (no `duckdb_value` round-trip, no dependency on the `DuckValue` temporal
//! representation). It therefore coexists with `chrono` — enabling `time` does not
//! change what type reads produce.
//!
//! Reading a row back into a `time` type goes through [`DuckValue`](crate::types::value::DuckValue)
//! and an explicit conversion by the caller.

use time::{Date, Duration, OffsetDateTime, PrimitiveDateTime, Time};

use crate::error::Result;
use crate::ffi::{
    duckdb_append_date, duckdb_append_interval, duckdb_append_time, duckdb_append_timestamp,
    duckdb_appender, duckdb_bind_date, duckdb_bind_interval, duckdb_bind_time,
    duckdb_bind_timestamp, duckdb_bind_timestamp_tz, duckdb_date, duckdb_interval,
    duckdb_prepared_statement, duckdb_time, duckdb_timestamp,
};
use crate::helpers::duck_result::{check_append, check_state};
use crate::types::appendable::AppendAble;

/// Julian day number of the Unix epoch (1970-01-01); `Date::to_julian_day` is relative
/// to the Julian calendar epoch, so subtracting this yields days-since-Unix-epoch.
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;

fn date_to_raw(d: &Date) -> duckdb_date {
    duckdb_date { days: d.to_julian_day() - UNIX_EPOCH_JULIAN_DAY }
}

fn time_to_micros(t: &Time) -> i64 {
    (t.hour() as i64 * 3_600 + t.minute() as i64 * 60 + t.second() as i64) * 1_000_000
        + (t.microsecond() as i64)
}

fn primitive_to_micros(dt: &PrimitiveDateTime) -> i64 {
    let days = (dt.date().to_julian_day() - UNIX_EPOCH_JULIAN_DAY) as i64;
    days * 86_400_000_000 + time_to_micros(&dt.time())
}

fn offset_to_micros(dt: &OffsetDateTime) -> i64 {
    (dt.unix_timestamp_nanos() / 1_000) as i64
}

impl AppendAble for Date {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        // SAFETY: `stmt`/`idx` are valid; `date_to_raw` yields a valid duckdb_date.
        check_state(unsafe { duckdb_bind_date(stmt, idx, date_to_raw(self)) })
    }
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        // SAFETY: `appender` is a valid appender; the raw date is valid.
        let rc = unsafe { duckdb_append_date(appender, date_to_raw(self)) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { check_append(rc, appender) }
    }
}

impl AppendAble for Time {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        let raw = duckdb_time { micros: time_to_micros(self) };
        // SAFETY: `stmt`/`idx` are valid; `raw` is a valid duckdb_time.
        check_state(unsafe { duckdb_bind_time(stmt, idx, raw) })
    }
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        let raw = duckdb_time { micros: time_to_micros(self) };
        // SAFETY: `appender` is valid; `raw` is a valid duckdb_time.
        let rc = unsafe { duckdb_append_time(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { check_append(rc, appender) }
    }
}

impl AppendAble for PrimitiveDateTime {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        let raw = duckdb_timestamp { micros: primitive_to_micros(self) };
        // SAFETY: `stmt`/`idx` are valid; `raw` is a valid duckdb_timestamp.
        check_state(unsafe { duckdb_bind_timestamp(stmt, idx, raw) })
    }
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        let raw = duckdb_timestamp { micros: primitive_to_micros(self) };
        // SAFETY: `appender` is valid; `raw` is a valid duckdb_timestamp.
        let rc = unsafe { duckdb_append_timestamp(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { check_append(rc, appender) }
    }
}

impl AppendAble for OffsetDateTime {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        let raw = duckdb_timestamp { micros: offset_to_micros(self) };
        // SAFETY: `stmt`/`idx` are valid; `raw` is a valid UTC-micros duckdb_timestamp.
        check_state(unsafe { duckdb_bind_timestamp_tz(stmt, idx, raw) })
    }
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        // No dedicated `duckdb_append_timestamp_tz`; TIMESTAMPTZ shares the UTC-micros
        // wire format with TIMESTAMP, so append via the timestamp path.
        let raw = duckdb_timestamp { micros: offset_to_micros(self) };
        // SAFETY: `appender` is valid; `raw` is a valid UTC-micros duckdb_timestamp.
        let rc = unsafe { duckdb_append_timestamp(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { check_append(rc, appender) }
    }
}

impl AppendAble for Duration {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: duckdb_prepared_statement,
    ) -> Result<()> {
        let raw = duckdb_interval { months: 0, days: 0, micros: self.whole_microseconds() as i64 };
        // SAFETY: `stmt`/`idx` are valid; `raw` is a valid duckdb_interval.
        check_state(unsafe { duckdb_bind_interval(stmt, idx, raw) })
    }
    fn appender_append(
        &mut self,
        appender: duckdb_appender,
    ) -> Result<()> {
        let raw = duckdb_interval { months: 0, days: 0, micros: self.whole_microseconds() as i64 };
        // SAFETY: `appender` is valid; `raw` is a valid duckdb_interval.
        let rc = unsafe { duckdb_append_interval(appender, raw) };
        // SAFETY: `appender` is valid and non-null.
        unsafe { check_append(rc, appender) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::types::value::DuckValue;
    use time::Month;

    /// Binds `value` and asserts `SELECT <value> <op>` compares equal to the DuckDB
    /// literal, so the check is independent of what type reads produce.
    fn assert_binds_equal(
        sql: &str,
        value: &mut dyn AppendAble,
    ) {
        let mut conn = Connection::open_in_memory().unwrap();
        let mut rows = conn.execute_with(sql, &mut [value]).unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("eq"), Some(&DuckValue::Boolean(true)), "for `{sql}`");
    }

    #[test]
    fn binds_date() {
        let mut d = Date::from_calendar_date(2024, Month::February, 29).unwrap();
        assert_binds_equal("SELECT $1::DATE = DATE '2024-02-29' AS eq", &mut d);
    }

    #[test]
    fn binds_time() {
        let mut t = Time::from_hms_micro(12, 30, 45, 123_456).unwrap();
        assert_binds_equal("SELECT $1::TIME = TIME '12:30:45.123456' AS eq", &mut t);
    }

    #[test]
    fn binds_primitive_datetime() {
        let date = Date::from_calendar_date(2024, Month::February, 29).unwrap();
        let time = Time::from_hms_micro(12, 30, 45, 123_456).unwrap();
        let mut dt = PrimitiveDateTime::new(date, time);
        assert_binds_equal(
            "SELECT $1::TIMESTAMP = TIMESTAMP '2024-02-29 12:30:45.123456' AS eq",
            &mut dt,
        );
    }

    #[test]
    fn binds_offset_datetime() {
        let date = Date::from_calendar_date(2024, Month::February, 29).unwrap();
        let time = Time::from_hms_micro(12, 30, 45, 123_456).unwrap();
        let mut odt = PrimitiveDateTime::new(date, time).assume_utc();
        assert_binds_equal(
            "SELECT $1::TIMESTAMPTZ = TIMESTAMPTZ '2024-02-29 12:30:45.123456+00' AS eq",
            &mut odt,
        );
    }

    #[test]
    fn binds_duration() {
        let mut dur = Duration::microseconds(90_000_007);
        assert_binds_equal("SELECT $1::INTERVAL = INTERVAL 90000007 MICROSECOND AS eq", &mut dur);
    }
}
