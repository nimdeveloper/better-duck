//! Lossless ±infinity for DuckDB `DATE`/`TIMESTAMP` values.
//!
//! DuckDB reserves the extreme values of a temporal column's physical integer for
//! ±infinity: a `DATE` of `i32::MAX`/`i32::MIN` days, and a timestamp of
//! `i64::MAX`/`i64::MIN` in its native unit (µs/s/ms/ns). Those are *not* finite
//! instants, so a chrono `NaiveDate`/`NaiveDateTime` cannot represent them — the
//! finite conversion would either overflow to a wrong date or fail with a generic
//! range error, silently corrupting an infinite value.
//!
//! [`DuckValue::TemporalInfinity`](crate::types::value::DuckValue::TemporalInfinity)
//! keeps ±infinity as a first-class value tagged with the temporal family it came
//! from ([`TemporalKind`]) and its [`Sign`], so it round-trips losslessly while the
//! finite variants keep holding ordinary `NaiveDate`/`NaiveDateTime` (or the
//! non-chrono equivalents). The read path guards every temporal column with the
//! matching `duckdb_is_finite_*` check; the write path rebuilds the exact sentinel.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::ffi::{
    duckdb_create_date, duckdb_create_timestamp, duckdb_create_timestamp_ms,
    duckdb_create_timestamp_ns, duckdb_create_timestamp_s, duckdb_create_timestamp_tz, duckdb_date,
    duckdb_timestamp, duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s, duckdb_type,
    duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_DATE, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
};

/// Which temporal family an infinite value belongs to.
///
/// DuckDB stores `TIMESTAMP` and `TIMESTAMP WITH TIME ZONE` in the same
/// microsecond wire format, but they are distinct logical types; keeping the kind
/// lets an infinite value round-trip back into the *same* column type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalKind {
    /// `DATE`.
    Date,
    /// `TIMESTAMP` (microseconds).
    Timestamp,
    /// `TIMESTAMP_S` (seconds).
    TimestampS,
    /// `TIMESTAMP_MS` (milliseconds).
    TimestampMs,
    /// `TIMESTAMP_NS` (nanoseconds).
    TimestampNs,
    /// `TIMESTAMP WITH TIME ZONE` (microseconds).
    TimestampTz,
}

/// The direction of an infinite temporal value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sign {
    /// `+infinity` — the largest sentinel (`i32::MAX` / `i64::MAX`).
    Positive,
    /// `-infinity` — the smallest sentinel (`i32::MIN` / `i64::MIN`).
    Negative,
}

impl TemporalKind {
    /// The `duckdb_type` id of the column this infinite value belongs to.
    #[must_use]
    pub fn type_id(self) -> duckdb_type {
        match self {
            TemporalKind::Date => DUCKDB_TYPE_DUCKDB_TYPE_DATE,
            TemporalKind::Timestamp => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
            TemporalKind::TimestampS => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
            TemporalKind::TimestampMs => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
            TemporalKind::TimestampNs => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
            TemporalKind::TimestampTz => DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
        }
    }
}

/// Builds the `duckdb_value` holding the ±infinity sentinel for `kind`/`sign`.
///
/// The caller owns the returned value and must destroy it with
/// `duckdb_destroy_value`.
#[must_use]
pub(crate) fn infinity_to_duck(
    kind: TemporalKind,
    sign: Sign,
) -> duckdb_value {
    // `i32::MAX`/`i64::MAX` are +infinity; the corresponding MINs are -infinity.
    match kind {
        TemporalKind::Date => {
            // DuckDB's date sentinels are ±INT32_MAX (ninfinity() == -INT32_MAX ==
            // i32::MIN + 1), *not* i32::MIN — using i32::MIN would read back as a
            // finite, out-of-range date instead of -infinity.
            let days = match sign {
                Sign::Positive => i32::MAX,
                Sign::Negative => -i32::MAX,
            };
            // SAFETY: `duckdb_date { days }` is a fully initialised sentinel.
            unsafe { duckdb_create_date(duckdb_date { days }) }
        },
        TemporalKind::Timestamp => {
            // SAFETY: fully initialised sentinel timestamp.
            unsafe { duckdb_create_timestamp(duckdb_timestamp { micros: sentinel_i64(sign) }) }
        },
        TemporalKind::TimestampS => {
            // SAFETY: fully initialised sentinel timestamp_s.
            unsafe { duckdb_create_timestamp_s(duckdb_timestamp_s { seconds: sentinel_i64(sign) }) }
        },
        TemporalKind::TimestampMs => {
            // SAFETY: fully initialised sentinel timestamp_ms.
            unsafe {
                duckdb_create_timestamp_ms(duckdb_timestamp_ms { millis: sentinel_i64(sign) })
            }
        },
        TemporalKind::TimestampNs => {
            // SAFETY: fully initialised sentinel timestamp_ns.
            unsafe { duckdb_create_timestamp_ns(duckdb_timestamp_ns { nanos: sentinel_i64(sign) }) }
        },
        TemporalKind::TimestampTz => {
            // TIMESTAMP_TZ shares the microsecond wire format with TIMESTAMP.
            // SAFETY: fully initialised sentinel timestamp.
            unsafe { duckdb_create_timestamp_tz(duckdb_timestamp { micros: sentinel_i64(sign) }) }
        },
    }
}

/// The `i64` infinity sentinel for a sign. DuckDB uses ±INT64_MAX
/// (`ninfinity() == -INT64_MAX == i64::MIN + 1`), *not* `i64::MIN`.
#[inline]
fn sentinel_i64(sign: Sign) -> i64 {
    match sign {
        Sign::Positive => i64::MAX,
        Sign::Negative => -i64::MAX,
    }
}

/// The [`Sign`] of an infinite value from its raw sentinel integer
/// (`>= 0` ⇒ `+infinity`, i.e. the `MAX` sentinel).
#[inline]
#[must_use]
pub(crate) fn sign_of(raw: i64) -> Sign {
    if raw >= 0 {
        Sign::Positive
    } else {
        Sign::Negative
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::types::value::DuckValue;

    /// Reads column `c` from `SELECT <expr> AS c` and returns the `DuckValue`.
    fn read_scalar(
        conn: &mut Connection,
        expr: &str,
    ) -> DuckValue {
        let sql = format!("SELECT {expr} AS c");
        let mut rows = conn.execute(sql).unwrap();
        let row = rows.next().unwrap().unwrap();
        row.get("c").unwrap().clone()
    }

    #[test]
    fn sign_of_maps_max_and_min_sentinels() {
        assert_eq!(sign_of(i64::MAX), Sign::Positive);
        assert_eq!(sign_of(0), Sign::Positive);
        assert_eq!(sign_of(i64::MIN), Sign::Negative);
        assert_eq!(sign_of(-1), Sign::Negative);
    }

    #[test]
    fn reads_date_infinities_without_becoming_a_sentinel_or_error() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert_eq!(
            read_scalar(&mut conn, "'infinity'::DATE"),
            DuckValue::TemporalInfinity { kind: TemporalKind::Date, sign: Sign::Positive }
        );
        assert_eq!(
            read_scalar(&mut conn, "'-infinity'::DATE"),
            DuckValue::TemporalInfinity { kind: TemporalKind::Date, sign: Sign::Negative }
        );
        // A finite date still reads as a finite Date, not an infinity.
        assert!(matches!(read_scalar(&mut conn, "'2024-03-15'::DATE"), DuckValue::Date(_)));
    }

    #[test]
    fn reads_timestamp_family_infinities() {
        let mut conn = Connection::open_in_memory().unwrap();
        for (expr, kind) in [
            ("'infinity'::TIMESTAMP", TemporalKind::Timestamp),
            ("'infinity'::TIMESTAMP_S", TemporalKind::TimestampS),
            ("'infinity'::TIMESTAMP_MS", TemporalKind::TimestampMs),
            ("'infinity'::TIMESTAMP_NS", TemporalKind::TimestampNs),
            ("'infinity'::TIMESTAMPTZ", TemporalKind::TimestampTz),
        ] {
            assert_eq!(
                read_scalar(&mut conn, expr),
                DuckValue::TemporalInfinity { kind, sign: Sign::Positive },
                "reading {expr}"
            );
        }
        assert_eq!(
            read_scalar(&mut conn, "'-infinity'::TIMESTAMP"),
            DuckValue::TemporalInfinity { kind: TemporalKind::Timestamp, sign: Sign::Negative }
        );
    }

    /// An infinite value written back through the appender reproduces the *same*
    /// sentinel — proving the write path rebuilds ±infinity rather than dropping it.
    #[test]
    fn infinities_round_trip_through_the_appender() {
        let mut conn = Connection::open_in_memory().unwrap();
        // One single-column table per kind (the appender's `append` writes one column
        // per call). Cover DATE, TIMESTAMP and each precision + TZ, both signs.
        let cases = [
            ("d DATE", "d", TemporalKind::Date, Sign::Positive),
            ("d DATE", "d", TemporalKind::Date, Sign::Negative),
            ("ts TIMESTAMP", "ts", TemporalKind::Timestamp, Sign::Negative),
            ("ts TIMESTAMP_S", "ts", TemporalKind::TimestampS, Sign::Positive),
            ("ts TIMESTAMP_MS", "ts", TemporalKind::TimestampMs, Sign::Positive),
            ("ts TIMESTAMP_NS", "ts", TemporalKind::TimestampNs, Sign::Negative),
            ("ts TIMESTAMPTZ", "ts", TemporalKind::TimestampTz, Sign::Positive),
        ];
        for (i, (col_def, col, kind, sign)) in cases.into_iter().enumerate() {
            let table = format!("t{i}");
            conn.execute_batch(format!("CREATE TABLE {table} ({col_def})")).unwrap();
            {
                let mut app = conn.appender(&table, "main").unwrap();
                app.append(&mut DuckValue::TemporalInfinity { kind, sign }).unwrap();
                app.save().unwrap();
            }
            let sql = format!("SELECT {col} AS c FROM {table}");
            let mut rows = conn.execute(sql).unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(
                row.get("c"),
                Some(&DuckValue::TemporalInfinity { kind, sign }),
                "round trip for {col_def} {sign:?}"
            );
        }
    }
}
