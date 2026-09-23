//! Temporal `DuckValue` variants, with the `chrono` feature ON.
//!
//! With `chrono` enabled these variants carry chrono types:
//!   `Date(NaiveDate)`, `Time(NaiveTime)`, `Timestamp(NaiveDateTime)`,
//!   `TimestampS/Ms/Ns(NaiveDateTime)`, `TimestampTz(DateTime<Utc>)`,
//!   `TimeTz(date_chrono::TimeTz)`, `TimeNs(NaiveTime)`, `Interval(chrono::Duration)`.
//! The feature-independent `TemporalInfinity { kind, sign }` represents DuckDB's
//! reserved ±infinity sentinels, which no finite chrono value can hold.
//!
//! Most values are easiest to read via SQL casts (`DATE '...'`, `'...'::TIMESTAMP_S`,
//! `INTERVAL 3 DAY`, `'infinity'::TIMESTAMP`). TIME_NS has no literal cast here, so it
//! is read from a `TIME_NS` column.

use better_duck_core::connection::Connection;
use better_duck_core::types::date_chrono::TimeTz;
use better_duck_core::types::temporal::{Sign, TemporalKind};
use better_duck_core::types::value::DuckValue;
use chrono::{Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use common::{section, show, Result};

/// Reads the single column of `SELECT <expr> AS v`.
fn read_scalar(
    conn: &mut Connection,
    expr: &str,
) -> Result<DuckValue> {
    let sql = format!("SELECT {expr} AS v");
    let row = conn.execute(sql)?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("Date / Time / Timestamp (chrono naive types)");
    let d = read_scalar(&mut conn, "DATE '2020-01-01'")?;
    show("DATE '2020-01-01'", &d);
    assert_eq!(d, DuckValue::Date(NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()));

    let t = read_scalar(&mut conn, "TIME '12:34:56'")?;
    show("TIME '12:34:56'", &t);
    assert_eq!(t, DuckValue::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()));

    let ts = read_scalar(&mut conn, "TIMESTAMP '2020-01-01 12:34:56'")?;
    show("TIMESTAMP", &ts);
    let expected_ts = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap().and_hms_opt(12, 34, 56).unwrap();
    assert_eq!(ts, DuckValue::Timestamp(expected_ts));

    section("precision-tagged timestamps: TIMESTAMP_S / _MS / _NS");
    // Second precision truncates any sub-second part.
    let tss = read_scalar(&mut conn, "'2020-01-01 12:34:56'::TIMESTAMP_S")?;
    show("TIMESTAMP_S", &tss);
    assert_eq!(tss, DuckValue::TimestampS(expected_ts));
    // Millisecond precision keeps .123.
    let tsm = read_scalar(&mut conn, "'2020-01-01 12:34:56.123'::TIMESTAMP_MS")?;
    show("TIMESTAMP_MS", &tsm);
    let expected_ms =
        NaiveDate::from_ymd_opt(2020, 1, 1).unwrap().and_hms_milli_opt(12, 34, 56, 123).unwrap();
    assert_eq!(tsm, DuckValue::TimestampMs(expected_ms));
    // Nanosecond precision keeps the full sub-second field.
    let tsn = read_scalar(&mut conn, "'2020-01-01 12:34:56.123456789'::TIMESTAMP_NS")?;
    show("TIMESTAMP_NS", &tsn);
    let expected_ns = NaiveDate::from_ymd_opt(2020, 1, 1)
        .unwrap()
        .and_hms_nano_opt(12, 34, 56, 123_456_789)
        .unwrap();
    assert_eq!(tsn, DuckValue::TimestampNs(expected_ns));

    section("timezone-aware: TIMESTAMPTZ / TIMETZ");
    // TIMESTAMPTZ decodes to a DateTime<Utc>.
    let tstz = read_scalar(&mut conn, "'2024-06-01 12:00:00+00'::TIMESTAMPTZ")?;
    show("TIMESTAMPTZ", &tstz);
    assert_eq!(tstz, DuckValue::TimestampTz(Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap()));
    // TIMETZ carries a NaiveTime plus a UTC offset in seconds. (Offset 0 avoids
    // version-dependent normalization ambiguity.)
    let ttz = read_scalar(&mut conn, "'12:00:00+00'::TIMETZ")?;
    show("TIMETZ", &ttz);
    assert_eq!(
        ttz,
        DuckValue::TimeTz(TimeTz {
            time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            offset_secs: 0,
        })
    );

    section("TIME_NS (read from a TIME_NS column)");
    conn.execute_batch("CREATE TABLE t_ns (v TIME_NS)")?;
    conn.execute_batch("INSERT INTO t_ns VALUES ('12:00:00')")?;
    let row = conn.execute("SELECT v FROM t_ns")?.next().expect("one row")?;
    show("TIME_NS", row.get("v"));
    assert_eq!(row.get("v"), Some(&DuckValue::TimeNs(NaiveTime::from_hms_opt(12, 0, 0).unwrap())));

    section("Interval (chrono::Duration)");
    // DuckDB normalizes months to 30 days; a bare `INTERVAL 3 DAY` is 3 days.
    let interval = read_scalar(&mut conn, "INTERVAL 3 DAY")?;
    show("INTERVAL 3 DAY", &interval);
    assert_eq!(interval, DuckValue::Interval(Duration::days(3)));

    section("TemporalInfinity — DuckDB's reserved ±infinity sentinels");
    // These are not finite instants, so they cannot be a NaiveDate/NaiveDateTime;
    // they decode to a tagged TemporalInfinity that round-trips into the same family.
    let pos = read_scalar(&mut conn, "'infinity'::TIMESTAMP")?;
    show("'infinity'::TIMESTAMP", &pos);
    assert_eq!(
        pos,
        DuckValue::TemporalInfinity { kind: TemporalKind::Timestamp, sign: Sign::Positive }
    );
    let neg = read_scalar(&mut conn, "'-infinity'::DATE")?;
    show("'-infinity'::DATE", &neg);
    assert_eq!(neg, DuckValue::TemporalInfinity { kind: TemporalKind::Date, sign: Sign::Negative });

    println!("\nall temporal variants verified");
    Ok(())
}
