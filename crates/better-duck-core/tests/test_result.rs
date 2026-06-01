use better_duck_core::{connection::Connection, types::value::DuckValue};
#[cfg(feature = "chrono")]
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

// DATE

#[cfg(feature = "chrono")]
#[test]
fn round_trip_date() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (d DATE)")?;
    conn.execute_batch("INSERT INTO t VALUES ('2024-03-15'::DATE)")?;
    let mut stmt = conn.db().prepare("SELECT d FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
    assert_eq!(row.get("d"), Some(&DuckValue::Date(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// TIME

#[cfg(feature = "chrono")]
#[test]
fn round_trip_time() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (t TIME)")?;
    conn.execute_batch("INSERT INTO t VALUES ('14:30:45'::TIME)")?;
    let mut stmt = conn.db().prepare("SELECT t FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
    assert_eq!(row.get("t"), Some(&DuckValue::Time(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// TIMESTAMP

#[cfg(feature = "chrono")]
#[test]
fn round_trip_timestamp() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (ts TIMESTAMP)")?;
    conn.execute_batch("INSERT INTO t VALUES ('2024-03-15 14:30:45'::TIMESTAMP)")?;
    let mut stmt = conn.db().prepare("SELECT ts FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 3, 15).unwrap(),
        NaiveTime::from_hms_opt(14, 30, 45).unwrap(),
    );
    assert_eq!(row.get("ts"), Some(&DuckValue::Timestamp(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// TIMESTAMP_S

#[cfg(feature = "chrono")]
#[test]
fn round_trip_timestamp_s() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (ts TIMESTAMP_S)")?;
    conn.execute_batch("INSERT INTO t VALUES ('2024-01-01 00:00:00'::TIMESTAMP_S)")?;
    let mut stmt = conn.db().prepare("SELECT ts FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    assert_eq!(row.get("ts"), Some(&DuckValue::TimestampS(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// TIMESTAMP_MS

#[cfg(feature = "chrono")]
#[test]
fn round_trip_timestamp_ms() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (ts TIMESTAMP_MS)")?;
    conn.execute_batch("INSERT INTO t VALUES ('2024-01-01 00:00:00'::TIMESTAMP_MS)")?;
    let mut stmt = conn.db().prepare("SELECT ts FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    assert_eq!(row.get("ts"), Some(&DuckValue::TimestampMs(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// TIMESTAMP_NS

#[cfg(feature = "chrono")]
#[test]
fn round_trip_timestamp_ns() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (ts TIMESTAMP_NS)")?;
    conn.execute_batch("INSERT INTO t VALUES ('2024-01-01 00:00:00'::TIMESTAMP_NS)")?;
    let mut stmt = conn.db().prepare("SELECT ts FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    let expected = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    assert_eq!(row.get("ts"), Some(&DuckValue::TimestampNs(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// INTERVAL

#[cfg(feature = "chrono")]
#[test]
fn round_trip_interval() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    // Use a pure-day interval to avoid month/day approximation ambiguity.
    conn.execute_batch("CREATE TABLE t (iv INTERVAL)")?;
    conn.execute_batch("INSERT INTO t VALUES (INTERVAL '2 days')")?;
    let mut stmt = conn.db().prepare("SELECT iv FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    // 2 days = 2 * 86_400 * 1_000_000 µs
    let expected = Duration::microseconds(2 * 86_400 * 1_000_000);
    assert_eq!(row.get("iv"), Some(&DuckValue::Interval(expected)));
    assert!(result.next().is_none());
    Ok(())
}

// BLOB

#[test]
fn round_trip_blob() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (b BLOB)")?;
    // DuckDB hex blob literal
    conn.execute_batch("INSERT INTO t VALUES ('\\xDE\\xAD\\xBE\\xEF'::BLOB)")?;
    let mut stmt = conn.db().prepare("SELECT b FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    assert_eq!(row.get("b"), Some(&DuckValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])));
    assert!(result.next().is_none());
    Ok(())
}

// ENUM

#[test]
fn round_trip_enum() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TYPE mood AS ENUM ('happy', 'sad')")?;
    conn.execute_batch("CREATE TABLE t (m mood)")?;
    conn.execute_batch("INSERT INTO t VALUES ('happy'::mood)")?;
    let mut stmt = conn.db().prepare("SELECT m FROM t")?;
    let mut result = stmt.execute()?;
    let row = result.next().expect("expected one row")?;
    assert_eq!(row.get("m"), Some(&DuckValue::Enum("happy".to_string())));
    assert!(result.next().is_none());
    Ok(())
}
