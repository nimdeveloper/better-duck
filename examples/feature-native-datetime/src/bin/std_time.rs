//! INTERVAL and TIMESTAMP mapped to `std::time` types (no `chrono` feature).
//!
//! With `chrono` off, DuckDB `INTERVAL` decodes into [`std::time::Duration`] and
//! `TIMESTAMP` into [`std::time::SystemTime`]. Both are demonstrated by binding a
//! value as a parameter and reading it back, and by decoding a SQL literal.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::value::DuckValue;
use std::time::{Duration, UNIX_EPOCH};

/// Binds `dv` as `$1` in `SELECT $1 AS v` and returns the decoded value.
fn round_trip(
    conn: &mut Connection,
    mut dv: DuckValue,
) -> Result<DuckValue> {
    let row = conn.execute_with("SELECT $1 AS v", &mut [&mut dv])?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    // INTERVAL <-> Duration, bound as a parameter (microsecond precision preserved).
    let dur = Duration::from_secs(90) + Duration::from_micros(7);
    let got = round_trip(&mut conn, DuckValue::Interval(dur))?;
    println!("Interval bind round trip -> {got:?}");
    assert_eq!(got, DuckValue::Interval(dur));

    // INTERVAL literal: whole days decode into the equivalent Duration.
    let row = conn.execute("SELECT INTERVAL '3 days' AS i")?.next().expect("one row")?;
    let i = row.get("i").expect("column i").clone();
    println!("INTERVAL '3 days' -> {i:?}");
    assert_eq!(i, DuckValue::Interval(Duration::from_secs(3 * 86_400)));

    // TIMESTAMP <-> SystemTime, bound as a parameter.
    let ts = UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_micros(123_456);
    let got = round_trip(&mut conn, DuckValue::Timestamp(ts))?;
    println!("Timestamp bind round trip -> {got:?}");
    assert_eq!(got, DuckValue::Timestamp(ts));

    // TIMESTAMP literal one second past the Unix epoch.
    let row =
        conn.execute("SELECT TIMESTAMP '1970-01-01 00:00:01' AS ts")?.next().expect("one row")?;
    let lit = row.get("ts").expect("column ts").clone();
    println!("TIMESTAMP '1970-01-01 00:00:01' -> {lit:?}");
    assert_eq!(lit, DuckValue::Timestamp(UNIX_EPOCH + Duration::from_secs(1)));

    println!("\nINTERVAL and TIMESTAMP mapped to std::time types");
    Ok(())
}
