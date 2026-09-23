//! Native (no-`chrono`) DATE / TIME / TIME_NS / TIME WITH TIME ZONE round trips.
//!
//! With the `chrono` feature OFF, DuckDB's temporal columns decode into
//! `better_duck_core`'s own component structs instead of `chrono` types:
//!   DATE    -> DuckDate  { year, month, day }
//!   TIME    -> DuckTime  { hour, min, sec, micros }
//!   TIME_NS -> DuckTimeNs { hour, min, sec, nanos }
//!   TIMETZ  -> DuckTimeTz { hour, min, sec, micros, offset_secs }
//!
//! Each value is produced from a SQL literal and matched back to its struct.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::date_native::{DuckDate, DuckTime, DuckTimeNs, DuckTimeTz};
use better_duck_core::types::value::DuckValue;

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    // DATE literal -> DuckDate.
    let row = conn.execute("SELECT DATE '2020-03-15' AS d")?.next().expect("one row")?;
    let d = row.get("d").expect("column d").clone();
    println!("DATE '2020-03-15' -> {d:?}");
    assert_eq!(d, DuckValue::Date(DuckDate { year: 2020, month: 3, day: 15 }));

    // TIME literal -> DuckTime (microsecond precision).
    let row = conn.execute("SELECT TIME '14:30:45.123456' AS t")?.next().expect("one row")?;
    let t = row.get("t").expect("column t").clone();
    println!("TIME '14:30:45.123456' -> {t:?}");
    assert_eq!(t, DuckValue::Time(DuckTime { hour: 14, min: 30, sec: 45, micros: 123_456 }));

    // TIME_NS literal -> DuckTimeNs (nanosecond precision).
    let row =
        conn.execute("SELECT '14:30:45.123456789'::TIME_NS AS tn")?.next().expect("one row")?;
    let tn = row.get("tn").expect("column tn").clone();
    println!("'14:30:45.123456789'::TIME_NS -> {tn:?}");
    assert_eq!(
        tn,
        DuckValue::TimeNs(DuckTimeNs { hour: 14, min: 30, sec: 45, nanos: 123_456_789 })
    );

    // TIME WITH TIME ZONE literal -> DuckTimeTz. A UTC (+00) offset keeps the
    // components stable across DuckDB versions (no offset normalization).
    let row =
        conn.execute("SELECT '14:30:45.123456+00'::TIMETZ AS ttz")?.next().expect("one row")?;
    let ttz = row.get("ttz").expect("column ttz").clone();
    println!("'14:30:45.123456+00'::TIMETZ -> {ttz:?}");
    assert_eq!(
        ttz,
        DuckValue::TimeTz(DuckTimeTz {
            hour: 14,
            min: 30,
            sec: 45,
            micros: 123_456,
            offset_secs: 0,
        })
    );

    println!("\nnative date/time literals decoded into their component structs");
    Ok(())
}
