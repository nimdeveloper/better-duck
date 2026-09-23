//! Standalone `OwnedValue` objects and their typed getters.
//!
//! `DuckValue::to_owned_value` materialises a value into an [`OwnedValue`] — DuckDB's
//! standalone `duckdb_value` — which exposes introspection the row API does not: SQL-NULL
//! check, SQL string rendering, `STRUCT` child access, and typed scalar getters for the
//! 128-bit, UUID, and temporal types. Each typed getter validates the value's type and
//! errors on a mismatch.

use std::collections::HashMap;

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::uuid::DuckUuid;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{IntervalParts, OwnedValue};
use common::{section, show, Result};

/// Materialise a `DuckValue` into an `OwnedValue`, mapping the conversion error into the
/// crate's `Error` so `?` works in `main`.
fn owned(value: DuckValue) -> Result<OwnedValue> {
    value.to_owned_value().map_err(Error::ConversionError)
}

fn main() -> Result<()> {
    section("is_null and to_sql_string");
    let null_v = owned(DuckValue::Null)?;
    show("Null.is_null", null_v.is_null());
    assert!(null_v.is_null());

    let int_v = owned(DuckValue::Int(42))?;
    show("Int(42).is_null", int_v.is_null());
    show("Int(42).to_sql_string", int_v.to_sql_string());
    assert!(!int_v.is_null());
    assert_eq!(int_v.to_sql_string().as_deref(), Some("42"));
    // DuckDB renders a VARCHAR value quoted.
    assert_eq!(owned(DuckValue::text("hi"))?.to_sql_string().as_deref(), Some("'hi'"));

    section("128-bit and UUID getters");
    assert_eq!(owned(DuckValue::HugeInt(-170))?.get_hugeint().map_err(Error::ConversionError)?, -170);
    assert_eq!(owned(DuckValue::UHugeInt(340))?.get_uhugeint().map_err(Error::ConversionError)?, 340);
    let uuid = DuckUuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    let got = owned(DuckValue::Uuid(uuid))?.get_uuid().map_err(Error::ConversionError)?;
    show("round-tripped uuid", got);
    assert_eq!(got, uuid);

    section("STRUCT child access");
    let s = owned(DuckValue::Struct(HashMap::from([("n".to_owned(), DuckValue::Int(7))])))?;
    let child = s.struct_child(0).expect("struct child 0");
    show("struct child 0 as SQL", child.to_sql_string());
    assert_eq!(child.to_sql_string().as_deref(), Some("7"));
    // Out-of-range child index yields None.
    assert!(s.struct_child(5).is_none());

    section("temporal getters (read real DATE / TIME / TIMESTAMP / INTERVAL values)");
    let (d_val, t_val, ts_val, iv_val) = temporal_values()?;
    // DATE -> day count since 1970-01-01. 2024-03-15 is 19797 days after the epoch.
    let d = owned(d_val)?;
    show("get_date_days", d.get_date_days().map_err(Error::ConversionError)?);
    assert_eq!(d.get_date_days().map_err(Error::ConversionError)?, 19_797);
    // TIME -> micros since midnight. 01:02:03 = 3723 s.
    let t = owned(t_val)?;
    assert_eq!(t.get_time_micros().map_err(Error::ConversionError)?, 3_723 * 1_000_000);
    // TIMESTAMP -> micros since the epoch. 1 second after the epoch.
    let ts = owned(ts_val)?;
    assert_eq!(ts.get_timestamp_micros().map_err(Error::ConversionError)?, 1_000_000);
    // INTERVAL -> (months, days, micros). 5 seconds lives in the micros field.
    let iv = owned(iv_val)?;
    let parts: IntervalParts = iv.get_interval().map_err(Error::ConversionError)?;
    show("get_interval", parts);
    assert_eq!(parts.months, 0);
    assert_eq!(parts.days, 0);
    assert_eq!(parts.micros, 5 * 1_000_000);

    section("typed getters reject a type mismatch");
    // A HUGEINT getter on a UHUGEINT value errors...
    assert!(owned(DuckValue::UHugeInt(1))?.get_hugeint().is_err());
    // ...and a TIMESTAMP getter on a plain integer errors.
    assert!(owned(DuckValue::Int(1))?.get_timestamp_micros().is_err());

    Ok(())
}

/// Read one row of temporal values and return each cell as an owned `DuckValue`, so the
/// borrowed result can be dropped before we introspect.
fn temporal_values() -> Result<(DuckValue, DuckValue, DuckValue, DuckValue)> {
    let mut conn = Connection::open_in_memory()?;
    let set = conn
        .execute(
            "SELECT DATE '2024-03-15' AS d,
                    TIME '01:02:03' AS t,
                    TIMESTAMP '1970-01-01 00:00:01' AS ts,
                    INTERVAL 5 SECOND AS iv",
        )?
        .materialize()?;
    let first = set.first().expect("one row");
    Ok((
        first.get("d").cloned().expect("d"),
        first.get("t").cloned().expect("t"),
        first.get("ts").cloned().expect("ts"),
        first.get("iv").cloned().expect("iv"),
    ))
}
