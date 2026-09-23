//! Parameterized queries with `execute_with` — positional `$1`, `$2`, … binds.
//!
//! `execute_with` takes `&mut [&mut dyn AppendAble]`. Any type implementing `AppendAble`
//! can be a bind value: primitives (`i32`, `f64`, `&str`/`String`, …) and `DuckValue`.
//! Mixed types coerce to `&mut dyn AppendAble` at the call site.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE events (id INTEGER, label TEXT, score DOUBLE);
         INSERT INTO events VALUES (1,'alpha',9.5),(2,'beta',7.2),(3,'gamma',3.1);",
    )?;

    section("single positional parameter (primitive bind)");
    let mut threshold = 8.0f64;
    let hits = conn
        .execute_with("SELECT id FROM events WHERE score > $1", &mut [&mut threshold])?
        .count();
    show("rows with score > 8.0", hits);
    assert_eq!(hits, 1);

    section("heterogeneous parameters (different Rust types)");
    let mut min_id = 1i32;
    let mut label = String::from("gamma");
    let matched = conn
        .execute_with(
            "SELECT id FROM events WHERE id >= $1 AND label = $2",
            &mut [&mut min_id, &mut label],
        )?
        .count();
    show("rows matching id>=1 AND label='gamma'", matched);
    assert_eq!(matched, 1);

    section("binding a DuckValue and reading NULL back");
    let mut name = DuckValue::text("beta");
    let rows: Vec<_> = conn
        .execute_with("SELECT score FROM events WHERE label = $1", &mut [&mut name])?
        .collect::<Result<_>>()?;
    assert_eq!(rows[0].get("score"), Some(&DuckValue::Double(7.2)));

    // Bind an explicit SQL NULL with DuckValue::Null.
    let mut null_bind = DuckValue::Null;
    let coalesced: Vec<_> = conn
        .execute_with("SELECT COALESCE($1, 'fallback') AS v", &mut [&mut null_bind])?
        .collect::<Result<_>>()?;
    show("COALESCE(NULL, 'fallback')", coalesced[0].get("v"));
    assert_eq!(coalesced[0].get("v"), Some(&DuckValue::text("fallback")));

    Ok(())
}
