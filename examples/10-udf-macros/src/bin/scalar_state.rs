//! Registration-time state shared into a scalar UDF via `state(...)` +
//! `duck_state!`.
//!
//! `#[duckdb_scalar(state(Type, init))]` gives the function a `Type` value fixed
//! at registration; the body reads a clone of it with `duck_state!(Type)`.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_state, duckdb_scalar};
use common::{section, show, Result};

/// Adds a fixed offset (10) — supplied as registration state — to every input.
#[duckdb_scalar(state(i32, 10))]
fn add_offset(x: i32) -> i32 {
    x + duck_state!(i32)
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    add_offset::register(&mut conn)?;

    section("add_offset(i32) -> i32 reads its registration-time state (10)");
    let mut r = conn.execute("SELECT add_offset(5)")?;
    let row = r.next().expect("one row")?;
    show("add_offset(5)", row.get_idx(0));
    assert_eq!(row.get_idx(0), Some(&DuckValue::Int(15)));

    // The same state is shared across every row of a scan.
    let result = conn.execute("SELECT add_offset(x) AS r FROM (VALUES (1), (2), (3)) t(x) ORDER BY x")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("r")).collect();
    show("add_offset over 1,2,3", &got);
    assert_eq!(got, vec![&DuckValue::Int(11), &DuckValue::Int(12), &DuckValue::Int(13)]);

    Ok(())
}
