//! A table UDF with a required SQL named (keyword) parameter and
//! projection-pushdown wiring.
//!
//! `named_params("step")` makes the second positional Rust argument bindable as
//! `step := ...` in SQL. `projection_pushdown` lets `duck_projection!()` report
//! which columns the query actually needs.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_projection, duckdb_table_function};
use common::{section, show, Result};

/// `stepped(start, step := s)`: five values `start, start+s, …` (column `n`).
#[duckdb_table_function(columns("n"), named_params("step"), projection_pushdown)]
fn stepped(
    start: i64,
    step: i64,
) -> impl Iterator<Item = i64> + Send {
    // `duck_projection!()` reports the columns the current query needs; wiring it
    // here proves the generated pushdown guard compiles and runs.
    let _wanted = duck_projection!();
    (0..5).map(move |i| start + i * step)
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    stepped::register(&mut conn)?;

    section("SELECT n FROM stepped(10, step := 2)");
    let result = conn.execute("SELECT n FROM stepped(10, step := 2) ORDER BY n")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("n")).collect();
    show("stepped(10, step := 2)", &got);
    assert_eq!(
        got,
        vec![
            &DuckValue::BigInt(10),
            &DuckValue::BigInt(12),
            &DuckValue::BigInt(14),
            &DuckValue::BigInt(16),
            &DuckValue::BigInt(18),
        ]
    );

    section("the named parameter is required: omitting it is a query error");
    let err = conn.execute("SELECT n FROM stepped(10)").err().expect("an error");
    show("error", err.to_string());

    Ok(())
}
