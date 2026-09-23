//! Scalar UDFs the ergonomic way: annotate a plain Rust `fn` with
//! `#[duckdb_scalar]` and call the generated `<fn>::register(&mut conn)`.
//!
//! The macro maps Rust arguments/returns to DuckDB types, wires NULL handling,
//! and routes a `Result::Err` return into a query error — no trait impls needed.

use better_duck_core::connection::Connection;
use better_duck_core::duckdb_scalar;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// Repeats `s` `n` times (`&str` in, `String` out).
#[duckdb_scalar]
fn repeat_str(
    s: &str,
    n: i32,
) -> String {
    s.repeat(n.max(0) as usize)
}

/// A custom SQL name and a fallible body: `Err` becomes a query error.
#[duckdb_scalar(name = "to_int")]
fn parse_int(s: &str) -> std::result::Result<i32, std::num::ParseIntError> {
    s.parse()
}

/// `Option` in/out: NULL flows through as `None`.
#[duckdb_scalar]
fn double_or_null(x: Option<i32>) -> Option<i32> {
    x.map(|v| v * 2)
}

/// A volatile zero-argument function (not constant-folded away).
#[duckdb_scalar(volatile)]
fn answer() -> i32 {
    42
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    repeat_str::register(&mut conn)?;
    parse_int::register(&mut conn)?;
    double_or_null::register(&mut conn)?;
    answer::register(&mut conn)?;

    section("repeat_str(&str, i32) -> String");
    let mut r = conn.execute("SELECT repeat_str('ab', 3)")?;
    let row = r.next().expect("one row")?;
    show("repeat_str('ab', 3)", row.get_idx(0));
    assert_eq!(row.get_idx(0), Some(&DuckValue::text("ababab")));

    section("to_int(&str) -> Result<i32>: Ok folds to a value");
    let mut r = conn.execute("SELECT to_int('42')")?;
    let row = r.next().expect("one row")?;
    show("to_int('42')", row.get_idx(0));
    assert_eq!(row.get_idx(0), Some(&DuckValue::Int(42)));

    section("to_int(&str): Err surfaces as a query error, connection stays usable");
    let err = conn.execute("SELECT to_int('not a number')").err().expect("an error");
    show("error", err.to_string());
    // The connection is still fully usable after a UDF error.
    let mut ok = conn.execute("SELECT 1 AS one")?;
    assert_eq!(ok.next().expect("row")?.get("one"), Some(&DuckValue::Int(1)));

    section("double_or_null(Option<i32>) -> Option<i32>: NULL propagation");
    let result = conn
        .execute("SELECT double_or_null(x) AS r FROM (VALUES (21), (NULL)) t(x) ORDER BY x")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    show("double_or_null(21)", rows[0].get("r"));
    show("double_or_null(NULL)", rows[1].get("r"));
    assert_eq!(rows[0].get("r"), Some(&DuckValue::Int(42)));
    assert_eq!(rows[1].get("r"), Some(&DuckValue::Null));

    section("answer() -> i32 (volatile)");
    let mut r = conn.execute("SELECT answer()")?;
    let row = r.next().expect("one row")?;
    show("answer()", row.get_idx(0));
    assert_eq!(row.get_idx(0), Some(&DuckValue::Int(42)));

    // The annotated functions remain ordinary, directly callable Rust fns.
    assert_eq!(repeat_str("x", 3), "xxx");
    assert_eq!(parse_int("7").unwrap(), 7);

    Ok(())
}
