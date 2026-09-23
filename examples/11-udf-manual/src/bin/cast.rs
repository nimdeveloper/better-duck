//! Custom cast functions the low-level way: implement `VCast` and register with
//! `conn.register_cast_function::<T>()`.
//!
//! A cast declares its source/target types, an implicit-cast cost, and a per-row
//! conversion. DuckDB runs it in two modes: a normal `CAST` (a conversion error
//! fails the query) and `TRY_CAST` (a conversion error yields `NULL` for that
//! row) — the same `cast_row` body serves both.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::types::LogicalType;
use better_duck_core::udf::{UdfResult, VCast, VectorMut, VectorRef};
use common::{section, show, Result};

/// Casts VARCHAR -> INTEGER by parsing. An unparseable value is a row error:
/// `NULL` under `TRY_CAST`, a query failure under a normal `CAST`.
struct StrToInt;

impl VCast for StrToInt {
    type Shared = ();

    fn source_type() -> Result<LogicalType> {
        LogicalType::of::<String>()
    }

    fn target_type() -> Result<LogicalType> {
        LogicalType::of::<i32>()
    }

    fn implicit_cast_cost() -> i64 {
        // Negative: explicit CAST / TRY_CAST only, never applied implicitly.
        -1
    }

    fn cast_row(
        _shared: &(),
        input: &VectorRef<'_>,
        output: &mut VectorMut<'_>,
        row: usize,
    ) -> UdfResult<()> {
        let s: &str = input.get(row)?;
        let v: i32 = s.trim().parse().map_err(|_| format!("not an integer: {s:?}"))?;
        output.set(row, v)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.register_cast_function::<StrToInt>()?;

    section("CAST('42' AS INTEGER): a valid value converts");
    let mut r = conn.execute("SELECT CAST('42' AS INTEGER) AS n")?;
    let row = r.next().expect("one row")?;
    show("CAST('42' AS INTEGER)", row.get("n"));
    assert_eq!(row.get("n"), Some(&DuckValue::Int(42)));

    section("TRY_CAST('oops' AS INTEGER): a bad value becomes NULL");
    let mut r = conn.execute("SELECT TRY_CAST('oops' AS INTEGER) AS n")?;
    let row = r.next().expect("one row")?;
    show("TRY_CAST('oops' AS INTEGER)", row.get("n"));
    assert_eq!(row.get("n"), Some(&DuckValue::Null));

    section("CAST('oops' AS INTEGER): a bad value fails the query");
    let err = conn.execute("SELECT CAST('oops' AS INTEGER) AS n").err().expect("an error");
    show("error", err.to_string());
    // The connection stays usable after the failed cast.
    let mut ok = conn.execute("SELECT 1 AS one")?;
    assert_eq!(ok.next().expect("row")?.get("one"), Some(&DuckValue::Int(1)));

    Ok(())
}
