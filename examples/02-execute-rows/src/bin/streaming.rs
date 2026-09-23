//! Executing SQL and streaming rows out of a `DuckResult` iterator.
//!
//! `execute` returns a `DuckResult` that is BOTH a lazy row iterator and a carrier of
//! result metadata (column names, `changes()`, statement/result type). `execute_batch`
//! runs one or more statements and discards the result — use it for DDL / fire-and-forget
//! DML.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::DuckRow;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("execute_batch: DDL + seed data (result discarded)");
    conn.execute_batch(
        "CREATE TABLE events (id INTEGER, label TEXT, score DOUBLE);
         INSERT INTO events VALUES (1, 'alpha', 9.5), (2, 'beta', 7.2);",
    )?;

    section("execute + stream rows");
    let result = conn.execute("SELECT id, label, score FROM events ORDER BY id")?;
    // Column metadata is available before consuming any rows.
    let columns: Vec<Box<str>> = result.column_names().to_vec();
    show("columns", &columns);

    // The iterator yields `Result<DuckRow>`; collect into a Vec to inspect.
    let rows: Vec<DuckRow> = result.collect::<Result<_>>()?;
    assert_eq!(rows.len(), 2);
    for row in &rows {
        println!(
            "  id={:?} label={:?} score={:?}",
            row.get("id"),
            row.get("label"),
            row.get("score"),
        );
    }

    // Access columns by name or by zero-based index.
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[0].get_idx(1), Some(&DuckValue::Text("alpha".to_string())));
    assert_eq!(rows[1].get("label"), Some(&DuckValue::Text("beta".to_string())));

    Ok(())
}
