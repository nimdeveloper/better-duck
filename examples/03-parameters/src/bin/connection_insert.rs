//! `Connection::insert` — bind a homogeneous iterator of values and run one DML statement.
//!
//! `insert` is the ergonomic path when every parameter is the same Rust type. It binds
//! each item as consecutive positional parameters and errors if no rows change.

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER)")?;

    section("insert a homogeneous row");
    // Turbofish picks the item type; `[10, 20]` binds to `$1, $2`.
    conn.insert::<i32, _>("INSERT INTO t VALUES ($1, $2)", [10, 20])?;
    let row = conn.execute("SELECT a, b FROM t")?.next().expect("row")?;
    show("a", row.get("a"));
    show("b", row.get("b"));
    assert_eq!(row.get("a"), Some(&DuckValue::Int(10)));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(20)));

    section("insert reports an error when no rows change");
    // A statement that changes nothing (a bare SELECT) is treated as a failed insert.
    let err = conn.insert::<i32, _>("SELECT $1", std::iter::once(1)).unwrap_err();
    show("error", &err);
    assert!(matches!(
        err,
        Error::DuckDBFailure(_, Some(message)) if message == "Failed to insert values"
    ));

    Ok(())
}
