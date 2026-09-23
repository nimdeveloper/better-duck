//! `CachedStatement` — a `'static`, reusable prepared statement (retains the connection).
//!
//! Unlike `Statement`, a `CachedStatement` holds its own handle to the connection, so it
//! does not borrow `conn` and can be reused freely. Binds are 1-based; `bind_named` targets
//! `$name` parameters; `reset_bindings` clears for the next execution.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::CachedStatement;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (v INTEGER)")?;

    section("prepare once, execute twice with different bindings");
    let mut stmt = CachedStatement::prepare(conn.db(), "INSERT INTO t VALUES ($1)")?;
    show("parameter_count", stmt.parameter_count());

    stmt.bind(1, &mut 100i32)?;
    stmt.execute()?;
    stmt.reset_bindings()?;
    stmt.bind(1, &mut 200i32)?;
    stmt.execute()?;

    let rows: Vec<_> = conn.execute("SELECT v FROM t ORDER BY v")?.collect::<Result<_>>()?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("v"), Some(&DuckValue::Int(100)));

    section("named parameters with bind_named");
    let mut named = CachedStatement::prepare(conn.db(), "SELECT $x + $y AS s")?;
    named.bind_named("x", &mut 3i32)?;
    named.bind_named("y", &mut 4i32)?;
    let row = named.execute()?.next().expect("row")?;
    show("$x + $y", row.get("s"));
    assert_eq!(row.get("s"), Some(&DuckValue::Int(7)));

    section("RETURNING clause");
    let mut ret = CachedStatement::prepare(conn.db(), "INSERT INTO t VALUES (7) RETURNING v")?;
    let row = ret.execute()?.next().expect("row")?;
    assert_eq!(row.get("v"), Some(&DuckValue::Int(7)));

    Ok(())
}
