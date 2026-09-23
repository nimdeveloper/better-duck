//! Small introspection helpers: table descriptions, client context, table names.
//!
//! `table_description` / `table_description_ext` give indexed column names and DEFAULT
//! flags for a catalog table. `client_context()` exposes the connection's stable id.
//! `table_names(query, qualified)` reports which tables a query reads, via DuckDB's own
//! parser. NOTE: `table_names` aborts the process on syntactically invalid SQL — only feed
//! it valid SQL.

use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR DEFAULT 'x')")?;

    section("table_description: indexed column names and DEFAULT flags");
    let desc = conn.table_description("main", "t")?;
    // A successful description carries no error.
    show("error", desc.error());
    assert!(desc.error().is_none());
    // Column bounds come from a trusted source (the DDL declares 2 columns).
    show("column_name(0)", desc.column_name(0));
    show("column_name(1)", desc.column_name(1));
    assert_eq!(desc.column_name(0).as_deref(), Some("id"));
    assert_eq!(desc.column_name(1).as_deref(), Some("name"));
    // `id` has no DEFAULT; `name` does.
    show("column_has_default(0)", desc.column_has_default(0)?);
    show("column_has_default(1)", desc.column_has_default(1)?);
    assert!(!desc.column_has_default(0)?);
    assert!(desc.column_has_default(1)?);

    section("table_description_ext: explicit catalog (None = default)");
    let desc = conn.table_description_ext(None, "main", "t")?;
    assert_eq!(desc.column_name(0).as_deref(), Some("id"));

    section("client_context: the connection's stable id");
    let id = conn.client_context().expect("client context").connection_id();
    show("connection_id", id);
    // The same connection reports the same id on a second fetch.
    assert_eq!(conn.client_context().expect("client context").connection_id(), id);
    // A separate connection to the same database gets its own id.
    let other = conn.try_clone()?;
    let other_id = other.client_context().expect("client context").connection_id();
    show("second connection_id", other_id);
    assert_ne!(id, other_id, "each connection has a distinct id");

    section("table_names: which tables a query reads (valid SQL only)");
    conn.execute_batch("CREATE TABLE orders (id INTEGER)")?;
    let names = conn.table_names("SELECT * FROM t JOIN orders ON t.id = orders.id", false)?;
    show("tables (unqualified)", &names);
    assert!(names.iter().any(|n| n == "t"));
    assert!(names.iter().any(|n| n == "orders"));
    // A query that reads no tables yields an empty list.
    let none = conn.table_names("SELECT 1", false)?;
    show("tables in `SELECT 1`", &none);
    assert!(none.is_empty());

    Ok(())
}
