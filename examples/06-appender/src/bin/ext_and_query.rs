//! Catalog-aware and query appenders: `appender_ext` and `appender_query`.
//!
//! `appender_ext(catalog, schema, table)` targets a table in a specific attached catalog
//! (`None` uses the default). `appender_query(query, types, table_name, column_names)`
//! feeds appended rows into an arbitrary INSERT/UPDATE/DELETE/MERGE, referring to the
//! appended data by `table_name` (default `appended_data`); the `types` give the appended
//! columns' `LogicalType`s.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::types::LogicalType;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER)")?;

    section("appender_ext: default catalog vs. an explicit catalog name");
    {
        // `None` selects DuckDB's default catalog.
        let mut app = conn.appender_ext(None, "main", "t")?;
        app.append(&mut DuckValue::Int(1))?;
        app.save()?;
    }
    {
        // The in-memory database's catalog is named `memory`.
        let mut app = conn.appender_ext(Some("memory"), "main", "t")?;
        app.append(&mut DuckValue::Int(2))?;
        app.save()?;
    }
    let rows: Vec<_> = conn.execute("SELECT id FROM t ORDER BY id")?.collect::<Result<_>>()?;
    show("rows via appender_ext", rows.iter().map(|r| r.get("id").cloned()).collect::<Vec<_>>());
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[1].get("id"), Some(&DuckValue::Int(2)));

    section("appender_query: rows feed an INSERT referring to `appended_data`");
    conn.execute_batch("CREATE TABLE dest (v INTEGER)")?;
    // Declare the appended column types explicitly.
    let types = [LogicalType::of::<i32>()?];
    {
        let mut app = conn.appender_query(
            "INSERT INTO dest SELECT * FROM appended_data",
            &types,
            None,
            None,
        )?;
        app.append(&mut DuckValue::Int(42))?;
        app.append(&mut DuckValue::Int(43))?;
        app.save()?;
    }
    let dest: Vec<_> = conn.execute("SELECT v FROM dest ORDER BY v")?.collect::<Result<_>>()?;
    show("rows via appender_query", dest.iter().map(|r| r.get("v").cloned()).collect::<Vec<_>>());
    assert_eq!(dest.len(), 2);
    assert_eq!(dest[0].get("v"), Some(&DuckValue::Int(42)));
    assert_eq!(dest[1].get("v"), Some(&DuckValue::Int(43)));

    section("appender_query with a named source table and column names");
    conn.execute_batch("CREATE TABLE dest2 (v INTEGER)")?;
    let types = [LogicalType::of::<i32>()?];
    {
        // Refer to the appended data as `incoming(n)` instead of the defaults.
        let mut app = conn.appender_query(
            "INSERT INTO dest2 SELECT n FROM incoming",
            &types,
            Some("incoming"),
            Some(&["n"]),
        )?;
        app.append(&mut DuckValue::Int(7))?;
        app.save()?;
    }
    let n = conn.execute("SELECT v FROM dest2")?.next().expect("row")?;
    show("named-source row", n.get("v"));
    assert_eq!(n.get("v"), Some(&DuckValue::Int(7)));

    Ok(())
}
