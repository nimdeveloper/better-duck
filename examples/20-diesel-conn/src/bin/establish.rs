//! Establishing a `DuckDbConnection` — the three URL forms Diesel accepts.
//!
//! `DuckDbConnection::establish` is Diesel's standard entry point. DuckDB accepts a bare
//! `:memory:`, an ordinary filesystem path, or a `duckdb://` URL (the scheme is stripped
//! automatically). `SimpleConnection::batch_execute` runs one or more `;`-separated DDL/DML
//! statements in a single call.

use diesel::connection::SimpleConnection;
use diesel::prelude::*;

diesel::table! {
    widgets (id) {
        id   -> Integer,
        name -> Text,
    }
}

fn seed_and_count(conn: &mut DuckDbConnection) -> QueryResult<i64> {
    // batch_execute takes several statements at once — ideal for schema setup.
    conn.batch_execute(
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL);
         INSERT INTO widgets VALUES (1, 'alpha'), (2, 'beta');",
    )?;
    widgets::table.count().first(conn)
}

use better_duck_diesel::DuckDbConnection;

fn main() -> QueryResult<()> {
    println!("=== in-memory (\":memory:\") ===");
    let mut mem = DuckDbConnection::establish(":memory:").expect("open :memory:");
    let n = seed_and_count(&mut mem)?;
    println!("  rows in in-memory database = {n}");
    assert_eq!(n, 2);

    println!("=== on-disk (filesystem path) ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("widgets.duckdb");
    let path_str = path.to_str().expect("utf-8 path");
    {
        let mut disk = DuckDbConnection::establish(path_str).expect("open file path");
        let n = seed_and_count(&mut disk)?;
        println!("  rows written to {path_str} = {n}");
        assert_eq!(n, 2);
    }
    // Re-open the same file: DDL/data persisted across connections.
    let mut reopened = DuckDbConnection::establish(path_str).expect("reopen file path");
    let persisted: i64 = widgets::table.count().first(&mut reopened)?;
    println!("  rows read back from file = {persisted}");
    assert_eq!(persisted, 2);
    assert!(path.exists(), "database file should exist on disk");

    println!("=== duckdb:// URL form ===");
    // The `duckdb://` scheme prefix is recognised and stripped, leaving `:memory:`.
    let mut url = DuckDbConnection::establish("duckdb://:memory:").expect("open duckdb:// URL");
    let n = seed_and_count(&mut url)?;
    println!("  rows in duckdb:// database = {n}");
    assert_eq!(n, 2);

    println!("\nAll establish forms verified.");
    Ok(())
}
