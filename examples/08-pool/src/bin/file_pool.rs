//! A file-backed r2d2 connection pool.
//!
//! `DuckDbConnectionManager::file(path)` backs the pool with an on-disk database. Every
//! pooled connection opens onto the same file, so a row written through one checked-out
//! connection is visible through another.

use better_duck_core::error::Error;
use better_duck_core::pool::{DuckDbConnectionManager, Pool};
use common::{section, show, Result};

fn main() -> Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pool.duckdb");

    section("build a file-backed pool");
    let mgr = DuckDbConnectionManager::file(&path)?;
    let pool = Pool::builder().max_size(4).build(mgr).map_err(|e| Error::Pool(e.to_string()))?;
    assert!(path.exists(), "the database file should exist once the pool is built");

    section("write through one checked-out connection");
    {
        let mut writer = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
        writer.execute_batch(
            "CREATE TABLE events (id INTEGER, kind TEXT);
             INSERT INTO events VALUES (1, 'open'), (2, 'click'), (3, 'close');",
        )?;
    } // writer returns to the pool here

    section("read through a different checked-out connection");
    let mut reader = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
    let seen = reader.execute("SELECT id FROM events ORDER BY id")?.count();
    show("rows visible to a second pooled connection", seen);
    assert_eq!(seen, 3, "both connections share the same on-disk database");

    Ok(())
}
