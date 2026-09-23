//! r2d2 pooling over a shared DuckDB database.
//!
//! `SharedDuckDbConnectionManager` hands every pooled connection a handle onto ONE shared
//! `better_duck_core::database::Database`, so an in-memory pool observes a single, consistent
//! database: a table created through one checked-out connection is visible through the next.
//!
//! Contrast this with Diesel's generic `diesel::r2d2::ConnectionManager<DuckDbConnection>`,
//! which calls `establish` per connection — for a `:memory:` URL that gives each pooled
//! connection its OWN independent, empty database, so writes made through one would be
//! invisible to the others.

use better_duck_diesel::pool::SharedDuckDbConnectionManager;
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;
use diesel::r2d2::Pool;

#[derive(diesel::QueryableByName, Debug)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

fn count_rows(conn: &mut DuckDbConnection) -> QueryResult<i64> {
    Ok(diesel::sql_query("SELECT count(*) AS count FROM pool_items")
        .get_result::<CountRow>(conn)?
        .count)
}

fn main() -> QueryResult<()> {
    println!("=== shared in-memory pool ===");
    let manager = SharedDuckDbConnectionManager::memory().expect("open shared in-memory database");
    let pool = Pool::builder().max_size(4).build(manager).expect("build pool");

    // Create + seed via one checked-out connection.
    {
        let mut a = pool.get().expect("check out connection a");
        diesel::sql_query("CREATE TABLE pool_items (id INTEGER)").execute(&mut a)?;
        diesel::sql_query("INSERT INTO pool_items VALUES (1), (2), (3)").execute(&mut a)?;
    } // returned to the pool here

    // A different checked-out connection sees the same shared database.
    let mut b = pool.get().expect("check out connection b");
    let seen = count_rows(&mut b)?;
    println!("  rows visible through a second pooled connection = {seen}");
    assert_eq!(seen, 3);

    diesel::sql_query("INSERT INTO pool_items VALUES (4)").execute(&mut b)?;
    let mut c = pool.get().expect("check out connection c");
    let after = count_rows(&mut c)?;
    println!("  rows after an insert through yet another connection = {after}");
    assert_eq!(after, 4);

    println!("=== shared file-backed pool ===");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pool.duckdb");
    let file_manager =
        SharedDuckDbConnectionManager::file(&path).expect("open shared file database");
    let file_pool = Pool::builder().max_size(2).build(file_manager).expect("build file pool");
    {
        let mut fa = file_pool.get().expect("check out file connection");
        diesel::sql_query("CREATE TABLE pool_items (id INTEGER)").execute(&mut fa)?;
        diesel::sql_query("INSERT INTO pool_items VALUES (10), (20)").execute(&mut fa)?;
    }
    let mut fb = file_pool.get().expect("check out second file connection");
    let file_seen = count_rows(&mut fb)?;
    println!("  rows visible across the file-backed pool = {file_seen}");
    assert_eq!(file_seen, 2);
    assert!(path.exists(), "backing database file should exist");

    println!("\nShared pooling verified.");
    Ok(())
}
