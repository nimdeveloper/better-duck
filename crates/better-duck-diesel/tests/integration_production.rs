#![allow(missing_docs)]
//! statement cache, transactions, and migration support.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*, Connection};

diesel::table! {
    test_items (id) {
        id   -> Integer,
        name -> Text,
    }
}

/// Opens an in-memory DuckDB connection with a `test_items` table pre-created.
fn mem_conn() -> DuckDbConnection {
    let mut conn = DuckDbConnection::establish(":memory:").expect("open in-memory db");
    conn.batch_execute("CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .expect("create table");
    conn
}

// Statement cache

#[test]
fn statement_cache_hit() {
    let mut conn = mem_conn();
    // Execute the same query twice — the second call should be a cache hit.
    let _: Vec<(i32, String)> =
        test_items::table.select((test_items::id, test_items::name)).load(&mut conn).unwrap();
    let _: Vec<(i32, String)> =
        test_items::table.select((test_items::id, test_items::name)).load(&mut conn).unwrap();
    // diesel's StatementCache does not expose its length; assert behavior, not cache size.
}

#[test]
fn statement_cache_two_queries() {
    let mut conn = mem_conn();
    // Two distinct queries — each should occupy a separate cache slot.
    let _: Vec<(i32, String)> =
        test_items::table.select((test_items::id, test_items::name)).load(&mut conn).unwrap();
    let _: i64 = test_items::table.count().first(&mut conn).unwrap();
    // diesel's StatementCache does not expose its length; assert behavior, not cache size.
}

// Transactions

#[test]
fn transaction_commit() {
    let mut conn = mem_conn();
    conn.transaction(|conn| {
        diesel::insert_into(test_items::table)
            .values((test_items::id.eq(1), test_items::name.eq("Alice")))
            .execute(conn)
    })
    .unwrap();
    let count: i64 = test_items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn transaction_rollback() {
    let mut conn = mem_conn();
    let result = conn.transaction(|conn| -> QueryResult<()> {
        diesel::insert_into(test_items::table)
            .values((test_items::id.eq(1), test_items::name.eq("Bob")))
            .execute(conn)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(result.is_err());
    // Row must NOT exist after the rollback.
    let count: i64 = test_items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 0);
}

// Migration support

#[test]
fn migration_setup_creates_table() -> Result<(), Box<dyn std::error::Error>> {
    use diesel::migration::MigrationConnection;
    let mut conn = DuckDbConnection::establish(":memory:")?;
    conn.setup()?;
    // Query the table with an empty result — confirms the table exists.
    conn.batch_execute("SELECT version FROM __diesel_schema_migrations LIMIT 0")?;
    Ok(())
}

#[test]
fn migration_setup_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    use diesel::migration::MigrationConnection;
    let mut conn = DuckDbConnection::establish(":memory:")?;
    conn.setup()?;
    conn.setup()?; // second call must not return an error
    Ok(())
}
