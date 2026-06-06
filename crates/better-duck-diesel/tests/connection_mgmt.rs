#![allow(missing_docs)]
//! Connection management tests: establishment, URL schemes, statement cache, migrations, r2d2.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*, Connection};

diesel::table! {
    cm_items (id) {
        id  -> Integer,
        val -> Text,
    }
}

// Establishment

#[test]
fn establish_in_memory() {
    let conn = DuckDbConnection::establish(":memory:");
    assert!(conn.is_ok(), "in-memory connection failed: {:?}", conn.err());
}

#[test]
fn establish_file_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let url = path.to_str().unwrap();
    let conn = DuckDbConnection::establish(url);
    assert!(conn.is_ok(), "file-path connection failed: {:?}", conn.err());
}

#[test]
fn establish_duckdb_url_prefix() {
    // The `duckdb://` scheme prefix must be stripped automatically.
    let conn = DuckDbConnection::establish("duckdb://:memory:");
    assert!(conn.is_ok(), "duckdb:// prefix failed: {:?}", conn.err());
}

// batch_execute (multi-statement)

#[test]
fn batch_execute_multi_statement() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute(
        "CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL);
         INSERT INTO cm_items VALUES (1, 'first');
         INSERT INTO cm_items VALUES (2, 'second');",
    )
    .unwrap();
    let count: i64 = cm_items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 2);
}

// Statement cache

#[test]
fn statement_cache_hit() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute("CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)")
        .unwrap();
    // Execute the same query twice — second call is a cache hit.
    let _: Vec<(i32, String)> =
        cm_items::table.select((cm_items::id, cm_items::val)).load(&mut conn).unwrap();
    let _: Vec<(i32, String)> =
        cm_items::table.select((cm_items::id, cm_items::val)).load(&mut conn).unwrap();
    // No assertion needed beyond "doesn't crash".
}

#[test]
fn statement_cache_two_distinct_queries() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute("CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)")
        .unwrap();
    let _: Vec<(i32, String)> =
        cm_items::table.select((cm_items::id, cm_items::val)).load(&mut conn).unwrap();
    // A structurally different query gets its own cache slot.
    let _: i64 = cm_items::table.count().first(&mut conn).unwrap();
}

// Migration support

#[test]
fn migration_setup_creates_table() {
    use diesel::migration::MigrationConnection;
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.setup().unwrap();
    conn.batch_execute("SELECT version FROM __diesel_schema_migrations LIMIT 0").unwrap();
}

#[test]
fn migration_setup_idempotent() {
    use diesel::migration::MigrationConnection;
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.setup().unwrap();
    conn.setup().unwrap(); // must not error on second call
}

// r2d2 connection pool

#[cfg(feature = "r2d2")]
#[test]
fn r2d2_pool_acquire_and_ping() {
    use diesel::r2d2::{ConnectionManager, Pool, R2D2Connection};
    let manager = ConnectionManager::<DuckDbConnection>::new(":memory:");
    let pool = Pool::builder().max_size(2).build(manager).expect("build pool");
    let mut conn = pool.get().expect("acquire connection");
    conn.ping().expect("ping");
}
