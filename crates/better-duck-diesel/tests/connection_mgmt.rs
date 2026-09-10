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

#[test]
fn cached_insert_resets_bound_values() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute("CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)")
        .unwrap();

    for (id, val) in [(1, "first"), (2, "second"), (3, "third")] {
        diesel::insert_into(cm_items::table)
            .values((cm_items::id.eq(id), cm_items::val.eq(val)))
            .execute(&mut conn)
            .unwrap();
    }

    let rows: Vec<(i32, String)> = cm_items::table
        .select((cm_items::id, cm_items::val))
        .order(cm_items::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows, [(1, "first".to_owned()), (2, "second".to_owned()), (3, "third".to_owned()),]);
}

#[test]
fn cached_select_resets_bound_values() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute(
        "CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL);
         INSERT INTO cm_items VALUES (1, 'first'), (2, 'second'), (3, 'third');",
    )
    .unwrap();

    for (id, expected) in [(3, "third"), (1, "first"), (2, "second")] {
        let actual: String = cm_items::table
            .filter(cm_items::id.eq(id))
            .select(cm_items::val)
            .first(&mut conn)
            .unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn from_core_connections_share_database() {
    let database = better_duck_core::database::Database::open_in_memory().unwrap();
    let mut first = DuckDbConnection::from_core(database.connect().unwrap());
    let mut second = DuckDbConnection::from_core(database.connect().unwrap());

    first
        .batch_execute(
            "CREATE TABLE cm_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL);
             INSERT INTO cm_items VALUES (1, 'shared');",
        )
        .unwrap();
    let value: String = cm_items::table.select(cm_items::val).first(&mut second).unwrap();
    assert_eq!(value, "shared");
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
fn shared_r2d2_manager_exposes_database_and_trait_operations() {
    use better_duck_diesel::pool::SharedDuckDbConnectionManager;
    use diesel::r2d2::ManageConnection;

    let database = better_duck_core::database::Database::open_in_memory().unwrap();
    let manager = SharedDuckDbConnectionManager::new(database);
    let mut direct = manager.database().connect().unwrap();
    direct.execute_batch("CREATE TABLE manager_items (id INTEGER)").unwrap();

    let mut conn = ManageConnection::connect(&manager).unwrap();
    conn.batch_execute("INSERT INTO manager_items VALUES (1)").unwrap();
    assert!(ManageConnection::is_valid(&manager, &mut conn).is_ok());
    assert!(!ManageConnection::has_broken(&manager, &mut conn));
}

#[cfg(feature = "r2d2")]
#[test]
fn shared_r2d2_file_manager_shares_file_backing() {
    use better_duck_diesel::pool::SharedDuckDbConnectionManager;
    use diesel::r2d2::ManageConnection;

    let dir = tempfile::tempdir().unwrap();
    let manager = SharedDuckDbConnectionManager::file(dir.path().join("shared.duckdb")).unwrap();
    let mut first = ManageConnection::connect(&manager).unwrap();
    first
        .batch_execute(
            "CREATE TABLE shared_items (id INTEGER); INSERT INTO shared_items VALUES (1)",
        )
        .unwrap();

    let mut second = ManageConnection::connect(&manager).unwrap();
    let count: i64 = diesel::sql_query("SELECT count(*) AS count FROM shared_items")
        .get_result::<CountRow>(&mut second)
        .unwrap()
        .count;
    assert_eq!(count, 1);
}

#[cfg(feature = "r2d2")]
#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[cfg(feature = "r2d2")]
#[test]
fn shared_r2d2_file_manager_rejects_nul_path() {
    use better_duck_diesel::pool::SharedDuckDbConnectionManager;

    assert!(SharedDuckDbConnectionManager::file(std::path::Path::new("invalid\0path")).is_err());
}
