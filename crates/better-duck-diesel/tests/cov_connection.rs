#![allow(missing_docs)]

//! Coverage: exercise `DuckDbConnection` surface methods and the `Row`/`Field`
//! name/null accessors that the plain Queryable-by-index path doesn't hit.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::{CacheSize, Connection, SimpleConnection};
use diesel::migration::MigrationConnection;
use diesel::prelude::*;
#[cfg(feature = "r2d2")]
use diesel::r2d2::R2D2Connection;
use diesel::sql_types::{Integer, Nullable, Text};

#[test]
fn connection_surface_methods() {
    let mut c = DuckDbConnection::establish("duckdb://:memory:").unwrap();
    // Shared + mutable handles to the core connection.
    let _ = c.inner();
    let _ = c.inner_mut();
    // Instrumentation + cache-size setters.
    c.set_instrumentation(diesel::connection::get_default_instrumentation());
    c.set_prepared_statement_cache_size(CacheSize::Unbounded);
    // r2d2 ping (only when the `r2d2` feature is enabled) + migration setup.
    #[cfg(feature = "r2d2")]
    R2D2Connection::ping(&mut c).unwrap();
    MigrationConnection::setup(&mut c).unwrap();
    // batch_execute happy path.
    c.batch_execute("SELECT 1").unwrap();
}

#[test]
fn begin_test_transaction_sets_flag() {
    let mut c = DuckDbConnection::establish(":memory:").unwrap();
    c.begin_test_transaction().unwrap();
    c.batch_execute("CREATE TABLE t (id INTEGER)").unwrap();
}

#[derive(QueryableByName, Debug, PartialEq)]
struct NamedRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Text>)]
    name: Option<String>,
}

#[test]
fn row_field_name_and_null_accessors() {
    let mut c = DuckDbConnection::establish(":memory:").unwrap();
    // QueryableByName looks fields up by name (Field::field_name); the NULL column
    // exercises the null path (Field::value → None / Field::is_null).
    let rows: Vec<NamedRow> =
        diesel::sql_query("SELECT 1 AS id, NULL AS name").get_results(&mut c).unwrap();
    assert_eq!(rows, vec![NamedRow { id: 1, name: None }]);

    let rows: Vec<NamedRow> =
        diesel::sql_query("SELECT 2 AS id, 'x' AS name").get_results(&mut c).unwrap();
    assert_eq!(rows, vec![NamedRow { id: 2, name: Some("x".to_owned()) }]);
}
