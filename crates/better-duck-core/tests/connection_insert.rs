//! Public-API coverage for `Connection::insert` — bind a homogeneous iterator of
//! values as positional parameters and execute one DML statement.
#![allow(missing_docs)]

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::value::DuckValue;

#[test]
fn insert_binds_values_and_persists_the_row() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();
    conn.insert::<i32, _>("INSERT INTO t VALUES ($1, $2)", [10, 20]).unwrap();

    let row = conn.execute("SELECT a, b FROM t").unwrap().next().unwrap().unwrap();
    assert_eq!(row.get("a"), Some(&DuckValue::Int(10)));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(20)));
}

#[test]
fn insert_reports_when_no_rows_change() {
    let mut conn = Connection::open_in_memory().unwrap();
    let err = conn.insert::<i32, _>("SELECT $1", std::iter::once(1)).unwrap_err();
    assert!(matches!(
        err,
        Error::DuckDBFailure(_, Some(message)) if message == "Failed to insert values"
    ));
}
