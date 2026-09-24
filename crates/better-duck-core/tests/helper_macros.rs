#![allow(missing_docs)]

//! End-to-end tests for the `transaction!` and `params!` helper macros.

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{params, transaction};

fn count(conn: &mut Connection) -> i64 {
    let mut rows = conn.execute("SELECT count(*) AS n FROM t").unwrap();
    match rows.next().unwrap().unwrap().get("n") {
        Some(DuckValue::BigInt(n)) => *n,
        other => panic!("unexpected count: {other:?}"),
    }
}

#[test]
fn transaction_macro_commits_and_params_binds() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (a INTEGER, b VARCHAR)").unwrap();
    transaction!(conn, {
        conn.execute_with("INSERT INTO t VALUES ($1, $2)", &mut params![1_i32, "hi".to_owned()])?;
        Ok::<_, Error>(())
    })
    .unwrap();
    assert_eq!(count(&mut conn), 1);
}

#[test]
fn transaction_macro_rolls_back_on_err() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t (a INTEGER)").unwrap();
    let outcome = transaction!(conn, {
        conn.execute_batch("INSERT INTO t VALUES (1)")?;
        Err::<(), _>(Error::InvalidQuery)
    });
    assert!(outcome.is_err());
    assert_eq!(count(&mut conn), 0);
}
