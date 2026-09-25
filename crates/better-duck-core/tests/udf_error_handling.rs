#![allow(missing_docs)]
#![cfg(feature = "udf")]

//! Regression tests: user-defined functions that fail — a scalar returning `Err`,
//! a scalar that panics, and a fallible aggregate whose `update`/`finalize`
//! returns `Err` — must surface a **clean query error** and leave the connection
//! usable, never unwind across the FFI boundary or abort the process.

use better_duck_core::connection::Connection;
use better_duck_core::{duckdb_aggregate, duckdb_scalar};

#[duckdb_scalar(name = "err_scalar")]
fn err_scalar(_x: i32) -> Result<i32, String> {
    Err("scalar failed".to_owned())
}

#[duckdb_scalar(name = "panic_scalar")]
fn panic_scalar(_x: i32) -> i32 {
    panic!("scalar panicked");
}

/// Aggregate whose `update` always fails.
#[duckdb_aggregate(name = "update_err_agg")]
mod update_err_agg {
    fn init() -> i64 {
        0
    }
    fn update(
        _s: &mut i64,
        _v: i64,
    ) -> std::result::Result<(), String> {
        Err("update failed".to_owned())
    }
    fn combine(
        _t: &mut i64,
        _s: &i64,
    ) {
    }
    fn finalize(s: &i64) -> i64 {
        *s
    }
}

/// Aggregate whose `finalize` always fails.
#[duckdb_aggregate(name = "finalize_err_agg")]
mod finalize_err_agg {
    fn init() -> i64 {
        0
    }
    fn update(
        _s: &mut i64,
        _v: i64,
    ) {
    }
    fn combine(
        _t: &mut i64,
        _s: &i64,
    ) {
    }
    fn finalize(_s: &i64) -> std::result::Result<i64, String> {
        Err("finalize failed".to_owned())
    }
}

/// Whether evaluating `sql` surfaces an error (at execute or on the first row).
fn query_errors(
    conn: &mut Connection,
    sql: &str,
) -> bool {
    match conn.execute(sql) {
        Err(_) => true,
        Ok(mut rows) => rows.next().is_none_or(|r| r.is_err()),
    }
}

#[test]
fn scalar_error_and_panic_are_contained() {
    let mut conn = Connection::open_in_memory().unwrap();
    err_scalar::register(&mut conn).unwrap();
    panic_scalar::register(&mut conn).unwrap();

    assert!(query_errors(&mut conn, "SELECT err_scalar(1) AS v"));
    assert!(query_errors(&mut conn, "SELECT panic_scalar(1) AS v"));
    // The connection recovers after each failure.
    assert!(!query_errors(&mut conn, "SELECT 1 AS v"));
}

#[test]
fn fallible_aggregate_errors_fail_the_query_cleanly() {
    let mut conn = Connection::open_in_memory().unwrap();
    update_err_agg::register(&mut conn).unwrap();
    finalize_err_agg::register(&mut conn).unwrap();

    assert!(query_errors(&mut conn, "SELECT update_err_agg(v) FROM (VALUES (1),(2)) t(v)"));
    assert!(query_errors(&mut conn, "SELECT finalize_err_agg(v) FROM (VALUES (1),(2)) t(v)"));
    assert!(!query_errors(&mut conn, "SELECT 1 AS v"));
}
