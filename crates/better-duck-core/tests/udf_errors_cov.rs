#![allow(missing_docs)]
#![cfg(feature = "udf")]

//! Coverage: scalar UDF error scenarios that exercise the FFI-callback error
//! propagation and error-chain formatting paths (which the happy-path UDF tests
//! don't reach).

use better_duck_core::connection::Connection;
use better_duck_core::duckdb_scalar;

// A scalar function that always returns an error (single-level, no source).
#[duckdb_scalar(name = "always_err")]
fn always_err(_x: i32) -> Result<i32, String> {
    Err("scalar boom".to_owned())
}

// A source-chained error, to exercise the error-chain walk in `describe_error`.
#[derive(Debug)]
struct Inner;
impl std::fmt::Display for Inner {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "inner cause")
    }
}
impl std::error::Error for Inner {}

#[derive(Debug)]
struct Outer(Inner);
impl std::fmt::Display for Outer {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "outer failure")
    }
}
impl std::error::Error for Outer {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

#[duckdb_scalar(name = "chained_err")]
fn chained_err(_x: i32) -> Result<i32, Outer> {
    Err(Outer(Inner))
}

/// Returns whether evaluating `sql` surfaces an error (at execute or on the first row).
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
fn scalar_error_and_chained_error_are_reported() {
    let mut conn = Connection::open_in_memory().unwrap();
    always_err::register(&mut conn).unwrap();
    chained_err::register(&mut conn).unwrap();

    assert!(query_errors(&mut conn, "SELECT always_err(1) AS v"));
    assert!(query_errors(&mut conn, "SELECT chained_err(1) AS v"));

    // The connection is still usable after each failure.
    assert!(!query_errors(&mut conn, "SELECT 1 AS v"));
}
