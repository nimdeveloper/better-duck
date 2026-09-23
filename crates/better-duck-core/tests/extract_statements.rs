//! Public-API coverage for multi-statement extraction
//! (`Connection::extract_statements`).
//!
//! DuckDB parses the batch; there is no Rust-side statement splitting. These tests
//! drive the feature exactly as an external user would — through the high-level
//! `Connection`, not the internal `raw` layer.
#![allow(missing_docs)]

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::value::DuckValue;

#[test]
fn extracts_and_runs_each_statement_of_a_batch() {
    let conn = Connection::open_in_memory().unwrap();
    let batch = conn.extract_statements("SELECT 1 AS a; SELECT 'two' AS b; SELECT 3 AS c").unwrap();
    assert_eq!(batch.len(), 3);
    assert!(!batch.is_empty());

    let mut first = batch.prepare(0).unwrap();
    let row = first.execute().unwrap().next().unwrap().unwrap();
    assert_eq!(row.get("a"), Some(&DuckValue::Int(1)));

    let mut third = batch.prepare(2).unwrap();
    let row = third.execute().unwrap().next().unwrap().unwrap();
    assert_eq!(row.get("c"), Some(&DuckValue::Int(3)));
}

#[test]
fn semicolon_inside_a_string_literal_is_not_a_split() {
    let conn = Connection::open_in_memory().unwrap();
    let batch = conn.extract_statements("SELECT 'a;b' AS s").unwrap();
    assert_eq!(batch.len(), 1);

    let mut stmt = batch.prepare(0).unwrap();
    let row = stmt.execute().unwrap().next().unwrap().unwrap();
    assert_eq!(row.get("s"), Some(&DuckValue::text("a;b")));
}

#[test]
fn out_of_range_index_is_rejected() {
    let conn = Connection::open_in_memory().unwrap();
    let batch = conn.extract_statements("SELECT 1").unwrap();
    assert!(batch.prepare(5).is_err());
}

#[test]
fn interior_nul_query_is_rejected() {
    let conn = Connection::open_in_memory().unwrap();
    assert!(matches!(conn.extract_statements("SELECT 1\0; SELECT 2"), Err(Error::NulError(_))));
}
