#![allow(missing_docs)]
#![cfg(feature = "udf")]

//! End-to-end tests for the `#[duckdb_cast]` and `#[duckdb_aggregate]` macros:
//! they exercise the *generated* code (registration + query execution), which the
//! macro crate's own unit tests (token output only) cannot.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duckdb_aggregate, duckdb_cast};

/// VARCHAR → INTEGER by parsing; a bad value is a per-row error (NULL under
/// `TRY_CAST`, a query failure under a normal `CAST`).
#[duckdb_cast]
fn parse_to_int(s: &str) -> std::result::Result<i32, std::num::ParseIntError> {
    s.trim().parse()
}

/// `int_sum(x)`: fold INTEGER inputs into a BIGINT running sum (infallible update).
#[duckdb_aggregate(name = "int_sum")]
mod int_sum {
    fn init() -> i64 {
        0
    }
    fn update(
        state: &mut i64,
        value: i32,
    ) {
        *state += i64::from(value);
    }
    fn combine(
        acc: &mut i64,
        other: &i64,
    ) {
        *acc += *other;
    }
    fn finalize(state: &i64) -> i64 {
        *state
    }
}

/// `checked_product(x)`: BIGINT product with a fallible update (exercises the
/// `Result`-returning `update` codegen).
#[duckdb_aggregate(name = "checked_product")]
mod checked_product {
    fn init() -> i64 {
        1
    }
    fn update(
        state: &mut i64,
        value: i32,
    ) -> std::result::Result<(), String> {
        *state = state.checked_mul(i64::from(value)).ok_or_else(|| "overflow".to_owned())?;
        Ok(())
    }
    fn combine(
        acc: &mut i64,
        other: &i64,
    ) {
        *acc *= *other;
    }
    fn finalize(state: &i64) -> i64 {
        *state
    }
}

#[test]
fn custom_cast_converts_and_try_cast_nulls_bad_rows() {
    let mut conn = Connection::open_in_memory().unwrap();
    parse_to_int::register(&mut conn).unwrap();

    let mut ok = conn.execute("SELECT CAST('42' AS INTEGER) AS n").unwrap();
    assert_eq!(ok.next().unwrap().unwrap().get("n"), Some(&DuckValue::Int(42)));

    let mut bad = conn.execute("SELECT TRY_CAST('oops' AS INTEGER) AS n").unwrap();
    assert_eq!(bad.next().unwrap().unwrap().get("n"), Some(&DuckValue::Null));

    assert!(conn.execute("SELECT CAST('oops' AS INTEGER) AS n").is_err());
    let _ = conn.execute("SELECT 1").unwrap();
}

#[test]
fn aggregate_sums_and_groups() {
    let mut conn = Connection::open_in_memory().unwrap();
    int_sum::register(&mut conn).unwrap();
    conn.execute_batch("CREATE TABLE t (k INTEGER, v INTEGER)").unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,7),(2,100)").unwrap();

    let total = conn.execute("SELECT int_sum(v) AS s FROM t").unwrap();
    let rows: Vec<_> = total.collect::<Result<_>>().unwrap();
    assert_eq!(rows[0].get("s"), Some(&DuckValue::BigInt(142)));

    let mut grouped =
        conn.execute("SELECT k, int_sum(v) AS s FROM t GROUP BY k ORDER BY k").unwrap();
    assert_eq!(grouped.next().unwrap().unwrap().get("s"), Some(&DuckValue::BigInt(30)));
    assert_eq!(grouped.next().unwrap().unwrap().get("s"), Some(&DuckValue::BigInt(112)));
}

#[test]
fn fallible_aggregate_computes_product() {
    let mut conn = Connection::open_in_memory().unwrap();
    checked_product::register(&mut conn).unwrap();
    let mut r =
        conn.execute("SELECT checked_product(v) AS p FROM (VALUES (2),(3),(4)) t(v)").unwrap();
    assert_eq!(r.next().unwrap().unwrap().get("p"), Some(&DuckValue::BigInt(24)));
}
