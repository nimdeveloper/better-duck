#![allow(missing_docs)]
#![cfg(feature = "udf")]

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_state, duckdb_scalar};

/// Repeats `s` `n` times.
#[duckdb_scalar]
fn repeat_str(
    s: &str,
    n: i32,
) -> String {
    s.repeat(n.max(0) as usize)
}

/// Parses `s`, failing the query on bad input.
#[duckdb_scalar(name = "to_int")]
fn parse_int(s: &str) -> Result<i32, std::num::ParseIntError> {
    s.parse()
}

/// Propagates NULL through explicitly via `Option`.
#[duckdb_scalar]
fn double_or_null(x: Option<i32>) -> Option<i32> {
    x.map(|v| v * 2)
}

/// A volatile (non-constant-folded) zero-argument function.
#[duckdb_scalar(volatile)]
fn answer() -> i32 {
    42
}

/// Reads its shared `State` (set once at registration) via `duck_state!`.
#[duckdb_scalar(state(i32, 10))]
fn add_offset(x: i32) -> i32 {
    x + duck_state!(i32)
}

#[test]
fn repeat_str_computes_expected_output() {
    let mut conn = Connection::open_in_memory().unwrap();
    repeat_str::register(&mut conn).unwrap();
    conn.execute_batch("CREATE TABLE t (s VARCHAR, n INTEGER)").unwrap();
    conn.execute_batch("INSERT INTO t VALUES ('ab', 3)").unwrap();
    let mut result = conn.execute("SELECT repeat_str(s, n) AS r FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::text("ababab")));
}

#[test]
fn custom_name_is_used_as_the_sql_function_name() {
    let mut conn = Connection::open_in_memory().unwrap();
    parse_int::register(&mut conn).unwrap();
    let mut result = conn.execute("SELECT to_int('42') AS r").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::Int(42)));
}

#[test]
fn fallible_function_error_surfaces_as_query_error() {
    let mut conn = Connection::open_in_memory().unwrap();
    parse_int::register(&mut conn).unwrap();
    let err = match conn.execute("SELECT to_int('not a number')") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    // The connection must still be usable after a UDF error.
    let _ = conn.execute("SELECT 1").unwrap();
    drop(err);
}

#[test]
fn option_parameter_and_return_round_trip_null_and_value() {
    let mut conn = Connection::open_in_memory().unwrap();
    double_or_null::register(&mut conn).unwrap();
    conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute_batch("INSERT INTO t VALUES (21), (NULL)").unwrap();
    let result = conn.execute("SELECT double_or_null(x) AS r FROM t ORDER BY x").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 2);
    // NULL sorts last in DuckDB's default ORDER BY.
    assert_eq!(rows[0].get("r"), Some(&DuckValue::Int(42)));
    assert_eq!(rows[1].get("r"), Some(&DuckValue::Null));
}

#[test]
fn volatile_zero_arg_function_is_not_constant_folded_away() {
    let mut conn = Connection::open_in_memory().unwrap();
    answer::register(&mut conn).unwrap();
    let mut result = conn.execute("SELECT answer() AS r").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("r"), Some(&DuckValue::Int(42)));
}

/// The original function must still be an ordinary, callable Rust function.
#[test]
fn original_function_is_still_directly_callable() {
    assert_eq!(repeat_str("x", 3), "xxx");
    assert_eq!(parse_int("7").unwrap(), 7);
}

#[test]
fn state_option_shares_registration_time_value_across_calls() {
    let mut conn = Connection::open_in_memory().unwrap();
    add_offset::register(&mut conn).unwrap();
    conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();
    conn.execute_batch("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let result = conn.execute("SELECT add_offset(x) AS r FROM t ORDER BY x").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    let got: Vec<i32> = rows
        .iter()
        .map(|r| match r.get("r").unwrap() {
            DuckValue::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![11, 12, 13]);
}
