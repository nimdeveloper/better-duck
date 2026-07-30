#![allow(missing_docs)]
#![cfg(feature = "udf")]

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{duck_extra_info, duck_projection, duckdb_table_function};

/// The integers in `[start, stop)`.
#[duckdb_table_function(name = "series", columns("n"))]
fn series(
    start: i64,
    stop: i64,
) -> impl Iterator<Item = i64> + Send {
    start..stop
}

/// Splits `text` on whitespace, one word per row: `(index, word)`.
#[duckdb_table_function(columns("idx", "word"))]
fn words(text: String) -> impl Iterator<Item = (i64, String)> + Send {
    text.split_whitespace()
        .map(String::from)
        .enumerate()
        .map(|(i, w)| (i as i64, w))
        .collect::<Vec<_>>()
        .into_iter()
}

/// A single-column function with no `columns(...)` attribute: the column
/// defaults to the SQL function name.
#[duckdb_table_function]
fn evens(limit: i64) -> impl Iterator<Item = i64> + Send {
    (0..limit).filter(|n| n % 2 == 0)
}

/// A fallible bind: validates its arguments before producing the iterator.
#[duckdb_table_function(name = "checked_series", columns("n"))]
fn checked_series(
    start: i64,
    stop: i64,
) -> Result<impl Iterator<Item = i64> + Send, String> {
    if stop < start {
        return Err(format!("stop ({stop}) must be >= start ({start})"));
    }
    Ok(start..stop)
}

/// `start` is positional, `step` is a required SQL named (keyword) parameter.
#[duckdb_table_function(columns("n"), named_params("step"))]
fn stepped(
    start: i64,
    step: i64,
) -> impl Iterator<Item = i64> + Send {
    (0..5).map(move |i| start + i * step)
}

/// Two columns; `duck_projection!()` reports which the query actually needs
/// (whether DuckDB's optimizer triggers pushdown for a given query shape
/// isn't something a unit test should pin down — this just proves the
/// macro-generated guard wiring compiles and doesn't panic).
#[duckdb_table_function(columns("a", "b"), projection_pushdown)]
fn two_cols() -> impl Iterator<Item = (i32, i32)> + Send {
    let _wanted = duck_projection!();
    std::iter::once((1, 2))
}

/// Reads its shared extra info (set once at registration) via `duck_extra_info!`.
#[duckdb_table_function(columns("n"), extra_info(i64, 100))]
fn with_extra_info() -> impl Iterator<Item = i64> + Send {
    std::iter::once(duck_extra_info!(i64))
}

#[test]
fn series_produces_expected_rows() {
    let mut conn = Connection::open_in_memory().unwrap();
    series::register(&mut conn).unwrap();
    let result = conn.execute("SELECT n FROM series(1, 6) ORDER BY n").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("n").unwrap() {
            DuckValue::BigInt(n) => *n,
            other => panic!("expected BigInt, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);
}

#[test]
fn series_spanning_multiple_chunks_has_correct_sum() {
    let mut conn = Connection::open_in_memory().unwrap();
    series::register(&mut conn).unwrap();
    let mut result = conn.execute("SELECT sum(n) AS total FROM series(0, 10000)").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("total"), Some(&DuckValue::HugeInt(49_995_000)));
}

#[test]
fn multi_column_table_function_produces_expected_rows() {
    let mut conn = Connection::open_in_memory().unwrap();
    words::register(&mut conn).unwrap();
    let result =
        conn.execute("SELECT idx, word FROM words('hello there world') ORDER BY idx").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("idx"), Some(&DuckValue::BigInt(0)));
    assert_eq!(rows[0].get("word"), Some(&DuckValue::text("hello")));
    assert_eq!(rows[2].get("word"), Some(&DuckValue::text("world")));
}

#[test]
fn single_column_without_columns_attr_defaults_to_the_function_name() {
    let mut conn = Connection::open_in_memory().unwrap();
    evens::register(&mut conn).unwrap();
    let result = conn.execute("SELECT evens FROM evens(6) ORDER BY evens").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("evens").unwrap() {
            DuckValue::BigInt(n) => *n,
            other => panic!("expected BigInt, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![0, 2, 4]);
}

#[test]
fn fallible_bind_error_surfaces_as_query_error_and_connection_stays_usable() {
    let mut conn = Connection::open_in_memory().unwrap();
    checked_series::register(&mut conn).unwrap();
    let err = match conn.execute("SELECT n FROM checked_series(10, 1)") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("must be >= start"), "{err}");
    let _ = conn.execute("SELECT 1").unwrap();
}

#[test]
fn fallible_bind_succeeds_for_valid_arguments() {
    let mut conn = Connection::open_in_memory().unwrap();
    checked_series::register(&mut conn).unwrap();
    let mut result = conn.execute("SELECT sum(n) AS total FROM checked_series(1, 4)").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("total"), Some(&DuckValue::HugeInt(6)));
}

/// The original function must still be an ordinary, callable Rust function.
#[test]
fn original_function_is_still_directly_callable() {
    assert_eq!(series(1, 4).collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[test]
fn named_parameter_is_bound_by_keyword_not_position() {
    let mut conn = Connection::open_in_memory().unwrap();
    stepped::register(&mut conn).unwrap();
    let result = conn.execute("SELECT n FROM stepped(10, step := 2) ORDER BY n").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    let got: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("n").unwrap() {
            DuckValue::BigInt(n) => *n,
            other => panic!("expected BigInt, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![10, 12, 14, 16, 18]);
}

#[test]
fn missing_required_named_parameter_surfaces_as_query_error() {
    let mut conn = Connection::open_in_memory().unwrap();
    stepped::register(&mut conn).unwrap();
    let err = match conn.execute("SELECT n FROM stepped(10)") {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("step"), "{err}");
}

#[test]
fn projection_pushdown_flag_does_not_break_normal_execution() {
    let mut conn = Connection::open_in_memory().unwrap();
    two_cols::register(&mut conn).unwrap();

    let result = conn.execute("SELECT a FROM two_cols()").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(1)));

    let result2 = conn.execute("SELECT a, b FROM two_cols()").unwrap();
    let rows2: Vec<_> = result2.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get("b"), Some(&DuckValue::Int(2)));
}

#[test]
fn extra_info_option_shares_registration_time_value() {
    let mut conn = Connection::open_in_memory().unwrap();
    with_extra_info::register(&mut conn).unwrap();
    let mut result = conn.execute("SELECT n FROM with_extra_info()").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("n"), Some(&DuckValue::BigInt(100)));
}
