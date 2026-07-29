#![allow(missing_docs)]
#![cfg(feature = "udf")]

use better_duck_core::connection::Connection;
use better_duck_core::duckdb_table_function;
use better_duck_core::types::value::DuckValue;

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
