#![allow(missing_docs)]
//! Integration tests for the columnar bulk read/write API (`crate::chunk`,
//! `DuckResult::fetch_chunk`, `Appender::append_chunk` and friends).

use better_duck_core::{
    chunk::{DataChunkHandle, LogicalType},
    connection::Connection,
    error::Error,
    types::value::DuckValue,
};

fn conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

// 1. Chunk round-trip: build a 2-column chunk, `append_chunk`, query back,
// values match.
#[test]
fn chunk_round_trip_through_append_chunk() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (a INTEGER, b VARCHAR)").unwrap();

    let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<String>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut cols = chunk.vectors_mut().unwrap();
        cols[0].set(0, 1i32).unwrap();
        cols[1].set(0, "one").unwrap();
        cols[0].set(1, 2i32).unwrap();
        cols[1].set(1, "two").unwrap();
    }
    chunk.set_len(2).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    app.append_chunk(&mut chunk).unwrap();
    app.save().unwrap();

    let result = c.execute("SELECT a, b FROM t ORDER BY a").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[0].get("b"), Some(&DuckValue::text("one")));
    assert_eq!(rows[1].get("a"), Some(&DuckValue::Int(2)));
    assert_eq!(rows[1].get("b"), Some(&DuckValue::text("two")));
}

// 2 & 3 (`as_slice` for every fixed-width type, and the type-mismatch error
// path) are unit-tested directly in `src/chunk/vector.rs` — they need no
// database connection at all.

// 4. `as_slice::<String>()` does not compile — see `tests/compile_fail/`.
#[test]
fn as_slice_on_variable_width_type_does_not_compile() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/as_slice_variable_width.rs");
}

// 5. NULL round-trip: mark rows invalid, append chunk, read back
// `DuckValue::Null`.
#[test]
fn null_rows_round_trip_through_append_chunk() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();

    let types = [LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut col = chunk.vector_mut(0).unwrap();
        col.set(0, 1i32).unwrap();
        col.set_null(1);
        col.set(2, 3i32).unwrap();
    }
    chunk.set_len(3).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    app.append_chunk(&mut chunk).unwrap();
    app.save().unwrap();

    let result = c.execute("SELECT v FROM t ORDER BY v NULLS FIRST").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("v"), Some(&DuckValue::Null));
    assert_eq!(rows[1].get("v"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[2].get("v"), Some(&DuckValue::Int(3)));
}

// 6. `set_row_validity(row, true)` un-marks a previously invalid row — the
// gap this closes (previously only `set_null` existed, no way back).
#[test]
fn set_row_valid_un_marks_a_null_row_before_append() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();

    let types = [LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut col = chunk.vector_mut(0).unwrap();
        col.set_null(0);
        col.set(0, 42i32).unwrap();
        col.set_row_valid(0); // un-mark the NULL the set_null call above left behind
    }
    chunk.set_len(1).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    app.append_chunk(&mut chunk).unwrap();
    app.save().unwrap();

    let mut result = c.execute("SELECT v FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("v"), Some(&DuckValue::Int(42)));
}

// 7. Chunk reuse: `reset()` between 3 batches, all 3 batches land, no
// cross-batch bleed.
#[test]
fn reset_allows_reusing_one_chunk_across_multiple_batches() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();

    let types = [LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    let mut app = c.appender("t", "main").unwrap();

    for batch in 0..3i32 {
        chunk.reset();
        {
            let mut col = chunk.vector_mut(0).unwrap();
            col.set(0, batch * 10).unwrap();
            col.set(1, batch * 10 + 1).unwrap();
        }
        chunk.set_len(2).unwrap();
        app.append_chunk(&mut chunk).unwrap();
    }
    app.save().unwrap();

    let result = c.execute("SELECT v FROM t ORDER BY v").unwrap();
    let rows: Vec<_> = result.collect::<better_duck_core::error::Result<_>>().unwrap();
    let got: Vec<i32> = rows
        .iter()
        .map(|r| match r.get("v").unwrap() {
            DuckValue::Int(v) => *v,
            other => panic!("expected Int, got {other:?}"),
        })
        .collect();
    assert_eq!(got, vec![0, 1, 10, 11, 20, 21]);
}

// 8. `append_chunk` with wrong column count → clear `Err`, appender still
// usable afterward.
#[test]
fn append_chunk_with_wrong_column_count_errors_and_appender_stays_usable() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER)").unwrap();

    let wrong_types = [LogicalType::of::<i32>().unwrap()]; // 1 column, table has 2
    let mut wrong_chunk = DataChunkHandle::new(&wrong_types).unwrap();
    wrong_chunk.vector_mut(0).unwrap().set(0, 1i32).unwrap();
    wrong_chunk.set_len(1).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    let err = app.append_chunk(&mut wrong_chunk).unwrap_err();
    assert!(matches!(err, Error::ConversionError(_)), "{err}");

    // The appender must still work after the rejected chunk.
    let good_types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<i32>().unwrap()];
    let mut good_chunk = DataChunkHandle::new(&good_types).unwrap();
    {
        let mut cols = good_chunk.vectors_mut().unwrap();
        cols[0].set(0, 1i32).unwrap();
        cols[1].set(0, 2i32).unwrap();
    }
    good_chunk.set_len(1).unwrap();
    app.append_chunk(&mut good_chunk).unwrap();
    app.save().unwrap();

    let mut result = c.execute("SELECT a, b FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("a"), Some(&DuckValue::Int(1)));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(2)));
}

// 9. `append_chunk` with mismatched column type → clear `Err`.
#[test]
fn append_chunk_with_mismatched_column_type_errors() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v VARCHAR)").unwrap();

    let wrong_types = [LogicalType::of::<i32>().unwrap()]; // table column is VARCHAR
    let mut chunk = DataChunkHandle::new(&wrong_types).unwrap();
    chunk.vector_mut(0).unwrap().set(0, 1i32).unwrap();
    chunk.set_len(1).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    let err = app.append_chunk(&mut chunk).unwrap_err();
    assert!(matches!(err, Error::ConversionError(_)), "{err}");

    // The same appender handle must still be usable after the rejection: a
    // correctly-typed chunk goes through fine on the very next call.
    let right_types = [LogicalType::of::<String>().unwrap()];
    let mut right_chunk = DataChunkHandle::new(&right_types).unwrap();
    right_chunk.vector_mut(0).unwrap().set(0, "ok").unwrap();
    right_chunk.set_len(1).unwrap();
    app.append_chunk(&mut right_chunk).unwrap();
    app.save().unwrap();

    let mut result = c.execute("SELECT v FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("v"), Some(&DuckValue::text("ok")));
}

// 10. Multi-chunk read: 5000 rows via `fetch_chunk` loop yields exactly
// 5000 (crosses the 2048 vector-size boundary).
#[test]
fn fetch_chunk_loop_reads_every_row_across_multiple_chunks() {
    let mut c = conn();
    let mut result = c.execute("SELECT * FROM range(5000) t(v)").unwrap();
    let mut total = 0usize;
    let mut chunks_seen = 0usize;
    while let Some(chunk) = result.fetch_chunk() {
        let chunk = chunk.unwrap();
        total += chunk.len();
        chunks_seen += 1;
    }
    assert_eq!(total, 5000);
    assert!(chunks_seen > 1, "5000 rows must span more than one chunk (vector size 2048)");
}

// 11. `fetch_chunk` after `next()` has started row iteration → `Err`, no
// rows lost.
#[test]
fn fetch_chunk_after_row_iteration_started_errors() {
    let mut c = conn();
    let mut result = c.execute("SELECT * FROM range(10) t(v)").unwrap();
    let _first_row = result.next().unwrap().unwrap();
    let err = result.fetch_chunk().unwrap().unwrap_err();
    assert!(matches!(err, Error::MixedIterationMode), "{err}");
}

// 12. `next()` after `fetch_chunk` → `Err` (both directions of the mode
// guard).
#[test]
fn next_after_fetch_chunk_started_errors() {
    let mut c = conn();
    let mut result = c.execute("SELECT * FROM range(10) t(v)").unwrap();
    let _first_chunk = result.fetch_chunk().unwrap().unwrap();
    let err = result.next().unwrap().unwrap_err();
    assert!(matches!(err, Error::MixedIterationMode), "{err}");
}

// 13. `append_default_row`/`set_chunk_default` honor a column's `DEFAULT`
// expression.
#[test]
fn append_default_row_honors_the_column_default() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER DEFAULT 99)").unwrap();

    let mut app = c.appender("t", "main").unwrap();
    app.append_default_row().unwrap();
    app.save().unwrap();

    let mut result = c.execute("SELECT v FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("v"), Some(&DuckValue::Int(99)));
}

#[test]
fn set_chunk_default_honors_the_column_default() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER DEFAULT 7)").unwrap();

    let types = [LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    chunk.set_len(1).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    app.set_chunk_default(&mut chunk, 0, 0).unwrap();
    app.append_chunk(&mut chunk).unwrap();
    app.save().unwrap();

    let mut result = c.execute("SELECT v FROM t").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("v"), Some(&DuckValue::Int(7)));
}

// 14. `column_logical_type` returns a valid handle, independent of row
// iteration. Asserting the *contents* of that handle (DECIMAL width/scale,
// LIST child type, ...) needs `LogicalType` introspection accessors, which
// are WS4's scope, not WS1's — deferred there.
#[test]
fn column_logical_type_returns_a_valid_handle_for_a_decimal_column() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v DECIMAL(18,4))").unwrap();
    let mut result = c.execute("SELECT v FROM t").unwrap();
    let lt = result.column_logical_type(0).unwrap();
    drop(lt);
}

#[test]
fn column_logical_type_out_of_range_errors() {
    let mut c = conn();
    let mut result = c.execute("SELECT 1 AS a").unwrap();
    assert!(result.column_logical_type(5).is_err());
}
