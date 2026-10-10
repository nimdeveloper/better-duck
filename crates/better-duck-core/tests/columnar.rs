//! Columnar read/write: `fetch_chunk`, zero-copy `VectorRef::as_slice`, and
//! build-and-bulk-insert via a `DataChunkHandle`.
//!
//! Requires the `columnar` (or `udf`) feature; the whole file compiles only then.
#![cfg(any(feature = "udf", feature = "columnar"))]

use better_duck_core::{
    connection::Connection, error::Error, types::value::DuckValue, types::LogicalType,
    DataChunkHandle,
};

fn conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

/// Builds a 2-column `(INTEGER, VARCHAR)` chunk from `rows`.
fn build_chunk(rows: &[(i32, &str)]) -> DataChunkHandle {
    let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<String>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut cols = chunk.vectors_mut().unwrap();
        for (i, (id, name)) in rows.iter().enumerate() {
            cols[0].set(i, *id).unwrap();
            cols[1].set(i, *name).unwrap();
        }
    }
    chunk.set_len(rows.len()).unwrap();
    chunk
}

#[test]
fn build_and_append_chunk_round_trips() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
    let chunk = build_chunk(&[(1, "a"), (2, "b"), (3, "c")]);
    {
        let mut app = c.appender("t", "main").unwrap();
        app.append_chunk_handle(&chunk).unwrap();
        app.save().unwrap();
    }
    let rows: Vec<_> =
        c.execute("SELECT id, name FROM t ORDER BY id").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[2].get("name"), Some(&DuckValue::Text("c".to_owned())));
}

#[test]
fn null_and_set_row_valid_round_trip() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
    let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<String>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut cols = chunk.vectors_mut().unwrap();
        // row 0: name is NULL.
        cols[0].set(0, 10_i32).unwrap();
        cols[1].set_null(0);
        // row 1: name nulled then un-marked valid and written — exercises set_row_valid.
        cols[0].set(1, 20_i32).unwrap();
        cols[1].set_null(1);
        cols[1].set_row_valid(1);
        cols[1].set(1, "back").unwrap();
    }
    chunk.set_len(2).unwrap();
    {
        let mut app = c.appender("t", "main").unwrap();
        app.append_chunk_handle(&chunk).unwrap();
        app.save().unwrap();
    }
    let rows: Vec<_> =
        c.execute("SELECT id, name FROM t ORDER BY id").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows[0].get("name"), Some(&DuckValue::Null));
    assert_eq!(rows[1].get("name"), Some(&DuckValue::Text("back".to_owned())));
}

#[test]
fn reset_reuses_a_chunk_across_batches() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
    let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<String>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut app = c.appender("t", "main").unwrap();
        for batch in 0..3i32 {
            {
                let mut cols = chunk.vectors_mut().unwrap();
                cols[0].set(0, batch).unwrap();
                cols[1].set(0, "x").unwrap();
            }
            chunk.set_len(1).unwrap();
            app.append_chunk_handle(&chunk).unwrap();
            chunk.reset();
        }
        app.save().unwrap();
    }
    let rows: Vec<_> =
        c.execute("SELECT id FROM t ORDER BY id").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(0)));
    assert_eq!(rows[2].get("id"), Some(&DuckValue::Int(2)));
}

#[test]
fn wrong_column_count_errors_and_appender_stays_usable() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE two (a INTEGER, b INTEGER)").unwrap();
    // A one-column chunk against a two-column appender.
    let types = [LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    chunk.vector_mut(0).unwrap().set(0, 1_i32).unwrap();
    chunk.set_len(1).unwrap();

    let mut app = c.appender("two", "main").unwrap();
    assert!(app.append_chunk_handle(&chunk).is_err(), "column-count mismatch must error");
    // Appender still works afterwards: a correctly-shaped append succeeds.
    let good = {
        let t = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<i32>().unwrap()];
        let mut ch = DataChunkHandle::new(&t).unwrap();
        {
            let mut cols = ch.vectors_mut().unwrap();
            cols[0].set(0, 7_i32).unwrap();
            cols[1].set(0, 8_i32).unwrap();
        }
        ch.set_len(1).unwrap();
        ch
    };
    app.append_chunk_handle(&good).unwrap();
    app.save().unwrap();
    let rows: Vec<_> =
        c.execute("SELECT a, b FROM two").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(7)));
}

#[test]
fn wrong_column_type_errors_and_appender_stays_usable() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)").unwrap();
    // Columns swapped: VARCHAR where INTEGER is expected, and vice versa.
    let types = [LogicalType::of::<String>().unwrap(), LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    {
        let mut cols = chunk.vectors_mut().unwrap();
        cols[0].set(0, "oops").unwrap();
        cols[1].set(0, 1_i32).unwrap();
    }
    chunk.set_len(1).unwrap();

    let mut app = c.appender("t", "main").unwrap();
    assert!(app.append_chunk_handle(&chunk).is_err(), "column-type mismatch must error");
    // Appender still usable: a correctly-typed chunk appends fine.
    app.append_chunk_handle(&build_chunk(&[(1, "a")])).unwrap();
    app.save().unwrap();
    let rows: Vec<_> =
        c.execute("SELECT id, name FROM t").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&DuckValue::Text("a".to_owned())));
}

#[test]
fn fetch_chunk_reads_every_row_across_multiple_chunks_via_as_slice() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE big (n INTEGER)").unwrap();
    c.execute_batch("INSERT INTO big SELECT * FROM range(5000)").unwrap();

    let mut result = c.execute("SELECT n FROM big ORDER BY n").unwrap();
    let mut total = 0usize;
    let mut chunks = 0usize;
    let mut first_value = None;
    while let Some(chunk) = result.fetch_chunk() {
        let chunk = chunk.unwrap();
        let n = chunk.len();
        let col = chunk.vector(0).unwrap();
        let slice = col.as_slice::<i32>(n).unwrap();
        if first_value.is_none() && !slice.is_empty() {
            first_value = Some(slice[0]);
        }
        total += slice.len();
        chunks += 1;
    }
    assert_eq!(total, 5000);
    assert!(chunks > 1, "5000 rows should span more than one chunk, got {chunks}");
    assert_eq!(first_value, Some(0));
}

#[test]
fn as_slice_type_mismatch_errors() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (n INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1), (2)").unwrap();
    let mut result = c.execute("SELECT n FROM t").unwrap();
    let chunk = result.fetch_chunk().unwrap().unwrap();
    let col = chunk.vector(0).unwrap();
    // The column is INTEGER (i32); reading it as i64 must be rejected.
    assert!(col.as_slice::<i64>(chunk.len()).is_err());
    // Reading it as its real type works.
    assert_eq!(col.as_slice::<i32>(chunk.len()).unwrap().len(), 2);
}

#[test]
fn fetch_chunk_after_row_iteration_errors() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (n INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    let mut result = c.execute("SELECT n FROM t").unwrap();
    let _ = result.next();
    assert!(matches!(result.fetch_chunk(), Some(Err(Error::MixedIterationMode))));
}

#[test]
fn row_iteration_after_fetch_chunk_errors() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (n INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    let mut result = c.execute("SELECT n FROM t").unwrap();
    let _ = result.fetch_chunk();
    assert!(matches!(result.next(), Some(Err(Error::MixedIterationMode))));
}

#[test]
fn append_default_row_honors_column_defaults() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (a INTEGER DEFAULT 5, b INTEGER DEFAULT 99)").unwrap();
    {
        let mut app = c.appender("t", "main").unwrap();
        app.append_default_row().unwrap();
        app.save().unwrap();
    }
    let rows: Vec<_> = c.execute("SELECT a, b FROM t").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(5)));
    assert_eq!(rows[0].get("b"), Some(&DuckValue::Int(99)));
}

#[test]
fn set_chunk_default_honors_column_default() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER DEFAULT 7)").unwrap();
    let types = [LogicalType::of::<i32>().unwrap(), LogicalType::of::<i32>().unwrap()];
    let mut chunk = DataChunkHandle::new(&types).unwrap();
    chunk.vector_mut(0).unwrap().set(0, 1_i32).unwrap();
    {
        let mut app = c.appender("t", "main").unwrap();
        // Fill column 1 (`b`) of row 0 with its DEFAULT.
        app.set_chunk_default(&mut chunk, 1, 0).unwrap();
        chunk.set_len(1).unwrap();
        app.append_chunk_handle(&chunk).unwrap();
        app.save().unwrap();
    }
    let rows: Vec<_> = c.execute("SELECT a, b FROM t").unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("a"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[0].get("b"), Some(&DuckValue::Int(7)));
}
