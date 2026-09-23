//! Iterating a raw `DataChunk` fetched from a query result.
//!
//! A [`DataChunk`] is DuckDB's columnar batch. `DataChunk::from_result` pulls the next
//! chunk out of a `DuckResult`; from there the row cursor is driven manually with
//! `next_row` (returning each row index in turn), `current_row` reports the cursor,
//! `row_count` the chunk size, and `reset` clears it back to empty while keeping capacity.

use better_duck_core::connection::Connection;
use better_duck_core::ffi::duckdb_data_chunk_get_column_count;
use better_duck_core::DataChunk;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (a INTEGER, b INTEGER)")?;
    conn.execute_batch("INSERT INTO t VALUES (10, 100), (20, 200), (30, 300)")?;

    section("fetch a chunk and read its shape");
    let result = conn.execute("SELECT a, b FROM t ORDER BY a")?;
    let mut chunk = DataChunk::from_result(&result).expect("a chunk")?;
    let rows = chunk.row_count();
    show("row_count", rows);
    // The raw chunk exposes its column count through the C API (`*chunk` is the handle).
    // SAFETY: `*chunk` is a valid, non-null duckdb_data_chunk owned by `chunk`.
    let cols = unsafe { duckdb_data_chunk_get_column_count(*chunk) };
    show("num_columns", cols);
    show("current_row (start)", chunk.current_row());
    assert_eq!(rows, 3);
    assert_eq!(cols, 2);
    assert_eq!(chunk.current_row(), 0);

    section("walk the row cursor with next_row()");
    let mut visited = Vec::new();
    while let Some(idx) = chunk.next_row() {
        visited.push(idx);
    }
    show("row indices visited", &visited);
    assert_eq!(visited, vec![0, 1, 2]);

    section("reset() clears a fresh chunk back to empty");
    // `next_row` destroys the chunk once exhausted, so fetch a new one to reset.
    let result = conn.execute("SELECT a, b FROM t")?;
    let mut chunk = DataChunk::from_result(&result).expect("a chunk")?;
    assert!(chunk.row_count() > 0);
    chunk.reset();
    show("row_count after reset", chunk.row_count());
    assert_eq!(chunk.row_count(), 0);
    assert_eq!(chunk.current_row(), 0);

    Ok(())
}
