//! Chunk-level ingestion and the active column list.
//!
//! Beyond one-row-at-a-time `append`, an [`Appender`] can take a whole [`DataChunk`] at
//! once (`append_chunk`), append an all-`DEFAULT` row (`append_default_row`), and — before
//! the first row — restrict which columns each row supplies via the *active column list*
//! (`add_column` / `clear_columns`).

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use better_duck_core::DataChunk;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("append_chunk: copy a whole DataChunk in one call");
    conn.execute_batch("CREATE TABLE src (v INTEGER)")?;
    conn.execute_batch("INSERT INTO src VALUES (1), (2), (3)")?;
    conn.execute_batch("CREATE TABLE dst (v INTEGER)")?;
    // Fetch one chunk of rows out of a query result; it owns its data.
    let chunk = {
        let result = conn.execute("SELECT v FROM src ORDER BY v")?;
        DataChunk::from_result(&result).expect("a chunk")?
    };
    show("chunk row_count", chunk.row_count());
    {
        let mut app = conn.appender("dst", "main")?;
        app.append_chunk(&chunk)?;
        app.save()?;
    }
    let copied: Vec<_> = conn.execute("SELECT v FROM dst ORDER BY v")?.collect::<Result<_>>()?;
    assert_eq!(copied.len(), 3);
    assert_eq!(copied[0].get("v"), Some(&DuckValue::Int(1)));
    assert_eq!(copied[2].get("v"), Some(&DuckValue::Int(3)));

    section("add_column / clear_columns: set the active column list before the first row");
    conn.execute_batch("CREATE TABLE proj (a INTEGER, b INTEGER DEFAULT 99)")?;
    {
        let mut app = conn.appender("proj", "main")?;
        // With no projection the appender expects every column.
        show("column_count (full table)", app.column_count());
        assert_eq!(app.column_count(), 2);

        // Project only `a`; `b` will take its DEFAULT for appended rows.
        app.add_column("a")?;
        show("column_count (projected to `a`)", app.column_count());
        assert_eq!(app.column_count(), 1);

        // `clear_columns` restores the full projection...
        app.clear_columns()?;
        assert_eq!(app.column_count(), 2);
        // ...then project `a` again and append a single-value row.
        app.add_column("a")?;
        app.append(&mut DuckValue::Int(7))?;
        app.save()?;
    }
    let row = conn.execute("SELECT a, b FROM proj")?.next().expect("row")?;
    show("a", row.get("a"));
    show("b (from DEFAULT)", row.get("b"));
    assert_eq!(row.get("a"), Some(&DuckValue::Int(7)));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(99)));

    section("append_default_row: one row where every column takes its DEFAULT (or NULL)");
    conn.execute_batch("CREATE TABLE defs (a INTEGER DEFAULT 5, b INTEGER)")?;
    {
        let mut app = conn.appender("defs", "main")?;
        app.append_default_row()?;
        app.save()?;
    }
    let row = conn.execute("SELECT a, b FROM defs")?.next().expect("row")?;
    show("a (DEFAULT 5)", row.get("a"));
    show("b (no DEFAULT -> NULL)", row.get("b"));
    assert_eq!(row.get("a"), Some(&DuckValue::Int(5)));
    assert_eq!(row.get("b"), Some(&DuckValue::Null));

    Ok(())
}
