//! `ExtractedStatements` — parse a multi-statement batch, prepare/run each on demand.
//!
//! DuckDB does the parsing (no Rust-side splitting), so a semicolon inside a string
//! literal is not a split. Each statement is prepared individually by index (prepare a
//! given index at most once).

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    section("extract a 3-statement batch");
    let batch = conn.extract_statements("SELECT 1 AS a; SELECT 'two' AS b; SELECT 3 AS c")?;
    show("len", batch.len());
    show("is_empty", batch.is_empty());
    assert_eq!(batch.len(), 3);

    // Prepare and run each statement once, in order.
    for i in 0..batch.len() {
        let mut stmt = batch.prepare(i)?;
        let row = stmt.execute()?.next().expect("row")?;
        println!("  statement {i}: first column = {:?}", row.get_idx(0));
        if i == 0 {
            assert_eq!(row.get("a"), Some(&DuckValue::Int(1)));
        }
    }

    section("a semicolon inside a string is not a split");
    let one = conn.extract_statements("SELECT 'a;b' AS s")?;
    assert_eq!(one.len(), 1);
    let row = one.prepare(0)?.execute()?.next().expect("row")?;
    assert_eq!(row.get("s"), Some(&DuckValue::text("a;b")));

    section("out-of-range index is rejected");
    let small = conn.extract_statements("SELECT 1")?;
    assert!(small.prepare(5).is_err());

    Ok(())
}
