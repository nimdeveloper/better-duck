//! Materializing a query into an owned, thread-safe `ResultSet`.
//!
//! A `DuckResult` holds FFI handles and is neither `Send` nor `Sync`. Calling
//! `.materialize()` pulls every row into a `ResultSet`, which owns its data and is
//! `Send + Sync + Clone` — so it can be moved into another thread or returned from a
//! `spawn_blocking` closure. This is the second way (vs. streaming) to consume a query.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER, name TEXT);
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    )?;

    section("materialize a SELECT into a ResultSet");
    let set = conn.execute("SELECT id, name FROM t ORDER BY id")?.materialize()?;
    show("len", set.len());
    show("is_empty", set.is_empty());
    show("columns", set.column_names().to_vec());
    assert_eq!(set.len(), 3);
    assert!(!set.is_empty());

    let first = set.first().expect("at least one row");
    assert_eq!(first.get("id"), Some(&DuckValue::Int(1)));

    section("ResultSet is Send + Sync + Clone: move it into a thread");
    let moved = set.clone();
    let handle = std::thread::spawn(move || moved.len());
    let counted = handle.join().expect("thread ok");
    show("row count computed on another thread", counted);
    assert_eq!(counted, 3);

    // Iterate the owned rows.
    for row in &set {
        println!("  name = {:?}", row.get("name"));
    }

    Ok(())
}
