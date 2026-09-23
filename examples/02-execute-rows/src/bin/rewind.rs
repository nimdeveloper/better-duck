//! Peeking with `exists()` and replaying rows with `enable_rewind()` + `rewind()`.
//!
//! Forward iteration is the default and costs nothing. If you need to replay rows, call
//! `enable_rewind()` BEFORE consuming them (it caches each row as it is pulled), then
//! `rewind()` to restart from the beginning.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("exists(): peek without consuming");
    let mut peek = conn.execute("SELECT 1")?;
    let has_row = peek.exists()?;
    show("exists", has_row);
    assert!(has_row);
    // The peeked row is still yielded by the iterator.
    assert_eq!(peek.count(), 1);

    section("enable_rewind() + rewind(): replay the same rows");
    let mut result = conn.execute("SELECT * FROM range(3)")?;
    result.enable_rewind();

    let mut pass1 = Vec::new();
    while let Some(row) = result.next() {
        pass1.push(row?.get_idx(0).cloned());
    }
    result.rewind();
    let mut pass2 = Vec::new();
    while let Some(row) = result.next() {
        pass2.push(row?.get_idx(0).cloned());
    }

    show("pass 1", &pass1);
    show("pass 2 (after rewind)", &pass2);
    assert_eq!(pass1, pass2);
    assert_eq!(pass1.first(), Some(&Some(DuckValue::BigInt(0))));

    Ok(())
}
