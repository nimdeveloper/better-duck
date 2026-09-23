//! The core crate built with zero optional features (`--no-default-features`).
//!
//! No `chrono`, no `decimal`, no `pool`/`async`/`udf`: just open a connection,
//! create a table, insert rows, and read the fundamental scalar variants back —
//! `DuckValue::Int`, `Text`, `Double`, and `Boolean`.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::value::DuckValue;

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch(
        "CREATE TABLE items (id INTEGER, name VARCHAR, price DOUBLE, in_stock BOOLEAN)",
    )?;
    conn.execute_batch(
        "INSERT INTO items VALUES (1, 'widget', 9.99, true), (2, 'gadget', 19.5, false)",
    )?;

    let rows: Vec<_> = conn
        .execute("SELECT id, name, price, in_stock FROM items ORDER BY id")?
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    println!(
        "row 0: id={:?} name={:?} price={:?} in_stock={:?}",
        first.get("id"),
        first.get("name"),
        first.get("price"),
        first.get("in_stock"),
    );
    assert_eq!(first.get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(first.get("name"), Some(&DuckValue::text("widget")));
    assert_eq!(first.get("price"), Some(&DuckValue::Double(9.99)));
    assert_eq!(first.get("in_stock"), Some(&DuckValue::Boolean(true)));

    let second = &rows[1];
    assert_eq!(second.get("name"), Some(&DuckValue::text("gadget")));
    assert_eq!(second.get("price"), Some(&DuckValue::Double(19.5)));
    assert_eq!(second.get("in_stock"), Some(&DuckValue::Boolean(false)));

    println!("\nminimal build (no optional features) reads Int/Text/Double/Boolean");
    Ok(())
}
