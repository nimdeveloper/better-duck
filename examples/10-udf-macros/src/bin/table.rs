//! Table UDFs the ergonomic way: annotate a `fn` returning an `Iterator` with
//! `#[duckdb_table_function]` and use it in a `FROM` clause.
//!
//! A single-`Item` iterator yields a one-column table; a tuple `Item` yields one
//! column per tuple element, named via `columns(...)`.

use better_duck_core::connection::Connection;
use better_duck_core::duckdb_table_function;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// The integers in `[start, stop)`, one per row, in a column named `n`.
#[duckdb_table_function(name = "series", columns("n"))]
fn series(
    start: i64,
    stop: i64,
) -> impl Iterator<Item = i64> + Send {
    start..stop
}

/// Splits `text` on whitespace into `(idx, word)` rows (two columns).
#[duckdb_table_function(columns("idx", "word"))]
fn words(text: String) -> impl Iterator<Item = (i64, String)> + Send {
    text.split_whitespace()
        .map(String::from)
        .enumerate()
        .map(|(i, w)| (i as i64, w))
        .collect::<Vec<_>>()
        .into_iter()
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    series::register(&mut conn)?;
    words::register(&mut conn)?;

    section("SELECT sum(n) FROM series(1, 101)");
    let mut r = conn.execute("SELECT sum(n) AS total FROM series(1, 101)")?;
    let row = r.next().expect("one row")?;
    show("total", row.get("total"));
    // sum() over BIGINT widens to HUGEINT.
    assert_eq!(row.get("total"), Some(&DuckValue::HugeInt(5050)));

    section("row-by-row scan of series(1, 6)");
    let result = conn.execute("SELECT n FROM series(1, 6) ORDER BY n")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    let got: Vec<&DuckValue> = rows.iter().filter_map(|row| row.get("n")).collect();
    show("series(1, 6)", &got);
    assert_eq!(
        got,
        vec![
            &DuckValue::BigInt(1),
            &DuckValue::BigInt(2),
            &DuckValue::BigInt(3),
            &DuckValue::BigInt(4),
            &DuckValue::BigInt(5),
        ]
    );

    section("multi-column table function words(text)");
    let result = conn.execute("SELECT idx, word FROM words('hello there world') ORDER BY idx")?;
    let rows: Vec<_> = result.collect::<Result<_>>()?;
    for row in &rows {
        println!("  ({:?}, {:?})", row.get("idx"), row.get("word"));
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("idx"), Some(&DuckValue::BigInt(0)));
    assert_eq!(rows[0].get("word"), Some(&DuckValue::text("hello")));
    assert_eq!(rows[2].get("word"), Some(&DuckValue::text("world")));

    Ok(())
}
