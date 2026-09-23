//! UPDATE and DELETE with the Diesel DSL.
//!
//! `diesel::update(target).set(...)` accepts a single `col.eq(value)` or a tuple of them.
//! `diesel::delete(target)` removes rows; the `target` is a filtered query for a subset or the
//! bare table to clear everything. Both return the affected-row count from `.execute`.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

diesel::table! {
    items (id) {
        id    -> Integer,
        label -> Text,
        score -> Integer,
    }
}

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL, score INTEGER NOT NULL);
         INSERT INTO items VALUES (1,'a',5),(2,'b',5),(3,'c',10),(4,'d',10);",
    )
    .expect("seed items");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    println!("=== update a single field ===");
    let n = diesel::update(items::table.filter(items::id.eq(1)))
        .set(items::label.eq("renamed"))
        .execute(&mut conn)?;
    assert_eq!(n, 1);
    let lbl: String =
        items::table.filter(items::id.eq(1)).select(items::label).first(&mut conn)?;
    assert_eq!(lbl, "renamed");
    println!("  updated 1 row; id=1 label -> {lbl:?}");

    println!("=== update multiple fields (tuple .set) ===");
    let n = diesel::update(items::table.filter(items::id.eq(2)))
        .set((items::label.eq("two"), items::score.eq(999)))
        .execute(&mut conn)?;
    assert_eq!(n, 1);
    let row: (String, i32) =
        items::table.filter(items::id.eq(2)).select((items::label, items::score)).first(&mut conn)?;
    assert_eq!(row, ("two".to_owned(), 999));
    println!("  updated (label, score) -> {row:?}");

    println!("=== update many rows by filter (affected count) ===");
    let affected = diesel::update(items::table.filter(items::score.eq(10)))
        .set(items::score.eq(0))
        .execute(&mut conn)?;
    println!("  rows with score=10 zeroed = {affected}");
    assert_eq!(affected, 2);

    println!("=== delete by filter ===");
    let n = diesel::delete(items::table.filter(items::id.eq(4))).execute(&mut conn)?;
    assert_eq!(n, 1);
    let remaining: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(remaining, 3);
    println!("  deleted 1 row; remaining = {remaining}");

    println!("=== delete all rows ===");
    let n = diesel::delete(items::table).execute(&mut conn)?;
    assert_eq!(n, 3);
    let empty: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(empty, 0);
    println!("  cleared table; remaining = {empty}");

    println!("\nAll update / delete forms verified.");
    Ok(())
}
