//! Upsert: `ON CONFLICT ... DO NOTHING` and `DO UPDATE SET ... = excluded(...)`.
//!
//! `.on_conflict(col)` names the conflict target. `.do_nothing()` keeps the existing row;
//! `.do_update().set(...)` overwrites it. Inside a `DO UPDATE`, `excluded(col)` refers to the
//! value the failed insert *would* have written, so `col.eq(excluded(col))` copies the new
//! value over the old.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::upsert::excluded;

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
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL, score INTEGER NOT NULL)",
    )
    .expect("create items");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("first"), items::score.eq(10)))
        .execute(&mut conn)?;

    println!("=== on_conflict(id).do_nothing() keeps the existing row ===");
    let affected = diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("duplicate"), items::score.eq(20)))
        .on_conflict(items::id)
        .do_nothing()
        .execute(&mut conn)?;
    println!("  rows affected by do_nothing = {affected}");
    assert_eq!(affected, 0);
    let row: (String, i32) =
        items::table.select((items::label, items::score)).first(&mut conn)?;
    assert_eq!(row, ("first".to_owned(), 10));
    println!("  row unchanged -> {row:?}");

    println!("=== on_conflict(id).do_update().set(... = excluded(...)) overwrites ===");
    let affected = diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("replacement"), items::score.eq(30)))
        .on_conflict(items::id)
        .do_update()
        .set((items::label.eq(excluded(items::label)), items::score.eq(excluded(items::score))))
        .execute(&mut conn)?;
    println!("  rows affected by do_update = {affected}");
    assert_eq!(affected, 1);
    let row: (String, i32) =
        items::table.select((items::label, items::score)).first(&mut conn)?;
    assert_eq!(row, ("replacement".to_owned(), 30));
    println!("  row replaced -> {row:?}");

    // A brand-new key just inserts normally through the same upsert statement.
    let affected = diesel::insert_into(items::table)
        .values((items::id.eq(2), items::label.eq("fresh"), items::score.eq(5)))
        .on_conflict(items::id)
        .do_update()
        .set(items::score.eq(excluded(items::score)))
        .execute(&mut conn)?;
    assert_eq!(affected, 1);
    let total: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(total, 2);
    println!("  non-conflicting upsert inserted a new row; total = {total}");

    println!("\nUpsert behaviours verified.");
    Ok(())
}
