//! Nullable columns: `Nullable<T>` maps to `Option<T>`, and SQL `NULL` reads back as `None`.
//!
//! A `Nullable<T>` column accepts `Some(v)` or `None` on insert and yields `Option<T>` on
//! read. Selecting a normally-non-null expression as `.nullable()` (e.g. an aggregate over an
//! empty set) is the other common source of `Option`.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::dsl::max;
use diesel::prelude::*;

diesel::table! {
    items (id) {
        id   -> Integer,
        note -> Nullable<Text>,
        qty  -> Nullable<Integer>,
    }
}

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute("CREATE TABLE items (id INTEGER PRIMARY KEY, note VARCHAR, qty INTEGER)")
        .expect("create items");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    println!("=== insert Some(...) then read back Some ===");
    diesel::insert_into(items::table)
        .values((items::id.eq(1), items::note.eq(Some("present")), items::qty.eq(Some(7))))
        .execute(&mut conn)?;
    let note: Option<String> =
        items::table.filter(items::id.eq(1)).select(items::note).first(&mut conn)?;
    let qty: Option<i32> =
        items::table.filter(items::id.eq(1)).select(items::qty).first(&mut conn)?;
    assert_eq!(note, Some("present".to_owned()));
    assert_eq!(qty, Some(7));
    println!("  note={note:?} qty={qty:?}");

    println!("=== insert None (SQL NULL) then read back None ===");
    diesel::insert_into(items::table)
        .values((items::id.eq(2), items::note.eq(None::<String>), items::qty.eq(None::<i32>)))
        .execute(&mut conn)?;
    let note: Option<String> =
        items::table.filter(items::id.eq(2)).select(items::note).first(&mut conn)?;
    let qty: Option<i32> =
        items::table.filter(items::id.eq(2)).select(items::qty).first(&mut conn)?;
    assert_eq!(note, None);
    assert_eq!(qty, None);
    println!("  note={note:?} qty={qty:?} (both NULL)");

    println!("=== loading a mixed column yields a Vec<Option<T>> ===");
    let all_notes: Vec<Option<String>> =
        items::table.order(items::id).select(items::note).load(&mut conn)?;
    assert_eq!(all_notes, [Some("present".to_owned()), None]);
    println!("  notes -> {all_notes:?}");

    println!("=== aggregate over an empty set is None ===");
    let none_max: Option<i32> =
        items::table.filter(items::id.eq(9999)).select(max(items::qty)).first(&mut conn)?;
    assert_eq!(none_max, None);
    println!("  max(qty) over no rows -> {none_max:?}");

    println!("\nNullable / Option round-trips verified.");
    Ok(())
}
