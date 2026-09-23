//! INSERT with the Diesel DSL: single row, batch, and `RETURNING`.
//!
//! `diesel::insert_into(table).values(...)` accepts either a single column-tuple or a
//! `&Vec<_>` of tuples for a batch insert. `.execute` returns the affected-row count;
//! `.returning(cols).get_result()` / `.get_results()` run an `INSERT ... RETURNING` and read
//! the produced columns back.

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
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label VARCHAR NOT NULL, score INTEGER NOT NULL DEFAULT 0)",
    )
    .expect("create items");
    c
}

fn main() -> QueryResult<()> {
    let mut conn = mem_conn();

    println!("=== single-row insert ===");
    let n = diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("alpha"), items::score.eq(10)))
        .execute(&mut conn)?;
    println!("  rows inserted = {n}");
    assert_eq!(n, 1);

    println!("=== batch insert (&Vec of tuples) ===");
    let rows = vec![
        (items::id.eq(2), items::label.eq("beta"), items::score.eq(20)),
        (items::id.eq(3), items::label.eq("gamma"), items::score.eq(30)),
    ];
    let n = diesel::insert_into(items::table).values(&rows).execute(&mut conn)?;
    println!("  rows inserted in batch = {n}");
    assert_eq!(n, 2);
    let total: i64 = items::table.count().first(&mut conn)?;
    assert_eq!(total, 3);

    println!("=== RETURNING a single column (.returning(col).get_result()) ===");
    let returned: String = diesel::insert_into(items::table)
        .values((items::id.eq(4), items::label.eq("delta"), items::score.eq(40)))
        .returning(items::label)
        .get_result(&mut conn)?;
    println!("  RETURNING label = {returned:?}");
    assert_eq!(returned, "delta");

    println!("=== RETURNING a tuple over a batch (.returning((a,b)).get_results()) ===");
    let batch = vec![
        (items::id.eq(5), items::label.eq("e"), items::score.eq(50)),
        (items::id.eq(6), items::label.eq("f"), items::score.eq(60)),
    ];
    let produced: Vec<(i32, String)> = diesel::insert_into(items::table)
        .values(&batch)
        .returning((items::id, items::label))
        .get_results(&mut conn)?;
    println!("  RETURNING (id, label) = {produced:?}");
    assert_eq!(produced, [(5, "e".to_owned()), (6, "f".to_owned())]);

    println!("\nAll insert forms verified.");
    Ok(())
}
