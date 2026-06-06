#![allow(missing_docs)]
//! CRUD (INSERT / SELECT / UPDATE / DELETE) tests exercising the Diesel query DSL.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*};

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

// INSERT

#[test]
fn insert_single_row() {
    let mut conn = mem_conn();
    let n = diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("alpha"), items::score.eq(10)))
        .execute(&mut conn)
        .unwrap();
    assert_eq!(n, 1);
    let (lbl, sc): (String, i32) =
        items::table.select((items::label, items::score)).first(&mut conn).unwrap();
    assert_eq!(lbl, "alpha");
    assert_eq!(sc, 10);
}

#[test]
fn insert_batch_rows() {
    let mut conn = mem_conn();
    let rows = vec![
        (items::id.eq(1), items::label.eq("a"), items::score.eq(1)),
        (items::id.eq(2), items::label.eq("b"), items::score.eq(2)),
        (items::id.eq(3), items::label.eq("c"), items::score.eq(3)),
    ];
    let n = diesel::insert_into(items::table).values(&rows).execute(&mut conn).unwrap();
    assert_eq!(n, 3);
    let count: i64 = items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 3);
}

#[test]
fn insert_returning_label() {
    let mut conn = mem_conn();
    let returned: String = diesel::insert_into(items::table)
        .values((items::id.eq(1), items::label.eq("returned"), items::score.eq(0)))
        .returning(items::label)
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(returned, "returned");
}

#[test]
fn insert_get_results_multiple() {
    let mut conn = mem_conn();
    let rows = vec![
        (items::id.eq(10), items::label.eq("x"), items::score.eq(100)),
        (items::id.eq(11), items::label.eq("y"), items::score.eq(200)),
    ];
    let returned: Vec<(i32, String)> = diesel::insert_into(items::table)
        .values(&rows)
        .returning((items::id, items::label))
        .get_results(&mut conn)
        .unwrap();
    assert_eq!(returned, [(10, "x".to_string()), (11, "y".to_string())]);
}

// SELECT / load / first

#[test]
fn select_all_rows() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'a',1),(2,'b',2)").unwrap();
    let ids: Vec<i32> = items::table.order(items::id).select(items::id).load(&mut conn).unwrap();
    assert_eq!(ids, [1, 2]);
}

#[test]
fn get_result_first() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (5,'five',50)").unwrap();
    let (id, lbl): (i32, String) =
        items::table.select((items::id, items::label)).first(&mut conn).unwrap();
    assert_eq!(id, 5);
    assert_eq!(lbl, "five");
}

// UPDATE

#[test]
fn update_single_field() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'old',0)").unwrap();
    let n = diesel::update(items::table.filter(items::id.eq(1)))
        .set(items::label.eq("new"))
        .execute(&mut conn)
        .unwrap();
    assert_eq!(n, 1);
    let lbl: String = items::table.select(items::label).first(&mut conn).unwrap();
    assert_eq!(lbl, "new");
}

#[test]
fn update_multiple_fields() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'x',0)").unwrap();
    diesel::update(items::table.filter(items::id.eq(1)))
        .set((items::label.eq("updated"), items::score.eq(999)))
        .execute(&mut conn)
        .unwrap();
    let (lbl, sc): (String, i32) =
        items::table.select((items::label, items::score)).first(&mut conn).unwrap();
    assert_eq!(lbl, "updated");
    assert_eq!(sc, 999);
}

// DELETE

#[test]
fn delete_by_filter() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'keep',0),(2,'gone',0)").unwrap();
    let n = diesel::delete(items::table.filter(items::id.eq(2))).execute(&mut conn).unwrap();
    assert_eq!(n, 1);
    let remaining: i64 = items::table.count().first(&mut conn).unwrap();
    assert_eq!(remaining, 1);
    let lbl: String = items::table.select(items::label).first(&mut conn).unwrap();
    assert_eq!(lbl, "keep");
}

#[test]
fn delete_all_rows() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'a',0),(2,'b',0)").unwrap();
    diesel::delete(items::table).execute(&mut conn).unwrap();
    let count: i64 = items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 0);
}

// execute count

#[test]
fn execute_returns_affected_rows() {
    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO items VALUES (1,'a',5),(2,'b',5),(3,'c',10)").unwrap();
    let affected = diesel::update(items::table.filter(items::score.eq(5)))
        .set(items::score.eq(0))
        .execute(&mut conn)
        .unwrap();
    assert_eq!(affected, 2);
}
