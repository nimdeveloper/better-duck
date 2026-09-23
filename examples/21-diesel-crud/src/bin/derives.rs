//! Derive macros: `Queryable`, `Selectable`, `Insertable`, and `AsChangeset`.
//!
//! Instead of hand-writing column tuples, map a Rust struct to a table:
//! - `Queryable` reads a row positionally into a struct.
//! - `Selectable` (with `#[diesel(table_name = ...)]`) lets `.select(T::as_select())` pick the
//!   struct's columns explicitly, independent of column order.
//! - `Insertable` turns a struct (or `&struct`) into an `INSERT` value set.
//! - `AsChangeset` turns a struct into an `UPDATE ... SET` clause.
//!
//! Standard integer/text/double columns flow through these derives with no extra glue.

use better_duck_diesel::backend::DuckDb;
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

/// A row read out of `items`. `check_for_backend` makes the field/column type match a
/// compile-time error against the DuckDB backend rather than a runtime surprise.
#[derive(Queryable, Selectable, Debug, PartialEq)]
#[diesel(table_name = items)]
#[diesel(check_for_backend(DuckDb))]
struct Item {
    id: i32,
    label: String,
    score: i32,
}

/// A value set for inserting a new `items` row.
#[derive(Insertable)]
#[diesel(table_name = items)]
struct NewItem<'a> {
    id: i32,
    label: &'a str,
    score: i32,
}

/// A partial update — `Option` fields left `None` are omitted from the `SET` clause.
#[derive(AsChangeset)]
#[diesel(table_name = items)]
struct ItemPatch<'a> {
    label: Option<&'a str>,
    score: Option<i32>,
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

    println!("=== Insertable: insert a struct ===");
    let new = NewItem { id: 1, label: "alpha", score: 10 };
    let n = diesel::insert_into(items::table).values(&new).execute(&mut conn)?;
    assert_eq!(n, 1);
    diesel::insert_into(items::table)
        .values(&NewItem { id: 2, label: "beta", score: 20 })
        .execute(&mut conn)?;
    println!("  inserted 2 rows via NewItem");

    println!("=== Queryable + Selectable: load into a struct ===");
    let loaded: Vec<Item> =
        items::table.select(Item::as_select()).order(items::id).load(&mut conn)?;
    assert_eq!(
        loaded,
        [
            Item { id: 1, label: "alpha".to_owned(), score: 10 },
            Item { id: 2, label: "beta".to_owned(), score: 20 },
        ]
    );
    println!("  loaded {} rows: {loaded:?}", loaded.len());

    println!("=== AsChangeset: update via a patch struct ===");
    let patch = ItemPatch { label: Some("ALPHA"), score: None }; // only label changes
    let n = diesel::update(items::table.filter(items::id.eq(1))).set(&patch).execute(&mut conn)?;
    assert_eq!(n, 1);
    let updated: Item =
        items::table.filter(items::id.eq(1)).select(Item::as_select()).first(&mut conn)?;
    // label changed; score preserved because the patch left it None.
    assert_eq!(updated, Item { id: 1, label: "ALPHA".to_owned(), score: 10 });
    println!("  after patch -> {updated:?}");

    println!("\nDerive-based insert / load / update verified.");
    Ok(())
}
