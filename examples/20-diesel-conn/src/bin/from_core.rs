//! `DuckDbConnection::from_core` — wrap an existing core `Connection` so Diesel and the
//! low-level `better_duck_core` API share the very same database.
//!
//! `establish(":memory:")` gives each connection its own private in-memory database. When you
//! need Diesel to operate on a database you already opened through `better_duck_core` (or to
//! share one in-memory database across several Diesel handles), open a
//! `better_duck_core::database::Database` and hand each `connect()` to `from_core`.

use better_duck_core::database::Database;
use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

diesel::table! {
    shared_items (id) {
        id  -> Integer,
        val -> Text,
    }
}

fn main() -> QueryResult<()> {
    println!("=== two Diesel connections over one shared Database ===");
    // One underlying in-memory database, shared by every connection it hands out.
    let database = Database::open_in_memory().expect("open shared in-memory database");

    let mut first = DuckDbConnection::from_core(database.connect().expect("connect first"));
    let mut second = DuckDbConnection::from_core(database.connect().expect("connect second"));

    // Write through the first handle...
    first.batch_execute(
        "CREATE TABLE shared_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL);
         INSERT INTO shared_items VALUES (1, 'shared');",
    )?;

    // ...and read it straight back through the second — they see the same data.
    let value: String = shared_items::table.select(shared_items::val).first(&mut second)?;
    println!("  second handle reads value written by first = {value:?}");
    assert_eq!(value, "shared");

    // A write through the second handle is likewise visible to the first.
    diesel::insert_into(shared_items::table)
        .values((shared_items::id.eq(2), shared_items::val.eq("from_second")))
        .execute(&mut second)?;
    let count: i64 = shared_items::table.count().first(&mut first)?;
    println!("  rows visible via first handle after second's insert = {count}");
    assert_eq!(count, 2);

    println!("\nfrom_core sharing verified.");
    Ok(())
}
