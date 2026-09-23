//! Transactions: commit, rollback, nested savepoints, and `begin_test_transaction`.
//!
//! `conn.transaction(|conn| ...)` runs a closure inside a transaction: returning `Ok`
//! commits, returning `Err` rolls back. Returning `Err(Error::RollbackTransaction)` is the
//! idiomatic way to abort without surfacing a "real" error. Nested `transaction` calls become
//! SAVEPOINTs, so an inner rollback need not doom the outer transaction.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::result::Error;

diesel::table! {
    tx_items (id) {
        id  -> Integer,
        val -> Text,
    }
}

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute("CREATE TABLE tx_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)")
        .expect("create table");
    c
}

fn count(conn: &mut DuckDbConnection) -> i64 {
    tx_items::table.count().first(conn).unwrap()
}

fn main() -> QueryResult<()> {
    println!("=== commit: returning Ok persists the work ===");
    let mut conn = mem_conn();
    conn.transaction(|conn| {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("committed")))
            .execute(conn)
    })?;
    println!("  rows after committed transaction = {}", count(&mut conn));
    assert_eq!(count(&mut conn), 1);

    println!("=== rollback: returning Err(RollbackTransaction) discards the work ===");
    let outcome = conn.transaction(|conn| -> QueryResult<()> {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(2), tx_items::val.eq("discarded")))
            .execute(conn)?;
        // Abort cleanly — nothing from this closure is kept.
        Err(Error::RollbackTransaction)
    });
    assert!(outcome.is_err());
    println!("  rows after rolled-back transaction = {} (unchanged)", count(&mut conn));
    assert_eq!(count(&mut conn), 1);

    println!("=== nested savepoints: inner rolls back, outer commits ===");
    conn.transaction(|outer| {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(10), tx_items::val.eq("outer")))
            .execute(outer)?;
        // Inner savepoint: its rollback leaves the outer transaction intact.
        let _ = outer.transaction(|inner| -> QueryResult<()> {
            diesel::insert_into(tx_items::table)
                .values((tx_items::id.eq(11), tx_items::val.eq("inner")))
                .execute(inner)?;
            Err(Error::RollbackTransaction)
        });
        let visible: i64 = tx_items::table.filter(tx_items::id.ge(10)).count().first(outer)?;
        println!("  rows with id>=10 visible inside outer = {visible}");
        assert_eq!(visible, 1); // only "outer"; the inner savepoint was discarded
        QueryResult::Ok(())
    })?;
    let outer_val: String =
        tx_items::table.filter(tx_items::id.eq(10)).select(tx_items::val).first(&mut conn)?;
    assert_eq!(outer_val, "outer");
    assert_eq!(count(&mut conn), 2); // "committed" + "outer"

    println!("=== begin_test_transaction: an open transaction that never commits ===");
    // Handy in tests: every write on this connection is rolled back when it is dropped,
    // so the database is never mutated.
    let mut test_conn = mem_conn();
    test_conn.begin_test_transaction().expect("begin test transaction");
    diesel::insert_into(tx_items::table)
        .values((tx_items::id.eq(1), tx_items::val.eq("ephemeral")))
        .execute(&mut test_conn)?;
    let seen: i64 = tx_items::table.count().first(&mut test_conn)?;
    println!("  rows visible inside the test transaction = {seen}");
    assert_eq!(seen, 1);

    println!("\nAll transaction behaviours verified.");
    Ok(())
}
