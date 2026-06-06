#![allow(missing_docs)]
//! Transaction management tests: commit, rollback, nested savepoints, test_transaction.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*};

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

// Commit

#[test]
fn transaction_commit_persists() {
    let mut conn = mem_conn();
    conn.transaction(|conn| {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("committed")))
            .execute(conn)
    })
    .unwrap();
    assert_eq!(count(&mut conn), 1);
    let v: String = tx_items::table.select(tx_items::val).first(&mut conn).unwrap();
    assert_eq!(v, "committed");
}

// Rollback

#[test]
fn transaction_rollback_discards() {
    let mut conn = mem_conn();
    let _ = conn.transaction(|conn| -> QueryResult<()> {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("discarded")))
            .execute(conn)?;
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert_eq!(count(&mut conn), 0);
}

#[test]
fn transaction_error_aborts() {
    let mut conn = mem_conn();
    let result = conn.transaction(|conn| -> QueryResult<()> {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("will_not_commit")))
            .execute(conn)?;
        // Force an error by violating the PK constraint.
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("duplicate_pk")))
            .execute(conn)?;
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(count(&mut conn), 0);
}

// Nested savepoints

#[test]
fn nested_inner_rollback_outer_commits() {
    let mut conn = mem_conn();
    conn.transaction(|outer| {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("outer")))
            .execute(outer)?;

        // Inner savepoint rolled back.
        let _ = outer.transaction(|inner| -> QueryResult<()> {
            diesel::insert_into(tx_items::table)
                .values((tx_items::id.eq(2), tx_items::val.eq("inner")))
                .execute(inner)?;
            Err(diesel::result::Error::RollbackTransaction)
        });

        // Outer must still see its own row.
        let n: i64 = tx_items::table.count().first(outer)?;
        assert_eq!(n, 1);
        QueryResult::Ok(())
    })
    .unwrap();

    // After the outer commits, only "outer" survives.
    assert_eq!(count(&mut conn), 1);
    let v: String = tx_items::table.select(tx_items::val).first(&mut conn).unwrap();
    assert_eq!(v, "outer");
}

// test_transaction

#[test]
fn test_transaction_always_rolls_back() {
    let mut conn = mem_conn();
    conn.test_transaction(|conn| -> QueryResult<()> {
        diesel::insert_into(tx_items::table)
            .values((tx_items::id.eq(1), tx_items::val.eq("ephemeral")))
            .execute(conn)?;
        let n: i64 = tx_items::table.count().first(conn)?;
        assert_eq!(n, 1);
        Ok(())
    });
    // After test_transaction the connection sees no rows.
    assert_eq!(count(&mut conn), 0);
}
