#![allow(missing_docs)]
//! Error mapping tests: PK violations, NotFound, deserialization errors.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*, result::Error};

diesel::table! {
    err_items (id) {
        id  -> Integer,
        val -> Text,
    }
}

fn mem_conn() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute("CREATE TABLE err_items (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)")
        .expect("create table");
    c
}

// PK / constraint violation → DatabaseError

#[test]
fn pk_violation_returns_database_error() {
    let mut conn = mem_conn();
    diesel::insert_into(err_items::table)
        .values((err_items::id.eq(1), err_items::val.eq("first")))
        .execute(&mut conn)
        .unwrap();
    // Inserting the same PK a second time must produce DatabaseError.
    let result = diesel::insert_into(err_items::table)
        .values((err_items::id.eq(1), err_items::val.eq("duplicate")))
        .execute(&mut conn);
    assert!(
        matches!(result, Err(Error::DatabaseError(_, _))),
        "expected DatabaseError, got {result:?}"
    );
}

// NotFound when result set is empty

#[test]
fn first_on_empty_returns_not_found() {
    let mut conn = mem_conn();
    let result: Result<i32, _> =
        err_items::table.filter(err_items::id.eq(9999)).select(err_items::id).first(&mut conn);
    assert!(matches!(result, Err(Error::NotFound)), "expected NotFound, got {result:?}");
}

// DeserializationError on type mismatch

/// Use `QueryableByName` with a mismatched SQL type annotation so the mismatch
/// is detected at **run time** (when our `FromSql` impl sees the wrong variant)
/// rather than compile time.
#[test]
fn type_mismatch_returns_deserialization_error() {
    // Struct claims the `val` column is `Integer`, but the table has it as `VARCHAR`.
    // Our `FromSql<Integer, DuckDb>` returns Err when it receives a Text value.
    #[derive(diesel::QueryableByName, Debug)]
    struct MismatchRow {
        #[allow(dead_code)] // only used to trigger deserialization; we only inspect the Err path
        #[diesel(sql_type = diesel::sql_types::Integer)]
        val: i32,
    }

    let mut conn = mem_conn();
    conn.batch_execute("INSERT INTO err_items VALUES (1, 'hello')").unwrap();
    let result: Result<MismatchRow, _> =
        diesel::sql_query("SELECT val FROM err_items LIMIT 1").get_result(&mut conn);
    assert!(
        matches!(result, Err(Error::DeserializationError(_)) | Err(Error::DatabaseError(_, _))),
        "expected deserialization/database error, got {result:?}"
    );
}

// RollbackTransaction propagates cleanly

#[test]
fn rollback_error_is_recoverable() {
    let mut conn = mem_conn();
    // A RollbackTransaction error inside a transaction must not poison the connection.
    let result = conn.transaction(|_| -> QueryResult<()> { Err(Error::RollbackTransaction) });
    assert!(result.is_err());
    // Connection should still be usable afterwards.
    let count: i64 = err_items::table.count().first(&mut conn).unwrap();
    assert_eq!(count, 0);
}
