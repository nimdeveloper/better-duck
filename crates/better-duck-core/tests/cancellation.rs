#![allow(missing_docs)]
//! Integration tests for query cancellation, progress reporting, and typed
//! error classification (`InterruptHandle`, `QueryProgress`,
//! `Error::error_type`, `DuckResult::statement_type`/`return_type`).

use std::{thread, time::Duration};

use better_duck_core::{
    connection::Connection,
    error::{DuckDbErrorType, Error},
    ResultType, StatementType,
};

fn conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

fn assert_send_sync<T: Send + Sync>() {}

// 1. Interrupt a long query from a second thread -> the query returns Err.
//
// A successfully interrupted connection is left permanently unable to run
// further queries — this is DuckDB's own `duckdb_interrupt` behavior, not a
// bug in this crate (see `InterruptHandle`'s docs). So this test asserts
// what actually matters for correctness: the interrupted query returns a
// clean `Err` (not a crash or hang), and the connection stays in that clean
// "permanently erroring" state afterward rather than corrupting memory —
// while a *separate* connection to the same database is unaffected.
#[test]
fn interrupt_cancels_a_long_running_query_from_another_thread() {
    let mut c = conn();
    let handle = c.interrupt_handle();
    let mut unaffected = c.try_clone().unwrap();

    let interrupter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        handle.interrupt();
    });

    let result = c.execute("SELECT count(*) FROM range(50000000000)");
    assert!(result.is_err(), "an interrupted query must return Err");

    interrupter.join().unwrap();

    // The interrupted connection is spent: further queries fail cleanly.
    assert!(c.execute("SELECT 1 AS v").is_err());

    // A separate connection to the same database is untouched.
    let mut result = unaffected.execute("SELECT 1 AS v").unwrap();
    let row = result.next().unwrap().unwrap();
    assert_eq!(row.get("v"), Some(&better_duck_core::types::value::DuckValue::Int(1)));
}

// 2. Interrupting an idle connection is a harmless no-op.
#[test]
fn interrupt_on_an_idle_connection_is_a_no_op() {
    let mut c = conn();
    let handle = c.interrupt_handle();
    handle.interrupt();
    handle.interrupt();

    // The connection is still fully usable.
    let mut result = c.execute("SELECT 1 AS v").unwrap();
    assert!(result.next().is_some());
}

// 3. InterruptHandle outliving the Connection it came from is still safe (no
// use-after-free): drop the Connection first, then call interrupt().
#[test]
fn interrupt_handle_outliving_its_connection_is_safe() {
    let c = conn();
    let handle = c.interrupt_handle();
    drop(c);

    // Must not crash / UB, and must be a no-op.
    handle.interrupt();
    assert_eq!(handle.progress(), None);
}

// 4. Handle is Send + Sync (static assertion).
#[test]
fn interrupt_handle_is_send_and_sync() {
    assert_send_sync::<better_duck_core::interrupt::InterruptHandle>();
}

// 5. query_progress() returns a well-formed struct; percentage is None or
// within 0.0..=100.0 — deliberately *not* asserting monotonic progress,
// which would be timing-flaky.
#[test]
fn query_progress_on_an_idle_connection_is_well_formed() {
    let c = conn();
    let progress = c.query_progress();
    if let Some(pct) = progress.percentage {
        assert!((0.0..=100.0).contains(&pct), "percentage {pct} out of range");
    }
}

// 6. Error classification: NOT NULL violation -> constraint type; malformed
// SQL -> parser type; unknown table -> catalog type. Three distinct
// DuckDbErrorType values, asserted by variant, not by message text.
#[test]
fn error_classification_distinguishes_constraint_parser_and_catalog_errors() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER NOT NULL)").unwrap();

    let constraint_err = c.execute_batch("INSERT INTO t (v) VALUES (NULL)").unwrap_err();
    assert_eq!(constraint_err.error_type(), Some(DuckDbErrorType::Constraint));

    let parser_err = c.execute_batch("SELECT SELECT FROM").unwrap_err();
    assert_eq!(parser_err.error_type(), Some(DuckDbErrorType::Parser));

    let catalog_err = c.execute_batch("SELECT * FROM no_such_table").unwrap_err();
    assert_eq!(catalog_err.error_type(), Some(DuckDbErrorType::Catalog));

    let types = [constraint_err.error_type(), parser_err.error_type(), catalog_err.error_type()];
    assert_eq!(
        types.iter().collect::<std::collections::HashSet<_>>().len(),
        3,
        "all three error types must be distinct"
    );
}

// 7. error_type() is None for non-DuckDB errors (e.g. Error::NulError).
#[test]
fn error_type_is_none_for_a_non_duckdb_error() {
    let err = Error::from(std::ffi::CString::new("a\0b").unwrap_err());
    assert_eq!(err.error_type(), None);
}

// 8. statement_type() for SELECT / INSERT / UPDATE / DELETE / CREATE.
#[test]
fn statement_type_identifies_each_statement_kind() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    c.execute_batch("INSERT INTO t VALUES (1)").unwrap();

    let mut r = c.execute("SELECT v FROM t").unwrap();
    assert_eq!(r.statement_type(), StatementType::Select);

    let mut r = c.execute("INSERT INTO t VALUES (2)").unwrap();
    assert_eq!(r.statement_type(), StatementType::Insert);

    let mut r = c.execute("UPDATE t SET v = 3 WHERE v = 1").unwrap();
    assert_eq!(r.statement_type(), StatementType::Update);

    let mut r = c.execute("DELETE FROM t WHERE v = 3").unwrap();
    assert_eq!(r.statement_type(), StatementType::Delete);

    let mut r = c.execute("CREATE TABLE t2 (v INTEGER)").unwrap();
    assert_eq!(r.statement_type(), StatementType::Create);
}

// return_type() for a SELECT (query result) vs. an INSERT (changed rows).
#[test]
fn return_type_distinguishes_query_result_from_changed_rows() {
    let mut c = conn();
    c.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();

    let mut r = c.execute("INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(r.return_type(), ResultType::ChangedRows);

    let mut r = c.execute("SELECT v FROM t").unwrap();
    assert_eq!(r.return_type(), ResultType::QueryResult);
}

// 9. #[tokio::test]: AsyncConnection::interrupt_handle() cancels an in-flight
// execute().await, proving cancellation works while the mutex is held. As in
// the sync test above, the connection is left permanently erroring (not
// crashing) afterward — that's DuckDB's own behavior, not a bug here.
#[cfg(feature = "async")]
#[tokio::test]
async fn async_connection_interrupt_handle_cancels_an_in_flight_query() {
    use better_duck_core::AsyncConnection;

    let conn = AsyncConnection::open_in_memory().await.unwrap();
    let handle = conn.interrupt_handle();

    let interrupter = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.interrupt();
    });

    let result = conn.execute("SELECT count(*) FROM range(50000000000)").await;
    assert!(result.is_err(), "an interrupted async query must return Err");

    interrupter.await.unwrap();

    // The connection is spent: further queries fail cleanly, not crash.
    assert!(conn.execute("SELECT 1 AS v").await.is_err());
}

// 10. Error type survives materialization into ResultSet and out of the
// async facade.
#[cfg(feature = "async")]
#[tokio::test]
async fn error_type_survives_the_async_facade_boundary() {
    use better_duck_core::AsyncConnection;

    let conn = AsyncConnection::open_in_memory().await.unwrap();
    conn.execute_batch("CREATE TABLE t (v INTEGER NOT NULL)").await.unwrap();

    let err = conn.execute_batch("INSERT INTO t (v) VALUES (NULL)").await.unwrap_err();
    assert_eq!(err.error_type(), Some(DuckDbErrorType::Constraint));
}
