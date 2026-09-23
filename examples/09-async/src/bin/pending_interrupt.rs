//! Incremental execution with `execute_pending`, plus query control.
//!
//! `execute_pending` steps a query one DuckDB task per blocking dispatch, yielding to the
//! runtime between tasks — so a long-running query neither monopolises a blocking thread
//! nor blocks cancellation. It returns the same owned `ResultSet` as `execute`.
//!
//! `query_control` / `progress` / `interrupt` read and steer the running query through a
//! mutex-free control source, so they work even while a query holds the connection.

use better_duck_core::types::value::DuckValue;
use better_duck_core::AsyncConnection;
use common::{section, show, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let conn = AsyncConnection::open_in_memory().await?;

    section("query control is a mutex-free handle to the running query");
    // A fresh control targets the next query to run, so it is active before anything
    // runs. `progress()` reads through such a control; with no query in flight DuckDB
    // reports a sentinel progress (percentage -1) rather than nothing.
    let control = conn.query_control();
    assert!(control.is_active(), "a fresh control targets the next query");
    show("progress before any query", conn.progress());

    section("execute_pending: step a longer aggregate to completion");
    // Counting a large range is enough work to exercise incremental stepping while
    // staying fast and deterministic.
    let set = conn.execute_pending("SELECT count(*) AS n FROM range(200000)").await?;
    match set.first().and_then(|r| r.get("n")) {
        Some(DuckValue::BigInt(n)) => {
            show("count over range(200000)", n);
            assert_eq!(*n, 200000);
        },
        other => panic!("expected BigInt(200000), got {other:?}"),
    }

    section("interrupting when no query runs is a safe no-op");
    // With nothing in flight, an interrupt request signals DuckDB harmlessly.
    show("interrupt() while idle", conn.interrupt());

    section("execute_pending matches one-shot execute");
    let one_shot = conn.execute("SELECT count(*) AS n FROM range(200000)").await?;
    assert_eq!(
        one_shot.first().and_then(|r| r.get("n")),
        set.first().and_then(|r| r.get("n")),
        "incremental stepping yields the same result as execute()",
    );
    show("results agree", true);

    Ok(())
}
