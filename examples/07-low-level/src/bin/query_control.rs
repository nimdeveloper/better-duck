//! Observing and interrupting a query with `QueryControl`.
//!
//! `conn.query_control()` mints a `QueryControl` — a `Clone + Send + Sync` handle that can
//! `interrupt()` or read `progress()` of the query running on the connection, safely from
//! another thread. It is *generation-scoped*: it is minted for the current query, so once
//! that query finishes it goes stale — `is_active()` turns false, `progress()` returns
//! `None`, and `interrupt()` becomes a no-op — and cannot affect a later, unrelated query.

use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t AS SELECT * FROM range(1000) tbl(i)")?;

    section("a freshly minted control is active and can be signalled");
    let control = conn.query_control();
    show("is_active (before any query)", control.is_active());
    show("progress (before any query)", control.progress());
    assert!(control.is_active(), "freshly minted control is active");
    assert!(control.progress().is_some(), "an active control reports a progress snapshot");
    // interrupt() signals DuckDB while the control's generation is current.
    show("interrupt (active)", control.interrupt());
    assert!(control.interrupt(), "an active control signals the interrupt");
    // Run a benign query so DuckDB clears the interrupt flag; the result is irrelevant.
    let _ = conn.execute("SELECT 1");

    section("a control goes stale once its query completes");
    let control = conn.query_control();
    assert!(control.is_active());
    // Running (and fully consuming) a query advances the connection's generation.
    let counted = conn.execute("SELECT count(*) FROM t")?.count();
    show("rows produced by the query", counted);

    show("is_active (after the query)", control.is_active());
    show("progress (after the query)", control.progress());
    show("interrupt (stale)", control.interrupt());
    assert!(!control.is_active(), "control is stale once its query finished");
    assert!(control.progress().is_none(), "a stale control reports no progress");
    assert!(!control.interrupt(), "a stale interrupt is a no-op");

    Ok(())
}
