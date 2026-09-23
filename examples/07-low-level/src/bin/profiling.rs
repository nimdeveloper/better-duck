//! Reading a query's profiling tree with `profiling_info`.
//!
//! Enable profiling with `PRAGMA enable_profiling = 'no_output'`, run a query, then call
//! `conn.profiling_info()` for an owned [`ProfilingNode`] tree that survives later queries
//! and the connection itself. Each node exposes `metrics()` (name -> value), `metric(key)`
//! for one lookup, and `children()`. Prefer `profiling_info().metric(...)` over
//! `conn.profiling_metric(key)`: the latter aborts the process on an unknown key.

use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("profiling is off by default");
    conn.execute_batch("SELECT 42")?;
    show("profiling_info (disabled)", conn.profiling_info().is_some());
    assert!(conn.profiling_info().is_none(), "no profiling tree until it is enabled");

    section("enable profiling and run a query");
    conn.execute_batch("PRAGMA enable_profiling = 'no_output'")?;
    conn.execute_batch("CREATE TABLE t AS SELECT * FROM range(100) tbl(i)")?;
    let _ = conn.execute("SELECT count(*) FROM t WHERE i % 2 = 0")?.count();

    let tree = conn.profiling_info().expect("a profiling tree once enabled");
    show("root metric count", tree.metrics().len());
    show("root child count", tree.children().len());
    // The tree is a real hierarchy: the root records metrics and/or has children.
    assert!(!tree.metrics().is_empty() || !tree.children().is_empty());

    section("look up a single metric on the root node");
    // QUERY_NAME is always present once profiling is on; it holds the executed SQL.
    show("metric(QUERY_NAME)", tree.metric("QUERY_NAME"));
    assert_eq!(tree.metric("QUERY_NAME"), Some("SELECT count(*) FROM t WHERE i % 2 = 0"));
    // An unknown key on the owned tree is simply absent (no abort — unlike
    // conn.profiling_metric on an unknown key).
    show("metric(NOT_A_METRIC)", tree.metric("NOT_A_METRIC"));
    assert!(tree.metric("NOT_A_METRIC").is_none());

    section("the materialised tree outlives later queries");
    let _ = conn.execute("SELECT 99")?.count();
    // Still readable: the owned copy is unaffected by the borrowed pointer's invalidation.
    show("root metric count (after another query)", tree.metrics().len());
    assert!(!tree.metrics().is_empty() || !tree.children().is_empty());

    Ok(())
}
