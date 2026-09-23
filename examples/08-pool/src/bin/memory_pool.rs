//! An in-memory r2d2 connection pool.
//!
//! `DuckDbConnectionManager::memory()` backs the pool with a single shared in-memory
//! `Database`, so every connection checked out of the pool sees the same data. A
//! checked-out `PooledConnection` derefs to a normal `Connection`, so it is used exactly
//! like any other handle and returns to the pool when dropped.

use better_duck_core::error::Error;
use better_duck_core::pool::{DuckDbConnectionManager, Pool};
use common::{section, show, Result};

fn main() -> Result<()> {
    section("build a pool over a shared in-memory database");
    let mgr = DuckDbConnectionManager::memory()?;
    // r2d2's builder/checkout errors are their own type; map them into the core
    // error so the whole example can use `?`.
    let pool = Pool::builder().max_size(8).build(mgr).map_err(|e| Error::Pool(e.to_string()))?;

    // r2d2 eagerly fills the pool to its min-idle default (== max_size), so all eight
    // slots are established and idle before the first checkout.
    let initial = pool.state();
    show("connections", initial.connections);
    show("idle_connections", initial.idle_connections);
    assert_eq!(initial.connections, 8);
    assert_eq!(initial.idle_connections, 8);

    section("check out a connection and use it like a normal Connection");
    let mut conn = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
    conn.execute_batch("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1), (2), (3);")?;
    let rows = conn.execute("SELECT id FROM t ORDER BY id")?.count();
    show("rows via pooled connection", rows);
    assert_eq!(rows, 3);

    // While one connection is checked out, the pool reports one fewer idle slot.
    let while_held = pool.state();
    show("idle_connections while one is held", while_held.idle_connections);
    assert_eq!(while_held.idle_connections, 7);

    section("return the connection to the pool");
    drop(conn);
    let after = pool.state();
    show("idle_connections after release", after.idle_connections);
    assert_eq!(after.idle_connections, 8);

    Ok(())
}
