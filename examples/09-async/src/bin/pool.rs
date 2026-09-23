//! The async pool facade: `AsyncPool`.
//!
//! `AsyncPool` wraps a core r2d2 `Pool` and runs each operation on a blocking thread, so
//! a pooled connection is checked out, used, and released without ever being held across
//! an `.await` point. `with` is the primitive (ideal for transactions); `execute` /
//! `execute_batch` / `execute_with` are typed conveniences over it.

use better_duck_core::error::Error;
use better_duck_core::pool::{DuckDbConnectionManager, Pool};
use better_duck_core::types::value::DuckValue;
use better_duck_core::AsyncPool;
use common::{section, show, Result};

#[tokio::main]
async fn main() -> Result<()> {
    section("wrap a core Pool in an AsyncPool");
    let mgr = DuckDbConnectionManager::memory()?;
    let core_pool =
        Pool::builder().max_size(4).build(mgr).map_err(|e| Error::Pool(e.to_string()))?;
    let pool = AsyncPool::new(core_pool);

    section("execute_batch: set up a table");
    pool.execute_batch("CREATE TABLE t (id INTEGER)").await?;

    section("with: run a transaction on a checked-out connection");
    // A transaction must not span an `.await`; `with` keeps the whole unit on one
    // blocking dispatch, so BEGIN/COMMIT stay on a single pooled connection.
    pool.with(|conn| {
        conn.execute_batch("BEGIN")?;
        conn.execute_batch("INSERT INTO t VALUES (1), (2), (3)")?;
        conn.execute_batch("COMMIT")?;
        Ok(())
    })
    .await?;

    section("execute_with: parameterized insert");
    pool.execute_with("INSERT INTO t VALUES ($1)", vec![DuckValue::Int(4)]).await?;

    section("execute: read the result back");
    let counted = pool.execute("SELECT count(*) AS c FROM t").await?;
    match counted.first().and_then(|r| r.get("c")) {
        Some(DuckValue::BigInt(n)) => {
            show("row count", n);
            assert_eq!(*n, 4);
        },
        other => panic!("expected BigInt count, got {other:?}"),
    }

    section("state: inspect the pool");
    let state = pool.state().await;
    show("connections", state.connections);
    show("idle_connections", state.idle_connections);
    assert_eq!(state.connections, 4);

    Ok(())
}
