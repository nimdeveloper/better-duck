# Async, Pooling & Arrow/Polars

Three independent, opt-in integration surfaces.

## Contents

- [Async (feature `async`)](#async-feature-async)
- [Pooling (feature `pool`)](#pooling-feature-pool)
- [Async pool (`async` + `pool`)](#async-pool-async--pool)
- [Arrow (feature `arrow`)](#arrow-feature-arrow)
- [Polars (feature `polars`)](#polars-feature-polars)

## Async (feature `async`)

A Tokio facade: every call dispatches to `tokio::task::spawn_blocking`, so DuckDB never blocks the
async executor. Types re-export at the crate root. **Results are materialized** — the async methods
return a `ResultSet` (owned, `Send`), not a borrowing `DuckResult`.

```rust
use better_duck_core::AsyncConnection;

let conn = AsyncConnection::open_in_memory().await?;   // or ::open(path).await?
conn.execute_batch("CREATE TABLE t (id INTEGER)").await?;
let set = conn.execute("SELECT id FROM t").await?;      // ResultSet

// Parameters are passed by value as Vec<DuckValue> (not &mut dyn, which isn't Send).
use better_duck_core::types::value::DuckValue;
let set = conn
    .execute_with("SELECT id FROM t WHERE id = $1", vec![DuckValue::Int(2)])
    .await?;
```

`AsyncConnection` is `Clone + Send + Sync`; clones share one connection serialized by an internal
mutex. Key methods:

- `with_connection(|&mut Connection| -> Result<T>)` — the primitive; run anything (e.g. a
  transaction) on the blocking thread. Everything else is built on it.
- `execute_batch`, `execute`, `execute_with` — as above.
- `execute_pending(sql)` — steps the query one DuckDB task per dispatch, yielding to the runtime
  between tasks so a long query neither monopolizes a blocking thread nor blocks cancellation.
- `with_appender(table, schema, |&mut Appender| …)` — the appender holds raw FFI pointers, so it
  never leaves the closure.
- `AsyncConnection::new(conn)` wraps an existing `Connection`; `try_into_inner()` recovers it if this
  is the last clone.

**Cancellation (cancel-on-drop).** The query futures request interruption of the native query if the
future is dropped. Caveat: `spawn_blocking` tasks cannot be aborted, so the drop *signals* DuckDB to
interrupt and lets the blocking task wind down — it does not stop native work instantly. The
connection becomes usable again once that task releases the mutex.

- `query_control() -> QueryControl` / `interrupt() -> bool` / `progress() -> Option<QueryProgress>`
  go through a **mutex-free** control source, so they work *while* a query holds the connection mutex
  on a blocking thread. The control is generation-scoped (a stale one from a finished query no-ops).

`AsyncDatabase` mirrors `Database` for sharing one database across async connections:
`AsyncDatabase::open_in_memory().await?`, then `db.connect().await?` → an `AsyncConnection`;
`from_database(db)` / `database()` bridge to/from a sync `Database`. These methods panic if called
outside a Tokio runtime (matching `spawn_blocking`).

## Pooling (feature `pool`)

An `r2d2` pool whose connections all share **one** `Database` (so an in-memory pool observes one
consistent database, not one per slot).

```rust
use better_duck_core::pool::{DuckDbConnectionManager, Pool};

let manager = DuckDbConnectionManager::memory()?;        // shared in-memory DB
let pool: Pool = Pool::builder().max_size(8).build(manager)?;

let mut conn = pool.get()?;                              // PooledConnection, derefs to Connection
conn.execute_batch("CREATE TABLE t (id INTEGER)")?;
```

Managers: `memory()`, `memory_with_flags(Config)`, `file(path)`, `file_with_flags(path, Config)`,
`new(Database)` (wrap an existing one). `database()` returns the shared `Database`.
Re-exports: `Pool`, `PooledConnection`, `PoolBuilder`, `PoolError`, `PoolState`
(all under `better_duck_core::pool`). The manager is `Clone + Send + Sync`.

## Async pool (`async` + `pool`)

`AsyncPool` wraps a `Pool`. `with` is the primitive — it checks a connection out, runs a closure on
a blocking thread, and releases the checkout when the closure returns. This is **deliberately** the
only way to touch a pooled connection: holding a pooled-connection guard across an `.await` would pin
a pool slot for the whole suspension (a classic async-pool deadlock), and a transaction must not span
an `.await`.

```rust
use better_duck_core::AsyncPool;
use better_duck_core::pool::{DuckDbConnectionManager, Pool};

let pool = Pool::builder().max_size(4).build(DuckDbConnectionManager::memory()?)?;
let pool = AsyncPool::new(pool);

pool.execute_batch("CREATE TABLE t (id INTEGER)").await?;
let set = pool.execute("SELECT count(*) AS c FROM t").await?;

// Run a transaction correctly — entirely inside the closure, no await inside:
pool.with(|conn| {
    conn.execute_batch("BEGIN")?;
    conn.execute_batch("INSERT INTO t VALUES (1)")?;
    conn.execute_batch("COMMIT")?;
    Ok(())
}).await?;
```

Also: `pool()`, `state() -> PoolState`, and `execute_with(sql, Vec<DuckValue>)`.

## Arrow (feature `arrow`)

Export/import via the **Arrow C Data Interface** with **no Arrow library pulled in** — the crate
defines the ABI structs directly. On `Connection`:

- `query_arrow(sql, binds) -> Result<ArrowResult>` — run SQL and export the result. `binds` is the
  same `&mut [&mut dyn AppendAble]` as `execute_with`.
- `register_arrow(view_name, stream)` (`unsafe`) — register a caller-owned
  `ArrowArrayStream` pointer as a queryable DuckDB view; returns an `ArrowView` guard that drops the
  view when it falls out of scope. **Safety:** the stream must stay valid and untouched until the
  guard drops.

```rust
let arrow = conn.query_arrow("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4", &mut [])?;
let cols = arrow.schema().n_children();     // 2
let rows = arrow.row_count();               // 2
```

`ArrowResult` yields `schema() -> &ArrowSchemaHandle`, `arrays() -> &[ArrowArrayHandle]`,
`row_count()`, and `into_parts()`. A consumer with `arrow-rs` reinterprets the handles' `as_ptr()` /
`as_mut_ptr()` (or `export_to`) as `FFI_ArrowSchema` / `FFI_ArrowArray` — identical layout. Handles
call their `release` callback on drop.

## Polars (feature `polars`)

Implies `arrow`; bridges via the Arrow C Data Interface (`polars` + `polars-arrow`, no `arrow-rs`).
On `Connection`:

- `query_polars(sql, binds) -> Result<polars::prelude::DataFrame>` — run SQL and collect the result
  into a `DataFrame`.
- `register_polars(name, &DataFrame) -> Result<()>` — materialize a `DataFrame` into DuckDB as a
  real table named `name` (`CREATE OR REPLACE TABLE … AS SELECT`), so the `DataFrame` need not
  outlive the call.

```rust
let df = conn.query_polars("SELECT id, name FROM t ORDER BY id", &mut [])?;
conn.register_polars("dst", &df)?;         // round-trip back into DuckDB
```
