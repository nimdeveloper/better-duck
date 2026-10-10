---
name: better-duck-core
description: >-
  Rust API guide for the better-duck-core crate (the embedded DuckDB driver published as
  better-duck): opening connections, executing SQL, iterating rows and values, parameter binding,
  bulk insert, columnar chunks, transactions, pooling, async, and Arrow/Polars interop. Use when
  writing Rust against better-duck-core — Connection, execute, execute_with, execute_with_named,
  DuckResult / DuckValue, Appender, fetch_chunk, TableDescription, Database — or when choosing a
  Cargo flag feature = "chrono|decimal|arrow|polars|columnar|udf|async|pool|time|uuid" (plus
  json / parquet / icu for bundled extensions, and derive). For DuckDB SQL and engine behavior use
  the duckdb skill; for the UDF and data-derive macros use better-duck-macros.
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# better-duck-core

The driver crate: a safe Rust wrapper over DuckDB's C API (vendored via `better-duck-sys`, compiled
in — no system install). It is row-oriented by default, with opt-in features for Arrow, Polars,
columnar chunks, async, pooling, user-defined functions, and derives. On crates.io this crate is
published as **`better-duck`**; the library path is always `better_duck_core`.

This skill is the Rust-API map. For DuckDB's SQL dialect, type system, and engine behavior, use the
**`duckdb`** skill; for `#[duckdb_scalar]` / `#[derive(FromRow)]` and the other macros, use
**`better-duck-macros`**; for the ORM, **`better-duck-diesel`**. The top-level **`better-duck`**
skill routes between all of these.

## Conventions

- Everything fallible returns `better_duck_core::error::Result<T>` (alias for
  `Result<T, better_duck_core::error::Error>`). Examples here elide a `-> Result<()>` wrapper.
- A column value is a `better_duck_core::types::value::DuckValue` — a `#[non_exhaustive]` enum; match
  with a `_` arm. Rows come from a `DuckResult` as `DuckRow`.
- Bound parameters are `&mut [&mut dyn AppendAble]` (positional) or `&mut [(&str, &mut dyn AppendAble)]`
  (named). The `derive` feature's `params!` macro builds the positional slice.
- `Connection` methods that run SQL take `&mut self`; introspection (`table_description`, `table_names`,
  `client_context`) takes `&self`.

## Quickstart

```rust
use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;

fn main() -> better_duck_core::error::Result<()> {
    // In-memory connection (each `open_in_memory` is its own independent database).
    let mut conn = Connection::open_in_memory()?;

    // Run DDL / multi-statement batches; results are discarded.
    conn.execute_batch(
        "CREATE TABLE events (id INTEGER, label VARCHAR, score DOUBLE);
         INSERT INTO events VALUES (1, 'alpha', 9.5), (2, 'beta', 3.0);",
    )?;

    // Positional parameters ($1, $2, …) bound as &mut [&mut dyn AppendAble].
    let mut id = 3_i32;
    let mut label = String::from("gamma");
    let mut score = 7.25_f64;
    let changed = conn
        .execute_with(
            "INSERT INTO events VALUES ($1, $2, $3)",
            &mut [&mut id, &mut label, &mut score],
        )?
        .changes();
    assert_eq!(changed, 1);

    // Iterate rows: a DuckResult is an Iterator of Result<DuckRow>.
    // `execute` runs one statement with no params; use `execute_with` to bind.
    for row in conn.execute("SELECT id, label FROM events ORDER BY id")? {
        let row = row?;
        // Access by name (case-sensitive) or by zero-based index.
        let label = row.get_idx(1);      // Option<&DuckValue>
        match row.get("id") {            // Option<&DuckValue>
            Some(DuckValue::Int(n)) => println!("id={n} label={label:?}"),
            other => println!("unexpected: {other:?}"),
        }
    }
    Ok(())
}
```

Bulk insert instead of row-by-row `INSERT`:

```rust
use better_duck_core::types::value::DuckValue;
let mut app = conn.appender("events", "main")?;   // (table, schema)
app.append(&mut DuckValue::Int(4))?;              // one AppendAble per column, per row
// ...append the rest of the columns/rows...
app.save()?;                                       // flush (also auto-flushes on drop)
```

## API / topic map

| You want to… | Use | Reference |
|---|---|---|
| Open in-memory / on-disk; share one DB across connections; set config | `Connection::open`/`open_in_memory`/`open_with_flags`, `Database`, `Config`, `library_version` | [connections](references/connections.md) |
| Run DDL/DML batches | `Connection::execute_batch` | [queries-and-values](references/queries-and-values.md) |
| Run one statement, read rows / changed-row count | `Connection::execute` → `DuckResult` | [queries-and-values](references/queries-and-values.md) |
| Bind parameters (positional / named / homogeneous) | `execute_with`, `execute_with_named`, `insert` | [queries-and-values](references/queries-and-values.md) |
| Iterate / count / peek / materialize rows | `DuckResult` (`next`, `count`, `exists`, `materialize` → `ResultSet`), `DuckRow::get`/`get_idx` | [queries-and-values](references/queries-and-values.md) |
| The full `DuckValue` variant set + Rust mapping | `DuckValue` | [queries-and-values](references/queries-and-values.md) |
| Inspect a table's columns without a query | `Connection::table_description` → `TableDescription` | [queries-and-values](references/queries-and-values.md) |
| Bulk-insert rows fast | `Connection::appender` → `Appender` | [appender](references/appender.md) |
| Transactions (closure or RAII) | `Connection::transaction`, `begin_transaction` → `Transaction` | [transactions](references/transactions.md) |
| Zero-copy columnar read / build-and-insert chunks | `DuckResult::fetch_chunk`, `DataChunkHandle`, `VectorRef`/`VectorMut` | [columnar](references/columnar.md) |
| Async (Tokio), r2d2 pool, Arrow / Polars | `AsyncConnection`/`AsyncDatabase`/`AsyncPool`, `pool`, `query_arrow`/`query_polars` | [async-pool-arrow](references/async-pool-arrow.md) |
| Understand errors; load extensions | `error::{Error, Result}`, `install_extension`/`ensure_extension` | [errors-and-extensions](references/errors-and-extensions.md) |
| Map structs/enums, UDFs, `transaction!`/`params!` | derives & macros | **`better-duck-macros`** skill |

## Feature flags

`default = ["chrono", "decimal"]`. DuckDB is always compiled in. Pin the exact pre-release version
(Cargo's default requirement excludes `-beta.N`): `better-duck-core = "0.1.0-beta.8"`.

| Feature | Enables |
|---|---|
| `chrono` *(default)* | `chrono` ↔ DATE/TIME/TIMESTAMP[_S/_MS/_NS]/TIMESTAMPTZ/TIME_TZ/INTERVAL. Without it, native `DuckDate`/`DuckTime`/`SystemTime` types. |
| `decimal` *(default)* | `rust_decimal::Decimal` for DECIMAL |
| `json`, `parquet`, `icu` | bundle that DuckDB extension into the static lib (forwarded to `better-duck-sys`) |
| `arrow` | `query_arrow` / `register_arrow` via the Arrow C Data Interface (no added deps) |
| `polars` | `query_polars` / `register_polars` (implies `arrow`; pulls in `polars` + `polars-arrow`) |
| `columnar` | `DuckResult::fetch_chunk`, `DataChunkHandle`, `VectorRef`/`VectorMut`, `Appender::append_chunk_handle` |
| `udf` | the `#[duckdb_*]` function macros (also turns on `columnar`) |
| `derive` | `#[derive(FromRow/ToRow/DuckEnum/DuckStruct)]` + `transaction!` / `params!` |
| `async` | Tokio facade (`AsyncConnection`, `AsyncDatabase`, `AsyncPool`) |
| `pool` | `r2d2` pool over a shared `Database` |
| `time`, `uuid` | `time` crate temporals (write path) / `uuid::Uuid` interop |

> `columnar` and `udf` share the same chunk/vector machinery; `columnar` is the subset without the
> proc-macro dependency. Enabling `udf` enables `columnar` too. `AsyncPool` needs `async` + `pool`.

## Choosing an access path

- **One statement, a few rows** → `execute` + iterate `DuckRow` by name/index.
- **Need the result off-thread / `Send`** → `DuckResult::materialize()` → `ResultSet` (holds no FFI
  handles; `Send + Sync + Clone`), or an `AsyncConnection` which materializes for you.
- **Many rows in, fast** → the `Appender`, not repeated `INSERT`.
- **Many rows out, numeric, zero-copy** → `columnar` feature's `fetch_chunk` + `VectorRef::as_slice`.
- **Map a Rust struct ↔ row** → `#[derive(FromRow)]` / `#[derive(ToRow)]` (see **`better-duck-macros`**).
- **Hand data to Polars / another Arrow consumer** → `query_polars` / `query_arrow`.

Full rendered API docs: <https://docs.rs/better-duck-core>.
