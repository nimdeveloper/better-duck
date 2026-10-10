---
name: better-duck
description: >-
  Overview and task router for the better-duck DuckDB driver for Rust. Use when working with the
  better-duck / better-duck-core / better-duck-macros / better-duck-diesel / better-duck-tauri
  crates, when choosing a Cargo feature flag, or when deciding which API or crate fits a task
  (opening a connection, querying, parameter binding, bulk insert, columnar access, user-defined
  functions, Diesel ORM, or a Tauri plugin). Routes to the per-crate skills for depth.
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# better-duck

`better-duck` is a Rust driver for [DuckDB](https://duckdb.org) built directly on DuckDB's C API via
FFI. It favors a lean dependency tree and row-oriented access, with opt-in features for Arrow, Polars,
columnar chunks, async, pooling, user-defined functions, derives, a Diesel ORM backend, and a Tauri
plugin. DuckDB is compiled and embedded by default (no system install).

This skill is the map. Jump to the focused skill for real work:

| You are… | Skill |
|---|---|
| Writing SQL / reasoning about DuckDB types, dialect, extensions | **`duckdb`** |
| Using the driver's Rust API (connect, query, types, appender, pool, async, arrow, columnar) | **`better-duck-core`** |
| Writing UDFs or deriving row/struct/enum mappings | **`better-duck-macros`** |
| Using the Diesel ORM backend | **`better-duck-diesel`** |
| Exposing DuckDB to a Tauri v2 webview | **`better-duck-tauri`** |

## Crate map

| Crate | Purpose |
|---|---|
| `better-duck-core` | The driver: connections, queries, types, transactions, appender, pool, async, arrow/polars, columnar, extensions, UDF runtime. |
| `better-duck-macros` | Proc macros powering UDFs (`#[duckdb_scalar]`, …) and derives (`FromRow`, …). Pulled in via core's `udf`/`derive` features — not depended on directly. |
| `better-duck-diesel` | Diesel 2.3 backend (`DuckDbConnection`), DuckDB SQL DSL, type mapping, UDF calls through the DSL. |
| `better-duck-tauri` | Tauri v2 plugin exposing DuckDB to the webview (`core` or `diesel` backend). |
| `better-duck-sys` | Low-level FFI + vendored DuckDB. Rarely used directly. |

> The crate published as `better-duck` on crates.io is `better-duck-core`.

## Feature flags (core)

`default = ["chrono", "decimal"]`. Pre-release versions are excluded by Cargo's default version
match — pin the exact version (e.g. `better-duck-core = "0.1.0-beta.8"`).

| Feature | What it enables |
|---|---|
| `chrono` *(default)* | `chrono` date/time ↔ DATE/TIME/TIMESTAMP/TIMESTAMPTZ/INTERVAL |
| `decimal` *(default)* | `rust_decimal::Decimal` ↔ DECIMAL |
| `json`, `parquet`, `icu` | bundle the matching DuckDB extension into the static lib |
| `arrow` | export results via the Arrow C Data Interface (no added deps) |
| `columnar` | columnar data-chunk read/write: `fetch_chunk`, zero-copy `as_slice`, build-and-bulk-insert (also enabled by `udf`) |
| `udf` | `#[duckdb_scalar]` / `#[duckdb_table_function]` / `#[duckdb_aggregate]` / `#[duckdb_cast]` |
| `derive` | `#[derive(FromRow/ToRow/DuckEnum/DuckStruct)]` + `transaction!` / `params!` |
| `async` | Tokio facade (`AsyncConnection`, `AsyncDatabase`, `AsyncPool`) |
| `pool` | `r2d2` connection pool over a shared `Database` |
| `time`, `uuid` | `time` crate temporals / `uuid::Uuid` interop |

## 30-second quickstart

```toml
[dependencies]
better-duck-core = "0.1.0-beta.8"
```

```rust
use better_duck_core::connection::Connection;

fn main() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)")?;
    conn.execute_with("INSERT INTO t VALUES ($1, $2)", &mut [&mut 1_i32, &mut "ada"])?;

    let mut rows = conn.execute("SELECT id, name FROM t")?;
    let row = rows.next().unwrap()?;
    assert_eq!(row.get("name"), Some(&better_duck_core::types::value::DuckValue::text("ada")));
    Ok(())
}
```

## Picking the right tool

- **One value per row** in a `SELECT`/`WHERE` → scalar UDF (`#[duckdb_scalar]`, see `better-duck-macros`).
- **Rows from a `FROM`** → table UDF (`#[duckdb_table_function]`).
- **Bulk insert** → the `Appender` (`better-duck-core`), not row-by-row `INSERT`.
- **Read a big result fast / zero-copy columns** → the `columnar` feature's `fetch_chunk` + `as_slice`.
- **ORM / typed query builder** → `better-duck-diesel`.
- **Desktop app (webview) DB** → `better-duck-tauri`.
- **Map a Rust struct to a row** → `#[derive(FromRow)]` / `#[derive(ToRow)]` (`better-duck-macros`).

## Project conventions for these skills

- Error type is `better_duck_core::error::{Error, Result}`; most calls return `Result<_>`.
- Values are `better_duck_core::types::value::DuckValue`; rows are iterated from a `DuckResult`.
- Prefer the safe wrappers (`Connection`, `Appender`, UDF macros) over `better-duck-sys` FFI.
