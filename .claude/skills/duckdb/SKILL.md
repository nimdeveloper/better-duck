---
name: duckdb
description: >-
  DuckDB engine knowledge for driving it from Rust through better-duck: the SQL dialect, the DuckDB
  type system and how each type maps to driver values, extensions (json, parquet, icu, spatial),
  and runtime behaviors (transactions, appender semantics, named/positional parameters, replacement
  scans). Use when writing SQL for DuckDB, choosing or reasoning about a DuckDB column type, loading
  an extension, or diagnosing DuckDB-specific behavior seen through the better-duck driver.
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# DuckDB (for better-duck users)

DuckDB is an in-process OLAP SQL engine. `better-duck` embeds it and talks to its C API. This skill
covers the engine behaviors that matter when you drive it from Rust; for the Rust API itself see the
`better-duck-core` skill.

## SQL dialect — the parts that surprise people

- **PostgreSQL-like**, with analytics extras: `QUALIFY`, `GROUP BY ALL`, `SELECT * EXCLUDE (col)`,
  `SELECT * REPLACE (expr AS col)`, `COLUMNS(...)`, list/struct/map literals, `UNNEST`, window
  functions, `PIVOT`/`UNPIVOT`.
- **Friendly casting**: `CAST(x AS t)` or `x::t`. `TRY_CAST` yields NULL instead of erroring.
- **Parameters**: positional `$1, $2, …` or `?`; named `$name`. better-duck binds positional with
  `execute_with` and named with `execute_with_named`.
- **Statements**: `execute_batch` runs multiple `;`-separated statements; a single `execute`
  prepares one statement. DDL and `INSERT`/`UPDATE`/`DELETE` report a changed-row count; `SELECT`
  and `RETURNING` yield rows.
- **Replacement scans** let a bare identifier in `FROM` resolve to a registered source (e.g. an
  Arrow/Polars view, or a Rust replacement scan) — how `query_polars`/`register_arrow` surface data.

## Type system → driver values

DuckDB values arrive as `better_duck_core::types::value::DuckValue`. Core mappings:

| DuckDB type | DuckValue / Rust | Notes |
|---|---|---|
| BOOLEAN | `Boolean` / `bool` | |
| TINYINT…BIGINT | `TinyInt`/`SmallInt`/`Int`/`BigInt` (`i8`..`i64`) | |
| UTINYINT…UBIGINT | `UTinyInt`..`UBigInt` (`u8`..`u64`) | |
| HUGEINT / UHUGEINT | `HugeInt`(`i128`) / `UHugeInt`(`u128`) | 128-bit via DuckDB hugeint |
| FLOAT / DOUBLE | `Float`(`f32`) / `Double`(`f64`) | |
| DECIMAL(w,s) | `rust_decimal::Decimal` | feature `decimal`; preserves width/scale |
| VARCHAR | `Text(String)` | |
| BLOB / BIT | `Blob` / bit types | |
| DATE/TIME/TIMESTAMP[TZ]/INTERVAL | `chrono` types | feature `chrono` (or `time` feature) |
| UUID | `uuid::Uuid` | feature `uuid` |
| LIST / ARRAY / STRUCT / MAP / UNION / ENUM | nested `DuckValue` / derives | see `better-duck-macros` for `DuckEnum`/`DuckStruct` |
| NULL | `DuckValue::Null` | validity tracked per row/vector |

`TableDescription` (core) reads a table's column names, types, and `DEFAULT` flags without a query.
The lossless `TypeInfo` descriptor preserves DECIMAL width/scale, ENUM dictionaries, and nesting.

## NULL and validity

NULL is first-class: a value is `DuckValue::Null`, and in columnar access a column carries a validity
bitmask (see `better-duck-core`'s `columnar` skill — `VectorRef::validity`, `VectorMut::set_null` /
`set_row_valid`). `TRY_CAST` and `Option<T>` parameters/returns propagate NULL explicitly.

## Extensions

- Bundled at build time via core features: `json`, `parquet`, `icu` (timezones/collations). These
  compile the extension into the static library — no runtime `INSTALL`/`LOAD`.
- Loaded at runtime via core's extension API (`INSTALL`/`LOAD`), e.g. **spatial** for geometry.
- Writing your own loadable extension in Rust: `#[duckdb_entrypoint]` (feature `loadable-extension`,
  experimental) — see `better-duck-macros`.

## Transactions

DuckDB has no dedicated C-API transaction entry point; better-duck drives `BEGIN`/`COMMIT`/`ROLLBACK`
via SQL. Savepoints (`SAVEPOINT`/`RELEASE`/`ROLLBACK TO`) are **not** supported. Use core's
`Connection::transaction` (closure, commits on `Ok`, rolls back on `Err`/panic) or `begin_transaction`
(RAII guard). See `better-duck-core`.

## Appender semantics

The Appender is the fast bulk-insert path (`duckdb_appender_*`). It buffers rows and flushes on
`save()`/`finish()`; a failed append/flush invalidates the appender (then only error + destroy are
legal). Column defaults can be filled with `append_default_row` / `set_chunk_default`. Details in the
`better-duck-core` skill's appender/columnar references.

## Diagnosing engine errors

better-duck surfaces DuckDB's typed error classification as `Error::Engine(EngineError { kind, message })`,
where `kind` mirrors `duckdb_error_type`. A failed `prepare`/`query` carries DuckDB's own message.
When a query "returns results" from `execute` where you expected a changed-row count (or vice versa),
check the statement type — `SELECT`/`RETURNING` vs DML.

## Versioning

DuckDB ships an every-other-version LTS cadence from 1.4 (1.4 LTS, 1.5 current, …). The C API is
specified in `api_spec/` YAML upstream with per-function `lifecycle` (stable/deprecated/unstable) +
since-version. better-duck pins a vendored DuckDB in `better-duck-sys`.
