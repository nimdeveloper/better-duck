# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0-beta.3] — 2026-07-29

### `better-duck-core`

#### Added

**New types**
- `DuckUuid`, `DuckBit`, `DuckBignum` — new `DuckValue`/`DuckValueRef` variants and `Type`
  variants. UUID reads via the packed `duckdb_uhugeint` layout (no `duckdb_value` round-trip,
  mirroring HUGEINT); BIT/BIGNUM read via the `duckdb_string_t` layout, mirroring BLOB/VARCHAR.
  `GEOMETRY`/`VARIANT`/`ANY`/`INTEGER_LITERAL` remain unsupported — the DuckDB C API has no value
  accessor for them — and continue to panic on read.

**Shared database & pooling**
- `Database` — a shared, cloneable handle to an open DuckDB database; `Database::connect()`
  spawns connections that observe the same data, including for `:memory:` databases (unlike
  `Connection::open_in_memory()`, which gives each call an independent database).
- `Connection::try_clone()` / `Connection::database()` — open a second connection to the same
  database, or recover a shareable `Database` handle from an existing `Connection`.
- **`pool` feature** — `DuckDbConnectionManager` + `Pool`, an `r2d2` connection pool built on
  `Database`, so pooled connections share one database instead of one-per-slot.

**Async**
- **`async` feature** — `AsyncConnection`, `AsyncDatabase`, and (with `pool` also enabled)
  `AsyncPool`: a tokio-only facade over `spawn_blocking`. Bulk inserts are exposed via a scoped
  `with_appender` closure rather than a standalone handle, since `Appender` is `!Send`.
- `ResultSet` — an owned, `Send + Sync + Clone` materialized query result
  (`DuckResult::materialize()`), used by the async and pool facades to cross thread boundaries.

**User-defined functions**
- **`udf` feature** — `#[duckdb_scalar]` and `#[duckdb_table_function]` attribute macros register
  a plain Rust function as a DuckDB scalar or table function, with parameter/return types inferred
  from the Rust signature via `DuckLogicalType`. `Option<T>` propagates `NULL` explicitly; a
  `Result<T, E>` return fails the query with `E`'s message. Backed by hand-written `VScalar`/`VTab`
  traits (also usable directly) and a new `better-duck-macros` proc-macro crate, re-exported from
  `better-duck-core`. Callback panics are caught and reported as query errors under
  `panic = "unwind"`. Not yet supported: named parameters, projection pushdown, `max_threads`,
  `varargs`, LIST/STRUCT columns.

**Result API**
- `DuckResult::exists()` — peeks whether at least one row is available without consuming it.
- `DuckResult` row cache — `rewind()` replays already-pulled rows from the start.

**Generic binding**
- **Generic `LIST`/`ARRAY`/`MAP` binding** — `Vec<T>`, `Box<[T]>`, and `HashMap<K, V>` accept any
  `T`/`K`/`V` with a fixed DuckDB type (`DuckLogicalType`), converting via `Into<DuckValue>` — no
  need to pre-convert elements to `DuckValue` by hand, and empty collections work too, since the
  element type comes from the Rust type itself rather than from inspecting the first entry (which
  the untyped `DuckValue::List`/`Array`/`Map` must still do, and still can't for zero entries).
- `DuckStruct` newtype — `HashMap<String, DuckValue>` now means `MAP`; wrap it in `DuckStruct` to
  bind it as `STRUCT` (mirrors how `Blob` disambiguates `Vec<u8>` from a generic `LIST`).

**Misc**
- `Error::BackgroundTaskFailed`, `Error::Pool` — new non-exhaustive variants for the async and pool
  facades.
- `pub mod config` — `Config` and friends are now part of the public API (previously unreachable
  despite `Connection::open_with_flags` accepting one).

#### Changed

- **`Error::UNKNOWN`** now requires `Box<dyn Error + Send + Sync + 'static>` (previously
  `Box<dyn Error>`), matching `ToSqlConversionFailure`. This makes `Error` — and therefore
  `Result<T>` — `Send`, which the async facade requires. Only affects code constructing
  `Error::UNKNOWN` directly with a non-`Send` payload.
- `DuckRow` now derives `Clone`.

#### Fixed

- Deleted two stale `TODO: preserve TIME_TZ offset` comments — the offset was already preserved
  correctly on both read and write; only the comments were out of date.
- **`Decimal` insert/bind was allocating a `String` on every row** — `Decimal::to_duck()` computed
  its DuckDB `width` by `format!("{value}").len()`; replaced with integer-only digit counting
  (`checked_ilog10`), matching the allocation-free approach the `duckdb` crate itself uses.
- **`u128` (UHUGEINT) had no fast append/bind path** — only `i128` (HUGEINT) had a direct
  `AppendAble` impl calling the typed FFI functions; `u128` silently fell back to the generic
  `DuckValue`-based path (heap-allocates and destroys a `duckdb_value` per row). Added
  `AppendAble for u128` and `DuckDialect<duckdb_uhugeint> for u128`, mirroring the existing `i128`
  impls and using `duckdb_append_uhugeint`/`duckdb_bind_uhugeint` directly.
- **Every query execution heap-allocated a `Box<duckdb_result>` it didn't need** —
  `RawConnection::query`, `Statement::execute`, and `CachedStatement::execute` each boxed a
  `duckdb_result` purely to get a stable pointer for the FFI output parameter, then immediately
  copied it back out (`duckdb_result` is a small `Copy` struct with no self-referential fields, and
  `DuckResult::new` already takes it by value). Switched all three to a stack-local value, removing
  one malloc/free pair from every single query on the connection.
- **`DuckResult::count()` fully materialized every row it was about to discard** — the default
  `Iterator::count()` called `next()` in a loop, which for each row heap-allocates a `Vec<DuckValue>`
  and converts every column, even though the caller never reads the row. Added a specialized
  `count()` that advances the chunk cursor without building a `DuckRow` at all — safe because
  `count(self)` consumes the iterator, so no later call can observe the skipped cache/materialization.

**Benchmarks**
- `benches/comparison.rs` reworked: `prepared_reuse` now fairly reuses one `CachedStatement` per
  rep on the core side (previously re-prepared on every one of 100 calls, unlike the `duckdb` crate
  side it was compared against); `u128` folded into the generic type-benchmark loop now that it has
  a fast path; SVG charts show one rotated label per core/duckdb bar pair instead of a mesh-drawn
  tick label; the host/environment "System context" block was removed from `REPORT.md` and every
  SVG chart caption.

### `better-duck-diesel`

#### Added

- **`FromSql`/`ToSql` for STRUCT, MAP, UNION, ARRAY** (`DuckStruct`, `DuckMap`, `DuckUnion`,
  `DuckArray`) and for the three new core types (`DuckUuid`, `DuckBit`, `DuckBignum`).
- **Non-chrono date/time** — `date_native` is wired up; DATE/TIME/TIMESTAMP/INTERVAL/
  TIMESTAMPTZ/TIME_TZ/TIME_NS all work over Diesel without the `chrono` feature (previously the
  crate shipped with zero date/time support in a default, non-chrono build).
- **`DuckDbConnection::from_core()`** and **`pool::SharedDuckDbConnectionManager`** (`r2d2`
  feature) — an alternative to `diesel::r2d2::ConnectionManager<DuckDbConnection>` that shares one
  `better_duck_core::database::Database` across the pool instead of opening one database per
  connection. Both managers may be used side by side.

#### Fixed

- **`push_debug_binds`** — was `todo!()` (panicked on `debug_query`/`EXPLAIN`); now implemented.
- **`prepare_for_cache`** — the TODO's premise was wrong (Diesel does expose the hint); replaced
  with an accurate comment noting DuckDB's C API has a single prepare path, so there is nothing to
  honour.

### Infrastructure

#### Added

- **`better-duck-macros`** — new proc-macro crate (`#[duckdb_scalar]`, `#[duckdb_table_function]`),
  added to the workspace and to CI's feature matrix / docs / doctest / MSRV / coverage jobs.

#### Fixed

- **`publish_crate.yml` publish order** — `cargo metadata` returns workspace packages
  alphabetically, which would have tried to publish `better-duck-diesel` before its
  `better-duck-core` path dependency existed on crates.io; hardcoded the correct topological
  publish order (`better-duck-macros` → `better-duck-core` → `better-duck-diesel`).

### Still open

See the [roadmap](README.md#roadmap) — multi-arm `UNION` writes, `DECIMAL` precision, and the
exploratory `better-duck-tauri` / WASM items remain unimplemented.

---

## [0.1.0-beta.2] — 2026-06-07

First public beta of the `better-duck` workspace.  The core API is settled enough for
real use; breaking changes before `1.0` are still possible — check this file before
upgrading.

---

### `better-duck-core`

#### Added

**Connection API**
- `Connection` — safe high-level wrapper around `RawConnection`; `open`, `open_in_memory`, `open_with_flags`
- `Connection::execute` / `execute_batch` / `execute_with` — SQL execution with `$N` parameterised binds (1-based)
- `Connection::appender` — creates a bulk-insert `Appender` tied to a named table
- `CachedStatement` — reusable prepared statement that can be reset and re-executed with new bindings
- `DuckResult` / `DuckRow` — safe row-iterator over query results; `DuckResult::changes()`, `count()`
- `Send` + `Sync` implemented on `RawConnection`, `Connection`, and `CachedStatement`

**Type system**
- `DuckValue` enum — full DuckDB type coverage:
  `Null`, `Boolean`, integer types `TinyInt`–`HugeInt` (signed) and `UTinyInt`–`UHugeInt` (unsigned),
  `Float`, `Double`, `Text`, `Blob`,
  `Date`, `Time`, `TimeTz`, `TimeNs`,
  `Timestamp`, `TimestampS`, `TimestampMs`, `TimestampNs`, `TimestampTz`,
  `Interval`, `Decimal`, `Enum`, `List`, `Array`, `Struct`, `Map`, `Union`
- `DuckValueRef` — zero-copy borrowed variant; `Text`, `Blob`, and `Enum` slots use `Cow::Borrowed`; converts to `DuckValue` at any lifetime
- `DuckDialect<Raw>` trait — generic `Raw` parameter allows temporal types to pass a packed FFI struct (e.g. `duckdb_date`) directly from chunk vectors without an alloc/free round-trip; defaults to `duckdb_value` for scalar types
- `PartialEq` / `Eq` / `Hash` for `DuckValue` and `DuckValueRef` — `f32`/`f64` are canonicalised so `NaN == NaN` holds for map-key stability; `HashMap`-based variants (`Struct`, `Map`) hash order-independently
- Ergonomic `From<T>` conversions into `DuckValue` for all scalar Rust primitives, `String`, `Vec<u8>`, and container types

**Composite types**
- LIST / ARRAY — `read_list_or_array` with validity-bitmap checks; `AppendAble` for `Vec<T>` where `T: AppendAble`
- STRUCT — `build_struct` / `append_struct`; `AppendAble` for `HashMap<String, DuckValue>`
- MAP — `build_map` / `append_map`; `AppendAble` for `HashMap<DuckValue, DuckValue>`
- UNION — `read_union` helper

**Appender / binding**
- `AppendAble` trait — implemented for `bool`, all integer types, `f32`/`f64`, `String`, `Blob`, `Decimal` (optional), `HashMap<String, DuckValue>`, `HashMap<DuckValue, DuckValue>`, `Vec<T>`, dates and times (both native and chrono)
- `Appender::append` / `Appender::save` — row-at-a-time append with automatic `begin_row` / `end_row` framing

**No-chrono date/time fallback**
- `DuckDate` / `DuckTime` — lightweight date and time structs for builds without the `chrono` feature

**Feature flags**
| Flag | Default | Effect |
|---|---|---|
| `bundled` | ✓ | Compile DuckDB from source; no system library needed |
| `chrono` | ✓ | Chrono `NaiveDate`/`NaiveTime`/`NaiveDateTime`/`DateTime` support |
| `decimal` | ✓ | `rust_decimal::Decimal` support |
| `json` | — | DuckDB JSON extension |
| `parquet` | — | DuckDB Parquet extension |
| `buildtime_bindgen` | — | Regenerate FFI bindings at build time (requires LLVM/clang) |

**Benchmarks**
- `benches/my_benchmark.rs` — criterion benchmarks for `query_1000_rows`, `execute_with_param_100x`, `appender_10k_rows`
- `benches/comparison.rs` — Core-vs-CLI comparison harness over 5 workloads (CRUD basics, bulk ingest, analytical query, prepared-statement reuse, all-types scan); outputs `docs/benchmarks/REPORT.md`, `results.json`, and two SVG charts

#### Fixed

- **HUGEINT encoding** — previous implementation used `u64::MAX` (2⁶⁴−1) as a multiplier instead of a 128-bit two's-complement bit-shift; corrected to `(upper as i128) << 64 | lower as i128` for both `i128_from_hugeint` and `hugeint_from_i128`
- Text column reads switched to `String::from_utf8_lossy` to handle malformed UTF-8 data gracefully instead of panicking
- Blob `From` implementation produced the wrong `DuckValue` variant
- `Appender` construction now propagates an error on nul bytes in the table/schema name instead of panicking

---

### `better-duck-diesel`

#### Added

- `DuckDbConnection` — full Diesel 2.3 backend; implements `Connection`, `SimpleConnection`, `LoadConnection`, `TransactionManager`, `MigrationConnection`
- `DuckDbConnection::establish` — accepts `:memory:`, a bare file path, or a `duckdb://` URL prefix (prefix is stripped)
- Statement cache (`StatementCache<DuckDb, CachedStatement>`) with `prepare_cached`
- `r2d2` connection pool via the `r2d2` feature (`r2d2::ManageConnection` impl)
- `FromSql` / `ToSql` for: `bool`, `i8`/`i16`/`i32`/`i64`/`i128`, `u8`/`u16`/`u32`/`u64`, `f32`, `f64`, `String`, `Vec<u8>` (BLOB), `Vec<DuckValue>` (LIST via `DuckList`)
- `Decimal` `FromSql`/`ToSql` (optional, requires `decimal` feature)
- Chrono `FromSql`/`ToSql` for DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIME_TZ, TIME_NS (optional `chrono` feature — not enabled by default)
- `duck_sql_type!` macro + SQL type markers: `DuckBigInt`, `DuckBlob`, `DuckDecimal`, `DuckEnum`, `DuckList`, `DuckMap`, `DuckTimeTz`, `DuckTimeNs`, `DuckStruct`; all re-exported for use inside Diesel `table!` blocks
- Full integration test suite: CRUD via query DSL, raw SQL, transactions, error handling, type round-trips (integers, floats, text, blob, date/time, decimal, enum, list, NULL handling), statement-cache assertions

| Flag | Default | Effect |
|---|---|---|
| `bundled` | ✓ | Inherit bundled DuckDB from `better-duck-core` |
| `decimal` | ✓ | `rust_decimal::Decimal` `FromSql`/`ToSql` |
| `chrono` | — | Chrono date/time `FromSql`/`ToSql` |
| `r2d2` | — | `r2d2` connection pool |

---

### Infrastructure

#### Added

- **GitHub Actions CI** — format check, Clippy (`-D warnings`), docs (`-D warnings`), tests on Linux / Windows / macOS, iOS cross-builds (aarch64 + x86_64 simulator), doctest job
- **Feature-combination matrix** — 10 configurations across both crates (no-default-features compile-check, bundled-only, chrono-only, decimal-only, json, parquet for core; default, +chrono, +r2d2, +all for diesel)
- **MSRV job** — `dtolnay/rust-toolchain@1.96`; `rust-version = "1.96"` pinned in `[workspace.package]`
- **Security audit** — `rustsec/audit-check` on every push/PR; advisory-only on the weekly cron
- **Coverage** — `cargo-llvm-cov` → `lcov.info` → Codecov (`continue-on-error: true` until `CODECOV_TOKEN` is provisioned)
- `Swatinem/rust-cache` on all jobs — avoids recompiling bundled DuckDB on every run
- Dependabot for Cargo + GitHub Actions (weekly, Monday)
- `CODEOWNERS`, `FUNDING.yml`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `CONTRIBUTING.md`
- Issue templates (bug report + feature request form) and PR template

---

[0.1.0-beta.3]: https://github.com/nimdeveloper/better-duck/releases/tag/v0.1.0-beta.3
[0.1.0-beta.2]: https://github.com/nimdeveloper/better-duck/releases/tag/v0.1.0-beta.2
