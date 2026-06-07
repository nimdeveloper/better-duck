# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [Unreleased]

### Planned

See the [roadmap section of the README](README.md#roadmap) for the full list. Key items:

- **New core types** — `UUID`, `BIT`, `BIGNUM`/`VARINT`, `GEOMETRY`, `VARIANT`, `ANY`, `INTEGER_LITERAL` (currently return an error)
- **`TIME_TZ` offset round-trip** — the UTC offset is decoded but currently discarded
- **Diesel parity** — `FromSql`/`ToSql` for STRUCT, MAP, UNION; an ARRAY diesel module; new types above wired in once core supports them
- **Non-chrono diesel date/time** — `date_native` module for `better-duck-diesel` without the `chrono` feature
- **`DuckResult` rows cache** + `exists` helper; numeric precision surface (`decimal_value.width`)
- **Async API** and core-level connection pooling
- **`better-duck-tauri`** crate — Tauri plugin with auto-discovery, repository abstraction, and Tauri command bindings (exploratory / RFC)

---

[0.1.0-beta.2]: https://github.com/nimdeveloper/better-duck/releases/tag/v0.1.0-beta.2
