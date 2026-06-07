# better-duck

**A safe, embedded-first Rust client for [DuckDB](https://duckdb.org), with an optional [Diesel 2.3](https://diesel.rs) ORM backend.**

[![CI](https://github.com/nimdeveloper/better-duck/actions/workflows/ci.yml/badge.svg)](https://github.com/nimdeveloper/better-duck/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/better-duck-core.svg)](https://crates.io/crates/better-duck-core)
[![docs.rs](https://docs.rs/better-duck-core/badge.svg)](https://docs.rs/better-duck-core)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV: 1.96](https://img.shields.io/badge/rustc-1.96+-orange.svg)](#supported-platforms)

> **Beta** — the API is settling. Breaking changes before `1.0` are possible; check the [changelog](CHANGELOG.md) before upgrading.

---

## Why better-duck?

Most Rust DuckDB bindings depend on Arrow or require a system-installed DuckDB library. `better-duck` takes a different approach:

- **Bundled DuckDB** — ships with the DuckDB C library compiled in; no system package needed.
- **No Arrow dependency** — columnar I/O is great for data pipelines, but most app-level OLAP code just needs rows. We skip the Arrow overhead entirely.
- **Diesel ORM** — the `better-duck-diesel` crate is a full Diesel 2.3 backend, so your existing `table!` / query DSL code works without changes.
- **Embedded-first** — designed to run inside Tauri desktop apps, iOS cross-builds, and other environments where you can't rely on a system library.
- **Safe public API** — every FFI call is wrapped; nothing `unsafe` leaks into user code.

---

## Crates

| Crate | crates.io | Description |
|---|---|---|
| `better-duck-core` | [![crates.io](https://img.shields.io/crates/v/better-duck-core.svg)](https://crates.io/crates/better-duck-core) | Low-level DuckDB wrapper — connections, prepared statements, bulk appender, full type coverage |
| `better-duck-diesel` | [![crates.io](https://img.shields.io/crates/v/better-duck-diesel.svg)](https://crates.io/crates/better-duck-diesel) | Diesel 2.3 backend — full query DSL, migrations, r2d2 connection pool |

---

## Quick start

```toml
[dependencies]
# Core only
better-duck-core = "0.1"

# Or: Core + Diesel ORM backend
better-duck-core   = "0.1"
better-duck-diesel = "0.1"
```

---

## `better-duck-core`

A low-level, no-ORM DuckDB wrapper that gives you direct access to connections, prepared statements, the bulk appender, and the full `DuckValue` type hierarchy — without pulling in an ORM.

### Opening a connection

```rust
use better_duck_core::connection::Connection;

// in-memory (great for tests and one-shot scripts)
let mut conn = Connection::open_in_memory()?;

// on-disk file
let mut conn = Connection::open("my_database.duckdb")?;
```

### Execute and iterate rows

```rust
use better_duck_core::{connection::Connection, types::value::DuckValue};

fn main() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;

    conn.execute_batch(
        "CREATE TABLE events (id INTEGER, label TEXT, score DOUBLE);
         INSERT INTO events VALUES (1, 'alpha', 9.5), (2, 'beta', 7.2);",
    )?;

    let mut result = conn.execute("SELECT id, label, score FROM events ORDER BY id")?;
    for row in result {
        let row = row?;
        println!(
            "id={:?}  label={:?}  score={:?}",
            row.get("id"),
            row.get("label"),
            row.get("score"),
        );
    }
    Ok(())
}
```

### Parameterized queries

Parameters are positional (`$1`, `$2`, …) and passed as `&mut [&mut dyn AppendAble]`:

```rust
use better_duck_core::types::value::DuckValue;

let mut threshold = DuckValue::Double(8.0);
let mut rows = conn.execute_with(
    "SELECT id, label FROM events WHERE score > $1",
    &mut [&mut threshold],
)?;
for row in rows {
    let row = row?;
    println!("{:?}", row);
}
```

### Bulk insert with the Appender

The `Appender` streams rows directly into DuckDB's bulk-ingest path — much faster than individual INSERTs for large datasets:

```rust
use better_duck_core::{connection::Connection, types::appendable::AppendAble};
use better_duck_core::ffi::{duckdb_appender, duckdb_prepared_statement, duckdb_append_int32, duckdb_bind_int32};
use better_duck_core::error::Result;

struct IntRow(i32);

impl AppendAble for IntRow {
    fn appender_append(&mut self, appender: duckdb_appender) -> Result<()> {
        // SAFETY: appender is valid and the table has one INTEGER column.
        unsafe { duckdb_append_int32(appender, self.0) };
        Ok(())
    }
    fn stmt_append(&mut self, idx: u64, stmt: duckdb_prepared_statement) -> Result<()> {
        // SAFETY: stmt is valid; idx is a 1-based parameter index.
        unsafe { duckdb_bind_int32(stmt, idx, self.0) };
        Ok(())
    }
}

let mut conn = Connection::open_in_memory()?;
conn.execute_batch("CREATE TABLE nums (v INTEGER)")?;

let mut app = conn.appender("nums", "main")?;
for i in 0..10_000i32 {
    app.append(&mut IntRow(i))?;
}
app.save()?; // flush to DuckDB
```

The appender auto-flushes on drop (errors go to stderr); call `.save()` explicitly if you want to handle flush errors.

### `DuckValue` type hierarchy

Rows are yielded as `DuckRow`, and each column value is a `DuckValue`:

```rust
use better_duck_core::types::value::DuckValue;

match value {
    DuckValue::Int(n)     => println!("integer: {n}"),
    DuckValue::Text(s)    => println!("text: {s}"),
    DuckValue::Double(f)  => println!("float: {f}"),
    DuckValue::Null       => println!("null"),
    _ => println!("other: {value:?}"),
}
```

`DuckValue` is `#[non_exhaustive]` — match with `_` to stay forward-compatible as new types are added.

### Supported DuckDB types

| DuckDB type | Rust type |
|---|---|
| `BOOLEAN` | `bool` |
| `TINYINT` / `UTINYINT` | `i8` / `u8` |
| `SMALLINT` / `USMALLINT` | `i16` / `u16` |
| `INTEGER` / `UINTEGER` | `i32` / `u32` |
| `BIGINT` / `UBIGINT` | `i64` / `u64` |
| `HUGEINT` / `UHUGEINT` | `i128` / `u128` |
| `FLOAT` | `f32` |
| `DOUBLE` | `f64` |
| `DECIMAL` _(feature: decimal)_ | `rust_decimal::Decimal` |
| `VARCHAR` / `TEXT` | `String` |
| `BLOB` | `better_duck_core::types::blob::Blob` |
| `DATE` | `chrono::NaiveDate` _(chrono)_ / `DuckDate` |
| `TIME` | `chrono::NaiveTime` _(chrono)_ / `DuckTime` |
| `TIMESTAMP` | `chrono::NaiveDateTime` _(chrono)_ |
| `TIMESTAMPTZ` | `chrono::DateTime<Utc>` _(chrono)_ |
| `TIME_TZ` | `CoreTimeTz` (offset read; full preservation: see roadmap) |
| `INTERVAL` | `chrono::Duration` _(chrono)_ / `std::time::Duration` |
| `LIST` / `ARRAY` | `Vec<DuckValue>` / `Box<[DuckValue]>` |
| `STRUCT` | `HashMap<String, DuckValue>` |
| `MAP` | `HashMap<DuckValue, DuckValue>` |
| `UNION` | `Box<DuckValue>` (active member) |
| `ENUM` | `String` |

---

## `better-duck-diesel`

A full [Diesel 2.3](https://diesel.rs) backend for DuckDB. Write normal Diesel DSL code against any DuckDB database — including in-memory, on-disk, and (soon) remote.

### Connecting

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

// in-memory
let mut conn = DuckDbConnection::establish(":memory:")?;

// on-disk file
let mut conn = DuckDbConnection::establish("/path/to/db.duckdb")?;

// with duckdb:// URL prefix (prefix is stripped)
let mut conn = DuckDbConnection::establish("duckdb:///path/to/db.duckdb")?;
```

### INSERT, SELECT, UPDATE, DELETE

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*};

diesel::table! {
    products (id) {
        id    -> Integer,
        name  -> Text,
        price -> Double,
    }
}

fn main() -> QueryResult<()> {
    let mut conn = DuckDbConnection::establish(":memory:")?;
    conn.batch_execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DOUBLE NOT NULL)",
    )?;

    // INSERT with RETURNING
    let inserted: Vec<(i32, String)> = diesel::insert_into(products::table)
        .values(&vec![
            (products::id.eq(1), products::name.eq("widget"), products::price.eq(9.99)),
            (products::id.eq(2), products::name.eq("gadget"), products::price.eq(24.50)),
        ])
        .returning((products::id, products::name))
        .get_results(&mut conn)?;

    // SELECT with filter and ordering
    let cheap: Vec<(i32, String, f64)> = products::table
        .filter(products::price.lt(20.0))
        .order(products::name.asc())
        .select((products::id, products::name, products::price))
        .load(&mut conn)?;

    // UPDATE
    diesel::update(products::table.filter(products::id.eq(1)))
        .set(products::price.eq(11.99))
        .execute(&mut conn)?;

    // DELETE
    diesel::delete(products::table.filter(products::id.eq(2)))
        .execute(&mut conn)?;

    Ok(())
}
```

### Transactions

```rust
conn.transaction(|conn| {
    diesel::insert_into(products::table)
        .values((products::id.eq(3), products::name.eq("doohickey"), products::price.eq(4.99)))
        .execute(conn)?;
    // returning Err rolls back; Ok commits
    Ok(())
})?;
```

### DuckDB-specific SQL types

Use DuckDB types that don't have a standard Diesel equivalent by importing them via `sql_types`:

```rust
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    readings (id) {
        id       -> Integer,
        sensor   -> DuckEnum,          // DuckDB ENUM column
        value    -> Double,
        ts       -> DuckTimestamptz,   // TIMESTAMPTZ column
    }
}
```

### DuckDB ↔ Diesel ↔ Rust type mapping

**Standard Diesel types** (work out of the box):

| Diesel SQL type | DuckDB type | Rust type |
|---|---|---|
| `Bool` | `BOOLEAN` | `bool` |
| `SmallInt` | `SMALLINT` | `i16` |
| `Integer` | `INTEGER` | `i32` |
| `BigInt` | `BIGINT` | `i64` |
| `Float` | `FLOAT` | `f32` |
| `Double` | `DOUBLE` | `f64` |
| `Text` | `VARCHAR` | `String` |
| `Binary` | `BLOB` | `Vec<u8>` |
| `Date` | `DATE` | `chrono::NaiveDate` _(chrono)_ |
| `Time` | `TIME` | `chrono::NaiveTime` _(chrono)_ |
| `Timestamp` | `TIMESTAMP` | `chrono::NaiveDateTime` _(chrono)_ |
| `Numeric` | `DECIMAL` | `rust_decimal::Decimal` _(decimal)_ |

**DuckDB-specific types** (import via `better_duck_diesel::sql_types::*`):

| Diesel SQL type | DuckDB type | Rust type |
|---|---|---|
| `DuckTinyInt` | `TINYINT` | `i8` |
| `DuckUTinyInt` | `UTINYINT` | `u8` |
| `DuckUSmallInt` | `USMALLINT` | `u16` |
| `DuckUInt` | `UINTEGER` | `u32` |
| `DuckUBigInt` | `UBIGINT` | `u64` |
| `DuckHugeInt` | `HUGEINT` | `i128` |
| `DuckUHugeInt` | `UHUGEINT` | `u128` |
| `DuckTimestamptz` | `TIMESTAMPTZ` | `chrono::DateTime<Utc>` _(chrono)_ |
| `DuckInterval` | `INTERVAL` | `chrono::Duration` _(chrono)_ |
| `DuckTimeTz` | `TIME WITH TIME ZONE` | `CoreTimeTz` _(chrono)_ |
| `DuckTimeNs` | `TIME_NS` | `chrono::NaiveTime` _(chrono)_ |
| `DuckEnum` | `ENUM` | `String` |
| `DuckList` | `LIST` | `Vec<DuckValue>` |

> **Note:** date/time types in `better-duck-diesel` require the `chrono` feature (not enabled by default — add `features = ["chrono"]`).

---

## Feature flags

### `better-duck-core`

| Feature | Default | Description |
|---|---|---|
| `bundled` | ✓ | Compile and embed the DuckDB C library (no system install needed) |
| `chrono` | ✓ | `chrono` date/time conversions for DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL |
| `decimal` | ✓ | `rust_decimal::Decimal` support for DECIMAL columns |
| `json` | — | Enable DuckDB's JSON extension (requires `bundled`) |
| `parquet` | — | Enable DuckDB's Parquet extension (requires `bundled`) |
| `buildtime_bindgen` | — | Regenerate FFI bindings at build time (requires LLVM/clang) |

### `better-duck-diesel`

| Feature | Default | Description |
|---|---|---|
| `bundled` | ✓ | Forwards to `better-duck-core/bundled` |
| `decimal` | ✓ | Diesel `Numeric` ↔ `rust_decimal::Decimal` |
| `chrono` | — | Diesel date/time impls for DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIME_TZ, TIME_NS |
| `r2d2` | — | r2d2 connection pool support via `diesel::r2d2` |

---

## Benchmarks

The workspace includes a custom Core-vs-CLI benchmark harness at [`crates/better-duck-core/benches/comparison.rs`](crates/better-duck-core/benches/comparison.rs). Run it with:

```sh
cargo bench -p better-duck-core --bench comparison
```

Results are written to `docs/benchmarks/` (Markdown report + JSON + SVG charts). If the `duckdb` CLI binary is on your PATH, the harness will also time it as a comparison; otherwise the CLI column is skipped.

**Sample results** (Intel Core Ultra 7 155U, 12 cores, 16 GB RAM, Windows 11):

| Workload | Median latency | Throughput |
|---|---|---|
| CRUD basics (4 ops) | 2.36 ms | 1.7 k ops/s |
| Bulk ingest — 10k rows (appender) | 16.43 ms | 608 k rows/s |
| Analytical GROUP BY — 100k rows | 1.16 ms | 86 M rows/s |
| Prepared reuse — 100 queries | 27.75 ms | 3.6 k queries/s |
| All-types scan — 1k rows, 11 cols | 18.76 ms | 53 k rows/s |

![Latency comparison](docs/benchmarks/comparison-latency.svg)

---

## Migrating from the community `duckdb` crate

| Operation | `duckdb` crate | `better-duck-core` |
|---|---|---|
| Open in-memory | `Connection::open_in_memory()?` | `Connection::open_in_memory()?` |
| Execute DDL | `conn.execute_batch(sql)?` | `conn.execute_batch(sql)?` |
| Insert / DML | `conn.execute(sql, [])?` | `conn.execute(sql)?.changes()` |
| SELECT rows | `conn.prepare(sql)?.query([])` | `conn.execute(sql)?` (is an `Iterator`) |
| Parameterized | `conn.execute(sql, params![v])?` | `conn.execute_with(sql, &mut [&mut v])?` |
| Bulk insert | `conn.appender(table)?` | `conn.appender(table, schema)?` |

---

## Supported platforms

| Platform | Status |
|---|---|
| Linux x86_64 | ✓ CI-tested |
| macOS Apple Silicon (aarch64) | ✓ CI-tested |
| macOS x86_64 | ✓ CI-tested |
| Windows x86_64 | ✓ CI-tested |
| iOS aarch64 | ✓ CI cross-build |
| iOS Simulator x86_64 | ✓ CI cross-build |

---

## Roadmap

The library is usable today for most workloads. Here's an honest list of what's still in progress — contributions are very welcome.

### Near-term (before `1.0`)

- **New core types** — `UUID`, `BIT`, `BIGNUM`/`VARINT`, `GEOMETRY`, `VARIANT`, `ANY`, and `INTEGER_LITERAL` are not yet handled; reading a column of these types currently returns an error. Each needs a `DuckValue` variant, a read path in `value.rs`, and a matching `DuckValueRef` variant.
- **`TIME_TZ` timezone offset** — the UTC offset stored in `duckdb_time_tz` is read but discarded. Full round-trip support requires preserving it in `DuckValue::TimeTz`.
- **Diesel `FromSql`/`ToSql` for composite types** — STRUCT, MAP, UNION, and ARRAY have full core support but no Diesel impl yet. The gap is documented in [`crates/better-duck-diesel/tests/README.md`](crates/better-duck-diesel/tests/README.md).
- **Diesel date/time without `chrono`** — the `date_native` module in `better-duck-diesel` is not yet wired up; date/time columns over Diesel currently require `features = ["chrono"]`.
- **`DuckResult::exists()`** — a convenience method to check whether a SELECT returned any rows, without consuming the iterator.
- **`DuckResult` row cache** — allow rewinding / iterating a result set more than once.

### Mid-term

- **Diesel wiring for new types** — once UUID, BIT, GEOMETRY, etc. land in core, add the corresponding Diesel SQL type markers and `FromSql`/`ToSql` impls.
- **`push_debug_binds`** — Diesel's debug-bind output currently panics (unimplemented); fix it so `EXPLAIN` / logging works.
- **Diesel `prepare_for_cache` distinction** — honor the `PrepareForCache::No` / `Yes` hint from Diesel's statement-cache API once a stable path exists for third-party backends.
- **Multi-arm UNION write** — the current write path only builds single-member unions. Real multi-arm unions need a richer `DuckValue::Union` variant or a builder API.
- **Empty-collection type inference** — `Appender`/`struct_to_duck`/`map_to_duck` currently return an error when given an empty `Vec`/`HashMap` because the element type can't be inferred. A `TypedEmpty` wrapper or a default-type convention would fix this.
- **`async` API** — `Connection` is synchronous. An async facade wrapping `spawn_blocking` would make `better-duck` usable in async runtimes without blocking the executor.
- **Core-level connection pooling** — pooling is currently only available via `better-duck-diesel` + `r2d2`. A standalone pool (e.g. `deadpool`-backed) would help non-ORM users.

### Exploratory / RFC

- **`better-duck-tauri` crate** — a Tauri plugin that wraps `better-duck-core` with auto-discovery of the app data directory, a repository/unit-of-work abstraction, and Tauri command bindings. Filed as an idea; design input welcome.
- **WASM / browser target** — DuckDB has a WASM build; exploring whether `better-duck-core` can compile to `wasm32-unknown-unknown` is on the list.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide — environment setup, git flow, commit conventions, how to add a new DuckDB type, and the PR checklist.

If you hit a bug or want to propose a feature, please [open an issue](https://github.com/nimdeveloper/better-duck/issues).

---

## License

Licensed under either of:

- [MIT License](LICENSE)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
