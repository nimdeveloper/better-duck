# better-duck

**A safe, embedded-first Rust client for [DuckDB](https://duckdb.org), with an optional [Diesel 2.3](https://diesel.rs) ORM backend.**

[![CI](https://img.shields.io/github/actions/workflow/status/nimdeveloper/better-duck/ci.yml?branch=main&style=flat-square&label=ci&logo=github)](https://github.com/nimdeveloper/better-duck/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/nimdeveloper/better-duck?logo=codecov&label=codecov&style=flat-square)](https://codecov.io/gh/nimdeveloper/better-duck)
[![crates.io](https://img.shields.io/crates/v/better-duck-core.svg?style=flat-square)](https://crates.io/crates/better-duck-core)
[![docs.rs](https://img.shields.io/docsrs/better-duck-core/latest?style=flat-square&logo=docsdotrs)](https://docs.rs/better-duck-core)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg?style=flat-square)](#license)
[![MSRV: 1.96](https://img.shields.io/badge/rustc-1.96+-orange.svg?style=flat-square&logo=rust&label=MSRV)](#supported-platforms)

> [!WARNING]
> **Beta** — the API is settling. Breaking changes before `1.0` are possible; check the [changelog](CHANGELOG.md) before upgrading.

---

## Highlights

- **Bundled DuckDB** — the DuckDB C library compiles straight in; no system package, no runtime dependency to install.
- **Row-oriented, Arrow-free** — direct access to rows and values with a lean dependency tree, ideal for application-level OLAP.
- **Safe by construction** — every FFI call is wrapped; nothing `unsafe` leaks into your code, across the full DuckDB type system.
- **Comprehensive type coverage** — integers of every width, `DECIMAL`, `UUID`, `BIT`, `BIGNUM`, temporals, and the composite `LIST` / `ARRAY` / `STRUCT` / `MAP` / `UNION` / `ENUM` types.
- **Diesel 2.3 ORM backend** — a full custom backend: query DSL, migrations, an r2d2 pool, and a large library of DuckDB-specific SQL functions, aggregates, and operators.
- **User-defined functions & derives** — register plain Rust functions as scalar/table/aggregate/cast functions, and map Rust structs/enums to DuckDB rows and types with `#[derive(…)]` — no `unsafe`, no manual vector handling.
- **Embedded-first** — designed to run inside Tauri desktop apps, iOS cross-builds, and other environments where a system library isn't an option.

---

## Crates

| Crate                | crates.io                                                                                                                               | Description                                                                                      |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `better-duck-core`   | [![crates.io](https://img.shields.io/crates/v/better-duck-core.svg?style=for-the-badge)](https://crates.io/crates/better-duck-core)     | DuckDB client — connections, prepared statements, bulk appender, full type coverage, UDFs        |
| `better-duck-diesel` | [![crates.io](https://img.shields.io/crates/v/better-duck-diesel.svg?style=for-the-badge)](https://crates.io/crates/better-duck-diesel) | Diesel 2.3 backend — query DSL, migrations, r2d2 pool, DuckDB SQL functions/aggregates/operators |
| `better-duck-macros` | [![crates.io](https://img.shields.io/crates/v/better-duck-macros.svg?style=for-the-badge)](https://crates.io/crates/better-duck-macros) | Procedural macros powering the UDFs and data derives (used via core/diesel feature flags)        |
| `better-duck-sys`    | Internal                                                                                                                                | Vendored DuckDB C API bindings used by the workspace                                             |

---

## Installation

```toml
[dependencies]
# DuckDB client
better-duck-core = "0.1.0-beta.4"

# Optional: Diesel ORM backend
better-duck-diesel = "0.1.0-beta.4"
```

> [!NOTE]
> Cargo's default version requirement (e.g. `"0.1"`) excludes pre-releases like `-beta.4`. Pin the
> exact version as shown, or run `cargo add better-duck-core --version 0.1.0-beta.4`.
> `better-duck-macros` is pulled in automatically by the `udf` / `derive` feature flags — you don't
> depend on it directly.

## What sets better-duck apart

Beyond being a safe DuckDB client, better-duck adds capabilities that other Rust DuckDB bindings don't offer. Each is opt-in behind a feature flag, and each is designed to need **no `unsafe` and no manual FFI** on your side.

### Map Rust types to DuckDB with derives — `feature = "derive"`

Four derives turn plain Rust structs and enums into DuckDB rows, values, and types — no hand-written column indexing, no manual `DuckValue` matching.

- **`#[derive(FromRow)]`** — read a query row into a struct by column name; `ResultSet::to_structs::<T>()` collects a whole result into `Vec<T>`.
- **`#[derive(ToRow)]`** — append a struct's fields as one bulk-appender row, or bind them as consecutive statement parameters.
- **`#[derive(DuckEnum)]`** — map a Rust unit enum to a DuckDB `ENUM` (read + write), with `#[duck_enum(rename_all = "…")]` / per-variant `rename`.
- **`#[derive(DuckStruct)]`** — map a Rust struct to a DuckDB `STRUCT` value (read + write).

```rust
use better_duck_core::{connection::Connection, DuckEnum, FromRow};

#[derive(DuckEnum, Debug, PartialEq, Clone, Copy)]
#[duck_enum(rename_all = "snake_case")]
enum Priority { Low, High }

#[derive(FromRow, Debug)]
struct Task { id: i32, priority: Priority }

let tasks: Vec<Task> = conn.execute("SELECT id, priority FROM tasks")?.to_structs()?;
```

Field/variant names honor `#[duck(rename = "…")]` and `#[duck(rename_all = "…")]`. With `better-duck-diesel`'s `derive` feature on, `DuckEnum`/`DuckStruct` **also** emit Diesel `FromSql`/`ToSql`, so the same type works through the ORM at its `sql_types::DuckEnum` / `sql_types::DuckStruct` column type.

### User-defined functions in plain Rust — `feature = "udf"`

Register ordinary Rust functions as DuckDB functions with an attribute macro. Parameter and return types are inferred from the signature; `Option<T>` propagates `NULL`, a `Result<T, E>` return fails the query with `E`'s message, and panics are caught and surfaced as a query error rather than crossing the FFI boundary.

| Macro                      | Applies to                                | Becomes                                                                             |
| -------------------------- | ----------------------------------------- | ----------------------------------------------------------------------------------- |
| `#[duckdb_scalar]`         | `fn(args) -> T`                           | a scalar function (one value per row)                                               |
| `#[duckdb_table_function]` | `fn(args) -> impl Iterator`               | a table function usable in `FROM` (named params, projection pushdown, shared state) |
| `#[duckdb_aggregate]`      | `mod { init, update, combine, finalize }` | a custom aggregate                                                                  |
| `#[duckdb_cast]`           | `fn(from) -> to`                          | a `CAST` / `TRY_CAST` implementation                                                |

```rust
use better_duck_core::{connection::Connection, duckdb_scalar, duckdb_table_function};

#[duckdb_scalar]
fn shout(s: &str) -> String { s.to_uppercase() }

#[duckdb_table_function(name = "series", columns("n"))]
fn series(start: i64, stop: i64) -> impl Iterator<Item = i64> + Send { start..stop }

shout::register(&mut conn)?;
series::register(&mut conn)?;
conn.execute("SELECT shout('hi')")?;             // "HI"
conn.execute("SELECT sum(n) FROM series(1, 101)")?; // 5050
```

### Call your UDFs through Diesel — no raw SQL — `better-duck-diesel` `feature = "udf"`

Register a UDF on the connection, declare it once as a Diesel function, and then call it as a typed method inside the query DSL — the same Rust function, reachable from the ORM without dropping to `sql_query`.

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

#[duckdb_scalar(name = "add_one")]
fn add_one(x: i32) -> i32 { x + 1 }

diesel::define_sql_function! { fn add_one(x: Integer) -> Integer; }

add_one_impl::register(conn.inner_mut())?;          // register on the underlying connection
let bumped: Vec<i32> = items::table.select(add_one(items::id)).load(&mut conn)?;
```

For connection pools, `SharedDuckDbConnectionManager::on_connect(|c| …)` runs a per-connection setup hook — the place to register UDFs (or `LOAD` extensions) so every pooled connection carries them.

### The SQL surface Diesel doesn't ship — `use better_duck_diesel::dsl::*;`

A large library of DuckDB-specific SQL, exposed as typed Diesel DSL that stock Diesel has no equivalent for:

- **Operators as methods**: `.ilike()`, `.not_ilike()`, `.similar_to()`, `.glob()`, `.regexp_matches()`, `.starts_with_op()`, `.is_distinct_from()` / `.is_not_distinct_from()`.
- **Scalar functions**: string / search / similarity (`levenshtein`, `jaro_winkler_similarity`, `lpad`, `split_part`, …), numeric & trig (`sqrt`, `pow`, `gcd`, `sin`, `atan2`, …), regex, date/time (`date_trunc`, `strftime`, `make_timestamp`, `datediff`, …), UUID, hashing (`sha256`), encoding (`base64`, `hex`), JSON, and `LIST` / `MAP` helpers.
- **Aggregates**: general (`string_agg`, `array_agg`, `bool_and/or`, `arg_max/min`), statistical (`stddev`, `corr`, `regr_slope`, `skewness`, …), approximate (`approx_count_distinct`, `approx_quantile`), bitwise, and JSON aggregates.

```rust
use better_duck_diesel::dsl::*;

let hits = products::table
    .filter(products::name.ilike("wid%"))   // case-insensitive LIKE operator
    .select(products::id)
    .load::<i32>(&mut conn)?;
```

Custom-typed values (`u64`, `i128`, `UUID`, `BIT`, `BIGNUM`, …) bind and load through the `better_duck_diesel::values::*` newtypes, which sidestep the coherence limits that block binding those types directly.

### Full DuckDB type coverage, safely

Every scalar and composite DuckDB type round-trips through `DuckValue` and the Diesel `sql_types`: integers of every width (including `HUGEINT`/`UHUGEINT` ↔ `i128`/`u128`), `DECIMAL`, `UUID`, `BIT`, `BIGNUM`, all temporal precisions (`TIMESTAMP_S/MS/NS`, `TIMESTAMPTZ`, `TIME_TZ` with its offset preserved), and the composites `LIST` / `ARRAY` / `STRUCT` / `MAP` / `UNION` / `ENUM`. Date/time types map to `chrono` (`feature = "chrono"`) or to plain native structs without it.

### Transaction & parameter ergonomics — `feature = "derive"`

- **`transaction!(conn, { … })`** — a scope that commits on `Ok` and rolls back on `Err` or panic. It works for both the core `Connection` and a `DuckDbConnection`, since both expose `transaction(|c| -> Result<_>)`.
- **`params![a, b, Null, …]`** — build the `&mut [&mut dyn AppendAble]` slice for `execute_with` without the boilerplate.

### Extensions & spatial — `feature = "spatial"`

`Connection` can `install` / `load` / `ensure` / query extensions (`ensure_extension("json")`). The diesel `spatial` module adds `ST_*` DSL functions plus `ensure_loaded(conn)`, which installs and loads DuckDB's spatial extension on demand; geometries travel as WKB.

### Async & pooling — `feature = "async"` / `feature = "pool"` / `r2d2`

An `async` facade (`AsyncConnection`, `AsyncDatabase`, `AsyncPool`) runs each call on `spawn_blocking` so it never blocks the executor, and connection pooling is available both through Diesel's own `r2d2` manager and a `SharedDuckDbConnectionManager` that shares one database across the pool.

### Feature flags

| Crate  | Flag                  | What it enables                                                             |
| ------ | --------------------- | --------------------------------------------------------------------------- |
| core   | `chrono` _(default)_  | `chrono` date/time conversions                                              |
| core   | `decimal` _(default)_ | `rust_decimal::Decimal` for `DECIMAL`                                       |
| core   | `json` / `parquet`    | bundle DuckDB's JSON / Parquet extensions                                   |
| core   | `udf`                 | the `#[duckdb_*]` function macros                                           |
| core   | `derive`              | `#[derive(FromRow/ToRow/DuckEnum/DuckStruct)]` + `transaction!` / `params!` |
| core   | `async` / `pool`      | async facade / r2d2 pool over a shared database                             |
| diesel | `derive`              | re-export the data derives with Diesel `FromSql`/`ToSql` emission           |
| diesel | `udf`                 | register UDFs on a `DuckDbConnection` and call them in the DSL              |
| diesel | `spatial`             | `ST_*` DSL functions + `ensure_loaded` helper                               |
| diesel | `json`                | bundle the JSON extension so the JSON DSL works offline                     |
| diesel | `r2d2` / `chrono`     | r2d2 pooling / Diesel date-time impls                                       |

Full API documentation lives on [docs.rs](https://docs.rs/better-duck-core).

## Quick Reference

Full per-crate documentation. Each section is collapsed by default — expand what you need, or read the rendered API docs on [docs.rs](https://docs.rs/better-duck-core).

<details>
<summary><b><code>better-duck-core</code></b> — DuckDB client reference</summary>

### Opening a connection

```rust
use better_duck_core::connection::Connection;

let mut conn = Connection::open_in_memory()?;      // in-memory
let mut conn = Connection::open("my_database.duckdb")?; // on-disk
```

### Executing and iterating rows

`execute` returns a `DuckResult`, which is an `Iterator` of `DuckRow`:

```rust
let mut result = conn.execute("SELECT id, label FROM events ORDER BY id")?;
for row in result {
    let row = row?;
    println!("{:?} {:?}", row.get("id"), row.get("label"));
}
```

### Parameterized queries

Parameters are positional (`$1`, `$2`, …). Pass them as `&mut [&mut dyn AppendAble]` — or use the
`params!` macro (feature `derive`):

```rust
use better_duck_core::types::value::DuckValue;

let mut threshold = DuckValue::Double(8.0);
let rows = conn.execute_with(
    "SELECT id, label FROM events WHERE score > $1",
    &mut [&mut threshold],
)?;
```

### Bulk insert with the Appender

The `Appender` streams rows into DuckDB's bulk-ingest path — far faster than individual INSERTs.
Implement `AppendAble` by hand, or derive it with `#[derive(ToRow)]` (feature `derive`):

```rust
use better_duck_core::{connection::Connection, ToRow};

#[derive(ToRow)]
struct NewItem { id: i32, name: String }

let mut conn = Connection::open_in_memory()?;
conn.execute_batch("CREATE TABLE items (id INTEGER, name TEXT)")?;

let mut app = conn.appender("items", "main")?;
app.append(&mut NewItem { id: 1, name: "a".to_owned() })?;
app.save()?; // flush (also auto-flushes on drop)
```

### Sharing a database, pooling, and async

`open_in_memory()` gives each connection its own database. To share one database across connections,
open a `Database` and `connect()` from it:

```rust
use better_duck_core::database::Database;

let db = Database::open_in_memory()?;
let mut a = db.connect()?;
let mut b = db.connect()?; // sees a's tables
```

- **`pool`** — `Database` backs an r2d2 pool: `DuckDbConnectionManager::memory()?` → `Pool::builder().build(manager)?`.
- **`async`** — `AsyncConnection` runs each call on `spawn_blocking`, never blocking the executor; `async` + `pool` gives `AsyncPool::with(...)`.

### Extension management

```rust
conn.ensure_extension("json")?;                 // load, installing first if needed
let loaded = conn.is_extension_loaded("json")?; // query duckdb_extensions()
```

### `DuckValue` and supported types

Rows are `DuckRow`; each column value is a `#[non_exhaustive]` `DuckValue` — match with a `_` arm to stay forward-compatible.

| DuckDB type                    | Rust type                                                               |
| ------------------------------ | ----------------------------------------------------------------------- |
| `BOOLEAN`                      | `bool`                                                                  |
| `TINYINT` / `UTINYINT`         | `i8` / `u8`                                                             |
| `SMALLINT` / `USMALLINT`       | `i16` / `u16`                                                           |
| `INTEGER` / `UINTEGER`         | `i32` / `u32`                                                           |
| `BIGINT` / `UBIGINT`           | `i64` / `u64`                                                           |
| `HUGEINT` / `UHUGEINT`         | `i128` / `u128`                                                         |
| `FLOAT` / `DOUBLE`             | `f32` / `f64`                                                           |
| `DECIMAL` _(feature: decimal)_ | `rust_decimal::Decimal`                                                 |
| `VARCHAR` / `TEXT`             | `String`                                                                |
| `BLOB`                         | `better_duck_core::types::blob::Blob`                                   |
| `DATE` / `TIME` / `TIMESTAMP`  | `chrono::NaiveDate` / `NaiveTime` / `NaiveDateTime` _(chrono)_ / native |
| `TIMESTAMPTZ`                  | `chrono::DateTime<Utc>` _(chrono)_                                      |
| `TIME_TZ`                      | `date_chrono::TimeTz` _(chrono)_ / `DuckTimeTz` — UTC offset preserved  |
| `INTERVAL`                     | `chrono::Duration` _(chrono)_ / `std::time::Duration`                   |
| `LIST` / `ARRAY`               | `Vec<DuckValue>` / `Box<[DuckValue]>`                                   |
| `STRUCT` / `MAP`               | `HashMap<String, DuckValue>` / `HashMap<DuckValue, DuckValue>`          |
| `UNION`                        | active member (see roadmap for multi-arm write)                         |
| `ENUM`                         | `String` / a derived enum (`#[derive(DuckEnum)]`)                       |
| `UUID` / `BIT` / `BIGNUM`      | `DuckUuid` / `DuckBit` / `DuckBignum`                                   |

### User-defined functions _(feature: udf)_

Register plain Rust functions with attribute macros — parameter/return types inferred from the signature, no `unsafe`. See the [`macros` reference](#reference) below for the full set (`#[duckdb_scalar]`, `#[duckdb_table_function]`, `#[duckdb_aggregate]`, `#[duckdb_cast]`), named parameters, projection pushdown, scalar state, and replacement scans.

### Feature flags

| Feature             | Default | Description                                                                                |
| ------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `bundled`           | ✓       | Compile and embed the DuckDB C library (no system install needed)                          |
| `chrono`            | ✓       | `chrono` date/time conversions for DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL            |
| `decimal`           | ✓       | `rust_decimal::Decimal` support for DECIMAL columns                                        |
| `json`              | —       | Bundle DuckDB's JSON extension (requires `bundled`)                                        |
| `parquet`           | —       | Bundle DuckDB's Parquet extension (requires `bundled`)                                     |
| `async`             | —       | Tokio async facade (`AsyncConnection`, `AsyncDatabase`, `AsyncPool`)                       |
| `pool`              | —       | `r2d2` connection pool backed by a shared `Database` handle                                |
| `udf`               | —       | `#[duckdb_scalar]` / `#[duckdb_table_function]` / `#[duckdb_aggregate]` / `#[duckdb_cast]` |
| `derive`            | —       | `#[derive(FromRow/ToRow/DuckEnum/DuckStruct)]` + `transaction!` / `params!`                |
| `buildtime_bindgen` | —       | Regenerate FFI bindings at build time (requires LLVM/clang)                                |

</details>

<details>
<summary><b><code>better-duck-diesel</code></b> — Diesel 2.3 backend reference</summary>

### Connecting

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

let mut conn = DuckDbConnection::establish(":memory:")?;               // in-memory
let mut conn = DuckDbConnection::establish("/path/to/db.duckdb")?;     // on-disk
let mut conn = DuckDbConnection::establish("duckdb:///path/db.duckdb")?; // duckdb:// prefix stripped
```

### CRUD

Standard Diesel — INSERT (with `RETURNING`), SELECT with filter/order, UPDATE, DELETE all work:

```rust
diesel::insert_into(products::table)
    .values((products::id.eq(1), products::name.eq("widget"), products::price.eq(9.99)))
    .returning(products::id)
    .get_results::<i32>(&mut conn)?;

let rows: Vec<(i32, String, f64)> = products::table
    .filter(products::price.lt(20.0))
    .order(products::name.asc())
    .load(&mut conn)?;

diesel::update(products::table.filter(products::id.eq(1)))
    .set(products::price.eq(11.99))
    .execute(&mut conn)?;

diesel::delete(products::table.filter(products::id.eq(2))).execute(&mut conn)?;
```

### Transactions

```rust
conn.transaction(|conn| {
    diesel::insert_into(products::table)
        .values((products::id.eq(3), products::name.eq("doohickey"), products::price.eq(4.99)))
        .execute(conn)?;
    Ok::<_, diesel::result::Error>(()) // Err rolls back; Ok commits
})?;
```

### DuckDB-specific SQL types

Import types that have no standard Diesel equivalent via `sql_types`:

```rust
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    readings (id) {
        id     -> Integer,
        sensor -> DuckEnum,        // ENUM
        ts     -> DuckTimestamptz, // TIMESTAMPTZ
        tags   -> DuckList,        // LIST
    }
}
```

### DuckDB SQL function / operator / aggregate DSL

`use better_duck_diesel::dsl::*;` brings in a large library of DuckDB SQL that Diesel doesn't ship by
default — string/search/similarity, numeric & trig, regex, date/time, hashing, encoding, JSON, list &
map functions; general/statistical/approximate/bit aggregates; and operator methods like `.ilike()`,
`.similar_to()`, `.glob()`, and `.is_distinct_from()`.

```rust
use better_duck_diesel::dsl::*;
use diesel::prelude::*;

let hits: Vec<i32> = products::table
    .filter(products::name.ilike("wid%"))          // case-insensitive LIKE operator
    .select(products::id)
    .load(&mut conn)?;

let n: f64 = diesel::select(sqrt(16.0_f64)).get_result(&mut conn)?; // scalar fn → 4.0
```

Custom-typed values (`u64`, `i128`, `UUID`, `BIT`, `BIGNUM`, …) bind and load through the
`better_duck_diesel::values::*` newtypes (e.g. `values::UBigInt`, `values::Uuid`).

### Spatial _(feature: spatial)_

```rust
use better_duck_diesel::spatial::{ensure_loaded, st_as_text, st_point};

ensure_loaded(&mut conn)?; // INSTALL + LOAD the spatial extension if needed
let wkt: String = diesel::select(st_as_text(st_point(1.0, 2.0))).get_result(&mut conn)?; // "POINT (1 2)"
```

### Type mapping

**Standard Diesel types** (work out of the box):

| Diesel SQL type               | DuckDB type                   | Rust type                           |
| ----------------------------- | ----------------------------- | ----------------------------------- |
| `Bool`                        | `BOOLEAN`                     | `bool`                              |
| `SmallInt`                    | `SMALLINT`                    | `i16`                               |
| `Integer`                     | `INTEGER`                     | `i32`                               |
| `BigInt`                      | `BIGINT`                      | `i64`                               |
| `Float` / `Double`            | `FLOAT` / `DOUBLE`            | `f32` / `f64`                       |
| `Text`                        | `VARCHAR`                     | `String`                            |
| `Binary`                      | `BLOB`                        | `Vec<u8>`                           |
| `Date` / `Time` / `Timestamp` | `DATE` / `TIME` / `TIMESTAMP` | `chrono::*` _(chrono)_              |
| `Numeric`                     | `DECIMAL`                     | `rust_decimal::Decimal` _(decimal)_ |

**DuckDB-specific types** (`use better_duck_diesel::sql_types::*;`):

| Diesel SQL type                              | DuckDB type                          | Rust type                                       |
| -------------------------------------------- | ------------------------------------ | ----------------------------------------------- |
| `DuckTinyInt` / `DuckUTinyInt`               | `TINYINT` / `UTINYINT`               | `i8` / `u8`                                     |
| `DuckUSmallInt` / `DuckUInt` / `DuckUBigInt` | `USMALLINT` / `UINTEGER` / `UBIGINT` | `u16` / `u32` / `u64`                           |
| `DuckHugeInt` / `DuckUHugeInt`               | `HUGEINT` / `UHUGEINT`               | `i128` / `u128`                                 |
| `DuckTimestamptz` / `DuckInterval`           | `TIMESTAMPTZ` / `INTERVAL`           | `chrono::DateTime<Utc>` / `Duration` _(chrono)_ |
| `DuckTimeTz` / `DuckTimeNs`                  | `TIME WITH TIME ZONE` / `TIME_NS`    | `TimeTz` / `NaiveTime` _(chrono)_               |
| `DuckEnum`                                   | `ENUM`                               | `String` or `#[derive(DuckEnum)]`               |
| `DuckList` / `DuckArray`                     | `LIST` / `ARRAY`                     | `Vec<DuckValue>`                                |
| `DuckStruct` / `DuckMap` / `DuckUnion`       | `STRUCT` / `MAP` / `UNION`           | `HashMap<…>` / active member                    |
| `DuckUuid` / `DuckBit` / `DuckBignum`        | `UUID` / `BIT` / `BIGNUM`            | `DuckUuid` / `DuckBit` / `DuckBignum`           |

> [!NOTE]
> Date/time types map to `chrono` with the `chrono` feature, or to `better_duck_core::types::date_native`
> plain structs without it. Only one set is compiled at a time.

### Connection pooling

- **`r2d2`** — Diesel's own `diesel::r2d2::ConnectionManager<DuckDbConnection>`, plus a
  `SharedDuckDbConnectionManager` that shares one `Database` across the pool (so an in-memory pool
  observes one consistent database). Its `.on_connect(|c| …)` hook registers UDFs / loads extensions
  per pooled connection.

### Data derives with Diesel _(feature: derive)_

With `better-duck-diesel`'s `derive` feature, `#[derive(DuckEnum)]` and `#[derive(DuckStruct)]` also
emit Diesel `FromSql`/`ToSql`, so a derived Rust enum/struct binds and loads at its `sql_types::DuckEnum`
/ `sql_types::DuckStruct` column type (via `sql_query` + `QueryableByName`).

### Feature flags

| Feature   | Default | Description                                                                               |
| --------- | ------- | ----------------------------------------------------------------------------------------- |
| `bundled` | ✓       | Forwards to `better-duck-core/bundled`                                                    |
| `decimal` | ✓       | Diesel `Numeric` ↔ `rust_decimal::Decimal`                                                |
| `chrono`  | —       | Diesel date/time impls for DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIME_TZ, TIME_NS |
| `r2d2`    | —       | r2d2 connection pool support (`SharedDuckDbConnectionManager` + `diesel::r2d2`)           |
| `udf`     | —       | Expose the underlying core connection for registering UDFs on a diesel connection         |
| `derive`  | —       | Re-export the data derives with Diesel `FromSql`/`ToSql` emission                         |
| `spatial` | —       | `ST_*` DSL functions + `ensure_loaded` helper for the DuckDB spatial extension            |
| `json`    | —       | Bundle the JSON extension so the JSON DSL works offline                                   |

</details>

<details>
<summary><b><code>better-duck-macros</code></b> — UDFs, derives & helper macros reference</summary>

`better-duck-macros` is not a direct dependency — it's re-exported through `better-duck-core`'s
`udf` and `derive` features (and `better-duck-diesel`'s `derive` feature for ORM emission).

### User-defined function macros _(feature: udf)_

| Macro                      | Applies to                                | Generates                                           |
| -------------------------- | ----------------------------------------- | --------------------------------------------------- |
| `#[duckdb_scalar]`         | `fn(args) -> T`                           | a scalar function (one value per row)               |
| `#[duckdb_table_function]` | `fn(args) -> impl Iterator`               | a table function (rows + columns, usable in `FROM`) |
| `#[duckdb_aggregate]`      | `mod { init, update, combine, finalize }` | a custom aggregate                                  |
| `#[duckdb_cast]`           | `fn(from) -> to`                          | a `CAST` / `TRY_CAST` implementation                |

```rust
use better_duck_core::{connection::Connection, duckdb_scalar, duckdb_table_function};

#[duckdb_scalar]
fn repeat_str(s: &str, n: i32) -> String { s.repeat(n.max(0) as usize) }

#[duckdb_table_function(name = "series", columns("n"))]
fn series(start: i64, stop: i64) -> impl Iterator<Item = i64> + Send { start..stop }

let mut conn = Connection::open_in_memory()?;
repeat_str::register(&mut conn)?;
series::register(&mut conn)?;
conn.execute("SELECT sum(n) FROM series(1, 101)")?; // 5050
```

Parameter/return types are inferred from the signature. `Option<T>` propagates `NULL`; a
`Result<T, E>` return fails the query with `E`'s message. Table functions also support named
(keyword) parameters, projection pushdown, and scalar functions support shared state — read via the
`duck_projection!` / `duck_extra_info!` / `duck_state!` macros. Panics are caught and surfaced as
query errors under `panic = "unwind"`.

### Data derives _(feature: derive)_

| Derive                  | Maps                                | Direction    |
| ----------------------- | ----------------------------------- | ------------ |
| `#[derive(FromRow)]`    | struct ← query row (by column name) | read         |
| `#[derive(ToRow)]`      | struct → appender row / bind params | write        |
| `#[derive(DuckEnum)]`   | unit enum ↔ DuckDB `ENUM`           | read + write |
| `#[derive(DuckStruct)]` | struct ↔ DuckDB `STRUCT` value      | read + write |

```rust
use better_duck_core::{connection::Connection, DuckEnum, FromRow};

#[derive(DuckEnum, Debug, PartialEq, Clone, Copy)]
#[duck_enum(rename_all = "snake_case")]
enum Priority { Low, High }

#[derive(FromRow, Debug)]
struct Task { id: i32, priority: Priority }
```

Field/variant naming is controlled with `#[duck(rename = "…")]` / `#[duck(rename_all = "…")]`
(and `#[duck_enum(...)]` for `DuckEnum`). `ResultSet::to_structs::<T>()` reads a whole result into
`Vec<T>` for a `FromRow` type. With `better-duck-diesel`'s `derive` feature on, `DuckEnum`/`DuckStruct`
additionally emit Diesel `FromSql`/`ToSql`.

### Helper macros _(feature: derive)_

```rust
use better_duck_core::{params, transaction};

// commit-on-Ok / rollback-on-Err transaction scope (works for core and diesel connections)
transaction!(conn, {
    conn.execute_with("INSERT INTO t VALUES ($1, $2)", &mut params![1_i32, "hi".to_owned()])?;
    Ok(())
})?;
```

</details>

## Supported platforms

| Platform                      | Status           |
| ----------------------------- | ---------------- |
| Linux x86_64                  | ✓ CI-tested      |
| macOS Apple Silicon (aarch64) | ✓ CI-tested      |
| macOS x86_64                  | ✓ CI-tested      |
| Windows x86_64                | ✓ CI-tested      |
| iOS aarch64                   | ✓ CI cross-build |
| iOS Simulator x86_64          | ✓ CI cross-build |

---

## Roadmap

The library is usable today for most workloads. A condensed view of what's here and what's next —
contributions are very welcome.

**Available now**

- Full scalar + composite type coverage (`LIST`/`ARRAY`/`STRUCT`/`MAP`/`UNION`/`ENUM`), plus `UUID`,
  `BIT`, and `BIGNUM` end-to-end (core read/write + Diesel `FromSql`/`ToSql`).
- `TIME_TZ` timezone offset preserved on read and write; Diesel date/time with or without `chrono`.
- User-defined functions: scalar, table (named params, projection pushdown, state), aggregate, and
  cast — plus experimental replacement scans.
- Data derives (`FromRow`/`ToRow`/`DuckEnum`/`DuckStruct`) with optional Diesel emission.
- Async facade (`async`) and connection pooling (`pool` / `r2d2`).
- An extensive Diesel DSL of DuckDB SQL functions, aggregates, and operators, plus a `spatial` module.

**In progress**

- `GEOMETRY` / `VARIANT` / `ANY` / `INTEGER_LITERAL` reads (no value accessor in the DuckDB C API yet).
- Multi-arm `UNION` writes (the current write path builds single-member unions).
- `DECIMAL` declared-precision round-tripping (width is read but not yet carried on the value).
- Ordered-set aggregates (`WITHIN GROUP` quantiles) in the Diesel query builder.

**Exploratory**

- A `better-duck-tauri` plugin crate, and a `wasm32` build target.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for environment setup, git flow, commit conventions, how to add
a new DuckDB type, and the PR checklist. Found a bug or have a feature idea?
[Open an issue](https://github.com/nimdeveloper/better-duck/issues).

---

## License

Licensed under either of [MIT](LICENSE) or [Apache-2.0](LICENSE-APACHE), at your option.
