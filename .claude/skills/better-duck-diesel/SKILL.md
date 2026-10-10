---
name: better-duck-diesel
description: >-
  The Diesel 2.3 ORM backend for DuckDB, built on better-duck. Use when working with the
  `better-duck-diesel` crate: establishing a `DuckDbConnection`, defining a `table!` schema, running
  INSERT/SELECT/UPDATE/DELETE/RETURNING or transactions through Diesel, mapping Rust types to DuckDB
  column types, or reaching DuckDB-only SQL that Diesel core omits via `use better_duck_diesel::dsl::*;`
  (ILIKE/SIMILAR TO/GLOB/regex operators; string, numeric, regex, date/time, JSON, list and map
  functions; general/statistical/approximate/bit aggregates). Also covers calling your `#[duckdb_*]`
  UDFs through the DSL with no raw SQL (`feature = "udf"`), spatial `ST_*` functions
  (`feature = "spatial"`), ORM-emitting data derives (`feature = "derive"`), and r2d2 pooling.
  Triggers: `better-duck-diesel`, Diesel + DuckDB, `DuckDbConnection`, `better_duck_diesel::dsl`,
  `feature = "udf"|"derive"|"spatial"`.
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# better-duck-diesel

`better-duck-diesel` is a third-party [Diesel](https://diesel.rs) 2.3 backend for DuckDB. It plugs the
[`better-duck-core`](../better-duck/SKILL.md) driver into Diesel's typed query builder, so you connect,
map a schema with `table!`, and build `INSERT`/`SELECT`/`UPDATE`/`DELETE` queries as Rust — while still
reaching the DuckDB-specific SQL (operators, scalar functions, aggregates, types) that stock Diesel has
no equivalent for.

Sibling skills: **`better-duck`** (the crate map / router), **`better-duck-core`** (the raw Rust driver
API), **`better-duck-macros`** (the `#[duckdb_*]` UDF and `FromRow`/`DuckEnum` derive macros), and
**`duckdb`** (SQL dialect + engine behavior). Reach for `better-duck-core` whenever you need something
below the ORM (the appender, columnar chunks, async, raw parameter binding).

DuckDB is compiled and embedded through core — no system install. This crate's default feature set is
`["decimal"]` only; note that **`chrono` is NOT on by default here** (unlike core), so turn it on for
date/time column support.

## 30-second quickstart

```toml
[dependencies]
better-duck-diesel = "0.1.0-beta.8"
diesel = "2.3"
```

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    products (id) {
        id    -> Integer,
        name  -> Text,
        price -> Double,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `:memory:` or a file path; a leading `duckdb://` is stripped.
    let mut conn = DuckDbConnection::establish(":memory:")?;
    conn.batch_execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DOUBLE NOT NULL)",
    )?;

    diesel::insert_into(products::table)
        .values((products::id.eq(1), products::name.eq("widget"), products::price.eq(9.99)))
        .execute(&mut conn)?;

    let rows: Vec<(i32, String, f64)> = products::table
        .filter(products::price.lt(20.0))
        .order(products::name.asc())
        .load(&mut conn)?;
    assert_eq!(rows, [(1, "widget".to_string(), 9.99)]);
    Ok(())
}
```

`DuckDbConnection` is a full Diesel `Connection` + `LoadConnection`: `.transaction(|c| …)`, prepared-
statement caching, instrumentation, `r2d2`, and `diesel migration` (it implements `MigrationConnection`)
all work. `.inner()` / `.inner_mut()` expose the underlying core `Connection` (for registering UDFs,
loading extensions, etc.).

## Topic map

| You want to… | Go to |
|---|---|
| Add the crate, open a `DuckDbConnection`, write a `table!` schema | `references/setup-crud.md` |
| INSERT / SELECT / UPDATE / DELETE, `RETURNING`, `ON CONFLICT`, transactions, migrations | `references/setup-crud.md` |
| DuckDB-only operators (`.ilike()`, `.glob()`, `.is_distinct_from()`, …) | `references/sql-dsl.md` |
| DuckDB scalar functions / aggregates the DSL exposes (`sqrt`, `levenshtein`, `string_agg`, `approx_quantile`, …) | `references/sql-dsl.md` |
| Call your `#[duckdb_scalar]` / `#[duckdb_aggregate]` UDF through the DSL (`feature = "udf"`) | `references/sql-dsl.md` |
| Spatial `ST_*` functions + `ensure_loaded` (`feature = "spatial"`) | `references/sql-dsl.md` |
| Rust ↔ DuckDB type mapping, `sql_types::Duck*`, the `values::*` newtypes | `references/types-and-pooling.md` |
| Connection pooling (`r2d2`, `SharedDuckDbConnectionManager`, `on_connect`) | `references/types-and-pooling.md` |
| ORM-emitting data derives — `DuckEnum`/`DuckStruct` through Diesel (`feature = "derive"`) | `references/types-and-pooling.md` |

## The two SQL surfaces

Diesel core already gives you the portable surface: `=`/`<`/`>`, `LIKE`, `count`/`sum`/`avg`/`min`/`max`,
`.filter`/`.order`/`.limit`, joins, `insert_into`/`update`/`delete`, `RETURNING`, `ON CONFLICT`. Use it
as-is — those work on `DuckDbConnection` with no extra imports.

The DuckDB-only surface lives behind one import:

```rust
use better_duck_diesel::dsl::*;   // alongside `use diesel::prelude::*;`
```

That prelude brings in two operator extension traits (`DuckExpressionMethods`,
`DuckTextExpressionMethods`) plus a large library of free functions and aggregates — all typed, all
emitting real DuckDB SQL, none requiring `sql_query`. See `references/sql-dsl.md` for the verified
inventory. (Spatial `ST_*` functions are **not** in this prelude — import them from
`better_duck_diesel::spatial::*` under `feature = "spatial"`.)

## Feature flags (this crate)

Verified against `crates/better-duck-diesel/Cargo.toml`.

| Feature | Default | Enables |
|---|---|---|
| `decimal` | ✓ | Diesel `Numeric` ↔ `rust_decimal::Decimal` (forwards to core `decimal`) |
| `chrono` | — | Diesel date/time impls (DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIME_TZ, TIME_NS) |
| `r2d2` | — | r2d2 pooling: `diesel::r2d2` support + `pool::SharedDuckDbConnectionManager` |
| `udf` | — | expose the core connection for registering `#[duckdb_*]` UDFs, callable in the DSL |
| `spatial` | — | the `spatial` module: `ST_*` DSL functions + `ensure_loaded` |
| `json` | — | bundle DuckDB's JSON extension so the JSON DSL works offline (forwards to core `json`) |
| `derive` | — | re-export `FromRow`/`ToRow`/`DuckEnum`/`DuckStruct` with Diesel `FromSql`/`ToSql` emission on |

Notes:
- There is **no `bundled` feature** on this crate (the README's feature table lists one, but
  `Cargo.toml` does not define it). DuckDB is embedded through core regardless.
- `spatial` carries no extra Rust deps — it only gates the module; the `ST_*` functions are plain SQL,
  and `ensure_loaded` installs/loads the extension at runtime (needs network unless already present).
- For async, Arrow, Polars, columnar chunks, or the appender, drop to **`better-duck-core`** — those are
  not part of the Diesel surface.

## Conventions

- Pre-release versions are excluded by Cargo's default match — pin exactly, e.g.
  `better-duck-diesel = "0.1.0-beta.8"`.
- Always import `diesel::prelude::*`; add `use better_duck_diesel::dsl::*;` only when you need the
  DuckDB-specific SQL.
- DuckDB-specific column types come from `better_duck_diesel::sql_types::*` (used inside `table!`).
- Values of custom-typed widths (`u64`, `i128`, `UUID`, `BIT`, `BIGNUM`, …) bind/load through the
  `better_duck_diesel::values::*` newtypes — bare `i128`/`u64` cannot implement `AsExpression` at a
  foreign SQL type from a third-party crate (coherence).
- DuckDB has no savepoints; Diesel's nested `.transaction` still works via `BEGIN`/`COMMIT`/`ROLLBACK`
  at the top level. See the `duckdb` skill for engine-level transaction details.
