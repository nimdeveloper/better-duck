# Types, pooling & ORM derives

The Rust ↔ DuckDB type mapping for Diesel, the DuckDB-specific `sql_types`, the `values::*` binding
newtypes, connection pooling, and the `derive`-feature ORM emission. Verified against
`crates/better-duck-diesel/src/types/duckdb_types.rs`, `src/values.rs`, `src/pool.rs`, and the tests.

## Standard Diesel types (work out of the box)

No extra imports — these are Diesel's own SQL types, with backend impls provided here.

| Diesel SQL type | DuckDB type | Rust type |
|---|---|---|
| `Bool` | `BOOLEAN` | `bool` |
| `SmallInt` | `SMALLINT` | `i16` |
| `Integer` | `INTEGER` | `i32` |
| `BigInt` | `BIGINT` | `i64` |
| `Float` / `Double` | `FLOAT` / `DOUBLE` | `f32` / `f64` |
| `Text` | `VARCHAR` | `String` |
| `Binary` | `BLOB` | `Vec<u8>` |
| `Date` / `Time` / `Timestamp` | `DATE` / `TIME` / `TIMESTAMP` | `chrono::*` *(feature `chrono`)* |
| `Numeric` | `DECIMAL` | `rust_decimal::Decimal` *(feature `decimal`, default)* |

## DuckDB-specific SQL types

`use better_duck_diesel::sql_types::*;` — use these inside `table!` and in `.bind::<T, _>` /
`.into_sql::<T>()`. Each is a zero-size type implementing Diesel's `SqlType` + `QueryId`.

| `sql_types::*` | DuckDB type | Rust / loads as |
|---|---|---|
| `DuckTinyInt` | `TINYINT`/`INT1` | `i8` |
| `DuckUTinyInt` | `UTINYINT` | `u8` |
| `DuckUSmallInt` | `USMALLINT` | `u16` |
| `DuckUInt` | `UINTEGER` | `u32` |
| `DuckUBigInt` | `UBIGINT` | `u64` |
| `DuckHugeInt` | `HUGEINT` | `i128` |
| `DuckUHugeInt` | `UHUGEINT` | `u128` |
| `DuckTimestamptz` | `TIMESTAMPTZ` | `chrono::DateTime<Utc>` *(chrono)* |
| `DuckInterval` | `INTERVAL` | `chrono::Duration` *(chrono)* / `std::time::Duration` |
| `DuckTimeTz` | `TIME WITH TIME ZONE` | `TimeTz` (offset preserved) *(chrono)* |
| `DuckTimeNs` | `TIME_NS` | `NaiveTime` (nanosecond) *(chrono)* |
| `DuckEnum` | `ENUM` | `String` or `#[derive(DuckEnum)]` |
| `DuckList` | `LIST` | `Vec<DuckValue>` |
| `DuckArray` | `ARRAY` | fixed-length `Vec<DuckValue>` |
| `DuckStruct` | `STRUCT` | `HashMap<…>` or `#[derive(DuckStruct)]` |
| `DuckMap` | `MAP` | key→value collection |
| `DuckUnion` | `UNION` | active member value |
| `DuckUuid` | `UUID` | `values::Uuid` (wraps `DuckUuid`) |
| `DuckBit` | `BIT` | `values::Bit` (wraps `DuckBit`) |
| `DuckBignum` | `BIGNUM` | `values::Bignum` (wraps `DuckBignum`) |

> Date/time types map to `chrono` with the `chrono` feature, or to
> `better_duck_core::types::date_native` plain structs without it. Only one set compiles at a time.

## The `values::*` binding newtypes

Diesel's blanket `impl<T: Expression> AsExpression<T::SqlType> for T` means a third-party crate can't make
a foreign value type (`i128`, `u64`, `DuckUuid`, …) usable as an expression at a custom `Duck*` SQL type —
coherence rejects it. The thin local newtypes in `better_duck_diesel::values::*` fix this: wrap the value
and it becomes bindable (`.eq(…)`, `.values(…)`, function arg, `.into_sql::<T>()`) and loadable
(`Queryable`). The standard widths (`i16`/`i32`/`i64`/`f32`/`f64`) already work directly and have no
wrapper.

| `values::*` | Inner | SQL type |
|---|---|---|
| `TinyInt(i8)` | `i8` | `DuckTinyInt` |
| `UTinyInt(u8)` | `u8` | `DuckUTinyInt` |
| `USmallInt(u16)` | `u16` | `DuckUSmallInt` |
| `UInt(u32)` | `u32` | `DuckUInt` |
| `UBigInt(u64)` | `u64` | `DuckUBigInt` |
| `HugeInt(i128)` | `i128` | `DuckHugeInt` |
| `UHugeInt(u128)` | `u128` | `DuckUHugeInt` |
| `Uuid(DuckUuid)` | `better_duck_core::types::DuckUuid` | `DuckUuid` |
| `Bit(DuckBit)` | `better_duck_core::types::DuckBit` | `DuckBit` |
| `Bignum(DuckBignum)` | `better_duck_core::types::DuckBignum` | `DuckBignum` |

Each has `From<inner>` / `From<newtype> for inner` both ways. Round-trip (verified in
`tests/values_dsl.rs`):

```rust
use better_duck_diesel::values::HugeInt;
use better_duck_diesel::sql_types::DuckHugeInt;
use diesel::{prelude::*, IntoSql};

let v = HugeInt(i128::MAX);
let got: HugeInt = diesel::select(v.into_sql::<DuckHugeInt>()).get_result(&mut conn)?;
assert_eq!(got, v);
```

## Connection pooling (`feature = "r2d2"`)

Two managers, usable side by side:

**1. Diesel's generic manager** — `diesel::r2d2::ConnectionManager<DuckDbConnection>`. Opens a fresh
`duckdb_open_ext` per pooled connection; for a `:memory:` pool, that means **each connection is its own
independent, empty database**. Fine for file-backed databases where all connections point at the same
file.

**2. `SharedDuckDbConnectionManager`** (`better_duck_diesel::pool`) — shares one
`better_duck_core::database::Database` across the pool, so even an in-memory pool observes **one
consistent database**. Prefer this for in-memory pools.

```rust
use better_duck_diesel::pool::SharedDuckDbConnectionManager;
use diesel::r2d2::Pool;

let manager = SharedDuckDbConnectionManager::memory()?;          // shared in-memory
// or ::file("app.duckdb")? / ::new(existing_database)
let pool = Pool::builder().max_size(4).build(manager)?;
let mut conn = pool.get()?;                                      // a DuckDbConnection
```

Constructors: `::new(Database)`, `::memory() -> Result<_>`, `::file(path) -> Result<_>`; `.database()`
returns the shared `Database`.

**Per-connection setup with `.on_connect`** — UDF registration and `LOAD`ing extensions live on a single
`duckdb` connection, not the shared database, so they must run on every pooled connection. The
`on_connect` hook is the place:

```rust
let manager = SharedDuckDbConnectionManager::memory()?
    .on_connect(|conn| conn.inner_mut().load_extension("json")); // runs per new connection
```

The closure receives `&mut DuckDbConnection` and returns `better_duck_core::error::Result<()>`; an `Err`
fails opening that connection. Use it to `register` your `#[duckdb_*]` UDFs (see `references/sql-dsl.md`)
so every connection in the pool can call them in the DSL. `DuckDbConnection` also implements
`R2D2Connection` (`.ping()` runs `SELECT 1`; `.is_broken()` checks the transaction manager).

## ORM-emitting data derives (`feature = "derive"`)

With this crate's `derive` feature on, `better-duck-core`'s data derives are re-exported from
`better_duck_diesel` **with Diesel `FromSql`/`ToSql` emission enabled**, so the same Rust type works
through the ORM at its `sql_types::DuckEnum` / `sql_types::DuckStruct` column type. The ORM path for these
DuckDB-specific types is `sql_query` + `QueryableByName` (not the plain `Queryable` builder).

```rust
use better_duck_diesel::{DuckDbConnection, DuckEnum};
use better_duck_diesel::sql_types::DuckEnum as DuckEnumSql;
use diesel::prelude::*;
use diesel::sql_types::Integer;

#[derive(DuckEnum, Debug, PartialEq, Clone, Copy)]
enum Priority { Low, High }

#[derive(QueryableByName, Debug, PartialEq)]
struct Row {
    #[diesel(sql_type = Integer)]      id: i32,
    #[diesel(sql_type = DuckEnumSql)]  p: Priority,
}

// bind (ToSql)
diesel::sql_query("INSERT INTO tasks VALUES (1, $1)")
    .bind::<DuckEnumSql, _>(Priority::High)
    .execute(&mut conn)?;

// read (FromSql via QueryableByName)
let rows: Vec<Row> =
    diesel::sql_query("SELECT id, p FROM tasks ORDER BY id").get_results(&mut conn)?;
```

`#[derive(DuckStruct)]` works the same way at `sql_types::DuckStruct`. Re-exports available under the
feature: `FromRow`, `ToRow`, `DuckEnum`, `DuckStruct`. Field/variant renaming honors
`#[duck(rename = "…")]` / `#[duck(rename_all = "…")]` (see the `better-duck-macros` skill). Without the
`derive` feature, those derives still exist in core but don't emit the Diesel `FromSql`/`ToSql` impls.
