# Setup, schema & CRUD

Establishing a `DuckDbConnection`, declaring a `table!` schema, and running the full
INSERT/SELECT/UPDATE/DELETE surface — all verified against `crates/better-duck-diesel/src/connection.rs`
and `crates/better-duck-diesel/tests/`.

## Adding the crate

```toml
[dependencies]
better-duck-diesel = "0.1.0-beta.8"   # pin exactly — pre-release
diesel = "2.3"
```

`diesel`'s derives (`Queryable`, `Insertable`, `QueryableByName`, …) come from `diesel` itself. This
crate only needs the backend. Opt into date/time with `features = ["chrono"]` (NOT on by default here),
`r2d2` for pooling, `udf`/`derive`/`spatial`/`json` as needed. See the skill's feature table.

## Establishing a connection

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

let mut conn = DuckDbConnection::establish(":memory:")?;                 // in-memory
let mut conn = DuckDbConnection::establish("/path/to/db.duckdb")?;       // on-disk
let mut conn = DuckDbConnection::establish("duckdb:///path/db.duckdb")?; // "duckdb://" prefix is stripped
```

`establish` is Diesel's `Connection::establish`. `":memory:"` opens a fresh in-memory database; any other
string (after stripping a `duckdb://` prefix) is treated as a file path.

Wrapping an existing core connection (e.g. to share one `Database` across a pool):

```rust
let conn = better_duck_core::database::Database::open_in_memory()?.connect()?;
let mut diesel_conn = DuckDbConnection::from_core(conn);
```

Reaching the underlying core connection (register UDFs, load extensions, run core-only APIs):

```rust
conn.inner_mut().load_extension("json")?;   // &mut better_duck_core::connection::Connection
let _core: &better_duck_core::connection::Connection = conn.inner();
```

## Running raw / DDL statements

`DuckDbConnection` implements `SimpleConnection`, so DDL and multi-statement batches go through
`batch_execute` (maps to core's `execute_batch`):

```rust
use diesel::connection::SimpleConnection;

conn.batch_execute(
    "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DOUBLE NOT NULL DEFAULT 0)",
)?;
```

For a single raw query with Diesel's `QueryableByName` mapping, use `diesel::sql_query(...)` —
this is also the path for DuckDB-specific types that bind via `QueryableByName` (see
`references/types-and-pooling.md`).

## Declaring a schema with `table!`

Standard Diesel. For DuckDB-specific column types, also `use better_duck_diesel::sql_types::*;` inside the
macro:

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

A table using only standard types needs no extra `use`:

```rust
diesel::table! {
    products (id) {
        id    -> Integer,
        name  -> Text,
        price -> Double,
    }
}
```

## INSERT

```rust
// single row
diesel::insert_into(products::table)
    .values((products::id.eq(1), products::name.eq("widget"), products::price.eq(9.99)))
    .execute(&mut conn)?;

// batch
let rows = vec![
    (products::id.eq(2), products::name.eq("gadget"), products::price.eq(4.99)),
    (products::id.eq(3), products::name.eq("gizmo"),  products::price.eq(14.0)),
];
let n = diesel::insert_into(products::table).values(&rows).execute(&mut conn)?; // n == 2
```

`execute` returns the affected-row count (`usize`).

## RETURNING

```rust
let id: i32 = diesel::insert_into(products::table)
    .values((products::id.eq(4), products::name.eq("doohickey"), products::price.eq(1.0)))
    .returning(products::id)
    .get_result(&mut conn)?;

let returned: Vec<(i32, String)> = diesel::insert_into(products::table)
    .values(&rows)
    .returning((products::id, products::name))
    .get_results(&mut conn)?;
```

## SELECT / load

```rust
let ids: Vec<i32> = products::table.order(products::id).select(products::id).load(&mut conn)?;

let (id, name): (i32, String) = products::table
    .select((products::id, products::name))
    .first(&mut conn)?;                                   // first row

let count: i64 = products::table.count().first(&mut conn)?;

let hits: Vec<(i32, String, f64)> = products::table
    .filter(products::price.lt(20.0))
    .order(products::name.asc())
    .load(&mut conn)?;
```

Map rows into a struct with Diesel's `#[derive(Queryable)]` as usual. (`better-duck-core`'s `FromRow`
derive is for the core driver's own row API, not Diesel's `Queryable`.)

## UPDATE

```rust
diesel::update(products::table.filter(products::id.eq(1)))
    .set(products::price.eq(11.99))
    .execute(&mut conn)?;

diesel::update(products::table.filter(products::id.eq(1)))
    .set((products::name.eq("widget-mk2"), products::price.eq(12.5)))
    .execute(&mut conn)?;
```

## DELETE

```rust
diesel::delete(products::table.filter(products::id.eq(2))).execute(&mut conn)?;
diesel::delete(products::table).execute(&mut conn)?; // all rows
```

## ON CONFLICT (upsert)

Verified in `tests/crud_dsl.rs`:

```rust
use diesel::upsert::excluded;

// DO NOTHING
diesel::insert_into(products::table)
    .values((products::id.eq(1), products::name.eq("dup"), products::price.eq(0.0)))
    .on_conflict(products::id)
    .do_nothing()
    .execute(&mut conn)?;

// DO UPDATE from the excluded (proposed) row
diesel::insert_into(products::table)
    .values((products::id.eq(1), products::name.eq("replacement"), products::price.eq(30.0)))
    .on_conflict(products::id)
    .do_update()
    .set((products::name.eq(excluded(products::name)), products::price.eq(excluded(products::price))))
    .execute(&mut conn)?;
```

## Transactions

Diesel's closure form — commits on `Ok`, rolls back on `Err` (or panic):

```rust
conn.transaction(|conn| {
    diesel::insert_into(products::table)
        .values((products::id.eq(5), products::name.eq("x"), products::price.eq(1.0)))
        .execute(conn)?;
    Ok::<_, diesel::result::Error>(()) // return Err to roll back
})?;
```

Return `Err(diesel::result::Error::RollbackTransaction)` to roll back explicitly without surfacing an
error. `begin_test_transaction` / `test_transaction` are implemented for test isolation.

> DuckDB has no savepoints (`SAVEPOINT`/`ROLLBACK TO` are unsupported — see the `duckdb` skill). The
> top-level `.transaction` closure drives `BEGIN`/`COMMIT`/`ROLLBACK` via SQL.

With `better-duck-core`'s `derive` feature, the **`transaction!(conn, { … })`** macro works against both
a core `Connection` and a `DuckDbConnection` (both expose `transaction(|c| -> Result<_>)`). See the
`better-duck-core` / `better-duck-macros` skills.

## Migrations

`DuckDbConnection` implements `diesel::migration::MigrationConnection`: `setup()` creates
`__diesel_schema_migrations` (a `VARCHAR(50)` primary key + a `TIMESTAMP DEFAULT current_timestamp`), so
`diesel migration run` works. The `embed_migrations!` / `MigrationHarness` machinery is Diesel's own.

## Error type

Backend errors surface as Diesel's `diesel::result::Error`; the underlying DuckDB error is wrapped by
`better_duck_diesel::result::DuckDbError` (carrying core's `EngineError` message). Catch them as
`QueryResult<_>` / `diesel::result::Error` like any Diesel backend.
