# better-duck

A safe, embedded-first Rust client for [DuckDB](https://duckdb.org) with optional [Diesel ORM](https://diesel.rs) support.

## Why better-duck?

- **No Arrow dependency** — zero-copy columnar I/O is not required for typical OLAP workloads.
- **Minimal footprint** — ships with an embedded DuckDB; no system library required.
- **Diesel ORM** — first-class Diesel 2.2 backend so your existing ORM code works.
- **Embedded-first** — designed for Tauri desktop apps and iOS cross-builds.
- **Safe API** — every FFI call is encapsulated; no `unsafe` exposed in the public API.

## Crates

| Crate | Description |
|---|---|
| `better-duck-core` | Low-level, no-ORM DuckDB wrapper. Connections, prepared statements, bulk appender. |
| `better-duck-diesel` | Diesel 2.2 backend. Enables the full Diesel query DSL against DuckDB. |

## Quick start

```toml
[dependencies]
better-duck-core   = "0.1"
# Optional: Diesel ORM backend
better-duck-diesel = "0.1"
```

### 1 — Core API: open, insert, query

```rust
use better_duck_core::connection::Connection;

fn main() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;

    conn.execute_batch("CREATE TABLE users (id INTEGER, name TEXT)")?;
    conn.execute("INSERT INTO users VALUES (1, 'Alice')")?;

    let mut result = conn.execute("SELECT id, name FROM users")?;
    for row in result {
        let row = row?;
        println!("{:?}", row);
    }
    Ok(())
}
```

### 2 — Bulk insert via the Appender

```rust
use better_duck_core::{connection::Connection, types::appendable::AppendAble};

struct User(i32, String);
// implement AppendAble for User (see crate docs)

fn main() -> better_duck_core::error::Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE users (id INTEGER, name TEXT)")?;
    let mut app = conn.appender("users", "main")?;
    let mut row = User(1, "Alice".into());
    app.append(&mut row)?;
    app.save()?;
    Ok(())
}
```

### 3 — Diesel DSL

```rust
use better_duck_diesel::DuckDbConnection;
use diesel::{prelude::*, Connection};

diesel::table! {
    users (id) { id -> Integer, name -> Text }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = DuckDbConnection::establish(":memory:")?;
    conn.batch_execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")?;
    let rows: Vec<(i32, String)> = users::table.select((users::id, users::name))
        .load(&mut conn)?;
    println!("{rows:?}");
    Ok(())
}
```

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `bundled` | ✓ | Embed DuckDB (no system library needed) |
| `chrono` | ✓ | `chrono` date/time conversions |
| `decimal` | ✓ | `rust_decimal` support |
| `r2d2` | — | R2D2 connection pool support (diesel crate only) |

## Diesel SQL type mapping

| DuckDB type | Diesel SQL type | Rust type |
|---|---|---|
| `TINYINT` | `DuckTinyInt` | `i8` |
| `UTINYINT` | `DuckUTinyInt` | `u8` |
| `USMALLINT` | `DuckUSmallInt` | `u16` |
| `UINTEGER` | `DuckUInt` | `u32` |
| `UBIGINT` | `DuckUBigInt` | `u64` |
| `HUGEINT` | `DuckHugeInt` | `i128` |
| `UHUGEINT` | `DuckUHugeInt` | `u128` |
| `TIMESTAMPTZ` | `DuckTimestamptz` | `NaiveDateTime` (UTC) |
| `INTERVAL` | `DuckInterval` | `chrono::Duration` |
| `LIST` | `DuckList` | — (type marker) |

Standard Diesel types (`Integer`, `BigInt`, `Text`, `Timestamp`, `Date`, `Time`, `Binary`,
`Bool`, `Float`, `Double`, `Numeric`) map to the corresponding DuckDB column types directly.

## Migrating from the community `duckdb` crate

| Operation | `duckdb` crate | `better-duck` |
|---|---|---|
| Open in-memory | `Connection::open_in_memory()?` | `Connection::open_in_memory()?` |
| Execute DDL | `conn.execute_batch(sql)?` | `conn.execute_batch(sql)?` |
| Insert / DML | `conn.execute(sql, [])?` | `conn.execute(sql)?.changes()` |
| SELECT rows | `conn.prepare(sql)?.query([])` | `conn.execute(sql)?` (Iterator) |
| Bulk insert | `conn.appender(tbl)?` | `conn.appender(tbl, schema)?` |

## Supported platforms

| Platform | Status |
|---|---|
| Linux x86_64 | ✓ CI-tested |
| macOS (Apple Silicon / x86_64) | ✓ CI-tested |
| Windows x86_64 | ✓ CI-tested |
| iOS aarch64 | ✓ CI cross-build |
| iOS Simulator x86_64 | ✓ CI cross-build |

## License

Licensed under either of:

- [MIT License](LICENSE)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
