# Connections, Database & Config

`better_duck_core::connection::Connection` is the main handle. `better_duck_core::database::Database`
is a shared, cloneable database handle you `connect()` from. `Config` tunes DuckDB at open time.

## Contents

- [Opening a connection](#opening-a-connection)
- [Cloning vs independent connections](#cloning-vs-independent-connections)
- [Closing](#closing)
- [Config](#config)
- [Library version](#library-version)
- [Interrupting a running query](#interrupting-a-running-query)

## Opening a connection

```rust
use better_duck_core::connection::Connection;

let mut conn = Connection::open_in_memory()?;              // private in-memory DB
let mut conn = Connection::open("app.duckdb")?;            // on-disk (created if absent)
```

- `open<P: AsRef<Path>>(path)` / `open_in_memory()` — the common entry points. Each returns a fresh
  `Connection`. Both are `#[must_use]`.
- `open_with_flags(path, config)` / `open_in_memory_with_flags(config)` — same, with a `Config`.
- A path with an interior NUL byte is rejected as `Error::NulError`.
- `Connection` is `Send` (safe to move between threads) but not `Sync`; SQL methods take `&mut self`.

Independent vs shared: **each** `Connection::open_in_memory()` call creates its own, isolated
in-memory database. Two such connections do **not** see each other's tables. To share one database
(including in-memory) across several connections, open a `Database` and `connect()` from it.

```rust
use better_duck_core::database::Database;

let db = Database::open_in_memory()?;   // or Database::open("app.duckdb")?
let mut a = db.connect()?;
let mut b = db.connect()?;              // sees whatever `a` writes
a.execute_batch("CREATE TABLE t (id INTEGER)")?;
b.execute_batch("INSERT INTO t VALUES (1)")?;   // ok: same database
```

`Database` is `Clone` (cheap `Arc` bump), `Send + Sync`, and outlives the connections made from it —
a connection keeps the backing database alive even after every `Database` handle is dropped.
`Database::{open, open_in_memory, open_with_flags, open_in_memory_with_flags, connect}` mirror the
`Connection` constructors.

## Cloning vs independent connections

- `Connection::try_clone()` → a second **independent** connection to the **same** database (one
  `duckdb_connect`, no file I/O). Both observe the same data. Use this to get a reader while an
  `Appender` or a long query holds the original.
- `Connection::database()` → a `Database` handle for the backing database, so you can `connect()`
  more connections to it later.
- `Connection::open_from_raw(db)` (`unsafe`) wraps a borrowed `duckdb_database` owned by a host
  process — for code running inside a loadable `.duckdb_extension`; it never closes that database.

```rust
let mut writer = Connection::open_in_memory()?;
writer.execute_batch("CREATE TABLE t (id INTEGER)")?;
let mut reader = writer.try_clone()?;     // same DB, separate connection
```

## Closing

A `Connection` closes on drop. `Connection::close(self)` closes explicitly and is `#[must_use]`; it
errors (and stays open) if appenders created from it are still alive, closing once the last is
dropped. `is_open()` always returns `true` (a closed connection no longer exists as a value).

## Config

`better_duck_core::config::Config` is a builder (also re-exported as `better_duck_core::Config`).
Each setter returns `Result<Config>`; chain with `?`. Pass it to `*_with_flags`.

```rust
use better_duck_core::config::{Config, AccessMode};

let config = Config::default()
    .threads(4)?
    .max_memory("1GB")?
    .access_mode(AccessMode::ReadOnly)?
    .default_order(better_duck_core::config::DefaultOrder::Desc)?
    .with("custom_setting", "value")?;    // arbitrary key/value escape hatch
let mut conn = Connection::open_with_flags("app.duckdb", config)?;
```

Typed setters: `threads(i64)`, `max_memory(&str)`, `access_mode(AccessMode)`
(`Automatic`/`ReadOnly`/`ReadWrite`), `default_order(DefaultOrder)` (`Asc`/`Desc`),
`default_null_order(DefaultNullOrder)` (`NullsFirst`/`NullsLast`), `enable_external_access(bool)`,
`enable_object_cache(bool)`, `enable_autoload_extension(bool)`, `allow_unsigned_extensions()`,
`custom_user_agent(&str)`. The generic `with(key, value)` / internal setter accept any DuckDB
setting name; DuckDB errors on an unknown or invalid one. An interior NUL in a key or value is
rejected as `Error::NulError`. `Config` is `Send`.

Discover flags at runtime: `Config::flag_count()`, `Config::flag(index) -> Option<ConfigFlag>`
(`{ name, description }`), and `Config::flags()` (an iterator over all of them).

## Library version

```rust
let v: String = better_duck_core::config::library_version();  // e.g. "v1.5.5"
// also re-exported as better_duck_core::library_version
```

Returns the linked DuckDB version string.

## Interrupting a running query

`Connection::query_control()` → a `QueryControl` (also at `better_duck_core::QueryControl`), a
thread-safe handle minted **before** starting a query. From another thread call
`QueryControl::interrupt()` to cancel the running query, or `QueryControl::progress()` for a
`QueryProgress`. The control is generation-scoped: once that query finishes it goes stale and cannot
affect the next query. The `async` facade uses this for cancel-on-drop — see
[async-pool-arrow](async-pool-arrow.md).
