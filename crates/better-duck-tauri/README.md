# better-duck-tauri

A [Tauri v2](https://tauri.app) plugin that exposes [better-duck](https://crates.io/crates/better-duck-core)
(embedded DuckDB) to the webview as a local **analytics** engine — well beyond a CRUD bridge.

> **Status:** beta. Query/analytics/migration/security features are implemented and tested.
> `queryArrow` (Arrow IPC) and a Diesel-native connection pool are pending upstream `better-duck-core`
> work; Android CI is pending an NDK toolchain.

## Install

```toml
[dependencies]
better-duck-tauri = { version = "0.1.0-beta.7", features = ["backend-core"] }
```

Pick one backend feature: `backend-core` (default) or `backend-diesel`.

## Register

```rust
tauri::Builder::default()
    // simplest:
    .plugin(better_duck_tauri::core::init())
    // …or configure migrations, security, and checkpointing:
    .plugin(
        better_duck_tauri::core::Builder::new()
            .add_migrations("duckdb:app.db", migrations)
            .read_only(false)
            .allow_path("/data")
            .deny_risky_statements(true)
            .checkpoint_interval(std::time::Duration::from_secs(300))
            .checkpoint_on_exit(true)
            .on_connect(|c| { my_udf::register(c).map_err(|e| better_duck_tauri::Error::Backend(e.to_string())) })
            .build(),
    )
    .run(tauri::generate_context!())?;
```

Grant permissions in a capability file (`src-tauri/capabilities/*.json`). `better-duck-tauri:default`
allows `load`/`close`/read-only `select`; writes and raw SQL are opt-in:

```json
{ "permissions": ["better-duck-tauri:default", "better-duck-tauri:allow-execute", "better-duck-tauri:allow-raw-sql"] }
```

## Frontend API

```ts
import Database from '@better-duck/tauri'

const db = await Database.load('duckdb:analytics.db') // or 'duckdb::memory:'

// queries (row-oriented JSON)
await db.execute('INSERT INTO t VALUES ($1, $2)', [1, 'a'])
const rows = await db.select('SELECT * FROM t WHERE id > $1', [10])

// analytics
await db.importParquet('sales', 'data/2026-*.parquet')
await db.exportCsv('SELECT * FROM sales', 'out.csv')
await db.appendRows('events', [{ id: 1, kind: 'click' }])
await db.loadExtension('spatial')

// streaming + cancellation
const total = await db.stream('SELECT * FROM huge', rows => render(rows), [], 1000)
await db.interrupt() // cancel an in-flight stream

// introspection & maintenance
await db.tables()
await db.columns('sales')
await db.explain('SELECT * FROM sales')
await db.checkpoint(true) // FORCE CHECKPOINT
await db.revert()         // undo the last migration
```

Values map to JSON with full type coverage; precision-sensitive types (`HUGEINT`, `DECIMAL`, `UUID`,
`BIT`, `BIGNUM`) serialize as strings, composites (`LIST`/`STRUCT`/`MAP`/`UNION`) as arrays/objects,
temporals as ISO-8601.

## Migrations

Register per-connection migrations on the builder; pending `Up` migrations run on `Database.load`,
each in its own transaction (auto-rollback on failure). On-disk databases are protected by a
**crash-safe backup protocol**: the file is backed up before migrating and restored automatically if
a prior run was interrupted. `db.revert()` runs the last-applied migration's `Down` SQL.

```rust
use better_duck_tauri::{Migration, MigrationKind};
let migrations = vec![
    Migration { version: 1, description: "init".into(),
                sql: "CREATE TABLE t (id INTEGER)".into(), kind: MigrationKind::Up },
    Migration { version: 1, description: "init-down".into(),
                sql: "DROP TABLE t".into(), kind: MigrationKind::Down },
];
```

## Security

The webview is untrusted and DuckDB SQL can reach the filesystem/network, so lock things down at init:

- `read_only(true)` — open databases `READ_ONLY` and reject writes.
- `allow_connection(...)` / `allow_path(...)` — allow-lists for `load` and import/export paths.
- `deny_risky_statements(true)` — reject `ATTACH`/`INSTALL`/`LOAD`/`PRAGMA`/`COPY`/… in `select`/`execute`.
- `allow_network(true)` — required to load network extensions (`httpfs`/`aws`/`azure`); off by default.
- `execute` (writes/DDL) is out of the default set — grant it via `better-duck-tauri:allow-execute` or the `better-duck-tauri:allow-raw-sql` umbrella.

> [!WARNING]
> `select` is in the default set, but DuckDB `SELECT` can still reach the filesystem through
> table functions (`read_csv_auto`, `read_parquet`, `glob`, …) — the leading-keyword
> `deny_risky_statements` filter does not catch these. For an untrusted webview, pair `select`
> with `read_only(true)` and/or a connection allow-list; a fully sandboxed read-only mode
> (disabling DuckDB external file access) is a planned follow-up.

## Checkpoint strategies

Composable WAL→file checkpointing: `checkpoint_threshold` (DuckDB auto-checkpoint), `checkpoint_after_writes(n)`,
`checkpoint_interval(dur)` (background sweep), `checkpoint_on_exit(true)`, and the explicit `db.checkpoint()`.
Background sweeps use `FORCE CHECKPOINT` when `checkpoint_force(true)` is set.

## Backends

| Feature | Built on | Notes |
| --- | --- | --- |
| `backend-core` *(default)* | `better-duck-core` | Lightweight; inline migration engine. |
| `backend-diesel` | `better-duck-diesel` | Shares the core engine today; a Diesel-native pool + `embed_migrations!` is planned. |

## License

Licensed under either of MIT or Apache-2.0, at your option.
