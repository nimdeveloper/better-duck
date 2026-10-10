# Migrations

The plugin runs schema migrations per connection string, triggered on `Database.load`. Two engines,
one model: `backend-core` uses an inline, `tauri-plugin-sql`-style engine; `backend-diesel` uses
Diesel's `embed_migrations!` + `MigrationHarness`.

## Core backend (`backend-core`)

Define `Migration` values and register them on the builder keyed by connection string:

```rust
use better_duck_tauri::{core::Builder, Migration, MigrationKind};

let migrations = vec![
    Migration { version: 1, description: "init".into(),
                sql: "CREATE TABLE t (id INTEGER)".into(), kind: MigrationKind::Up },
    Migration { version: 1, description: "init-down".into(),
                sql: "DROP TABLE t".into(), kind: MigrationKind::Down },
];

Builder::new().add_migrations("duckdb:app.db", migrations).build();
```

`Migration` fields:
- `version: i64` — the ordering + identity key.
- `description: String` — recorded alongside the applied version.
- `sql: String` — may contain multiple `;`-separated statements (run via `execute_batch`).
- `kind: MigrationKind` — `Up` (schema-forward) or `Down` (revert).

`add_migrations(conn_str, Vec<Migration>)` is repeatable per connection string (entries extend).

### How it runs

On `load`, pending `Up` migrations run in **ascending `version` order**, each inside its own
transaction (a failing migration rolls that one back and surfaces the error; already-committed
earlier migrations stay — the backup protocol below is the whole-batch safety net). Applied versions
are tracked in a `__better_duck_migrations` table (`version BIGINT PRIMARY KEY`, `description`,
`applied_at`). Re-running is idempotent: already-applied versions are skipped.

`db.revert()` (the `revert` command) runs the `Down` SQL of the **highest applied version**, in one
transaction, and deletes its bookkeeping row — returning the reverted version (or `null` if nothing
was applied). It errors if the last-applied version has no matching `Down` migration. Revert is a
write, so it is rejected under `read_only(true)`.

### Crash-safe backup protocol (on-disk databases)

On-disk migrations go through a backup-in-place protocol, because the per-migration transactions
mean a mid-batch failure could leave earlier migrations committed. The sequence (only when something
is pending):

1. If a `<db>.migrate-journal` exists from a prior interrupted run, restore from `<db>.bak` first.
2. `CHECKPOINT` to fold the WAL into the main file.
3. Copy the file to `<db>.bak` (fsynced) and write the journal (records "a migration is in flight").
4. Run the migrations. On success, delete the journal then the backup. On failure, restore from the
   backup and surface the error.

Sidecar files live beside the database: `<db>.bak`, `<db>.migrate-journal` (and `<db>.wal`,
`<db>.tmp` are cleaned on restore). This runs **before** the long-lived connection handle is opened,
so migration has exclusive file access.

Special cases:
- **In-memory** databases (`duckdb::memory:`) have no file: migrations run directly on the registered
  handle (no backup protocol).
- **`read_only(true)`** skips migrations entirely (they are writes).

## Diesel backend (`backend-diesel`)

Register Diesel embedded migrations instead of a `Vec`:

```rust
use better_duck_tauri::diesel::Builder;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

Builder::new().add_migrations("duckdb:app.db", MIGRATIONS).build();
```

On `load`, pending migrations run through Diesel's `MigrationHarness`
(`run_pending_migrations`), wrapped in the **same** crash-safe backup skeleton as the core engine
(backup → journal → run → restore-on-failure). `db.revert()` calls the harness's
`revert_last_migration` and best-effort parses the Diesel version string (numeric timestamp) to an
`i64`. In-memory migrations run on the shared pool handle; `read_only` skips them.

See the `better-duck-diesel` sibling skill for authoring the embedded migration directory and the
Diesel schema/DSL side.
