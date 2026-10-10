# Backends & checkpoint strategies

## Choosing a backend

The plugin builds on exactly one backend, selected by a Cargo feature. Both expose the **identical**
webview command API — the choice is purely about the Rust side (migrations, pooling, how connections
are managed). Building with neither feature is a `compile_error!`.

| Feature | Built on | Entry point | Migrations | Connection model |
|---|---|---|---|---|
| `backend-core` *(default)* | `better-duck-core` | `core::init()` / `core::Builder` | inline engine, `Vec<Migration>` | registry of shared `Database` handles; a short-lived `Connection` per operation |
| `backend-diesel` | `better-duck-diesel` | `diesel::init()` / `diesel::Builder` | Diesel `embed_migrations!` (`EmbeddedMigrations`) | r2d2 pool of `DuckDbConnection` per connection string |

Both engines implement the `DuckBackend` trait; commands are written against the trait and
dispatched through managed state. In the Diesel engine, dynamic-column SQL (the arbitrary statements
+ JSON params the plugin forwards) runs on the pooled connection's underlying core `Connection` via
`inner_mut()`, reusing the same `DuckValue`⇄JSON materialization as the core engine — Diesel's
compile-time `QueryableByName` can't express the plugin's dynamic result shape.

`on_connect` hook timing differs: the core engine runs the hooks on every checked-out connection; the
Diesel engine attaches them to the r2d2 manager so they run **once per physical connection** (r2d2
reuses connections, and re-registering a UDF on an initialized connection would fail).

> The README notes beta edges: a Diesel-native pool and `embed_migrations!` are described as the
> forward direction, and `queryArrow` + Android CI are the pending items. The `diesel::init()` path
> and the pool/harness wiring are present in the crate; verify against the exact release you pin.

### Choosing

- Default to **`backend-core`**: lighter dependency tree, inline migrations.
- Use **`backend-diesel`** when the app already uses `better-duck-diesel` (shared schema / DSL /
  `embed_migrations!`) and wants a connection pool. See the `better-duck-diesel` sibling skill.

## Backup

Two distinct mechanisms, don't confuse them:

- **Migration backup** (automatic, on-disk only): the crash-safe backup-in-place protocol around
  migrations — `<db>.bak` + `<db>.migrate-journal`, restored on an interrupted prior run. Covered in
  `references/migrations.md`.
- **Checkpointing** (below): folding the WAL into the main database file.

## Checkpoint strategies (WAL → main file)

DuckDB writes go to a WAL and are folded into the main file on checkpoint. All strategies are
**composable** (configured on the builder; default is fully passive — rely on DuckDB's own WAL-size
auto-checkpoint). `CHECKPOINT` fails while a transaction is active, so background and after-write
sweeps can use `FORCE CHECKPOINT` (waits for the lock).

| Builder method | Strategy | Behavior |
|---|---|---|
| `checkpoint_threshold("64MB")` | Automatic | `SET checkpoint_threshold=…` at load (DuckDB's own auto-checkpoint) |
| `checkpoint_after_writes(n)` | AfterWrites | Checkpoint after every `n`-th write operation |
| `checkpoint_interval(dur)` | Interval | A background thread sweeps **all** loaded connections every `dur` |
| `checkpoint_on_exit(true)` | OnAppLifecycle | Checkpoint all connections on the Tauri `Exit` event |
| `checkpoint_force(true)` | — | Use `FORCE CHECKPOINT` for background / after-write sweeps |
| `checkpoint_platform_defaults()` | per-platform | Mobile (Android/iOS) → `OnAppLifecycle` + `FORCE CHECKPOINT`; desktop stays passive |

`checkpoint_platform_defaults()` **merges** under anything you set explicitly: `Option` fields fall
back to the defaults, and the `on_exit`/`force` booleans are OR-ed in — a platform default can enable
a strategy but never disables one you turned on, so call order relative to the other `checkpoint_*`
setters doesn't matter. The mobile default exists because the OS can suspend/kill the app without a
clean process exit, so pending WAL data must be flushed on the lifecycle event.

Explicit checkpoint from the frontend: `db.checkpoint(force?)` → the `checkpoint` command runs
`CHECKPOINT` (or `FORCE CHECKPOINT` when `force` is true) on that one connection. `checkpoint_all`
(used by the Interval/OnAppLifecycle sweeps) best-effort checkpoints every loaded connection.

### Picking

- Desktop, moderate write volume: `checkpoint_interval(…)` and/or `checkpoint_on_exit(true)`.
- Mobile: `checkpoint_platform_defaults()` (or set `checkpoint_on_exit(true).checkpoint_force(true)`).
- Bursty writes you want flushed promptly: `checkpoint_after_writes(n)`.
- Large WAL tolerance tuning: `checkpoint_threshold("…")`.
- A critical moment (before export, on logout): call `db.checkpoint(true)` from JS.
