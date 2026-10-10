---
name: better-duck-tauri
description: >-
  The better-duck-tauri crate: a Tauri v2 plugin that exposes better-duck (embedded DuckDB) to the
  webview as a local analytics engine. Use when registering the plugin in `tauri::Builder`, choosing a
  backend Cargo feature (`feature = "backend-core"` default, `feature = "backend-diesel"`, or
  `feature = "arrow"`), calling the webview commands (`load`/`close`/`select`/`execute`, plus
  import/export/append/stream/checkpoint/revert) from JS/TS via `@better-duck/tauri`, wiring Tauri
  capabilities/permissions and the scope + policy security model, defining startup migrations, or
  configuring WAL checkpoint strategies. Triggers: "Tauri", "webview", the crate name, the command
  names, or the backend/arrow feature flags.
license: MIT OR Apache-2.0
metadata:
  author: Hamid Reza Shakibi
  version: "0.1.0"
---

# better-duck-tauri

A [Tauri v2](https://tauri.app) plugin that exposes [better-duck](https://crates.io/crates/better-duck-core)
(embedded DuckDB) to the webview as a local **analytics** engine — not just a CRUD bridge. The Rust
side registers the plugin on `tauri::Builder` with a chosen backend; the frontend talks to it through
the `@better-duck/tauri` npm package, which wraps a fixed set of `invoke` commands.

Status is beta: query / analytics / migration / security features are implemented and tested; the
`arrow` (Arrow IPC) command and Android CI are the pending edges.

Sibling skills: `duckdb` (SQL dialect + types you will write through this plugin), `better-duck-core`
(the Rust driver underneath), `better-duck-diesel` (the Diesel ORM backend), and `better-duck`
(the top-level crate map).

## How it fits together

- **One backend, picked by Cargo feature.** `backend-core` (default) builds on `better-duck-core`;
  `backend-diesel` builds on `better-duck-diesel` (an r2d2 pool of `DuckDbConnection` + Diesel
  `embed_migrations!`). The webview command surface is **identical** either way — both engines
  implement the same `DuckBackend` trait, so JS code never changes.
- **A fixed command set**, not arbitrary RPC. Every command is `#[tauri::command]` and runs DuckDB
  work on a blocking task so the async runtime is never blocked.
- **Two security layers**: an init-time Rust `Policy` (mandatory) and declarative per-capability
  `scope` entries, both enforced before the backend runs.

## 30-second quickstart

Rust (`src-tauri/Cargo.toml` + your builder):

```toml
[dependencies]
better-duck-tauri = { version = "0.1.0-beta.8", features = ["backend-core"] }
```

```rust
tauri::Builder::default()
    .plugin(better_duck_tauri::core::init()) // simplest; or ::diesel::init()
    .run(tauri::generate_context!())?;
```

Grant permissions in `src-tauri/capabilities/*.json` (default allows `load`/`close`/`select` only;
writes and raw SQL are opt-in):

```json
{ "permissions": ["better-duck-tauri:default", "better-duck-tauri:allow-execute"] }
```

Frontend (`@better-duck/tauri`):

```ts
import Database from '@better-duck/tauri'

const db = await Database.load('duckdb:analytics.db') // or 'duckdb::memory:'
await db.execute('CREATE TABLE t (id INTEGER, name VARCHAR)')
await db.execute('INSERT INTO t VALUES ($1, $2)', [1, 'ada'])
const rows = await db.select('SELECT * FROM t WHERE id > $1', [0]) // [{ id: 1, name: 'ada' }]
await db.close()
```

Connection strings: `duckdb:<path>` opens a file, `duckdb::memory:` (or empty) opens in-memory, and
a bare path works too — the plugin strips the `duckdb:` prefix to resolve the DuckDB path.

## The webview command API

Every method on the JS `Database` class maps to one Rust command (`plugin:better-duck-tauri|<name>`).
Parameters bind as **positional** `$1, $2, …` from the `values` array.

| JS method | Command | Does |
|---|---|---|
| `Database.load(path)` | `load` | Open + register a connection; runs pending migrations |
| `db.close()` | `close` | Drop the connection (→ `bool` existed) |
| `db.select(sql, values?)` | `select` | Read query → array of JSON row objects |
| `db.execute(sql, values?)` | `execute` | Write/DDL → `{ rowsAffected }` |
| `db.loadExtension(name)` | `load_extension` | `INSTALL`+`LOAD` a DuckDB extension |
| `db.import(table, src, fmt)` | `import` | `CREATE TABLE … AS SELECT * FROM read_*()` (parquet/csv/json) |
| `db.export(sql, path, fmt)` | `export` | `COPY (…) TO path` |
| `db.appendRows(table, rows)` | `append_rows` | Bulk `INSERT` of JSON object rows |
| `db.tables()` / `db.columns(t)` | `tables` / `columns` | Introspect `main` schema |
| `db.explain(sql)` | `explain` | `EXPLAIN` plan rows |
| `db.stream(sql, onBatch, values?, chunk?)` | `stream` | Chunked rows over a `Channel` → total count |
| `db.interrupt()` | `interrupt` | Cancel the in-flight stream (→ `bool`) |
| `db.checkpoint(force?)` | `checkpoint` | Flush WAL (`FORCE CHECKPOINT` when `force`) |
| `db.revert()` | `revert` | Run the last migration's `Down` (→ reverted version) |
| `db.queryArrow(sql, values?)` | `query_arrow` | Arrow IPC bytes (needs the `arrow` feature) |

JSON row shape: values map to JSON with full type coverage. Precision-sensitive scalars (`HUGEINT`,
`DECIMAL`, `UUID`, `BIT`, `BIGNUM`) serialize as **strings**, `BLOB` as a byte array, composites
(`LIST`/`STRUCT`/`MAP`/`UNION`/`ENUM`) as arrays/objects, temporals as ISO-8601 strings.

## Topic map — route to a reference

| You need to… | Read |
|---|---|
| Add the Rust dep + JS package, register `core` vs `diesel`, use every command, decode Arrow IPC | `references/register-and-frontend.md` |
| Define migrations, control ordering, understand startup + crash-safe backup | `references/migrations.md` |
| Lock down an untrusted webview: capabilities, permissions, scope entries, `Policy` | `references/security.md` |
| Compare `backend-core` vs `backend-diesel`, back up, and tune checkpoint strategies | `references/backends-and-checkpoint.md` |

## Conventions and gotchas

- **Error type** is `better_duck_tauri::Error` (`UnknownConnection` / `Denied` / `Backend` / `Tauri`),
  serialized to the webview as a plain string. The JS `invoke` promise rejects with that string.
- **`select` is filesystem-read-capable.** DuckDB `SELECT` can reach disk through table functions
  (`read_csv_auto`, `read_parquet`, `glob`, …), which the leading-keyword `deny_risky_statements`
  filter does **not** catch. For an untrusted webview, pair `select` with `read_only(true)` and/or a
  connection allow-list — see `references/security.md`.
- **Backend feature is required.** Building with neither `backend-core` nor `backend-diesel` is a
  compile error.
- **`on_connect` hooks** are the place to register UDFs or `LOAD` extensions so every opened
  connection carries them (see `better-duck-core` / `better-duck-macros` for the UDF side).
- **`query_arrow`** returns the raw IPC body via `tauri::ipc::Response`; without the `arrow` feature
  the command returns a backend error.
