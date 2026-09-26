# better-duck-tauri

A [Tauri v2](https://tauri.app) plugin that exposes [better-duck](https://crates.io/crates/better-duck-core)
(embedded DuckDB) to the webview as a local **analytics** engine.

> **Status:** early development. See the workspace roadmap.

## Usage

Add the plugin and pick a backend feature (`backend-core` is the default):

```toml
[dependencies]
better-duck-tauri = { version = "0.1.0-beta.4", features = ["backend-core"] }
```

Register it in your Tauri app:

```rust
tauri::Builder::default()
    .plugin(better_duck_tauri::core::init())      // or better_duck_tauri::diesel::init()
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
```

Grant permissions in a capability file (`src-tauri/capabilities/*.json`). The `duck:default`
set allows `load`, `close`, and read-only `select`. Writes and raw SQL are opt-in:

```json
{
  "permissions": ["duck:default", "duck:allow-execute"]
}
```

## Frontend

```ts
import Database from '@better-duck/tauri'

const db = await Database.load('duckdb:analytics.db') // or 'duckdb::memory:'
await db.execute('CREATE TABLE t (id INTEGER, name VARCHAR)')
await db.execute('INSERT INTO t VALUES ($1, $2)', [1, 'a'])
const rows = await db.select('SELECT * FROM t WHERE id >= $1', [1])
```

Results are row-oriented JSON. Precision-sensitive types (`HUGEINT`, `DECIMAL`, `UUID`, `BIT`,
`BIGNUM`) currently serialize as strings.

## Backends

| Feature | Built on | Notes |
| --- | --- | --- |
| `backend-core` *(default)* | `better-duck-core` | Lightweight; inline migration engine. |
| `backend-diesel` | `better-duck-diesel` | Shares the app's pool; Diesel migrations. |

## License

Licensed under either of MIT or Apache-2.0, at your option.
