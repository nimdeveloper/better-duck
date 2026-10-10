# Errors & Extensions

## The error type

Everything fallible returns `better_duck_core::error::Result<T>` — an alias for
`Result<T, better_duck_core::error::Error>`. `Error` is `#[non_exhaustive]`; match with a `_` arm.
Common variants:

| Variant | Meaning |
|---|---|
| `Engine(EngineError)` | A typed engine error carrying DuckDB's own classification + message. The main runtime failure. |
| `DuckDBFailure(ffi::Error, Option<String>)` | A raw C-API failure (status code + optional message) from APIs with no typed classification (prepare, extract, table description). |
| `ConversionError(DuckDBConversionError)` | A value conversion failed (`TypeMismatch { expected, found }`, `ConversionError(String)`, `NullValue`, `PrecisionLoss`). |
| `NulError(..)` | A `&str`/path contained an interior NUL byte (caught before the FFI call). |
| `InvalidParameterName(String)` | A `$name` passed to `execute_with_named` is not in the SQL. |
| `InvalidParameterCount(got, needed)` | Wrong number of bound parameters. |
| `InvalidColumnIndex(usize)` / `InvalidColumnName(String)` | Column lookup out of range / unknown name. |
| `MixedIterationMode` | Row-by-row and chunk (`fetch_chunk`) consumption mixed on one `DuckResult` — see [columnar](columnar.md). |
| `IntegralValueOutOfRange(col, i128)` | A value does not fit the requested integer type. |
| `BackgroundTaskFailed(String)` | A `spawn_blocking` task (async facade) panicked or was cancelled. |
| `Pool(String)` | A pool checkout timed out or the manager failed. |
| `Utf8Error`, `InvalidPath`, `InvalidQuery`, `MultipleStatement`, `StatementChangedRows`, `AppendError`, `UNKNOWN(..)` | Misc. |

`Error` implements `std::error::Error` (with `source()`), `Display`, and a partial `PartialEq`.

## EngineError — DuckDB's classification

```rust
use better_duck_core::error::{Error, EngineError, EngineErrorKind};

match conn.execute_batch("NOT VALID SQL") {
    Err(Error::Engine(EngineError { kind, message })) => {
        // kind: EngineErrorKind (e.g. Syntax, Parser, Catalog, Constraint, Conversion, …)
        // message: Option<String> — DuckDB's own text, copied out and owned.
        eprintln!("{kind}: {message:?}");
    }
    _ => {}
}
```

`EngineError { kind: EngineErrorKind, message: Option<String> }` is fully owned (every field copied
out of DuckDB memory when built), so it stays valid after the handle it came from — and the
connection — are gone. `EngineErrorKind` mirrors `duckdb_error_type` and is `#[non_exhaustive]`:

- Known kinds map 1:1 (`Syntax`, `Parser`, `Binder`, `Catalog`, `Constraint`, `Conversion`,
  `MismatchType`, `OutOfRange`, `NotImplemented`, `Io`, `OutOfMemory`, `Permission`,
  `MissingExtension`, `Transaction`, … — see `EngineErrorKind::KNOWN` for the full list).
- `Unknown(raw)` preserves a classification a future DuckDB adds (nothing is lost across upgrades).
- `Unavailable` is an **explicit absence** — the originating API exposes only a status code and a
  message, with no typed classification. The driver never invents a kind by parsing message text.

`EngineErrorKind::from_raw` / `to_raw` round-trip the raw code; `KNOWN` lists every mapped kind.

## Extension management

DuckDB extensions load at runtime. These methods on `Connection` drive `INSTALL`/`LOAD` and read
`duckdb_extensions()`, so a feature can **check** for an extension and force it to load rather than
failing later with an opaque SQL error. Extension names must be bare identifiers (`[A-Za-z0-9_]+`);
anything else is rejected up front (so the name can be embedded safely).

```rust
conn.ensure_extension("spatial")?;                 // load, installing first if needed
let loaded: bool = conn.is_extension_loaded("spatial")?;
// lower-level:
conn.install_extension("spatial")?;                // INSTALL (downloads if absent)
conn.load_extension("spatial")?;                   // LOAD into this connection
```

- `install_extension(name)` → `INSTALL name`.
- `load_extension(name)` → `LOAD name`.
- `is_extension_loaded(name) -> Result<bool>` — queries `duckdb_extensions()`; an unknown extension
  reports `false` (no row), not an error.
- `ensure_extension(name)` — no-op if already loaded, else `LOAD`, else `INSTALL` + `LOAD`.

Loading at runtime needs network/filesystem access and `enable_external_access` (default on). The
Diesel `spatial` module's `ensure_loaded` wraps this for `ST_*` DSL — see **`better-duck-diesel`**.

## Bundled extensions (build-time features)

The core features `json`, `parquet`, and `icu` compile that DuckDB extension **into the static
library** (forwarded to `better-duck-sys`), so it is available with **no runtime `INSTALL`/`LOAD`** —
useful for offline/embedded builds (e.g. Tauri, iOS). `icu` adds timezone/collation support and
requires a DuckDB archive re-vendored with ICU sources. For engine-side details of each extension,
see the **`duckdb`** skill.

## Loadable extensions (experimental)

Feature `loadable-extension` builds for a loadable `.duckdb_extension` (links no DuckDB, routes every
FFI call through the host's API table). It pairs with the `#[duckdb_entrypoint]` macro and implies
`udf`. See **`better-duck-macros`**. This is distinct from *using* extensions as above.
