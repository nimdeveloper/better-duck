# better-duck examples

A comprehensive, **self-verifying** tour of the `better-duck` crates. Every example is a
small binary that prints a short trace *and* `assert!`s its expected results — so running
one both demonstrates and tests the API it covers.

## Layout

This directory is its own **detached Cargo workspace** (kept out of the library workspace so
it never affects the library's build/test gate). Each `NN-topic/` folder is a crate; each
file under its `src/bin/` is one runnable example.

The `feature-*/` folders are **standalone workspaces** demonstrating the *other* side of a
mutually-exclusive feature fork (Cargo unifies features within a workspace, so these must be
built separately).

## Running

Run a single example:

```bash
cargo run -p ex02-execute-rows --bin streaming
```

Run **every** example (auto-discovers all binaries):

```bash
bash examples/run-all.sh
```

The detached feature crates are built with an explicit shared target dir, e.g.:

```bash
cargo run --manifest-path examples/feature-native-datetime/Cargo.toml --target-dir examples/target --bin native_dates
```

## Groups

### `better-duck-core`

| Group | Covers |
| --- | --- |
| `01-connection` | Opening (memory / file / `Config`), sharing one DB (`Database::connect`, `try_clone`, `InstanceCache`), `library_version` |
| `02-execute-rows` | `execute` vs `execute_batch`, streaming `DuckResult`, materialized `ResultSet`, column metadata, `changes()`, rewind/peek |
| `03-parameters` | `execute_with` positional binds, `insert()` homogeneous binds, `NULL` handling |
| `04-prepared` | `Statement` (via `conn.db().prepare`), `CachedStatement`, pending step-execution, `ExtractedStatements` (multi-statement) |
| `05-types` | Every `DuckValue` type and its Rust conversion (ints, floats, decimal, text, blob, bool, uuid, bit, bignum, enum, list, array, struct, map, union, temporal, interval), plus convert helpers |
| `06-appender` | Bulk ingest: `appender*`, a custom `AppendAble` impl, `DataChunk` chunks, lifecycle |
| `07-low-level` | Raw layer: `DataChunk`, `LogicalType`/`TypeInfo`, `OwnedValue` getters, `QueryControl`, profiling, `task_state`, table/client introspection |
| `08-pool` | `r2d2` connection pooling (`DuckDbConnectionManager`, `Pool`) |
| `09-async` | `AsyncConnection` / `AsyncDatabase` / `AsyncPool`, `execute` vs `execute_pending` |
| `10-udf-macros` | `#[duckdb_scalar]` / `#[duckdb_table_function]` and the `duck_state!`/`duck_projection!`/`duck_extra_info!`/`duck_bail!` helpers |
| `11-udf-manual` | The manual trait path: `VScalar`, `VTab`, `VAggregate`, `VCast`, replacement scans |

### `better-duck-diesel`

| Group | Covers |
| --- | --- |
| `20-diesel-conn` | `DuckDbConnection::establish` (memory / file / `duckdb://`), `from_core`, transactions & savepoints, migrations setup |
| `21-diesel-crud` | Insert / RETURNING / select / filter / order / limit / update / delete / ON CONFLICT / joins / aggregates / boxed queries / `#[derive(Queryable/Insertable/Selectable/AsChangeset)]` |
| `22-diesel-types` | Standard sql_types via the DSL, DuckDB-specific `Duck*` sql_types via `sql_query` + `QueryableByName`, `Nullable`, query-builder helpers |
| `23-diesel-pool` | `r2d2` with the shared `SharedDuckDbConnectionManager` |

### Feature forks (standalone workspaces)

| Crate | Covers |
| --- | --- |
| `feature-native-datetime` | The non-`chrono` build: native `DuckDate`/`DuckTime`/… and `std::time` types |
| `feature-minimal` | `--no-default-features`: minimal build, `DuckDecimal` without `rust_decimal` |
| `feature-extensions` | The `json` + `parquet` extensions (first build compiles a DuckDB variant, ~10-15 min) |
