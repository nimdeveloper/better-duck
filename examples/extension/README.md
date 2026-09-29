# Example: a DuckDB loadable extension in Rust

Builds a `.duckdb_extension` that adds a scalar function to a **running** DuckDB —
no C++ glue, no CMake.

> **Experimental.** Depends on DuckDB's unstable C extension API
> (`DUCKDB_EXTENSION_API_VERSION_UNSTABLE`), mirroring upstream `duckdb-rs`, whose
> equivalent feature is also experimental.

## How it differs from every other example

Other examples link DuckDB into the binary. An extension is loaded *into* a DuckDB that
is already running, so it must **not** carry its own copy. `better-duck-core`'s
`loadable-extension` feature therefore:

- links no DuckDB (`better-duck-sys/build.rs` compiles nothing in this mode), and
- routes every `duckdb_*` call through the host's `duckdb_ext_api_v1` function-pointer
  table, which `#[duckdb_entrypoint]` populates before anything else runs.

The resulting library is a few hundred KB rather than the ~100 MB a linked build produces.

## Build

```bash
cargo build --release
```

Then rename the cdylib to the name DuckDB expects (`<name>.duckdb_extension`, where
`<name>` matches the macro's `name = "..."`):

| Platform | Built file | Rename to |
| --- | --- | --- |
| Linux | `target/release/libbetter_duck_extension_example.so` | `better_duck_example.duckdb_extension` |
| macOS | `target/release/libbetter_duck_extension_example.dylib` | `better_duck_example.duckdb_extension` |
| Windows | `target/release/better_duck_extension_example.dll` | `better_duck_example.duckdb_extension` |

## Load it

The extension is unsigned, so DuckDB must be started with unsigned extensions allowed:

```bash
duckdb -unsigned
```

```sql
LOAD 'path/to/better_duck_example.duckdb_extension';
SELECT hello_better_duck('world');
-- hello, world! (from a Rust .duckdb_extension)
```

From Rust, set the same option via `Config`:

```rust
let config = Config::default().allow_unsigned_extensions(true)?;
let mut conn = Connection::open_in_memory_with_flags(config)?;
conn.execute_batch("LOAD 'path/to/better_duck_example.duckdb_extension'")?;
```

## Version compatibility

`min_duckdb_version` in `#[duckdb_entrypoint]` is the oldest host the extension accepts.
If the host's C extension API is older, `get_api` returns null, the entrypoint returns
`false`, and DuckDB reports the mismatch. The host must also be a DuckDB whose extension
API matches the vendored `duckdb_extension.h` these bindings were generated from
(currently v1.5.5).

## Regenerating the loadable bindings (maintainers)

Needs libclang (`LIBCLANG_PATH`, plus that directory on `PATH` on Windows):

```bash
cargo run -p xtask -- upgrade-duckdb v1.5.5 --skip-bindings --loadable-bindings
cargo run -p xtask -- gen-loadable-wrappers
```

The first writes `crates/better-duck-sys/src/bindings_loadable.rs` (types +
`duckdb_ext_api_v1`); the second appends the pointer-backed wrappers and
`duckdb_rs_extension_api_init`. Both are idempotent.
