# better-duck-sys

Vendored DuckDB C API bindings for the [`better-duck`](https://github.com/nimdeveloper/better-duck)
workspace — an internal replacement for the external
[`libduckdb-sys`](https://crates.io/crates/libduckdb-sys) crate.

DuckDB's C++ source is vendored directly in this crate
(`vendor/duckdb.tar.gz`, sourced from a specific tagged release of
[`duckdb/duckdb`](https://github.com/duckdb/duckdb)) and compiled from source
by `build.rs` on every build — there is no runtime download and no git
submodule anywhere in this crate or its build. FFI bindings
(`src/bindings.rs`) are pregenerated and checked in, not produced by
`bindgen` at consumer build time, so building this crate never requires
LLVM/clang.

## Regenerating the vendored source and bindings

Only needed when bumping the DuckDB version. Requires `git`, `python3`, and
LLVM/clang (for `bindgen`) on the machine running it — not on a consumer's
machine.

```sh
cargo run -p xtask -- upgrade-duckdb --tag v1.5.5
```

This clones `duckdb/duckdb` at the given tag into a temporary directory
(deleted afterward — never left behind as a submodule), calls DuckDB's own
`scripts/package_build.py` to get the amalgamated (unity-build) source file
list, repackages it as `vendor/duckdb.tar.gz`, and regenerates
`src/bindings.rs` via `bindgen`. Review the diff, then commit both files.

## Features

| Feature | Default | Effect |
|---|---|---|
| `json` | — | Compiles in DuckDB's JSON extension |
| `parquet` | — | Compiles in DuckDB's Parquet extension |
