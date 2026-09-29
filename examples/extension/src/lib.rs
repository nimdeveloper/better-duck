//! An example DuckDB **loadable extension** written in Rust.
//!
//! Unlike every other example (which links DuckDB in), this crate builds a `cdylib`
//! that is loaded *into* an already-running DuckDB with `LOAD`. It therefore links no
//! DuckDB of its own: `better-duck-core`'s `loadable-extension` feature routes every
//! FFI call through the host's `duckdb_ext_api_v1` function-pointer table, which the
//! generated entrypoint populates before anything else runs.
//!
//! See `README.md` for building it and loading it into the DuckDB CLI.

use better_duck_core::connection::Connection;
use better_duck_core::{duckdb_entrypoint, duckdb_scalar};

/// A scalar function this extension adds to the host database.
#[duckdb_scalar(name = "hello_better_duck")]
fn hello(name: &str) -> String {
    format!("hello, {name}! (from a Rust .duckdb_extension)")
}

/// The extension entrypoint. DuckDB calls the generated
/// `better_duck_example_init_c_api` symbol; the macro initializes the API table,
/// wraps the host database in a [`Connection`], and hands it to this function.
#[duckdb_entrypoint(name = "better_duck_example", min_duckdb_version = "v0.0.1")]
fn init(conn: &mut Connection) -> better_duck_core::error::Result<()> {
    hello::register(conn)?;
    Ok(())
}
