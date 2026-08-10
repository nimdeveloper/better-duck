// SPDX-License-Identifier: MIT OR Apache-2.0
//! Vendored DuckDB C API bindings for the `better-duck` workspace.
//!
//! This crate replaces the external `libduckdb-sys` dependency: DuckDB's C++
//! source is vendored directly in this crate (`vendor/duckdb.tar.gz`,
//! compiled from source by `build.rs` on every build — there is no toggle for
//! this, unlike upstream's `bundled` feature, since this crate exists
//! specifically to own that source), and the FFI bindings
//! ([`bindings`]) are pregenerated and checked in rather than produced by
//! `bindgen` at consumer build time. See `xtask/src/main.rs` (workspace
//! root) for the maintainer-only regeneration process.
#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
#![allow(missing_docs, clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]
#[allow(rustdoc::private_intra_doc_links)]

#[doc(hidden)]
mod bindings;
mod error;

pub use bindings::*;
pub use error::{DuckDBError, DuckDBSuccess, Error};

/// The upstream DuckDB release tag this crate's vendored source and bindings
/// were generated from (e.g. `"v1.5.5"`), set by `build.rs` from the vendored
/// archive's own `manifest.json`.
pub const DUCKDB_VENDORED_VERSION: &str = env!("DUCKDB_VENDORED_VERSION");
