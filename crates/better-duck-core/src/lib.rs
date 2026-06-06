// SPDX-License-Identifier: MIT OR Apache-2.0
//! `better-duck-core` — a safe, low-level Rust wrapper around the
//! [DuckDB](https://duckdb.org) C API (`libduckdb-sys`).
//!
//! This crate provides:
//! - A high-level [`connection::Connection`] for opening databases and executing SQL.
//! - Low-level raw types (`RawConnection`, `DuckResult`, `DuckRow`) for advanced use.
//! - Type conversion traits ([`types::DuckDialect`], [`types::appendable::AppendAble`]) for
//!   mapping between Rust types and DuckDB values.

pub extern crate libduckdb_sys;
/// Re-export of the raw `libduckdb_sys` FFI bindings.
pub use libduckdb_sys as ffi;

mod config;
/// High-level DuckDB connection type.
pub mod connection;
/// Error types returned by this crate.
pub mod error;
mod helpers;
mod raw;
/// DuckDB type system and value conversion traits.
pub mod types;

/// A fully iterable DuckDB query result.
pub use raw::result::DuckResult;
/// A single row from a DuckDB query result.
pub use raw::row::DuckRow;
/// A prepared statement suitable for caching and re-execution.
pub use raw::statement::CachedStatement;
/// Trait for binding values to DuckDB prepared statements and appenders.
pub use types::appendable::AppendAble;
/// A calendar date value for use without the `chrono` feature.
#[cfg(not(feature = "chrono"))]
pub use types::date_native::DuckDate;
/// A time-of-day value for use without the `chrono` feature.
#[cfg(not(feature = "chrono"))]
pub use types::date_native::DuckTime;
