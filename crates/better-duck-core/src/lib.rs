// SPDX-License-Identifier: MIT OR Apache-2.0
//! `better-duck-core` — a safe, low-level Rust wrapper around the
//! [DuckDB](https://duckdb.org) C API (vendored in-workspace via `better-duck-sys`).
//!
//! This crate provides:
//! - A high-level [`connection::Connection`] for opening databases and executing SQL.
//! - Low-level raw types (`RawConnection`, `DuckResult`, `DuckRow`) for advanced use.
//! - Type conversion traits ([`types::DuckDialect`], [`types::appendable::AppendAble`]) for
//!   mapping between Rust types and DuckDB values.

pub extern crate better_duck_sys as libduckdb_sys;
/// Re-export of the raw `better-duck-sys` FFI bindings.
pub use better_duck_sys as ffi;

/// Async facade over [`connection::Connection`], gated by the `async` feature.
#[cfg(feature = "async")]
pub mod asynchronous;
/// DuckDB database configuration.
pub mod config;
/// High-level DuckDB connection type.
pub mod connection;
/// A shared, cloneable handle to an open DuckDB database.
pub mod database;
/// Error types returned by this crate.
pub mod error;
mod helpers;
/// An `r2d2` connection pool backed by a shared [`Database`](database::Database).
#[cfg(feature = "pool")]
pub mod pool;
mod raw;
/// An owned, thread-safe, fully materialized query result.
pub mod result_set;
/// DuckDB type system and value conversion traits.
pub mod types;
/// User-defined DuckDB functions: scalar functions and table functions.
#[cfg(feature = "udf")]
pub mod udf;

/// Async facade over [`connection::Connection`].
#[cfg(feature = "async")]
pub use asynchronous::AsyncConnection;
/// Async facade over [`Database`].
#[cfg(feature = "async")]
pub use asynchronous::AsyncDatabase;
/// Async facade over [`pool::Pool`].
#[cfg(all(feature = "async", feature = "pool"))]
pub use asynchronous::AsyncPool;

/// Registers a plain Rust function as a DuckDB scalar function. See [`udf`].
#[cfg(feature = "udf")]
pub use better_duck_macros::duckdb_scalar;
/// Registers a plain Rust function as a DuckDB table function. See [`udf`].
#[cfg(feature = "udf")]
pub use better_duck_macros::duckdb_table_function;
/// DuckDB database configuration.
pub use config::{AccessMode, Config, DefaultNullOrder, DefaultOrder};
/// A shared, cloneable handle to an open DuckDB database.
pub use database::Database;
/// A DuckDB appender for bulk-inserting rows into a table.
pub use raw::appender::Appender;
/// A fully iterable DuckDB query result.
pub use raw::result::DuckResult;
/// A single row from a DuckDB query result.
pub use raw::row::DuckRow;
/// A prepared statement suitable for caching and re-execution.
pub use raw::statement::CachedStatement;
/// An owned, thread-safe, fully materialized query result.
pub use result_set::ResultSet;
/// Trait for binding values to DuckDB prepared statements and appenders.
pub use types::appendable::AppendAble;
/// A calendar date value for use without the `chrono` feature.
#[cfg(not(feature = "chrono"))]
pub use types::date_native::DuckDate;
/// A time-of-day value for use without the `chrono` feature.
#[cfg(not(feature = "chrono"))]
pub use types::date_native::DuckTime;
