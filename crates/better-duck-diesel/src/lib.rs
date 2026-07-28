// SPDX-License-Identifier: MIT OR Apache-2.0
//! DuckDB backend for the Diesel ORM.
//!
//! Provides [`DuckDbConnection`] and DuckDB-specific SQL types via the
//! [`sql_types`] module.

pub mod backend;
pub mod qb;
pub mod result;
pub use qb as query_builder;
mod bind_collector;
pub mod connection;
/// Internal helpers (transaction manager stub).
pub mod helpers;
/// A shared-database `r2d2` connection manager, coexisting with `diesel::r2d2::ConnectionManager`.
#[cfg(feature = "r2d2")]
pub mod pool;
pub mod row;
pub mod types;

pub use connection::DuckDbConnection;

/// DuckDB-specific SQL types for use in the `table!` macro and query DSL.
pub mod sql_types {
    pub use crate::types::duckdb_types::*;
}
