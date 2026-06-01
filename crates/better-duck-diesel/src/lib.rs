//! DuckDB backend for the Diesel ORM.
//!
//! Provides [`DuckDbConnection`] and DuckDB-specific SQL types via the
//! [`sql_types`] module.

#![warn(clippy::undocumented_unsafe_blocks)]
#![warn(missing_docs)]

pub mod backend;
pub mod qb;
pub mod result;
pub use qb as query_builder;
mod bind_collector;
pub mod connection;
/// Internal helpers (transaction manager stub).
pub mod helpers;
pub mod row;
pub mod types;

pub use connection::DuckDbConnection;

/// DuckDB-specific SQL types for use in the `table!` macro and query DSL.
pub mod sql_types {
    pub use crate::types::duckdb_types::*;
}
