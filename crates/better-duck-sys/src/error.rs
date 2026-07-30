//! Hand-written convenience layer over the raw `duckdb_state` result code.
//!
//! Not part of DuckDB's C header and not `bindgen`-generated — this mirrors
//! the same small hand-written addition the external `libduckdb-sys` crate
//! carries on top of its own raw bindings, which `better-duck-core`'s
//! `error::Error` type builds on (`DuckDBFailure(ffi::Error, ...)`).

use crate::bindings::{duckdb_state, duckdb_state_DuckDBError, duckdb_state_DuckDBSuccess};

/// Friendlier alias for the raw `duckdb_state_DuckDBSuccess` constant.
pub const DuckDBSuccess: duckdb_state = duckdb_state_DuckDBSuccess;
/// Friendlier alias for the raw `duckdb_state_DuckDBError` constant.
pub const DuckDBError: duckdb_state = duckdb_state_DuckDBError;

/// Wraps a raw DuckDB C API result code (`duckdb_state`) as a Rust error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error {
    /// The raw `duckdb_state` returned by the failing C API call.
    pub result_code: duckdb_state,
}

impl Error {
    /// Wraps `code`, the `duckdb_state` returned by a fallible C API call.
    #[must_use]
    pub fn new(code: duckdb_state) -> Self {
        Error { result_code: code }
    }
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "DuckDB call failed with result code {}", self.result_code)
    }
}

impl std::error::Error for Error {}
