//! The `DuckBackend` abstraction and its result types.
//!
//! Commands are written against [`DuckBackend`] so the webview-facing API is
//! identical regardless of which Rust backend the app selected
//! (`backend-core` or `backend-diesel`).

use serde_json::{Map, Value};

use crate::error::Result;

#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
mod engine;
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub use engine::DuckEngine;

/// One result row as a JSON object (column name → JSON value).
pub type Row = Map<String, Value>;

/// Outcome of a write/DDL (`execute`) statement.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResult {
    /// Number of rows changed by the statement (`0` for pure DDL).
    pub rows_affected: u64,
}

/// A pluggable DuckDB execution backend behind the plugin's commands.
pub trait DuckBackend: Send + Sync + 'static {
    /// Open and register a connection for `conn_str`
    /// (`duckdb:app.db`, `duckdb::memory:`, or a bare path).
    fn load(
        &self,
        conn_str: &str,
    ) -> Result<()>;

    /// Close and drop a previously loaded connection. Returns whether one existed.
    fn close(
        &self,
        conn_str: &str,
    ) -> Result<bool>;

    /// Run a read query, returning rows as JSON objects.
    fn select(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<Row>>;

    /// Run a write/DDL statement, returning the affected-row count.
    fn execute(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ExecuteResult>;
}

/// Normalize a connection string to a DuckDB path: strips a leading `duckdb:` and
/// maps the empty/`:memory:` forms to an in-memory database.
#[cfg(any(feature = "backend-core", feature = "backend-diesel"))]
pub(crate) fn resolve_path(conn_str: &str) -> String {
    let stripped = conn_str.strip_prefix("duckdb:").unwrap_or(conn_str);
    if stripped.is_empty() || stripped == ":memory:" {
        ":memory:".to_owned()
    } else {
        stripped.to_owned()
    }
}
