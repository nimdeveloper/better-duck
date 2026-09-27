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
pub use engine::{ConnectHook, DuckEngine, EngineConfig};

/// One result row as a JSON object (column name → JSON value).
pub type Row = Map<String, Value>;

/// Outcome of a write/DDL (`execute`) statement.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResult {
    /// Number of rows changed by the statement (`0` for pure DDL).
    pub rows_affected: u64,
}

/// A columnar file format for import/export.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    /// Apache Parquet.
    Parquet,
    /// CSV (auto-detected schema on import).
    Csv,
    /// Newline-delimited / array JSON (auto-detected schema on import).
    Json,
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

    /// Load (installing first if needed) a DuckDB extension.
    fn load_extension(
        &self,
        conn_str: &str,
        name: &str,
    ) -> Result<()>;

    /// Create `table` from a data file via `read_parquet` / `read_csv_auto` /
    /// `read_json_auto` (`source` may be a path or glob).
    fn import(
        &self,
        conn_str: &str,
        table: &str,
        source: &str,
        format: DataFormat,
    ) -> Result<ExecuteResult>;

    /// Export a query's result to a file via `COPY (…) TO`.
    fn export(
        &self,
        conn_str: &str,
        query: &str,
        path: &str,
        format: DataFormat,
    ) -> Result<ExecuteResult>;

    /// Bulk-insert JSON object rows into `table`. Column order is taken from the first
    /// row; missing keys in later rows bind `NULL`.
    fn append_rows(
        &self,
        conn_str: &str,
        table: &str,
        rows: Vec<Map<String, Value>>,
    ) -> Result<ExecuteResult>;

    /// List the tables in the `main` schema.
    fn list_tables(
        &self,
        conn_str: &str,
    ) -> Result<Vec<Row>>;

    /// List a table's columns (name, type, nullability).
    fn list_columns(
        &self,
        conn_str: &str,
        table: &str,
    ) -> Result<Vec<Row>>;

    /// Return the query plan for `sql` (`EXPLAIN`).
    fn explain(
        &self,
        conn_str: &str,
        sql: &str,
    ) -> Result<Vec<Row>>;

    /// Run a read query, delivering rows to `on_batch` in chunks of `chunk_size`.
    /// Returns the total number of rows streamed.
    fn select_stream(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
        chunk_size: usize,
        on_batch: &mut dyn FnMut(Vec<Row>) -> Result<()>,
    ) -> Result<u64>;

    /// Flush the WAL into the main database file (`FORCE CHECKPOINT` when `force`).
    fn checkpoint(
        &self,
        conn_str: &str,
        force: bool,
    ) -> Result<()>;

    /// Checkpoint every currently-loaded connection (used by background strategies).
    fn checkpoint_all(
        &self,
        force: bool,
    );

    /// Cancel the in-flight streaming query on `conn_str`, if any. Returns whether an
    /// interrupt was actually signalled.
    fn interrupt(
        &self,
        conn_str: &str,
    ) -> bool;

    /// Revert the most recently applied migration (runs its `Down` SQL). Returns the
    /// reverted version, or `None` if nothing was applied.
    fn revert(
        &self,
        conn_str: &str,
    ) -> Result<Option<i64>>;

    /// Execute a read query and return the result as Arrow IPC stream bytes.
    #[cfg(feature = "arrow")]
    fn query_arrow(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<u8>>;
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
