//! Webview-invokable commands. Each hops onto a blocking task so DuckDB work never
//! blocks the async runtime. Keep the list in sync with `build.rs`'s `COMMANDS`.

use serde_json::{Map, Value};
use tauri::State;

use crate::backend::{DataFormat, ExecuteResult, Row};
use crate::error::{Error, Result};
use crate::state::DuckState;

/// Maps a blocking-task join failure to a backend error.
fn join_err(e: tauri::Error) -> Error {
    Error::Backend(format!("task join failed: {e}"))
}

/// Open (and register) a database connection.
#[tauri::command]
pub(crate) async fn load(
    state: State<'_, DuckState>,
    db: String,
) -> Result<()> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.load(&db)).await.map_err(join_err)?
}

/// Close a previously loaded connection. Returns whether one existed.
#[tauri::command]
pub(crate) async fn close(
    state: State<'_, DuckState>,
    db: String,
) -> Result<bool> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.close(&db)).await.map_err(join_err)?
}

/// Run a read query, returning rows as JSON objects.
#[tauri::command]
pub(crate) async fn select(
    state: State<'_, DuckState>,
    db: String,
    query: String,
    values: Vec<Value>,
) -> Result<Vec<Row>> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.select(&db, &query, values))
        .await
        .map_err(join_err)?
}

/// Run a write/DDL statement, returning the affected-row count.
#[tauri::command]
pub(crate) async fn execute(
    state: State<'_, DuckState>,
    db: String,
    query: String,
    values: Vec<Value>,
) -> Result<ExecuteResult> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.execute(&db, &query, values))
        .await
        .map_err(join_err)?
}

/// Load (installing if needed) a DuckDB extension.
#[tauri::command]
pub(crate) async fn load_extension(
    state: State<'_, DuckState>,
    db: String,
    name: String,
) -> Result<()> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.load_extension(&db, &name))
        .await
        .map_err(join_err)?
}

/// Create a table from a data file (Parquet/CSV/JSON).
#[tauri::command]
pub(crate) async fn import(
    state: State<'_, DuckState>,
    db: String,
    table: String,
    source: String,
    format: DataFormat,
) -> Result<ExecuteResult> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.import(&db, &table, &source, format))
        .await
        .map_err(join_err)?
}

/// Export a query result to a data file (Parquet/CSV/JSON).
#[tauri::command]
pub(crate) async fn export(
    state: State<'_, DuckState>,
    db: String,
    query: String,
    path: String,
    format: DataFormat,
) -> Result<ExecuteResult> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.export(&db, &query, &path, format))
        .await
        .map_err(join_err)?
}

/// Bulk-insert JSON object rows into a table.
#[tauri::command]
pub(crate) async fn append_rows(
    state: State<'_, DuckState>,
    db: String,
    table: String,
    rows: Vec<Map<String, Value>>,
) -> Result<ExecuteResult> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.append_rows(&db, &table, rows))
        .await
        .map_err(join_err)?
}
