//! Webview-invokable commands. Each hops onto a blocking task so DuckDB work never
//! blocks the async runtime. Keep the list in sync with `build.rs`'s `COMMANDS`.

use serde_json::{Map, Value};
use tauri::ipc::Channel;
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

/// List the tables in the `main` schema.
#[tauri::command]
pub(crate) async fn tables(
    state: State<'_, DuckState>,
    db: String,
) -> Result<Vec<Row>> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.list_tables(&db)).await.map_err(join_err)?
}

/// List a table's columns.
#[tauri::command]
pub(crate) async fn columns(
    state: State<'_, DuckState>,
    db: String,
    table: String,
) -> Result<Vec<Row>> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.list_columns(&db, &table))
        .await
        .map_err(join_err)?
}

/// Return the query plan for a statement.
#[tauri::command]
pub(crate) async fn explain(
    state: State<'_, DuckState>,
    db: String,
    query: String,
) -> Result<Vec<Row>> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.explain(&db, &query))
        .await
        .map_err(join_err)?
}

/// Stream a read query's rows to the frontend in chunks over a channel.
/// Resolves to the total number of rows streamed.
#[tauri::command]
pub(crate) async fn stream(
    state: State<'_, DuckState>,
    db: String,
    query: String,
    values: Vec<Value>,
    chunk: Option<usize>,
    channel: Channel<Vec<Row>>,
) -> Result<u64> {
    let backend = state.backend();
    let size = chunk.unwrap_or(1024);
    tauri::async_runtime::spawn_blocking(move || {
        let mut sink =
            |batch: Vec<Row>| channel.send(batch).map_err(|e| Error::Backend(e.to_string()));
        backend.select_stream(&db, &query, values, size, &mut sink)
    })
    .await
    .map_err(join_err)?
}

/// Flush the WAL into the main database file (`FORCE CHECKPOINT` when `force`).
#[tauri::command]
pub(crate) async fn checkpoint(
    state: State<'_, DuckState>,
    db: String,
    force: Option<bool>,
) -> Result<()> {
    let backend = state.backend();
    let force = force.unwrap_or(false);
    tauri::async_runtime::spawn_blocking(move || backend.checkpoint(&db, force))
        .await
        .map_err(join_err)?
}
