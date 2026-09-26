//! Webview-invokable commands. Each hops onto a blocking task so DuckDB work never
//! blocks the async runtime. Keep the list in sync with `build.rs`'s `COMMANDS`.

use serde_json::Value;
use tauri::State;

use crate::backend::{ExecuteResult, Row};
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
