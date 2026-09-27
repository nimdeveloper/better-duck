//! Webview-invokable commands. Each hops onto a blocking task so DuckDB work never
//! blocks the async runtime. Keep the list in sync with `build.rs`'s `COMMANDS`.

use serde_json::{Map, Value};
use tauri::ipc::{Channel, CommandScope, GlobalScope};
use tauri::State;

use crate::backend::{DataFormat, ExecuteResult, Row};
use crate::error::{Error, Result};
use crate::scope::Entry;
use crate::state::DuckState;

/// Maps a blocking-task join failure to a backend error.
fn join_err(e: tauri::Error) -> Error {
    Error::Backend(format!("task join failed: {e}"))
}

/// Decides a capability-scope check for one value against allow/deny pattern lists.
///
/// Deny-first: a matching deny pattern always rejects. Then, if any allow patterns are
/// present, the value must match one. An **empty** allow list means *unconstrained* — the
/// capability didn't narrow this field, and the init-time [`Policy`](crate::policy::Policy)
/// remains the mandatory layer — so we don't lock out apps that declare no scope.
fn scope_decision(
    value: &str,
    allow: &[&str],
    deny: &[&str],
    matches: impl Fn(&str, &str) -> bool,
) -> Result<()> {
    if deny.iter().any(|pat| matches(pat, value)) {
        return Err(Error::Denied(format!("capability scope denies {value:?}")));
    }
    if !allow.is_empty() && !allow.iter().any(|pat| matches(pat, value)) {
        return Err(Error::Denied(format!("capability scope does not allow {value:?}")));
    }
    Ok(())
}

/// Enforces the merged global + command capability scope for one [`Entry`] field.
fn enforce_scope(
    value: &str,
    global: &GlobalScope<Entry>,
    command: &CommandScope<Entry>,
    field: impl Fn(&Entry) -> Option<&str>,
    matches: impl Fn(&str, &str) -> bool,
) -> Result<()> {
    let allow: Vec<&str> =
        global.allows().iter().chain(command.allows().iter()).filter_map(|e| field(e)).collect();
    let deny: Vec<&str> =
        global.denies().iter().chain(command.denies().iter()).filter_map(|e| field(e)).collect();
    scope_decision(value, &allow, &deny, matches)
}

/// Exact-match comparator for connection-string scope entries.
fn conn_matches(
    pattern: &str,
    value: &str,
) -> bool {
    pattern == value
}

/// Path-prefix comparator for filesystem scope entries (a directory prefix allows its
/// files). Component-aware and traversal-safe: a `..` segment in the value is rejected
/// outright (so `data/` never allows `data/../secret`), and matching is on whole path
/// components (so `data` allows `data/x` but not `database/x`). Backslashes are treated
/// as separators for Windows paths.
fn path_matches(
    prefix: &str,
    value: &str,
) -> bool {
    let value = value.replace('\\', "/");
    // A `..` segment escapes the prefix — deny any traversal.
    if value.split('/').any(|seg| seg == "..") {
        return false;
    }
    let prefix = prefix.replace('\\', "/");
    let prefix = prefix.trim_end_matches('/');
    prefix.is_empty() || value == prefix || value.starts_with(&format!("{prefix}/"))
}

/// Open (and register) a database connection.
#[tauri::command]
pub(crate) async fn load(
    state: State<'_, DuckState>,
    global_scope: GlobalScope<Entry>,
    command_scope: CommandScope<Entry>,
    db: String,
) -> Result<()> {
    enforce_scope(&db, &global_scope, &command_scope, |e| e.connection.as_deref(), conn_matches)?;
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
    global_scope: GlobalScope<Entry>,
    command_scope: CommandScope<Entry>,
    db: String,
    table: String,
    source: String,
    format: DataFormat,
) -> Result<ExecuteResult> {
    enforce_scope(&source, &global_scope, &command_scope, |e| e.path.as_deref(), path_matches)?;
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.import(&db, &table, &source, format))
        .await
        .map_err(join_err)?
}

/// Export a query result to a data file (Parquet/CSV/JSON).
#[tauri::command]
pub(crate) async fn export(
    state: State<'_, DuckState>,
    global_scope: GlobalScope<Entry>,
    command_scope: CommandScope<Entry>,
    db: String,
    query: String,
    path: String,
    format: DataFormat,
) -> Result<ExecuteResult> {
    enforce_scope(&path, &global_scope, &command_scope, |e| e.path.as_deref(), path_matches)?;
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
    tauri::async_runtime::spawn_blocking(move || backend.list_tables(&db))
        .await
        .map_err(join_err)?
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

/// Cancel the in-flight streaming query on a connection. Returns whether one was signalled.
#[tauri::command]
pub(crate) async fn interrupt(
    state: State<'_, DuckState>,
    db: String,
) -> Result<bool> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || -> Result<bool> { Ok(backend.interrupt(&db)) })
        .await
        .map_err(join_err)?
}

/// Revert the most recently applied migration. Resolves to the reverted version, if any.
#[tauri::command]
pub(crate) async fn revert(
    state: State<'_, DuckState>,
    db: String,
) -> Result<Option<i64>> {
    let backend = state.backend();
    tauri::async_runtime::spawn_blocking(move || backend.revert(&db)).await.map_err(join_err)?
}

/// Execute a read query and return the result as Arrow IPC bytes (raw response body).
#[tauri::command]
pub(crate) async fn query_arrow(
    state: State<'_, DuckState>,
    db: String,
    query: String,
    values: Vec<Value>,
) -> Result<tauri::ipc::Response> {
    #[cfg(feature = "arrow")]
    {
        let backend = state.backend();
        let bytes =
            tauri::async_runtime::spawn_blocking(move || backend.query_arrow(&db, &query, values))
                .await
                .map_err(join_err)??;
        Ok(tauri::ipc::Response::new(bytes))
    }
    #[cfg(not(feature = "arrow"))]
    {
        let _ = (&state, db, query, values);
        Err(Error::Backend("the `arrow` feature is not enabled".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{conn_matches, path_matches, scope_decision};

    #[test]
    fn empty_allow_is_unconstrained() {
        // No scope declared → the capability doesn't narrow this field.
        assert!(scope_decision("duckdb:anything.db", &[], &[], conn_matches).is_ok());
    }

    #[test]
    fn allow_list_requires_a_match() {
        let allow = ["duckdb:app.db"];
        assert!(scope_decision("duckdb:app.db", &allow, &[], conn_matches).is_ok());
        assert!(scope_decision("duckdb:other.db", &allow, &[], conn_matches).is_err());
    }

    #[test]
    fn deny_always_wins() {
        let allow = ["duckdb:app.db"];
        let deny = ["duckdb:app.db"];
        // Even an allowed value is rejected when it also matches a deny entry.
        assert!(scope_decision("duckdb:app.db", &allow, &deny, conn_matches).is_err());
    }

    #[test]
    fn path_scope_matches_by_prefix() {
        let allow = ["data/"];
        assert!(scope_decision("data/exports/out.parquet", &allow, &[], path_matches).is_ok());
        assert!(scope_decision("/etc/passwd", &allow, &[], path_matches).is_err());
    }

    #[test]
    fn path_scope_rejects_traversal() {
        let allow = ["data/"];
        // `..` must never escape the allowed prefix, even though the raw string starts with it.
        assert!(scope_decision("data/../secret.db", &allow, &[], path_matches).is_err());
        assert!(scope_decision("data/../../etc/passwd", &allow, &[], path_matches).is_err());
        assert!(scope_decision("data\\..\\secret", &allow, &[], path_matches).is_err());
    }

    #[test]
    fn path_scope_matches_whole_components() {
        // A prefix of `data` allows `data/x` but not sibling names sharing the prefix bytes.
        let allow = ["data"];
        assert!(scope_decision("data/x.parquet", &allow, &[], path_matches).is_ok());
        assert!(scope_decision("database/x", &allow, &[], path_matches).is_err());
        assert!(scope_decision("data_secret/y", &allow, &[], path_matches).is_err());
    }
}
