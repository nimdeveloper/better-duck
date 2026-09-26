//! `DuckEngine`: the connection registry + query execution built on `better-duck-core`.
//!
//! Both `core::init()` and (for now) `diesel::init()` construct this. The Diesel-native
//! path (shared `DuckDbConnection` r2d2 pool + Diesel migrations) is layered on in a later
//! phase (see TASKS T4.5); the webview API is identical meanwhile.

use std::collections::HashMap;
use std::sync::Mutex;

use better_duck_core::connection::Connection;
use better_duck_core::database::Database;
use better_duck_core::types::appendable::AppendAble;
use serde_json::{Map, Value};

use super::{resolve_path, DuckBackend, ExecuteResult, Row};
use crate::error::{Error, Result};
use crate::json::{duck_to_json, json_to_duck};
use crate::migration::Migration;

/// A registry of open DuckDB databases keyed by connection string.
///
/// `Database` is cheap to clone (an `Arc`) and `Send + Sync`; each operation opens a
/// short-lived `Connection` from it, so the registry needs no per-connection locking
/// beyond guarding the map itself.
#[derive(Default)]
pub struct DuckEngine {
    dbs: Mutex<HashMap<String, Database>>,
    /// Pending migrations per connection string, run on `load` (backend-core engine).
    migrations: HashMap<String, Vec<Migration>>,
}

impl DuckEngine {
    /// Creates an empty engine with no migrations.
    pub fn new() -> DuckEngine {
        DuckEngine::default()
    }

    /// Creates an engine that runs the given per-connection migrations on `load`.
    pub fn with_migrations(migrations: HashMap<String, Vec<Migration>>) -> DuckEngine {
        DuckEngine { dbs: Mutex::new(HashMap::new()), migrations }
    }

    fn with_conn<T>(
        &self,
        conn_str: &str,
        f: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        let dbs = self.dbs.lock().expect("duck registry poisoned");
        let db = dbs.get(conn_str).ok_or_else(|| Error::UnknownConnection(conn_str.to_owned()))?;
        let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
        f(&mut conn)
    }
}

/// Binds JSON params as consecutive positional parameters and runs `f` with the result.
fn run_with_params<T>(
    conn: &mut Connection,
    sql: &str,
    params: &[Value],
    f: impl FnOnce(better_duck_core::DuckResult) -> Result<T>,
) -> Result<T> {
    let mut duck: Vec<better_duck_core::types::value::DuckValue> =
        params.iter().map(json_to_duck).collect();
    let mut binds: Vec<&mut dyn AppendAble> =
        duck.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
    let result = conn.execute_with(sql, &mut binds).map_err(|e| Error::Backend(e.to_string()))?;
    f(result)
}

impl DuckBackend for DuckEngine {
    fn load(
        &self,
        conn_str: &str,
    ) -> Result<()> {
        let path = resolve_path(conn_str);
        let db =
            if path == ":memory:" { Database::open_in_memory() } else { Database::open(&path) }
                .map_err(|e| Error::Backend(e.to_string()))?;

        // Run any configured migrations before publishing the connection, so a failed
        // migration never leaves a half-migrated database registered.
        if let Some(migrations) = self.migrations.get(conn_str) {
            if !migrations.is_empty() {
                let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
                crate::migration::run_pending(&mut conn, migrations)?;
            }
        }

        self.dbs.lock().expect("duck registry poisoned").insert(conn_str.to_owned(), db);
        Ok(())
    }

    fn close(
        &self,
        conn_str: &str,
    ) -> Result<bool> {
        Ok(self.dbs.lock().expect("duck registry poisoned").remove(conn_str).is_some())
    }

    fn select(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<Row>> {
        self.with_conn(conn_str, |conn| {
            run_with_params(conn, sql, &params, |result| {
                let rs = result.materialize().map_err(|e| Error::Backend(e.to_string()))?;
                let names = rs.column_names();
                let mut rows = Vec::with_capacity(rs.len());
                for row in rs.rows() {
                    let mut obj = Map::new();
                    for (i, name) in names.iter().enumerate() {
                        let value = row.get_idx(i).map_or(Value::Null, duck_to_json);
                        obj.insert(name.to_string(), value);
                    }
                    rows.push(obj);
                }
                Ok(rows)
            })
        })
    }

    fn execute(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ExecuteResult> {
        self.with_conn(conn_str, |conn| {
            run_with_params(conn, sql, &params, |mut result| {
                Ok(ExecuteResult { rows_affected: result.changes() })
            })
        })
    }
}
