//! `DuckEngine`: the connection registry + query execution built on `better-duck-core`.
//!
//! Both `core::init()` and (for now) `diesel::init()` construct this. The Diesel-native
//! path (shared `DuckDbConnection` r2d2 pool + Diesel migrations) is layered on in a later
//! phase (see TASKS T4.5); the webview API is identical meanwhile.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use better_duck_core::connection::Connection;
use better_duck_core::database::Database;
use better_duck_core::types::appendable::AppendAble;
use better_duck_core::{AccessMode, Config, QueryControl};
use serde_json::{Map, Value};

use super::{resolve_path, DataFormat, DuckBackend, ExecuteResult, Row};
use crate::checkpoint::CheckpointConfig;
use crate::error::{Error, Result};
use crate::json::{duck_to_json, json_to_duck};
use crate::migration::Migration;
use crate::policy::Policy;

/// A per-connection setup hook, run on every opened connection — the place to register
/// UDFs (`#[duckdb_*]` macros) or `LOAD` extensions so each connection carries them.
pub type ConnectHook = Arc<dyn Fn(&mut Connection) -> Result<()> + Send + Sync>;

/// Full engine configuration, assembled by `core::Builder`.
#[derive(Default)]
pub struct EngineConfig {
    /// Per-connection migrations, run on `load`.
    pub migrations: HashMap<String, Vec<Migration>>,
    /// Security policy.
    pub policy: Policy,
    /// Per-connection setup hooks.
    pub on_connect: Vec<ConnectHook>,
    /// Checkpoint strategy configuration.
    pub checkpoint: CheckpointConfig,
}

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
    /// Security policy enforced on every operation.
    policy: Policy,
    /// Hooks run on every opened connection (e.g. UDF registration).
    on_connect: Vec<ConnectHook>,
    /// Checkpoint strategy configuration.
    checkpoint_config: CheckpointConfig,
    /// Count of write operations, for the `AfterWrites` strategy.
    write_count: AtomicU64,
    /// Interrupt handles for in-flight streaming queries, keyed by connection string.
    active_queries: Mutex<HashMap<String, QueryControl>>,
}

impl DuckEngine {
    /// Creates an empty engine with no migrations and a permissive policy.
    pub fn new() -> DuckEngine {
        DuckEngine::default()
    }

    /// Creates an engine that runs the given per-connection migrations on `load`.
    pub fn with_migrations(migrations: HashMap<String, Vec<Migration>>) -> DuckEngine {
        DuckEngine { migrations, ..DuckEngine::default() }
    }

    /// Creates an engine from a full [`EngineConfig`].
    pub fn with_config(config: EngineConfig) -> DuckEngine {
        DuckEngine {
            dbs: Mutex::new(HashMap::new()),
            migrations: config.migrations,
            policy: config.policy,
            on_connect: config.on_connect,
            checkpoint_config: config.checkpoint,
            write_count: AtomicU64::new(0),
            active_queries: Mutex::new(HashMap::new()),
        }
    }

    /// Honors the `AfterWrites` checkpoint strategy after a write operation.
    fn note_write(
        &self,
        conn_str: &str,
    ) {
        if let Some(n) = self.checkpoint_config.after_writes {
            if n > 0 {
                let count = self.write_count.fetch_add(1, Ordering::Relaxed) + 1;
                if count.is_multiple_of(n) {
                    let _ = self.checkpoint(conn_str, self.checkpoint_config.force);
                }
            }
        }
    }

    fn with_conn<T>(
        &self,
        conn_str: &str,
        f: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        // Clone the handle (cheap Arc bump) and release the registry lock before running
        // `f`, so a long operation (e.g. a stream) doesn't block other connections.
        let db = {
            let dbs = self.dbs.lock().expect("duck registry poisoned");
            dbs.get(conn_str).cloned().ok_or_else(|| Error::UnknownConnection(conn_str.to_owned()))?
        };
        let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
        for hook in &self.on_connect {
            hook(&mut conn)?;
        }
        f(&mut conn)
    }

    /// Runs a read query and collects all rows as JSON. Does not apply the statement
    /// policy — callers that forward raw webview SQL must check the policy first.
    fn query(
        &self,
        conn_str: &str,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Row>> {
        self.with_conn(conn_str, |conn| run_with_params(conn, sql, params, collect_rows))
    }
}

/// Materializes a query result into JSON row objects.
fn collect_rows(result: better_duck_core::DuckResult) -> Result<Vec<Row>> {
    let rs = result.materialize().map_err(|e| Error::Backend(e.to_string()))?;
    let names = rs.column_names();
    let mut rows = Vec::with_capacity(rs.len());
    for row in rs.rows() {
        let mut obj = Map::new();
        for (i, name) in names.iter().enumerate() {
            obj.insert(name.to_string(), row.get_idx(i).map_or(Value::Null, duck_to_json));
        }
        rows.push(obj);
    }
    Ok(rows)
}

/// Iterates a query result lazily, delivering JSON row batches of at most `chunk` rows.
fn stream_batches(
    result: better_duck_core::DuckResult,
    names: &[String],
    chunk: usize,
    on_batch: &mut dyn FnMut(Vec<Row>) -> Result<()>,
) -> Result<u64> {
    let mut batch: Vec<Row> = Vec::with_capacity(chunk);
    let mut total = 0u64;
    for row in result {
        let row = row.map_err(|e| Error::Backend(e.to_string()))?;
        let mut obj = Map::new();
        for (i, name) in names.iter().enumerate() {
            obj.insert(name.clone(), row.get_idx(i).map_or(Value::Null, duck_to_json));
        }
        batch.push(obj);
        total += 1;
        if batch.len() >= chunk {
            on_batch(std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        on_batch(batch)?;
    }
    Ok(total)
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
        self.policy.check_connection(conn_str)?;
        let path = resolve_path(conn_str);
        let migrations = self.migrations.get(conn_str).filter(|m| !m.is_empty());
        let in_memory = path == ":memory:";

        // For on-disk databases, run migrations through the crash-safe backup protocol
        // BEFORE opening the long-lived handle, so it has exclusive file access. Skipped
        // in read-only mode (migrations are writes) and for in-memory DBs (no file, and
        // the data must live on the handle we actually register — handled below).
        if let Some(migrations) = migrations {
            if !in_memory && !self.policy.read_only {
                crate::backup::migrate_with_backup(Path::new(&path), migrations)?;
            }
        }

        let db = if in_memory {
            Database::open_in_memory().map_err(|e| Error::Backend(e.to_string()))?
        } else if self.policy.read_only {
            let config = Config::default()
                .access_mode(AccessMode::ReadOnly)
                .map_err(|e| Error::Backend(e.to_string()))?;
            Database::open_with_flags(&path, config).map_err(|e| Error::Backend(e.to_string()))?
        } else {
            Database::open(&path).map_err(|e| Error::Backend(e.to_string()))?
        };

        // In-memory migrations must run on the registered handle (a separate open would
        // discard them). No file to back up, so no crash-safe protocol applies.
        if in_memory && !self.policy.read_only {
            if let Some(migrations) = migrations {
                let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
                crate::migration::run_pending(&mut conn, migrations)?;
            }
        }

        // `Automatic` checkpoint strategy: set DuckDB's WAL-size auto-checkpoint threshold.
        if let Some(threshold) = &self.checkpoint_config.threshold {
            let mut conn = db.connect().map_err(|e| Error::Backend(e.to_string()))?;
            conn.execute_batch(format!("SET checkpoint_threshold='{}'", threshold.replace('\'', "''")))
                .map_err(|e| Error::Backend(e.to_string()))?;
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
        self.policy.check_statement(sql)?;
        self.query(conn_str, sql, &params)
    }

    fn execute(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<ExecuteResult> {
        self.policy.check_writable()?;
        self.policy.check_statement(sql)?;
        let result = self.with_conn(conn_str, |conn| {
            run_with_params(conn, sql, &params, |mut result| {
                Ok(ExecuteResult { rows_affected: result.changes() })
            })
        })?;
        self.note_write(conn_str);
        Ok(result)
    }

    fn load_extension(
        &self,
        conn_str: &str,
        name: &str,
    ) -> Result<()> {
        self.policy.check_extension(name)?;
        self.with_conn(conn_str, |conn| {
            conn.ensure_extension(name).map_err(|e| Error::Backend(e.to_string()))
        })
    }

    fn import(
        &self,
        conn_str: &str,
        table: &str,
        source: &str,
        format: DataFormat,
    ) -> Result<ExecuteResult> {
        self.policy.check_writable()?;
        self.policy.check_path(source)?;
        validate_identifier(table)?;
        let reader = match format {
            DataFormat::Parquet => "read_parquet",
            DataFormat::Csv => "read_csv_auto",
            DataFormat::Json => "read_json_auto",
        };
        let sql =
            format!("CREATE TABLE {table} AS SELECT * FROM {reader}({})", quote_literal(source));
        let result = self.with_conn(conn_str, |conn| {
            run_with_params(conn, &sql, &[], |mut result| {
                Ok(ExecuteResult { rows_affected: result.changes() })
            })
        })?;
        self.note_write(conn_str);
        Ok(result)
    }

    fn export(
        &self,
        conn_str: &str,
        query: &str,
        path: &str,
        format: DataFormat,
    ) -> Result<ExecuteResult> {
        self.policy.check_path(path)?;
        let fmt = match format {
            DataFormat::Parquet => "PARQUET",
            DataFormat::Csv => "CSV",
            DataFormat::Json => "JSON",
        };
        let sql = format!("COPY ({query}) TO {} (FORMAT {fmt})", quote_literal(path));
        self.with_conn(conn_str, |conn| {
            run_with_params(conn, &sql, &[], |mut result| {
                Ok(ExecuteResult { rows_affected: result.changes() })
            })
        })
    }

    fn append_rows(
        &self,
        conn_str: &str,
        table: &str,
        rows: Vec<Map<String, Value>>,
    ) -> Result<ExecuteResult> {
        self.policy.check_writable()?;
        validate_identifier(table)?;
        if rows.is_empty() {
            return Ok(ExecuteResult { rows_affected: 0 });
        }
        let columns: Vec<&str> = rows[0].keys().map(String::as_str).collect();
        for &column in &columns {
            validate_identifier(column)?;
        }

        let mut values: Vec<better_duck_core::types::value::DuckValue> =
            Vec::with_capacity(rows.len() * columns.len());
        let mut tuples: Vec<String> = Vec::with_capacity(rows.len());
        let mut idx = 1usize;
        for row in &rows {
            let mut placeholders = Vec::with_capacity(columns.len());
            for &column in &columns {
                let value = row.get(column).cloned().unwrap_or(Value::Null);
                values.push(json_to_duck(&value));
                placeholders.push(format!("${idx}"));
                idx += 1;
            }
            tuples.push(format!("({})", placeholders.join(", ")));
        }

        let column_list = columns.join(", ");
        let sql = format!("INSERT INTO {table} ({column_list}) VALUES {}", tuples.join(", "));
        let result = self.with_conn(conn_str, |conn| {
            let mut binds: Vec<&mut dyn AppendAble> =
                values.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            let mut result =
                conn.execute_with(&sql, &mut binds).map_err(|e| Error::Backend(e.to_string()))?;
            Ok(ExecuteResult { rows_affected: result.changes() })
        })?;
        self.note_write(conn_str);
        Ok(result)
    }

    fn list_tables(
        &self,
        conn_str: &str,
    ) -> Result<Vec<Row>> {
        self.query(
            conn_str,
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'main' ORDER BY table_name",
            &[],
        )
    }

    fn list_columns(
        &self,
        conn_str: &str,
        table: &str,
    ) -> Result<Vec<Row>> {
        self.query(
            conn_str,
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[Value::String(table.to_owned())],
        )
    }

    fn explain(
        &self,
        conn_str: &str,
        sql: &str,
    ) -> Result<Vec<Row>> {
        self.policy.check_statement(sql)?;
        self.query(conn_str, &format!("EXPLAIN {sql}"), &[])
    }

    fn select_stream(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
        chunk_size: usize,
        on_batch: &mut dyn FnMut(Vec<Row>) -> Result<()>,
    ) -> Result<u64> {
        self.policy.check_statement(sql)?;
        let chunk = chunk_size.max(1);
        self.with_conn(conn_str, |conn| {
            let mut duck: Vec<better_duck_core::types::value::DuckValue> =
                params.iter().map(json_to_duck).collect();
            let mut binds: Vec<&mut dyn AppendAble> =
                duck.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            let result =
                conn.execute_with(sql, &mut binds).map_err(|e| Error::Backend(e.to_string()))?;
            let names: Vec<String> =
                result.column_names().iter().map(std::string::ToString::to_string).collect();

            // Register an interrupt handle so `interrupt(conn_str)` can cancel this scan
            // from another thread; always deregister when the stream ends.
            self.active_queries
                .lock()
                .expect("duck query registry poisoned")
                .insert(conn_str.to_owned(), conn.query_control());
            let outcome = stream_batches(result, &names, chunk, on_batch);
            self.active_queries.lock().expect("duck query registry poisoned").remove(conn_str);
            outcome
        })
    }

    fn interrupt(
        &self,
        conn_str: &str,
    ) -> bool {
        self.active_queries
            .lock()
            .expect("duck query registry poisoned")
            .get(conn_str)
            .is_some_and(QueryControl::interrupt)
    }

    fn checkpoint(
        &self,
        conn_str: &str,
        force: bool,
    ) -> Result<()> {
        let sql = if force { "FORCE CHECKPOINT" } else { "CHECKPOINT" };
        self.with_conn(conn_str, |conn| {
            conn.execute_batch(sql).map_err(|e| Error::Backend(e.to_string()))
        })
    }

    fn checkpoint_all(
        &self,
        force: bool,
    ) {
        let sql = if force { "FORCE CHECKPOINT" } else { "CHECKPOINT" };
        // Snapshot the handles under the lock, then checkpoint outside it (best-effort).
        let dbs: Vec<Database> =
            self.dbs.lock().expect("duck registry poisoned").values().cloned().collect();
        for db in dbs {
            if let Ok(mut conn) = db.connect() {
                let _ = conn.execute_batch(sql);
            }
        }
    }

    fn revert(
        &self,
        conn_str: &str,
    ) -> Result<Option<i64>> {
        self.policy.check_writable()?;
        let migrations = self.migrations.get(conn_str).map_or(&[][..], Vec::as_slice);
        let reverted =
            self.with_conn(conn_str, |conn| crate::migration::revert_last(conn, migrations))?;
        self.note_write(conn_str);
        Ok(reverted)
    }
}

/// Quotes a value as a SQL single-quoted string literal (doubling embedded quotes).
fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Validates that `name` is a safe bare SQL identifier (ASCII alnum + `_`). This is a
/// baseline guard until the P2 scope model enforces table/path allow-lists.
fn validate_identifier(name: &str) -> Result<()> {
    if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(Error::Backend(format!("invalid table identifier: {name:?}")))
    }
}
