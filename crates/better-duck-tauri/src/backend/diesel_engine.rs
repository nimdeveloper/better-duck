//! `DieselEngine`: the Diesel-native backend (feature `backend-diesel`).
//!
//! Unlike [`DuckEngine`](super::engine::DuckEngine), which opens a short-lived core
//! `Connection` per operation from a shared `Database`, this engine keeps a Diesel r2d2
//! pool of [`DuckDbConnection`]s per connection string and runs Diesel
//! (`embed_migrations!`) migrations through the `MigrationHarness`. Dynamic-column SQL
//! (the plugin forwards arbitrary statements + JSON params) is executed on the pooled
//! connection's underlying core `Connection` via `inner_mut()`, reusing the same
//! `DuckValue`⇄JSON materialization as the core engine — Diesel's compile-time
//! `QueryableByName` can't express the plugin's dynamic result shape.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use better_duck_core::connection::Connection;
use better_duck_core::database::Database;
use better_duck_core::types::appendable::AppendAble;
use better_duck_core::types::value::DuckValue;
use better_duck_core::{AccessMode, Config, QueryControl};
use better_duck_diesel::pool::SharedDuckDbConnectionManager;
use better_duck_diesel::DuckDbConnection;
use diesel::backend::Backend;
use diesel::migration::{Migration, MigrationSource};
use diesel::Connection as _;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use r2d2::Pool;
use serde_json::{Map, Value};

use super::engine::{
    collect_rows, quote_literal, run_with_params, stream_batches, validate_identifier, ConnectHook,
};
use super::{resolve_path, DataFormat, DuckBackend, ExecuteResult, Row};
use crate::checkpoint::CheckpointConfig;
use crate::error::{Error, Result};
use crate::json::json_to_duck;
use crate::policy::Policy;

/// Full configuration for the Diesel-native engine, assembled by `diesel::Builder`.
#[derive(Default)]
pub struct DieselEngineConfig {
    /// Per-connection Diesel embedded migrations, run on `load`.
    pub migrations: HashMap<String, EmbeddedMigrations>,
    /// Security policy.
    pub policy: Policy,
    /// Per-connection setup hooks (run on the underlying core connection).
    pub on_connect: Vec<ConnectHook>,
    /// Checkpoint strategy configuration.
    pub checkpoint: CheckpointConfig,
}

/// Borrows a stored [`EmbeddedMigrations`] as an owned [`MigrationSource`]: the Diesel
/// harness takes its source by value, but `EmbeddedMigrations` is neither `Clone` nor
/// `Copy`, so we pass a thin by-value wrapper around a (Copy) reference and delegate.
struct MigrationsRef<'a>(&'a EmbeddedMigrations);

impl<DB: Backend> MigrationSource<DB> for MigrationsRef<'_> {
    fn migrations(&self) -> diesel::migration::Result<Vec<Box<dyn Migration<DB>>>> {
        MigrationSource::<DB>::migrations(self.0)
    }
}

/// Reports whether `embedded` has pending migrations against the on-disk DB at `path`.
/// Opens (and drops) its own short-lived connection so the file is quiesced for the
/// surrounding backup protocol.
fn diesel_has_pending(
    path: &str,
    embedded: &EmbeddedMigrations,
) -> Result<bool> {
    let mut conn = DuckDbConnection::establish(path).map_err(|e| Error::Backend(e.to_string()))?;
    conn.has_pending_migration(MigrationsRef(embedded)).map_err(|e| Error::Backend(e.to_string()))
}

/// Applies `embedded`'s pending migrations to the on-disk DB at `path` (own connection).
fn diesel_run(
    path: &str,
    embedded: &EmbeddedMigrations,
) -> Result<()> {
    let mut conn = DuckDbConnection::establish(path).map_err(|e| Error::Backend(e.to_string()))?;
    conn.run_pending_migrations(MigrationsRef(embedded))
        .map_err(|e| Error::Backend(e.to_string()))?;
    Ok(())
}

// PLACEHOLDER_ENGINE

/// A registry of Diesel r2d2 pools keyed by connection string.
#[derive(Default)]
pub struct DieselEngine {
    pools: Mutex<HashMap<String, Pool<SharedDuckDbConnectionManager>>>,
    migrations: HashMap<String, EmbeddedMigrations>,
    policy: Policy,
    on_connect: Vec<ConnectHook>,
    checkpoint_config: CheckpointConfig,
    write_count: AtomicU64,
    active_queries: Mutex<HashMap<String, QueryControl>>,
}

impl DieselEngine {
    /// Creates an empty engine with no migrations and a permissive policy.
    pub fn new() -> DieselEngine {
        DieselEngine::default()
    }

    /// Creates an engine from a full [`DieselEngineConfig`].
    pub fn with_config(config: DieselEngineConfig) -> DieselEngine {
        DieselEngine {
            pools: Mutex::new(HashMap::new()),
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

    /// Runs `f` with the underlying core `Connection` of a pooled Diesel connection,
    /// applying the per-connection setup hooks first (matching `DuckEngine`'s semantics).
    fn with_conn<T>(
        &self,
        conn_str: &str,
        f: impl FnOnce(&mut Connection) -> Result<T>,
    ) -> Result<T> {
        let pool = {
            let pools = self.pools.lock().expect("diesel pool registry poisoned");
            pools
                .get(conn_str)
                .cloned()
                .ok_or_else(|| Error::UnknownConnection(conn_str.to_owned()))?
        };
        let mut conn = pool.get().map_err(|e| Error::Backend(e.to_string()))?;
        // `on_connect` hooks are applied once per physical connection by the pool manager
        // (see `load`), not here — re-running them on every checkout would, e.g., re-register
        // UDFs on an already-initialized connection and fail.
        f(conn.inner_mut())
    }

    /// Runs a read query and collects all rows as JSON (no statement-policy check here).
    fn query(
        &self,
        conn_str: &str,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Row>> {
        self.with_conn(conn_str, |conn| run_with_params(conn, sql, params, collect_rows))
    }
}

// PLACEHOLDER_IMPL

impl DuckBackend for DieselEngine {
    fn load(
        &self,
        conn_str: &str,
    ) -> Result<()> {
        self.policy.check_connection(conn_str)?;
        let path = resolve_path(conn_str);
        let in_memory = path == ":memory:";

        // On-disk: run Diesel migrations through the crash-safe backup protocol BEFORE the
        // pool opens a long-lived handle, so the file is quiesced for backup/restore. The
        // per-migration transactions still apply; the backup guards a whole-batch failure.
        if !in_memory && !self.policy.read_only {
            if let Some(embedded) = self.migrations.get(conn_str) {
                crate::backup::with_backup(
                    Path::new(&path),
                    || diesel_has_pending(&path, embedded),
                    || diesel_run(&path, embedded),
                )?;
            }
        }

        let manager = if in_memory {
            SharedDuckDbConnectionManager::memory().map_err(|e| Error::Backend(e.to_string()))?
        } else if self.policy.read_only {
            let config = Config::default()
                .access_mode(AccessMode::ReadOnly)
                .map_err(|e| Error::Backend(e.to_string()))?;
            let db = Database::open_with_flags(&path, config)
                .map_err(|e| Error::Backend(e.to_string()))?;
            SharedDuckDbConnectionManager::new(db)
        } else {
            SharedDuckDbConnectionManager::file(&path).map_err(|e| Error::Backend(e.to_string()))?
        };
        // Attach the plugin's per-connection setup hooks to the manager so they run exactly
        // once per physical connection (r2d2 reuses connections across checkouts).
        let hooks = self.on_connect.clone();
        let manager = manager.on_connect(move |c: &mut DuckDbConnection| {
            for hook in &hooks {
                hook(c.inner_mut()).map_err(|e| {
                    better_duck_core::error::Error::ToSqlConversionFailure(Box::new(e))
                })?;
            }
            Ok(())
        });
        let pool = Pool::builder().build(manager).map_err(|e| Error::Backend(e.to_string()))?;

        // In-memory: migrations run on the shared pool handle (a separate open would discard
        // them, and there is no file to back up).
        if in_memory && !self.policy.read_only {
            if let Some(embedded) = self.migrations.get(conn_str) {
                let mut conn = pool.get().map_err(|e| Error::Backend(e.to_string()))?;
                conn.run_pending_migrations(MigrationsRef(embedded))
                    .map_err(|e| Error::Backend(e.to_string()))?;
            }
        }

        // `Automatic` checkpoint strategy: set DuckDB's WAL-size auto-checkpoint threshold.
        if let Some(threshold) = &self.checkpoint_config.threshold {
            let mut conn = pool.get().map_err(|e| Error::Backend(e.to_string()))?;
            conn.inner_mut()
                .execute_batch(format!(
                    "SET checkpoint_threshold='{}'",
                    threshold.replace('\'', "''")
                ))
                .map_err(|e| Error::Backend(e.to_string()))?;
        }

        self.pools.lock().expect("diesel pool registry poisoned").insert(conn_str.to_owned(), pool);
        Ok(())
    }

    fn close(
        &self,
        conn_str: &str,
    ) -> Result<bool> {
        Ok(self.pools.lock().expect("diesel pool registry poisoned").remove(conn_str).is_some())
    }

    // PLACEHOLDER_METHODS

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
        self.policy.check_statement(query)?;
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

    // PLACEHOLDER_METHODS2

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

        let mut values: Vec<DuckValue> = Vec::with_capacity(rows.len() * columns.len());
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

    // PLACEHOLDER_METHODS3

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
            let mut duck: Vec<DuckValue> = params.iter().map(json_to_duck).collect();
            let mut binds: Vec<&mut dyn AppendAble> =
                duck.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            let result =
                conn.execute_with(sql, &mut binds).map_err(|e| Error::Backend(e.to_string()))?;
            let names: Vec<String> =
                result.column_names().iter().map(std::string::ToString::to_string).collect();
            self.active_queries
                .lock()
                .expect("diesel query registry poisoned")
                .insert(conn_str.to_owned(), conn.query_control());
            let outcome = stream_batches(result, &names, chunk, on_batch);
            self.active_queries.lock().expect("diesel query registry poisoned").remove(conn_str);
            outcome
        })
    }

    fn interrupt(
        &self,
        conn_str: &str,
    ) -> bool {
        self.active_queries
            .lock()
            .expect("diesel query registry poisoned")
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
        let pools: Vec<Pool<SharedDuckDbConnectionManager>> =
            self.pools.lock().expect("diesel pool registry poisoned").values().cloned().collect();
        for pool in pools {
            if let Ok(mut conn) = pool.get() {
                let _ = conn.inner_mut().execute_batch(sql);
            }
        }
    }

    fn revert(
        &self,
        conn_str: &str,
    ) -> Result<Option<i64>> {
        self.policy.check_writable()?;
        let Some(embedded) = self.migrations.get(conn_str) else {
            return Ok(None);
        };
        // Revert on a pooled connection; map the Diesel version string to i64 best-effort
        // (default Diesel migration versions are numeric timestamps). A missing last
        // migration surfaces as an error from the harness.
        let pool = {
            let pools = self.pools.lock().expect("diesel pool registry poisoned");
            pools
                .get(conn_str)
                .cloned()
                .ok_or_else(|| Error::UnknownConnection(conn_str.to_owned()))?
        };
        let mut conn = pool.get().map_err(|e| Error::Backend(e.to_string()))?;
        let version = conn
            .revert_last_migration(MigrationsRef(embedded))
            .map_err(|e| Error::Backend(e.to_string()))?;
        self.note_write(conn_str);
        Ok(version.to_string().parse::<i64>().ok())
    }

    #[cfg(feature = "arrow")]
    fn query_arrow(
        &self,
        conn_str: &str,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<Vec<u8>> {
        self.policy.check_statement(sql)?;
        self.with_conn(conn_str, |conn| {
            let mut duck: Vec<DuckValue> = params.iter().map(json_to_duck).collect();
            let mut binds: Vec<&mut dyn AppendAble> =
                duck.iter_mut().map(|v| v as &mut dyn AppendAble).collect();
            let result =
                conn.query_arrow(sql, &mut binds).map_err(|e| Error::Backend(e.to_string()))?;
            crate::arrow_ipc::to_ipc(result)
        })
    }
}
