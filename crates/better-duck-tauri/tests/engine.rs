//! End-to-end tests for the plugin's execution engine (the `DuckBackend`
//! implementation behind the `load`/`execute`/`select`/`close` commands), plus the
//! core inline migration path. These exercise the real logic without a webview.

use std::collections::HashMap;

use better_duck_tauri::backend::{DataFormat, DuckBackend, DuckEngine, EngineConfig};
use better_duck_tauri::{Migration, MigrationKind, Policy};
use serde_json::json;

const MEM: &str = "duckdb::memory:";

#[test]
fn load_execute_select_close_roundtrip() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();

    engine.execute(MEM, "CREATE TABLE t (id INTEGER, name VARCHAR)", vec![]).unwrap();
    engine.execute(MEM, "INSERT INTO t VALUES ($1, $2)", vec![json!(1), json!("duck")]).unwrap();

    let rows = engine.select(MEM, "SELECT id, name FROM t ORDER BY id", vec![]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(1));
    assert_eq!(rows[0]["name"], json!("duck"));

    assert!(engine.close(MEM).unwrap(), "close should report the connection existed");
    // After close, the connection is unknown.
    assert!(engine.select(MEM, "SELECT 1", vec![]).is_err());
}

#[test]
fn select_on_unloaded_connection_errors() {
    let engine = DuckEngine::new();
    assert!(engine.select(MEM, "SELECT 1", vec![]).is_err());
}

#[test]
fn select_maps_composite_and_decimal_to_json() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    let rows = engine
        .select(
            MEM,
            "SELECT CAST(1234.56 AS DECIMAL(6,2)) AS d, [1, 2, 3] AS lst, \
             {'a': 1, 'b': 'x'} AS s",
            vec![],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["d"], json!("1234.56")); // DECIMAL → exact string
    assert_eq!(rows[0]["lst"], json!([1, 2, 3])); // LIST → array
    assert_eq!(rows[0]["s"], json!({ "a": 1, "b": "x" })); // STRUCT → object
}

#[test]
fn export_then_import_csv_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.csv");
    let path_str = path.to_str().unwrap();

    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    engine.execute(MEM, "CREATE TABLE src (id INTEGER, name VARCHAR)", vec![]).unwrap();
    engine.execute(MEM, "INSERT INTO src VALUES (1, 'a'), (2, 'b')", vec![]).unwrap();

    engine.export(MEM, "SELECT * FROM src ORDER BY id", path_str, DataFormat::Csv).unwrap();
    engine.import(MEM, "dst", path_str, DataFormat::Csv).unwrap();

    let rows = engine.select(MEM, "SELECT id, name FROM dst ORDER BY id", vec![]).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["name"], json!("a"));
    assert_eq!(rows[1]["id"], json!(2));
}

#[test]
fn import_rejects_bad_table_identifier() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    assert!(engine.import(MEM, "bad; DROP TABLE x", "whatever.csv", DataFormat::Csv).is_err());
}

#[test]
fn append_rows_bulk_inserts() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    engine.execute(MEM, "CREATE TABLE t (id INTEGER, name VARCHAR)", vec![]).unwrap();

    let rows = vec![
        json!({ "id": 1, "name": "a" }).as_object().unwrap().clone(),
        json!({ "id": 2, "name": "b" }).as_object().unwrap().clone(),
    ];
    let res = engine.append_rows(MEM, "t", rows).unwrap();
    assert_eq!(res.rows_affected, 2);

    let out = engine.select(MEM, "SELECT id, name FROM t ORDER BY id", vec![]).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[1]["name"], json!("b"));
}

#[test]
fn migrations_run_on_load() {
    let mut map = HashMap::new();
    map.insert(
        MEM.to_owned(),
        vec![
            Migration {
                version: 1,
                description: "create widgets".to_owned(),
                sql: "CREATE TABLE widgets (id INTEGER)".to_owned(),
                kind: MigrationKind::Up,
            },
            Migration {
                version: 2,
                description: "seed".to_owned(),
                sql: "INSERT INTO widgets VALUES (42)".to_owned(),
                kind: MigrationKind::Up,
            },
        ],
    );
    let engine = DuckEngine::with_migrations(map);
    engine.load(MEM).unwrap();

    let rows = engine.select(MEM, "SELECT id FROM widgets", vec![]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(42));
}

#[test]
fn read_only_policy_rejects_writes_but_allows_reads() {
    let policy = Policy { read_only: true, ..Policy::default() };
    let engine = DuckEngine::with_config(EngineConfig { policy, ..Default::default() });
    engine.load(MEM).unwrap();
    assert!(engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).is_err());
    assert!(engine.select(MEM, "SELECT 42 AS n", vec![]).is_ok());
}

#[test]
fn risky_statement_policy_rejects_attach() {
    let policy = Policy { deny_risky_statements: true, ..Policy::default() };
    let engine = DuckEngine::with_config(EngineConfig { policy, ..Default::default() });
    engine.load(MEM).unwrap();
    assert!(engine.execute(MEM, "ATTACH ':memory:' AS other", vec![]).is_err());
    // Ordinary DDL is not a "risky" statement kind and stays allowed.
    assert!(engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).is_ok());
}

#[test]
fn path_allow_list_blocks_outside_import() {
    let allowed = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let policy =
        Policy { allowed_paths: Some(vec![allowed.path().to_path_buf()]), ..Policy::default() };
    let engine = DuckEngine::with_config(EngineConfig { policy, ..Default::default() });
    engine.load(MEM).unwrap();

    let outside_file = outside.path().join("data.csv");
    assert!(engine
        .import(MEM, "t", outside_file.to_str().unwrap(), DataFormat::Csv)
        .is_err());
}

#[test]
fn connection_allow_list_blocks_unlisted() {
    let policy =
        Policy { allowed_connections: Some(vec!["duckdb:ok.db".to_owned()]), ..Policy::default() };
    let engine = DuckEngine::with_config(EngineConfig { policy, ..Default::default() });
    assert!(engine.load("duckdb::memory:").is_err());
}

#[test]
fn introspection_lists_tables_and_columns() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    engine.execute(MEM, "CREATE TABLE t (id INTEGER, name VARCHAR)", vec![]).unwrap();

    let tables = engine.list_tables(MEM).unwrap();
    assert!(tables.iter().any(|r| r["table_name"] == json!("t")));

    let cols = engine.list_columns(MEM, "t").unwrap();
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0]["column_name"], json!("id"));
}

#[test]
fn explain_returns_a_plan() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).unwrap();
    let plan = engine.explain(MEM, "SELECT * FROM t").unwrap();
    assert!(!plan.is_empty());
}

#[test]
fn select_stream_batches_rows() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).unwrap();
    engine.execute(MEM, "INSERT INTO t VALUES (1), (2), (3), (4), (5)", vec![]).unwrap();

    let mut batch_sizes: Vec<usize> = Vec::new();
    let mut collected = 0usize;
    let total = engine
        .select_stream(MEM, "SELECT id FROM t ORDER BY id", vec![], 2, &mut |batch| {
            batch_sizes.push(batch.len());
            collected += batch.len();
            Ok(())
        })
        .unwrap();

    assert_eq!(total, 5);
    assert_eq!(collected, 5);
    assert_eq!(batch_sizes, vec![2, 2, 1]); // chunk 2 over 5 rows
}

#[test]
fn on_connect_hook_runs_per_connection() {
    use std::sync::Arc;

    use better_duck_tauri::{ConnectHook, Connection, Error};

    // The hook registers a macro on every opened connection; a later query uses it.
    let hook: ConnectHook = Arc::new(|conn: &mut Connection| {
        conn.execute_batch("CREATE OR REPLACE MACRO plus_one(x) AS x + 1")
            .map_err(|e| Error::Backend(e.to_string()))
    });
    let engine = DuckEngine::with_config(EngineConfig { on_connect: vec![hook], ..Default::default() });
    engine.load(MEM).unwrap();

    let rows = engine.select(MEM, "SELECT plus_one(41) AS v", vec![]).unwrap();
    assert_eq!(rows[0]["v"], json!(42));
}

#[test]
fn network_extension_blocked_by_default() {
    // `httpfs` enables network egress; the default policy must refuse it before any
    // install/network attempt.
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    assert!(engine.load_extension(MEM, "httpfs").is_err());
}

#[test]
fn interrupt_tracks_active_stream_lifecycle() {
    let engine = DuckEngine::new();
    engine.load(MEM).unwrap();
    // Nothing streaming yet.
    assert!(!engine.interrupt(MEM));

    engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).unwrap();
    engine.execute(MEM, "INSERT INTO t VALUES (1), (2)", vec![]).unwrap();
    let mut n = 0u64;
    engine
        .select_stream(MEM, "SELECT id FROM t", vec![], 10, &mut |b| {
            n += b.len() as u64;
            Ok(())
        })
        .unwrap();
    assert_eq!(n, 2);
    // The interrupt handle is deregistered once the stream finishes.
    assert!(!engine.interrupt(MEM));
}

fn up(version: i64, sql: &str) -> Migration {
    Migration { version, description: format!("m{version}"), sql: sql.to_owned(), kind: MigrationKind::Up }
}

fn down(version: i64, sql: &str) -> Migration {
    Migration {
        version,
        description: format!("m{version}-down"),
        sql: sql.to_owned(),
        kind: MigrationKind::Down,
    }
}

#[test]
fn revert_undoes_migrations_in_reverse() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("rev.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    let mut map = HashMap::new();
    map.insert(
        conn.clone(),
        vec![
            up(1, "CREATE TABLE t (id INTEGER)"),
            down(1, "DROP TABLE t"),
            up(2, "CREATE TABLE u (id INTEGER)"),
            down(2, "DROP TABLE u"),
        ],
    );
    let engine = DuckEngine::with_migrations(map);
    engine.load(&conn).unwrap();
    assert!(engine.select(&conn, "SELECT * FROM u", vec![]).is_ok());

    // Revert v2, then v1, then nothing.
    assert_eq!(engine.revert(&conn).unwrap(), Some(2));
    assert!(engine.select(&conn, "SELECT * FROM u", vec![]).is_err());
    assert!(engine.select(&conn, "SELECT * FROM t", vec![]).is_ok());

    assert_eq!(engine.revert(&conn).unwrap(), Some(1));
    assert!(engine.select(&conn, "SELECT * FROM t", vec![]).is_err());

    assert_eq!(engine.revert(&conn).unwrap(), None);
}

#[test]
fn checkpoint_flushes_and_all_is_best_effort() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("c.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    let engine = DuckEngine::new();
    engine.load(&conn).unwrap();
    engine.execute(&conn, "CREATE TABLE t (id INTEGER)", vec![]).unwrap();
    engine.execute(&conn, "INSERT INTO t VALUES (1)", vec![]).unwrap();

    engine.checkpoint(&conn, false).unwrap();
    engine.checkpoint(&conn, true).unwrap();
    engine.checkpoint_all(true); // sweeps all loaded connections without panicking

    let rows = engine.select(&conn, "SELECT id FROM t", vec![]).unwrap();
    assert_eq!(rows[0]["id"], json!(1));
}

#[test]
fn checkpoint_threshold_is_applied_on_load() {
    use better_duck_tauri::CheckpointConfig;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("thr.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    let engine = DuckEngine::with_config(EngineConfig {
        checkpoint: CheckpointConfig { threshold: Some("64MB".to_owned()), ..Default::default() },
        ..Default::default()
    });
    engine.load(&conn).unwrap();

    let rows = engine
        .select(&conn, "SELECT current_setting('checkpoint_threshold') AS v", vec![])
        .unwrap();
    assert!(matches!(&rows[0]["v"], serde_json::Value::String(s) if !s.is_empty()));
}

#[test]
fn after_writes_strategy_runs_clean() {
    use better_duck_tauri::CheckpointConfig;
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("aw.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    let engine = DuckEngine::with_config(EngineConfig {
        checkpoint: CheckpointConfig { after_writes: Some(2), force: true, ..Default::default() },
        ..Default::default()
    });
    engine.load(&conn).unwrap();
    engine.execute(&conn, "CREATE TABLE t (id INTEGER)", vec![]).unwrap(); // write 1
    engine.execute(&conn, "INSERT INTO t VALUES (1)", vec![]).unwrap(); // write 2 → checkpoint

    let rows = engine.select(&conn, "SELECT count(*) AS c FROM t", vec![]).unwrap();
    assert_eq!(rows[0]["c"], json!(1));
}

#[test]
fn on_disk_migrations_apply_and_leave_no_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    let mut map = HashMap::new();
    map.insert(conn.clone(), vec![up(1, "CREATE TABLE t (id INTEGER)"), up(2, "INSERT INTO t VALUES (7)")]);
    let engine = DuckEngine::with_migrations(map);
    engine.load(&conn).unwrap();

    let rows = engine.select(&conn, "SELECT id FROM t", vec![]).unwrap();
    assert_eq!(rows[0]["id"], json!(7));

    // The backup protocol cleans up after a successful run.
    assert!(!dir.path().join("app.duckdb.bak").exists());
    assert!(!dir.path().join("app.duckdb.migrate-journal").exists());
}

#[test]
fn interrupted_migration_recovers_from_backup() {
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.duckdb");
    let conn = format!("duckdb:{}", db.to_str().unwrap());

    // Establish the pre-batch state and capture a backup of it.
    let setup = DuckEngine::new();
    setup.load(&conn).unwrap();
    setup.execute(&conn, "CREATE TABLE base (id INTEGER)", vec![]).unwrap();
    setup.execute(&conn, "INSERT INTO base VALUES (1)", vec![]).unwrap();
    setup.close(&conn).unwrap(); // clean close checkpoints, so the file is self-contained
    fs::copy(&db, dir.path().join("app.duckdb.bak")).unwrap();

    // Simulate an interrupted run: a half-applied change plus a leftover journal.
    let dirty = DuckEngine::new();
    dirty.load(&conn).unwrap();
    dirty.execute(&conn, "CREATE TABLE partial (x INTEGER)", vec![]).unwrap();
    dirty.close(&conn).unwrap();
    fs::write(dir.path().join("app.duckdb.migrate-journal"), "{}").unwrap();

    // Recovery: restore the backup (dropping `partial`), then apply the migration.
    let mut map = HashMap::new();
    map.insert(conn.clone(), vec![up(10, "CREATE TABLE migrated (id INTEGER)")]);
    let engine = DuckEngine::with_migrations(map);
    engine.load(&conn).unwrap();

    assert_eq!(engine.select(&conn, "SELECT id FROM base", vec![]).unwrap()[0]["id"], json!(1));
    assert!(engine.select(&conn, "SELECT * FROM migrated", vec![]).is_ok());
    assert!(engine.select(&conn, "SELECT * FROM partial", vec![]).is_err(), "restore should drop the half-applied table");
    assert!(!dir.path().join("app.duckdb.bak").exists());
    assert!(!dir.path().join("app.duckdb.migrate-journal").exists());
}




