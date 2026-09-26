//! End-to-end tests for the plugin's execution engine (the `DuckBackend`
//! implementation behind the `load`/`execute`/`select`/`close` commands), plus the
//! core inline migration path. These exercise the real logic without a webview.

use std::collections::HashMap;

use better_duck_tauri::backend::{DataFormat, DuckBackend, DuckEngine};
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
    let engine = DuckEngine::with_config(HashMap::new(), policy, Vec::new());
    engine.load(MEM).unwrap();
    assert!(engine.execute(MEM, "CREATE TABLE t (id INTEGER)", vec![]).is_err());
    assert!(engine.select(MEM, "SELECT 42 AS n", vec![]).is_ok());
}

#[test]
fn risky_statement_policy_rejects_attach() {
    let policy = Policy { deny_risky_statements: true, ..Policy::default() };
    let engine = DuckEngine::with_config(HashMap::new(), policy, Vec::new());
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
    let engine = DuckEngine::with_config(HashMap::new(), policy, Vec::new());
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
    let engine = DuckEngine::with_config(HashMap::new(), policy, Vec::new());
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
    let engine = DuckEngine::with_config(HashMap::new(), Policy::default(), vec![hook]);
    engine.load(MEM).unwrap();

    let rows = engine.select(MEM, "SELECT plus_one(41) AS v", vec![]).unwrap();
    assert_eq!(rows[0]["v"], json!(42));
}



