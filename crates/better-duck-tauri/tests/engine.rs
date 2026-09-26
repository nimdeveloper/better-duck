//! End-to-end tests for the plugin's execution engine (the `DuckBackend`
//! implementation behind the `load`/`execute`/`select`/`close` commands), plus the
//! core inline migration path. These exercise the real logic without a webview.

use std::collections::HashMap;

use better_duck_tauri::backend::{DuckBackend, DuckEngine};
use better_duck_tauri::{Migration, MigrationKind};
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
