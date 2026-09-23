//! The `parquet` extension: write a table to a Parquet file and read it back.
//!
//! `COPY (...) TO '<path>' (FORMAT PARQUET)` materializes a query as Parquet, and
//! `read_parquet('<path>')` scans it back into a result set. The extension is
//! statically linked in this build (the `parquet` feature), so no runtime install
//! is needed.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::value::DuckValue;

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER, name VARCHAR)")?;
    conn.execute_batch("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")?;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("data.parquet");
    // DuckDB accepts forward slashes on every platform; normalize Windows paths.
    let path_sql = path.to_string_lossy().replace('\\', "/");

    conn.execute_batch(&format!(
        "COPY (SELECT id, name FROM t ORDER BY id) TO '{path_sql}' (FORMAT PARQUET)"
    ))?;
    assert!(path.exists(), "parquet file should have been written");

    let rows: Vec<_> = conn
        .execute(&format!("SELECT id, name FROM read_parquet('{path_sql}') ORDER BY id"))?
        .collect::<Result<Vec<_>>>()?;
    println!("read {} rows back from {path_sql}", rows.len());
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[0].get("name"), Some(&DuckValue::text("alpha")));
    assert_eq!(rows[2].get("id"), Some(&DuckValue::Int(3)));
    assert_eq!(rows[2].get("name"), Some(&DuckValue::text("gamma")));

    println!("\nParquet write + read_parquet round trip verified");
    Ok(())
}
