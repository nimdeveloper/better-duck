//! Opening connections: in-memory, on-disk, and with a custom `Config`.
//!
//! `Connection` is the high-level handle. Each `open_in_memory()` connection gets its own
//! **independent** in-memory database (to share one, see `sharing.rs`).

use better_duck_core::config::{AccessMode, Config};
use better_duck_core::connection::Connection;
use common::{section, show, Result};

fn main() -> Result<()> {
    section("in-memory connection");
    let mut conn = Connection::open_in_memory()?;
    conn.execute_batch("CREATE TABLE t (id INTEGER)")?;
    show("is_open", conn.is_open());
    // `close` consumes the connection and surfaces any close-time error.
    conn.close()?;

    section("on-disk connection");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("demo.duckdb");
    {
        let mut disk = Connection::open(&path)?;
        disk.execute_batch("CREATE TABLE persisted (v INTEGER)")?;
        disk.execute_batch("INSERT INTO persisted VALUES (1), (2)")?;
        disk.close()?;
    }
    // Re-open the same file: the data is still there.
    let mut reopened = Connection::open(&path)?;
    let rows = reopened.execute("SELECT v FROM persisted ORDER BY v")?.count();
    show("rows read back from file", rows);
    assert_eq!(rows, 2, "persisted rows should survive close + reopen");
    assert!(path.exists(), "database file should have been created");

    section("connection with custom Config");
    // A read-write, single-threaded, memory-capped configuration.
    let config = Config::default()
        .access_mode(AccessMode::ReadWrite)?
        .threads(1)?
        .max_memory("256MB")?;
    let mut configured = Connection::open_in_memory_with_flags(config)?;
    configured.execute_batch("CREATE TABLE cfg (v INTEGER)")?;
    show("configured connection open", configured.is_open());

    Ok(())
}
