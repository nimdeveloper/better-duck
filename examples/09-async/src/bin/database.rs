//! The async database facade: `AsyncDatabase`.
//!
//! `AsyncDatabase` opens one database and hands out `AsyncConnection`s via `connect()`.
//! Every connection shares the same underlying database, so data written through one is
//! visible through another.

use better_duck_core::types::value::DuckValue;
use better_duck_core::AsyncDatabase;
use common::{section, show, Result};

#[tokio::main]
async fn main() -> Result<()> {
    section("open one async database, connect twice");
    let db = AsyncDatabase::open_in_memory().await?;
    let a = db.connect().await?;
    let b = db.connect().await?;

    section("write through connection a");
    a.execute_batch(
        "CREATE TABLE metrics (name TEXT, value INTEGER);
         INSERT INTO metrics VALUES ('cpu', 40), ('mem', 70);",
    )
    .await?;

    section("read the shared data through connection b");
    let rows = b.execute("SELECT name, value FROM metrics ORDER BY name").await?;
    show("rows visible via connection b", rows.len());
    assert_eq!(rows.len(), 2, "both connections share one database");
    let first = rows.first().expect("one row");
    assert_eq!(first.get("name"), Some(&DuckValue::Text("cpu".to_owned())));

    section("aggregate across the shared database");
    let total = b.execute("SELECT sum(value) AS total FROM metrics").await?;
    match total.first().and_then(|r| r.get("total")) {
        Some(DuckValue::HugeInt(n)) => {
            show("sum(value)", n);
            assert_eq!(*n, 110);
        },
        other => panic!("expected HugeInt total, got {other:?}"),
    }

    Ok(())
}
