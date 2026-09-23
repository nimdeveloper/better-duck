//! The async connection facade: `AsyncConnection`.
//!
//! Every method dispatches to a blocking thread so the connection never blocks the async
//! executor. Reads materialize into an owned `ResultSet` (`Send + Sync`) that can cross
//! `.await` points freely — unlike a streaming `DuckResult`, which holds FFI handles.

use better_duck_core::types::value::DuckValue;
use better_duck_core::{AsyncConnection, ResultSet};
use common::{section, show, Result};

#[tokio::main]
async fn main() -> Result<()> {
    section("open an in-memory async connection");
    let conn = AsyncConnection::open_in_memory().await?;

    section("execute_batch: DDL + insert in one call");
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER, name TEXT);
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    )
    .await?;

    section("execute: read into an owned ResultSet");
    let set: ResultSet = conn.execute("SELECT id, name FROM t ORDER BY id").await?;
    show("len", set.len());
    show("columns", set.column_names().to_vec());
    assert_eq!(set.len(), 3);
    let first = set.first().expect("at least one row");
    assert_eq!(first.get("id"), Some(&DuckValue::Int(1)));

    section("execute_with: bind a parameter");
    let filtered =
        conn.execute_with("SELECT name FROM t WHERE id = $1", vec![DuckValue::Int(2)]).await?;
    show("rows matching id = 2", filtered.len());
    assert_eq!(filtered.len(), 1);
    let matched = filtered.first().expect("one matching row");
    assert_eq!(matched.get("name"), Some(&DuckValue::Text("b".to_owned())));

    Ok(())
}
