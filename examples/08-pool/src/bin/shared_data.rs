//! Building a pool over an existing shared `Database`.
//!
//! When you already hold a `Database`, `DuckDbConnectionManager::new(db)` builds a pool
//! whose connections all share that exact handle. This is the same sharing guarantee as
//! `memory()` (which opens the `Database` for you), but lets you keep your own handle to
//! the database alongside the pool.

use better_duck_core::database::Database;
use better_duck_core::error::Error;
use better_duck_core::pool::{DuckDbConnectionManager, Pool};
use common::{section, show, Result};

fn main() -> Result<()> {
    section("build a manager from a Database we already hold");
    let db = Database::open_in_memory()?;
    let mgr = DuckDbConnectionManager::new(db);
    // The manager exposes the shared database it was built from.
    let _shared: &Database = mgr.database();
    let pool = Pool::builder().max_size(4).build(mgr).map_err(|e| Error::Pool(e.to_string()))?;

    section("create data through one pooled connection");
    {
        let mut creator = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
        creator.execute_batch(
            "CREATE TABLE inventory (sku TEXT, qty INTEGER);
             INSERT INTO inventory VALUES ('a', 10), ('b', 5);",
        )?;
    }

    section("every pooled connection sees the same data");
    let mut reader = pool.get().map_err(|e| Error::Pool(e.to_string()))?;
    let total = reader.execute("SELECT sum(qty) AS total FROM inventory")?.count();
    // One aggregate row is returned; assert the shared table is visible.
    show("aggregate rows returned", total);
    assert_eq!(total, 1);
    let sku_count = reader.execute("SELECT sku FROM inventory ORDER BY sku")?.count();
    show("distinct rows visible via a second connection", sku_count);
    assert_eq!(sku_count, 2, "all pooled connections observe the one shared database");

    Ok(())
}
