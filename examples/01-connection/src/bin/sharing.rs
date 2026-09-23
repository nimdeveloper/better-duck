//! Sharing one database across connections — three ways.
//!
//! `Connection::open_in_memory()` gives each connection its OWN database. To let several
//! connections see the same data, use one of the approaches below.

use better_duck_core::connection::Connection;
use better_duck_core::database::Database;
use better_duck_core::{Config, InstanceCache};
use common::{section, show, Result};

fn main() -> Result<()> {
    section("way A: Database::connect (shared handle)");
    let db = Database::open_in_memory()?;
    let mut a = db.connect()?;
    let mut b = db.connect()?;
    a.execute_batch("CREATE TABLE t (id INTEGER)")?;
    b.execute_batch("INSERT INTO t VALUES (1), (2), (3)")?; // b sees a's table
    let seen = a.execute("SELECT id FROM t")?.count();
    show("rows visible to connection a", seen);
    assert_eq!(seen, 3);

    section("way B: Connection::try_clone (second handle to same data)");
    let mut primary = Connection::open_in_memory()?;
    primary.execute_batch("CREATE TABLE shared (v INTEGER)")?;
    let mut clone = primary.try_clone()?;
    clone.execute_batch("INSERT INTO shared VALUES (10), (20)")?;
    let via_primary = primary.execute("SELECT v FROM shared")?.count();
    show("rows visible via original handle", via_primary);
    assert_eq!(via_primary, 2);

    section("way C: InstanceCache (same path -> same instance)");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("cached.duckdb");
    let cache = InstanceCache::new();
    // Opening the same path twice through the cache yields the same underlying instance.
    let db1 = cache.get_or_create(&path, Config::default())?;
    let db2 = cache.get_or_create(&path, Config::default())?;
    let mut c1 = db1.connect()?;
    let mut c2 = db2.connect()?;
    c1.execute_batch("CREATE TABLE cached (n INTEGER)")?;
    c2.execute_batch("INSERT INTO cached VALUES (7)")?;
    let via_c1 = c1.execute("SELECT n FROM cached")?.count();
    show("rows visible across cache handles", via_c1);
    assert_eq!(via_c1, 1);

    Ok(())
}
