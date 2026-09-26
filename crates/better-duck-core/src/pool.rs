//! An [`r2d2`] connection pool backed by a shared [`Database`].
//!
//! Unlike opening one connection per pool slot, every connection this manager
//! creates shares a single [`Database`] handle — so an in-memory pool observes
//! one consistent database rather than one independent database per connection.

use std::path::Path;

use crate::{config::Config, connection::Connection, database::Database, error::Error};

/// An [`r2d2::ManageConnection`] that creates connections from a shared [`Database`].
#[derive(Clone, Debug)]
pub struct DuckDbConnectionManager {
    database: Database,
}

impl DuckDbConnectionManager {
    /// Creates a manager over an already-open [`Database`].
    pub fn new(database: Database) -> DuckDbConnectionManager {
        DuckDbConnectionManager { database }
    }

    /// Creates a manager backed by a file-based database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn file<P: AsRef<Path>>(path: P) -> crate::error::Result<DuckDbConnectionManager> {
        Database::open(path).map(DuckDbConnectionManager::new)
    }

    /// Creates a manager backed by a file-based database with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn file_with_flags<P: AsRef<Path>>(
        path: P,
        config: Config,
    ) -> crate::error::Result<DuckDbConnectionManager> {
        Database::open_with_flags(path, config).map(DuckDbConnectionManager::new)
    }

    /// Creates a manager backed by a shared in-memory database.
    ///
    /// All connections checked out of a pool built from this manager observe the
    /// same in-memory data.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn memory() -> crate::error::Result<DuckDbConnectionManager> {
        Database::open_in_memory().map(DuckDbConnectionManager::new)
    }

    /// Creates a manager backed by a shared in-memory database with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn memory_with_flags(config: Config) -> crate::error::Result<DuckDbConnectionManager> {
        Database::open_in_memory_with_flags(config).map(DuckDbConnectionManager::new)
    }

    /// Returns the shared [`Database`] backing this manager.
    pub fn database(&self) -> &Database {
        &self.database
    }
}

impl r2d2::ManageConnection for DuckDbConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> Result<Connection, Error> {
        self.database.connect()
    }

    fn is_valid(
        &self,
        conn: &mut Connection,
    ) -> Result<(), Error> {
        conn.execute_batch("SELECT 1")
    }

    fn has_broken(
        &self,
        conn: &mut Connection,
    ) -> bool {
        !conn.is_open()
    }
}

/// A DuckDB connection pool.
pub type Pool = r2d2::Pool<DuckDbConnectionManager>;
/// A connection checked out of a [`Pool`].
pub type PooledConnection = r2d2::PooledConnection<DuckDbConnectionManager>;

pub use r2d2::Builder as PoolBuilder;
pub use r2d2::Error as PoolError;
pub use r2d2::State as PoolState;

#[cfg(test)]
mod tests {
    use super::*;
    use r2d2::ManageConnection;

    #[test]
    fn pool_shares_one_in_memory_database() {
        let manager = DuckDbConnectionManager::memory().unwrap();
        let pool = Pool::builder().max_size(4).build(manager).unwrap();

        let mut a = pool.get().unwrap();
        a.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
        drop(a);

        let mut b = pool.get().unwrap();
        b.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let mut result = b.execute("SELECT count(*) AS c FROM t").unwrap();
        let row = result.next().unwrap().unwrap();
        match row.get("c").unwrap() {
            crate::types::value::DuckValue::BigInt(n) => assert_eq!(*n, 1),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[test]
    fn pool_max_size_is_enforced() {
        let manager = DuckDbConnectionManager::memory().unwrap();
        let pool = Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(100))
            .build(manager)
            .unwrap();

        let _held = pool.get().unwrap();
        assert!(pool.get().is_err());
    }

    #[test]
    fn manager_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DuckDbConnectionManager>();
    }

    #[test]
    fn manager_validates_open_connections() {
        let manager = DuckDbConnectionManager::memory().unwrap();
        let mut connection = manager.connect().unwrap();
        assert!(manager.is_valid(&mut connection).is_ok());
        assert!(!manager.has_broken(&mut connection));
    }

    #[test]
    fn manager_file_connections_share_persisted_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pool.duckdb");
        let manager = DuckDbConnectionManager::file(&path).unwrap();
        let mut first = manager.connect().unwrap();
        first
            .execute_batch("CREATE TABLE pooled (value INTEGER); INSERT INTO pooled VALUES (9)")
            .unwrap();
        drop(first);

        let mut second = manager.connect().unwrap();
        let mut rows = second.execute("SELECT value FROM pooled").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("value").unwrap(), &crate::types::value::DuckValue::Int(9));
    }

    #[test]
    fn memory_with_flags_builds_a_usable_manager() {
        let manager = DuckDbConnectionManager::memory_with_flags(Config::default()).unwrap();
        // `database()` exposes the shared handle; connections from it share state.
        let _db = manager.database();
        let mut a = manager.connect().unwrap();
        a.execute_batch("CREATE TABLE m (v INTEGER); INSERT INTO m VALUES (7)").unwrap();
        drop(a);
        let mut b = manager.connect().unwrap();
        let mut rows = b.execute("SELECT v FROM m").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("v").unwrap(), &crate::types::value::DuckValue::Int(7));
    }

    #[test]
    fn file_with_flags_opens_and_persists() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flags.duckdb");
        let manager = DuckDbConnectionManager::file_with_flags(&path, Config::default()).unwrap();
        let mut first = manager.connect().unwrap();
        first.execute_batch("CREATE TABLE t (v INTEGER); INSERT INTO t VALUES (3)").unwrap();
        drop(first);
        let mut second = manager.connect().unwrap();
        let mut rows = second.execute("SELECT v FROM t").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("v").unwrap(), &crate::types::value::DuckValue::Int(3));
    }

    #[test]
    fn new_wraps_an_existing_database_and_has_broken_tracks_open_state() {
        let db = Database::open_in_memory().unwrap();
        let manager = DuckDbConnectionManager::new(db);
        let mut conn = manager.connect().unwrap();
        // A live connection is valid and not broken.
        assert!(manager.is_valid(&mut conn).is_ok());
        assert!(!manager.has_broken(&mut conn));
        // Debug is derived; exercise it so the impl is covered.
        assert!(format!("{manager:?}").contains("DuckDbConnectionManager"));
    }
}
