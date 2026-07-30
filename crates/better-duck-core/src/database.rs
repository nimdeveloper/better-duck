use std::{path::Path, sync::Arc};

use crate::{
    config::Config,
    connection::Connection,
    error::Result,
    helpers::path::path_to_cstring,
    raw::connection::{RawConnection, RawDatabase},
};

/// A shared handle to an open DuckDB database.
///
/// Cloning a `Database` is cheap (an `Arc` bump) and does not touch DuckDB. Use
/// [`connect`](Database::connect) to spawn independent [`Connection`]s that share
/// this database — including, for `:memory:` databases, connections that observe
/// the same data.
///
/// This is different from calling [`Connection::open_in_memory`] multiple times,
/// which gives each connection its own independent in-memory database:
///
/// ```rust
/// use better_duck_core::{connection::Connection, database::Database};
///
/// // Shared: both connections see the same in-memory data.
/// let db = Database::open_in_memory().expect("open database");
/// let mut a = db.connect().expect("connect");
/// let mut b = db.connect().expect("connect");
/// a.execute_batch("CREATE TABLE t (id INTEGER)").expect("create table");
/// b.execute_batch("INSERT INTO t VALUES (1)").expect("insert");
///
/// // Independent: each has its own in-memory database.
/// let mut x = Connection::open_in_memory().expect("open");
/// let mut y = Connection::open_in_memory().expect("open");
/// ```
#[derive(Clone)]
pub struct Database {
    inner: Arc<RawDatabase>,
}

impl Database {
    /// Wraps an existing `Arc<RawDatabase>`, for use by [`Connection::database`].
    pub(crate) fn from_raw(inner: Arc<RawDatabase>) -> Database {
        Database { inner }
    }

    /// Opens a database at the given file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the path contains a nul byte.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database> {
        Self::open_with_flags(path, Config::default())
    }

    /// Opens a database at the given file path with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the path contains a nul byte.
    pub fn open_with_flags<P: AsRef<Path>>(
        path: P,
        config: Config,
    ) -> Result<Database> {
        let c_path = path_to_cstring(path.as_ref())?;
        let config = config.with("duckdb_api", "rust")?;
        RawDatabase::open_with_flags(&c_path, config).map(|db| Database { inner: Arc::new(db) })
    }

    /// Opens an in-memory database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open_in_memory() -> Result<Database> {
        Self::open_in_memory_with_flags(Config::default())
    }

    /// Opens an in-memory database with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open_in_memory_with_flags(config: Config) -> Result<Database> {
        Self::open_with_flags(":memory:", config)
    }

    /// Opens a new connection to this database.
    ///
    /// This is a single `duckdb_connect` call — no file I/O. All connections opened
    /// from this `Database` (or its clones) share the same underlying data.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    pub fn connect(&self) -> Result<Connection> {
        RawConnection::new(Arc::clone(&self.inner)).map(Connection::from_raw)
    }

    /// Returns the raw `duckdb_database` handle for internal FFI use (e.g. the
    /// `udf` module's replacement-scan registration, which needs the handle
    /// directly — DuckDB scopes replacement scans per-database, not
    /// per-connection).
    #[cfg(feature = "udf")]
    pub(crate) fn raw_db(&self) -> crate::ffi::duckdb_database {
        self.inner.0
    }
}

impl std::fmt::Debug for Database {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_connect_shares_in_memory_state() {
        let db = Database::open_in_memory().unwrap();
        let mut a = db.connect().unwrap();
        let mut b = db.connect().unwrap();
        a.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
        b.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let mut result = a.execute("SELECT count(*) AS c FROM t").unwrap();
        let row = result.next().unwrap().unwrap();
        match row.get("c").unwrap() {
            crate::types::value::DuckValue::BigInt(n) => assert_eq!(*n, 1),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[test]
    fn separate_open_in_memory_are_independent() {
        let mut a = Connection::open_in_memory().unwrap();
        let mut b = Connection::open_in_memory().unwrap();
        a.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
        // `b` never had `t` created, so this must fail — proving isolation.
        assert!(b.execute_batch("INSERT INTO t VALUES (1)").is_err());
    }

    #[test]
    fn database_outlives_connection() {
        let db = Database::open_in_memory().unwrap();
        let mut conn = db.connect().unwrap();
        drop(db);
        // The Arc<RawDatabase> is kept alive by `conn`'s RawConnection.
        conn.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
    }

    #[test]
    fn connection_try_clone_shares_state() {
        let mut a = Connection::open_in_memory().unwrap();
        a.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
        let mut b = a.try_clone().unwrap();
        b.execute_batch("INSERT INTO t VALUES (1)").unwrap();
        let mut result = a.execute("SELECT count(*) AS c FROM t").unwrap();
        let row = result.next().unwrap().unwrap();
        match row.get("c").unwrap() {
            crate::types::value::DuckValue::BigInt(n) => assert_eq!(*n, 1),
            other => panic!("expected BigInt, got {other:?}"),
        }
    }

    #[test]
    fn connection_database_round_trip() {
        let conn = Connection::open_in_memory().unwrap();
        let db = conn.database();
        let mut other = db.connect().unwrap();
        other.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
    }

    #[test]
    fn database_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
    }
}
