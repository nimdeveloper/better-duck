//! An [`r2d2::ManageConnection`](diesel::r2d2::ManageConnection) for
//! [`DuckDbConnection`], backed by a shared
//! [`better_duck_core::database::Database`].
//!
//! Diesel's own generic `diesel::r2d2::ConnectionManager<DuckDbConnection>` opens
//! a fresh `duckdb_open_ext` per pooled connection — for a `:memory:` pool, that
//! means every connection gets its own independent, empty database. This manager
//! instead shares one [`better_duck_core::database::Database`], so an in-memory pool observes one consistent
//! database. Both managers may be used side by side; neither is exclusive.

use std::path::Path;
use std::sync::Arc;

use diesel::connection::SimpleConnection;
use diesel::r2d2::{self, ManageConnection};
use diesel::result::ConnectionError;

use better_duck_core::database::Database;

use crate::connection::DuckDbConnection;

/// A per-connection setup callback run every time the pool opens a new
/// [`DuckDbConnection`] — the place to register UDFs or `LOAD` extensions so each
/// pooled connection carries them (registration is per-connection in DuckDB).
type OnConnect =
    Arc<dyn Fn(&mut DuckDbConnection) -> better_duck_core::error::Result<()> + Send + Sync>;

/// An [`r2d2::ManageConnection`] that creates [`DuckDbConnection`]s sharing one
/// [`Database`].
///
/// Named distinctly from `better_duck_core::pool::DuckDbConnectionManager` so
/// that a crate depending on both core's `pool` feature and this one gets no
/// import collision.
#[derive(Clone)]
pub struct SharedDuckDbConnectionManager {
    database: Database,
    on_connect: Option<OnConnect>,
}

impl std::fmt::Debug for SharedDuckDbConnectionManager {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SharedDuckDbConnectionManager")
            .field("database", &self.database)
            .field("on_connect", &self.on_connect.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl SharedDuckDbConnectionManager {
    /// Creates a manager over an already-open [`Database`].
    pub fn new(database: Database) -> SharedDuckDbConnectionManager {
        SharedDuckDbConnectionManager { database, on_connect: None }
    }

    /// Creates a manager backed by a shared in-memory database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn memory() -> better_duck_core::error::Result<SharedDuckDbConnectionManager> {
        Database::open_in_memory().map(SharedDuckDbConnectionManager::new)
    }

    /// Creates a manager backed by a file-based database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn file<P: AsRef<Path>>(
        path: P
    ) -> better_duck_core::error::Result<SharedDuckDbConnectionManager> {
        Database::open(path).map(SharedDuckDbConnectionManager::new)
    }

    /// Returns the shared [`Database`] backing this manager.
    pub fn database(&self) -> &Database {
        &self.database
    }

    /// Sets a setup callback run on every newly opened pooled connection.
    ///
    /// Use it to register UDFs or `LOAD` extensions per connection, since those
    /// registrations live on a single `duckdb` connection rather than the shared
    /// database. If the callback returns an error, opening that connection fails.
    ///
    /// ```no_run
    /// # use better_duck_diesel::pool::SharedDuckDbConnectionManager;
    /// let manager = SharedDuckDbConnectionManager::memory()
    ///     .unwrap()
    ///     .on_connect(|conn| conn.inner_mut().load_extension("json"));
    /// ```
    #[must_use]
    pub fn on_connect<F>(
        mut self,
        f: F,
    ) -> SharedDuckDbConnectionManager
    where
        F: Fn(&mut DuckDbConnection) -> better_duck_core::error::Result<()> + Send + Sync + 'static,
    {
        self.on_connect = Some(Arc::new(f));
        self
    }
}

impl ManageConnection for SharedDuckDbConnectionManager {
    type Connection = DuckDbConnection;
    type Error = r2d2::Error;

    fn connect(&self) -> Result<DuckDbConnection, r2d2::Error> {
        let conn =
            self.database.connect().map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        let mut conn = DuckDbConnection::from_core(conn);
        if let Some(setup) = &self.on_connect {
            setup(&mut conn).map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        }
        Ok(conn)
    }

    fn is_valid(
        &self,
        conn: &mut DuckDbConnection,
    ) -> Result<(), r2d2::Error> {
        conn.batch_execute("SELECT 1").map_err(r2d2::Error::QueryError)
    }

    fn has_broken(
        &self,
        conn: &mut DuckDbConnection,
    ) -> bool {
        std::thread::panicking() || diesel::r2d2::R2D2Connection::is_broken(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::r2d2::Pool;
    use diesel::RunQueryDsl;

    #[test]
    fn pool_shares_one_in_memory_database() {
        let manager = SharedDuckDbConnectionManager::memory().unwrap();
        let pool = Pool::builder().max_size(4).build(manager).unwrap();

        let mut a = pool.get().unwrap();
        diesel::sql_query("CREATE TABLE t (id INTEGER)").execute(&mut a).unwrap();
        drop(a);

        let mut b = pool.get().unwrap();
        diesel::sql_query("INSERT INTO t VALUES (1)").execute(&mut b).unwrap();

        #[derive(diesel::QueryableByName, Debug)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            c: i64,
        }
        let row: Count =
            diesel::sql_query("SELECT count(*) AS c FROM t").get_result(&mut b).unwrap();
        assert_eq!(row.c, 1);
    }

    #[test]
    fn on_connect_runs_per_connection() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let manager = SharedDuckDbConnectionManager::memory().unwrap().on_connect(move |conn| {
            seen.fetch_add(1, Ordering::SeqCst);
            // Prove the callback receives a usable core connection.
            let _ = conn.inner_mut();
            Ok(())
        });
        let pool = Pool::builder().max_size(2).build(manager).unwrap();
        let a = pool.get().unwrap();
        let b = pool.get().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop((a, b));
    }
}
