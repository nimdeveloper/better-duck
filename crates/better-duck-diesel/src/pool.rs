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

use diesel::connection::SimpleConnection;
use diesel::r2d2::{self, ManageConnection};
use diesel::result::ConnectionError;

use better_duck_core::database::Database;

use crate::connection::DuckDbConnection;

/// An [`r2d2::ManageConnection`] that creates [`DuckDbConnection`]s sharing one
/// [`Database`].
///
/// Named distinctly from `better_duck_core::pool::DuckDbConnectionManager` so
/// that a crate depending on both core's `pool` feature and this one gets no
/// import collision.
#[derive(Clone, Debug)]
pub struct SharedDuckDbConnectionManager {
    database: Database,
}

impl SharedDuckDbConnectionManager {
    /// Creates a manager over an already-open [`Database`].
    pub fn new(database: Database) -> SharedDuckDbConnectionManager {
        SharedDuckDbConnectionManager { database }
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
}

impl ManageConnection for SharedDuckDbConnectionManager {
    type Connection = DuckDbConnection;
    type Error = r2d2::Error;

    fn connect(&self) -> Result<DuckDbConnection, r2d2::Error> {
        let conn =
            self.database.connect().map_err(|e| ConnectionError::BadConnection(e.to_string()))?;
        Ok(DuckDbConnection::from_core(conn))
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
}
