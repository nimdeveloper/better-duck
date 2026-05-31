use std::path::Path;

use crate::{
    config::Config,
    error::Result,
    helpers::path::path_to_cstring,
    raw::{appender::Appender, connection::RawConnection},
};

// Removed Legacy methods:
// - open_from_raw
// -
pub struct Connection(RawConnection);

// File-db implementation
impl Connection {
    #[inline]
    #[allow(unused)]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        Self::open_with_flags(path, Config::default())
    }
    #[inline]
    #[allow(unused)]
    pub fn open_with_flags<P: AsRef<Path>>(
        path: P,
        config: Config,
    ) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        let config = config.with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).map(Connection)
    }
}

// In-memory implementation
impl Connection {
    /// Opens an in-memory DuckDB connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    #[must_use = "connection should be used or explicitly dropped"]
    #[inline]
    #[allow(unused)]
    pub fn open_in_memory() -> Result<Connection> {
        Self::open_in_memory_with_flags(Config::default())
    }

    /// Opens an in-memory DuckDB connection with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    #[must_use = "connection should be used or explicitly dropped"]
    #[inline]
    #[allow(unused)]
    pub fn open_in_memory_with_flags(config: Config) -> Result<Connection> {
        Self::open_with_flags(":memory:", config)
    }
}

impl Connection {
    /// Executes one or more SQL statements separated by semicolons.
    ///
    /// The result of each statement is discarded. Use this for DDL
    /// (`CREATE TABLE`, `DROP TABLE`) and simple DML (`INSERT`, `UPDATE`, `DELETE`).
    ///
    /// # Errors
    ///
    /// Returns an error if any statement fails to execute.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use better_duck_core::connection::Connection;
    /// let mut conn = Connection::open_in_memory().unwrap();
    /// conn.execute_batch("CREATE TABLE t (id INTEGER)").unwrap();
    /// conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    /// ```
    #[must_use = "execute_batch result should be checked"]
    #[allow(unused)]
    pub fn execute_batch(
        &mut self,
        sql: &str,
    ) -> Result<()> {
        self.0.execute(sql)
    }

    /// Executes a single SQL statement and discards the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement fails to execute.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use better_duck_core::connection::Connection;
    ///
    /// let mut conn = Connection::open_in_memory().unwrap();
    /// conn.execute("CREATE TABLE foo (bar INTEGER)").unwrap();
    /// conn.execute("INSERT INTO foo VALUES (42)").unwrap();
    /// ```
    #[must_use = "execute result should be checked"]
    #[allow(unused)]
    pub fn execute(
        &mut self,
        sql: &str,
    ) -> Result<()> {
        self.0.execute(sql)
    }

    /// Creates an appender for bulk-inserting rows into the given table and schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist or the appender cannot be created.
    #[must_use = "appender should be used to insert rows"]
    #[allow(unused)]
    pub fn appender(
        &mut self,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        self.0.appender(table, schema)
    }
}

impl Connection {
    /// Closes the connection explicitly.
    ///
    /// The connection is also closed automatically on drop.
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`.
    #[must_use = "close result should be checked"]
    #[inline]
    #[allow(unused)]
    pub fn close(&mut self) -> Result<()> {
        self.0.close()
    }

    /// Returns `true` if the connection is open.
    #[inline]
    #[allow(unused)]
    pub fn is_open(&self) -> bool {
        !self.0.con.is_null()
    }

    /// Returns a reference to the underlying [`RawConnection`].
    ///
    /// This provides access to low-level operations such as
    /// [`prepare`](RawConnection::prepare).
    #[inline]
    #[allow(unused)]
    #[allow(private_interfaces)]
    pub fn db(&self) -> &RawConnection {
        &self.0
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_open_in_memory() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert!(conn.is_open());
        conn.close().unwrap();
        assert!(!conn.is_open());
    }

    #[test]
    fn test_open_with_flags() {
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        let mut conn = Connection::open_with_flags(":memory:", config).unwrap();
        assert!(conn.is_open());
        conn.close().unwrap();
        assert!(!conn.is_open());
    }

    #[test]
    fn test_batch_execution() {
        let mut conn = Connection::open_in_memory().unwrap();
        let exec = conn.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)");
        assert!(exec.is_ok(), "{}", exec.unwrap_err());
        let exec = conn.execute_batch("INSERT INTO test VALUES (1, 'example')");
        assert!(exec.is_ok(), "{}", exec.unwrap_err());
        conn.close().unwrap();
    }
}
