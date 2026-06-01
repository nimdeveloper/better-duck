use std::path::Path;

use crate::{
    config::Config,
    error::Result,
    helpers::path::path_to_cstring,
    raw::{appender::Appender, connection::RawConnection, result::DuckResult},
    types::appendable::AppendAble,
};

/// A high-level DuckDB connection.
///
/// `Connection` wraps a [`RawConnection`] and exposes a safe, ergonomic API for
/// opening databases, executing SQL, and creating appenders.
///
/// # Example
///
/// ```rust,no_run
/// use better_duck_core::connection::Connection;
///
/// let mut conn = Connection::open_in_memory().expect("open in-memory db");
/// conn.execute_batch("CREATE TABLE t (id INTEGER)").expect("create table");
/// conn.execute_batch("INSERT INTO t VALUES (1)").expect("insert");
/// ```
pub struct Connection(RawConnection);

// File-db implementation
impl Connection {
    /// Opens a connection to a DuckDB database at the given file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the path contains a nul byte.
    #[must_use = "connection should be used or explicitly dropped"]
    #[inline]
    #[allow(unused)]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        Self::open_with_flags(path, Config::default())
    }

    /// Opens a connection to a DuckDB database at the given path with additional config.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the path contains a nul byte.
    #[must_use = "connection should be used or explicitly dropped"]
    #[inline]
    #[allow(unused)]
    pub fn open_with_flags<P: AsRef<Path>>(
        path: P,
        config: Config,
    ) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        let config = config.with("duckdb_api", "rust")?;
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
    /// ```rust
    /// # use better_duck_core::connection::Connection;
    /// # fn main() -> better_duck_core::error::Result<()> {
    /// let mut conn = Connection::open_in_memory()?;
    /// conn.execute_batch("CREATE TABLE t (id INTEGER)")?;
    /// conn.execute_batch("INSERT INTO t VALUES (1)")?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use = "execute_batch result should be checked"]
    #[allow(unused)]
    pub fn execute_batch(
        &mut self,
        sql: impl AsRef<str>,
    ) -> Result<()> {
        self.0.query(sql).map(|_| ())
    }

    /// Prepares and executes a SQL statement, returning the result.
    ///
    /// Works for all statement types:
    /// - **SELECT** — iterate rows via the [`Iterator`](std::iter::Iterator) impl on
    ///   [`DuckResult`].
    /// - **INSERT / UPDATE / DELETE** — check [`DuckResult::changes()`] for affected rows.
    /// - **DDL** (`CREATE TABLE`, `DROP TABLE`, etc.) — `.changes()` returns `0`, no rows.
    /// - **INSERT … RETURNING** — both iterate rows and check `.changes()`.
    ///
    /// For parameterized statements use [`execute_with`](Connection::execute_with).
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB cannot prepare or execute the statement.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use better_duck_core::connection::Connection;
    /// # fn main() -> better_duck_core::error::Result<()> {
    /// let mut conn = Connection::open_in_memory()?;
    /// conn.execute_batch("CREATE TABLE t (id INTEGER)")?;
    /// let n = conn.execute("INSERT INTO t VALUES (1)")?.changes();
    /// assert_eq!(n, 1);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use = "the DuckResult carries both affected-row count (.changes()) and a row iterator — consume it"]
    pub fn execute(
        &mut self,
        sql: impl AsRef<str>,
    ) -> Result<DuckResult> {
        self.0.execute(sql, &mut [])
    }

    /// Prepares and executes a parameterized SQL statement, returning the result.
    ///
    /// # Errors
    ///
    /// Returns an error if preparation, binding, or execution fails.
    #[must_use = "the DuckResult carries both affected-row count (.changes()) and a row iterator — consume it"]
    pub fn execute_with(
        &mut self,
        sql: impl AsRef<str>,
        binds: &mut [&mut dyn AppendAble],
    ) -> Result<DuckResult> {
        self.0.execute(sql, binds)
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

// SAFETY: DuckDB connections are safe to move between threads (they do not hold
// thread-local state). Each `Connection` owns its `RawConnection` exclusively.
unsafe impl Send for Connection {}

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
