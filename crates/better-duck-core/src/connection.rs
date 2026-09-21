use std::path::Path;

use crate::{
    config::Config,
    database::Database,
    error::Result,
    helpers::path::path_to_cstring,
    raw::{appender::Appender, connection::RawConnection, result::DuckResult},
    types::appendable::AppendAble,
};

/// A high-level DuckDB connection.
///
/// `Connection` wraps a `RawConnection` and exposes a safe, ergonomic API for
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

impl Connection {
    /// Wraps an existing [`RawConnection`], for use by [`Database::connect`].
    pub(crate) fn from_raw(raw: RawConnection) -> Connection {
        Connection(raw)
    }
}

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
    /// - **SELECT** — iterate rows via the [`Iterator`] impl on
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
    /// This consumes the connection so it cannot be used after closing. The
    /// connection is also closed automatically on drop.
    ///
    /// # Errors
    ///
    /// Returns an error if appenders created from this connection are still alive.
    /// The connection stays open in that case and closes once the last of them is
    /// dropped.
    #[must_use = "close result should be checked"]
    #[inline]
    pub fn close(self) -> Result<()> {
        self.0.close()
    }

    /// Returns `true` if the connection is open.
    ///
    /// Always `true`: [`close`](Connection::close) consumes the connection, so a
    /// `Connection` value can only ever refer to an open connection. Retained so
    /// existing callers keep compiling.
    #[inline]
    #[allow(unused)]
    pub fn is_open(&self) -> bool {
        true
    }

    /// Returns a reference to the underlying `RawConnection`.
    ///
    /// This provides access to low-level operations such as `prepare`.
    #[inline]
    #[allow(unused)]
    #[allow(private_interfaces)]
    pub fn db(&self) -> &RawConnection {
        &self.0
    }

    /// Opens a second, independent connection to the same database as this one.
    ///
    /// Cheap: one `duckdb_connect` call, no file I/O. See [`Database::connect`] for
    /// details on what "the same database" means for `:memory:` connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    #[inline]
    pub fn try_clone(&self) -> Result<Connection> {
        self.0.try_clone().map(Connection)
    }

    /// Returns a [`QueryControl`] for interrupting or observing the query running
    /// on this connection from another thread.
    ///
    /// Mint the control *before* starting the query (typically on another thread),
    /// then call [`QueryControl::interrupt`] or [`QueryControl::progress`] while it
    /// runs. The control is generation-scoped: once the query finishes, it can no
    /// longer affect a later query on the same connection.
    #[inline]
    #[must_use]
    pub fn query_control(&self) -> crate::raw::connection::QueryControl {
        self.0.query_control()
    }

    /// Returns the shared connection owner, for building a mutex-free
    /// [`QueryControl`] source.
    ///
    /// Only the async layer needs this, so it is gated on the `async` feature to
    /// avoid a dead-code warning in the default build.
    #[cfg(feature = "async")]
    #[inline]
    pub(crate) fn inner(&self) -> &std::sync::Arc<crate::raw::connection::ConnectionInner> {
        self.0.inner()
    }

    /// Returns a shareable handle to the database backing this connection.
    ///
    /// Use [`Database::connect`] to open further connections to the same database —
    /// including, for `:memory:` databases, connections that observe the same data.
    #[inline]
    pub fn database(&self) -> Database {
        Database::from_raw(std::sync::Arc::clone(self.0.database()))
    }

    /// Returns the raw `duckdb_connection` handle for internal FFI use (e.g. the
    /// `udf` module's function registration, which needs the handle directly).
    #[cfg(feature = "udf")]
    #[inline]
    pub(crate) fn raw_con(&self) -> crate::ffi::duckdb_connection {
        self.0.handle()
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
        let conn = Connection::open_in_memory().unwrap();
        assert!(conn.is_open());
        conn.close().unwrap();
    }

    #[test]
    fn test_open_with_flags() {
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        let conn = Connection::open_with_flags(":memory:", config).unwrap();
        assert!(conn.is_open());
        conn.close().unwrap();
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

    #[test]
    fn close_consumes_the_connection() {
        let conn = Connection::open_in_memory().unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn execute_with_binds_values_and_reports_changes() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE test (id INTEGER, name VARCHAR)").unwrap();
        let mut id = 7_i32;
        let mut name = String::from("bound");
        let mut result = conn
            .execute_with("INSERT INTO test VALUES ($1, $2)", &mut [&mut id, &mut name])
            .unwrap();
        assert_eq!(result.changes(), 1);

        let mut rows = conn.execute("SELECT id, name FROM test").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("id").unwrap(), &crate::types::value::DuckValue::Int(7));
        assert_eq!(
            row.get("name").unwrap(),
            &crate::types::value::DuckValue::Text("bound".to_owned())
        );
    }

    #[test]
    fn nul_sql_and_missing_appender_table_return_errors() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert!(matches!(
            conn.execute_batch("SELECT 1;\0SELECT 2"),
            Err(crate::error::Error::NulError(_))
        ));
        assert!(matches!(conn.execute("SELECT '\0'"), Err(crate::error::Error::NulError(_))));
        assert!(conn.appender("missing_table", "main").is_err());
    }
}
