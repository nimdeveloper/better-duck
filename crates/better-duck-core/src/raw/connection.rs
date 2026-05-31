use std::{
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    ptr, str,
    sync::Arc,
};

use crate::{
    config::Config,
    error::{Error, Result},
    ffi::{
        duckdb_close, duckdb_connect, duckdb_connection, duckdb_database, duckdb_disconnect,
        duckdb_free, duckdb_open_ext, duckdb_query, duckdb_result, DuckDBError, DuckDBSuccess,
        Error as FFIError,
    },
    helpers::duck_result::result_from_duckdb_result,
    raw::{
        appender::Appender,
        result::DuckResult,
        statement::{CachedStatement, Statement},
    },
    types::appendable::AppendAble,
};

/// `RawDatabase` is a low-level wrapper around a DuckDB database handle.
///
/// This struct provides direct access to the underlying DuckDB database pointer.
/// It is intended for advanced use cases where you need to manage the database handle manually.
///
/// **Thread Safety:**
/// `RawDatabase` itself is **not** thread-safe. If you need to share it between threads or
/// multiple connections, wrap it in a thread-safe container such as [`Arc`](std::sync::Arc).
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use better_duck_core::raw::RawDatabase; // raw is crate-private
/// use better_duck_core::ffi;
///
/// let mut db: ffi::duckdb_database = std::ptr::null_mut();
/// let path = std::ffi::CString::new(":memory:").unwrap();
/// let r = unsafe { ffi::duckdb_open(path.as_ptr(), &mut db) };
/// assert_eq!(r, ffi::DuckDBSuccess);
/// let raw_db = unsafe { RawDatabase::new(db).unwrap() };
/// let shared = Arc::new(raw_db);
/// ```
pub struct RawDatabase(pub(crate) duckdb_database);
impl RawDatabase {
    /// Creates a new [`RawDatabase`] from an existing raw database handle.
    ///
    /// # Safety
    ///
    /// `db` must be a valid, open `duckdb_database` obtained from a successful call to
    /// `duckdb_open` or `duckdb_open_ext`. Passing a null or invalid pointer is
    /// undefined behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if `db` is null.
    #[inline]
    pub unsafe fn new(db: duckdb_database) -> Result<RawDatabase> {
        if db.is_null() {
            return Err(Error::DuckDBFailure(
                FFIError::new(DuckDBError),
                Some("database is null".to_owned()),
            ));
        }
        Ok(RawDatabase(db))
    }
}
impl Drop for RawDatabase {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self.0` is a valid duckdb_database (or null). `duckdb_close` accepts
        // null and is idempotent. After this call the handle is invalidated.
        unsafe {
            if !self.0.is_null() {
                duckdb_close(&mut self.0);
            }
        }
    }
}

/// A low-level connection to a DuckDB database.
///
/// `RawConnection` manages both a connection handle and a reference to the underlying
/// database. It provides methods to execute SQL commands and manage the connection lifecycle.
///
/// # Thread Safety
///
/// While the database handle is shared through an [`Arc`], each connection is unique and
/// should not be shared between threads. Instead, create new connections using
/// [`try_clone`](RawConnection::try_clone) for each thread.
///
/// # Resource Management
///
/// Connections are automatically closed when dropped. The underlying database remains open
/// until all connections are dropped and the last [`Arc`] reference is released.
///
/// # Example
///
/// ```rust,ignore
/// use better_duck_core::raw::RawConnection; // raw is crate-private
/// use std::ffi::CString;
/// let path = CString::new(":memory:").unwrap();
/// let mut conn = RawConnection::open_with_flags(&path, Default::default()).unwrap();
/// let _ = conn.query("CREATE TABLE test (id INTEGER, name TEXT)").unwrap();
/// let mut conn2 = conn.try_clone().unwrap();
/// ```
pub struct RawConnection {
    /// Shared handle to the underlying database.
    pub db: Arc<RawDatabase>,
    /// The raw DuckDB connection handle.
    pub con: duckdb_connection,
}

impl RawConnection {
    /// Returns the underlying raw DuckDB connection handle.
    ///
    /// # Safety
    ///
    /// This function exposes the raw FFI connection handle. Improper use of this handle
    /// may lead to undefined behavior. Use with caution.
    #[allow(unused)]
    fn raw(&self) -> duckdb_connection {
        self.con
    }

    /// Creates a new `RawConnection` from an existing [`RawDatabase`].
    ///
    /// # Safety
    ///
    /// The `db` must contain a valid, open `duckdb_database`. This function is called
    /// internally by [`open_with_flags`](RawConnection::open_with_flags).
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    #[inline]
    unsafe fn new(db: Arc<RawDatabase>) -> Result<RawConnection> {
        let mut con: duckdb_connection = ptr::null_mut();
        // SAFETY: `db.0` is a valid open duckdb_database; `con` is a valid output pointer.
        let r = duckdb_connect(db.0, &mut con);
        if r != DuckDBSuccess {
            duckdb_disconnect(&mut con);
            return Err(Error::DuckDBFailure(FFIError::new(r), Some("connect error".to_owned())));
        }
        Ok(RawConnection { db, con })
    }

    /// Opens a new connection to the database at the given path with the specified config.
    ///
    /// Pass a path of `":memory:"` for an in-memory database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or the connection cannot be
    /// established.
    pub fn open_with_flags(
        c_path: &CStr,
        config: Config,
    ) -> Result<RawConnection> {
        // SAFETY: `c_path` is a valid null-terminated C string. `db` and `c_err` are valid
        // output pointers. On error we free `c_err` via `duckdb_free`.
        unsafe {
            let mut db: duckdb_database = ptr::null_mut();
            let mut c_err = std::ptr::null_mut();
            let r = duckdb_open_ext(c_path.as_ptr(), &mut db, config.duckdb_config(), &mut c_err);
            if r != DuckDBSuccess {
                let msg = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
                duckdb_free(c_err as *mut c_void);
                return Err(Error::DuckDBFailure(FFIError::new(r), msg));
            }
            RawConnection::new(Arc::new(RawDatabase::new(db)?))
        }
    }

    /// Closes the connection, releasing the underlying DuckDB handle.
    ///
    /// Subsequent calls are no-ops.
    ///
    /// # Errors
    ///
    /// Always returns `Ok(())`.
    pub fn close(&mut self) -> Result<()> {
        if self.con.is_null() {
            return Ok(());
        }
        // SAFETY: `self.con` is a valid open duckdb_connection. After disconnect it is
        // set to null so this code path cannot run twice.
        unsafe {
            duckdb_disconnect(&mut self.con);
            self.con = ptr::null_mut();
        }
        Ok(())
    }

    /// Creates a new connection to the same database as this one.
    ///
    /// The new connection shares the underlying database handle via [`Arc`].
    ///
    /// # Errors
    ///
    /// Returns `Error::DuckDBFailure` if the connection cannot be established.
    pub fn try_clone(&self) -> Result<Self> {
        // SAFETY: `self.db` is a valid Arc<RawDatabase> with a live database handle.
        unsafe { RawConnection::new(self.db.clone()) }
    }

    /// Executes a SQL statement and returns the result.
    ///
    /// Use this for DDL (`CREATE TABLE`, `DROP`, etc.) and DML (`INSERT`, `UPDATE`,
    /// `DELETE`). For reading data, use [`prepare`](RawConnection::prepare) and
    /// [`Statement::execute`](crate::raw::statement::Statement::execute).
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be executed or if `sql` contains a nul byte.
    #[must_use = "query returns a DuckResult; discard explicitly with `let _ = ...` if not needed"]
    pub fn query(
        &mut self,
        sql: impl AsRef<str>,
    ) -> Result<DuckResult> {
        let c_str = CString::new(sql.as_ref())?;
        // SAFETY: `mem::zeroed::<duckdb_result>()` produces an all-zeros value, which is
        // the correct initial state for a `duckdb_result` output parameter.
        let mut out = Box::new(unsafe { mem::zeroed::<duckdb_result>() });
        // SAFETY: `self.con` is a valid open duckdb_connection established in
        // `open_with_flags` and not yet disconnected. `c_str` is a valid null-terminated
        // CString that outlives this call. `&mut *out` provides a pointer to the
        // heap-allocated zeroed `duckdb_result`. Ownership transfers to `DuckResult::new`,
        // whose `Drop` calls `duckdb_destroy_result` exactly once.
        let r = unsafe {
            duckdb_query(self.con, c_str.as_ptr() as *const c_char, &mut *out as *mut duckdb_result)
        };
        result_from_duckdb_result(r, &mut *out as *mut duckdb_result)?;
        Ok(DuckResult::new(*out))
    }

    /// Prepares a SQL statement for execution.
    ///
    /// The returned [`Statement`] can be executed one or more times, optionally with
    /// different bound parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be compiled into a prepared statement or if
    /// `sql` contains a nul byte.
    #[must_use = "prepare returns a Statement; call execute() to run it"]
    #[allow(unused)]
    pub fn prepare(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<Statement<'_>> {
        Statement::new(self, sql.as_ref())
    }

    /// Creates a new appender for the specified table and schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the table does not exist or the appender cannot be created.
    #[must_use = "appender returns an Appender that must be used to insert rows"]
    pub fn appender(
        &mut self,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        Appender::new(self.clone(), table, schema)
    }

    /// Executes a parameterized INSERT statement for each value in `values`.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement fails to execute or if no rows were inserted.
    #[must_use = "insert result should be checked"]
    #[allow(unused)]
    pub fn insert<T: AppendAble, I>(
        &mut self,
        sql: &str,
        values: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = T>,
    {
        let mut stmt = Statement::new(self, sql)?;
        for mut each in values {
            stmt.bind(&mut each)?;
        }
        let mut res = stmt.execute()?;
        if res.changes() > 0 {
            Ok(())
        } else {
            Err(Error::DuckDBFailure(
                FFIError::new(DuckDBError),
                Some("Failed to insert values".to_owned()),
            ))
        }
    }

    /// Prepares `sql`, binds `binds` in order, executes it as DML, and returns the
    /// number of affected rows.
    ///
    /// Returns `0` for DDL statements. Pass `&mut []` for not parameterized queries.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if preparation, binding, or execution fails.
    #[must_use = "the affected row count should be checked"]
    pub fn execute_dml(
        &mut self,
        sql: impl AsRef<str>,
        binds: &mut [&mut dyn AppendAble],
    ) -> Result<u64> {
        let mut stmt = CachedStatement::prepare(self, sql)?;
        for (i, bind) in binds.iter_mut().enumerate() {
            stmt.bind((i + 1) as u64, *bind)?;
        }
        stmt.execute_dml()
    }

    /// Prepares `sql`, binds `binds` in order, executes it as a query, and returns a
    /// row iterator.
    ///
    /// Pass `&mut []` for not parameterized queries.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if preparation, binding, or execution fails.
    #[must_use = "the DuckResult must be consumed to read rows"]
    pub fn execute_query(
        &mut self,
        sql: impl AsRef<str>,
        binds: &mut [&mut dyn AppendAble],
    ) -> Result<DuckResult> {
        let mut stmt = CachedStatement::prepare(self, sql)?;
        for (i, bind) in binds.iter_mut().enumerate() {
            stmt.bind((i + 1) as u64, *bind)?;
        }
        stmt.execute_query()
    }
}

impl Clone for RawConnection {
    /// Creates a new connection to the same database.
    ///
    /// # Warning
    ///
    /// Cloning a `RawConnection` creates a new DuckDB connection to the same underlying
    /// database. Prefer using [`try_clone`](RawConnection::try_clone) if you need to
    /// handle connection errors explicitly.
    fn clone(&self) -> Self {
        match self.try_clone() {
            Ok(con) => con,
            Err(e) => panic!("Failed to clone RawConnection: {e:?}"),
        }
    }
}
impl Drop for RawConnection {
    #[inline]
    fn drop(&mut self) {
        use std::thread::panicking;
        if let Err(e) = self.close() {
            if panicking() {
                eprintln!("Error while closing DuckDB connection: {e:?}");
            } else {
                panic!("Error while closing DuckDB connection: {e:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raw_connection_open() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let conn = RawConnection::open_with_flags(&path, config);
        assert!(conn.is_ok());
    }

    #[test]
    fn test_raw_connection_execute() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let mut conn = RawConnection::open_with_flags(&path, config).unwrap();
        let result = conn.query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());
    }

    #[test]
    fn test_raw_connection_prepare() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let mut conn = RawConnection::open_with_flags(&path, config).unwrap();

        let result = conn.query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());

        let stmt = conn.prepare("SELECT * FROM test");
        assert!(stmt.is_ok(), "{}", stmt.err().unwrap());
    }

    #[test]
    fn test_raw_connection_appender() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let mut conn = RawConnection::open_with_flags(&path, config).unwrap();

        let result = conn.query("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());

        let appender = conn.appender("test_table", "main");
        assert!(appender.is_ok(), "{}", appender.err().unwrap());
    }
}
