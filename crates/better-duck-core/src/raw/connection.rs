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
    raw::{appender::Appender, statement::Statement},
};

/// `RawDatabase` is a low-level wrapper around a DuckDB database handle.
///
/// This struct provides direct access to the underlying DuckDB database pointer.
/// It is intended for advanced use cases where you need to manage the database handle manually.
///
/// **Thread Safety:**
/// `RawDatabase` itself is **not** thread-safe. If you need to share it between threads or multiple connections,
/// wrap it in a thread-safe container such as [`Arc`](std::sync::Arc) or [`Rc`](std::rc::Rc).
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use better_duck_core::raw::RawDatabase;
/// use better_duck_core::ffi;
///
/// // Open a DuckDB database (unsafe, for demonstration only)
/// let mut db: ffi::duckdb_database = std::ptr::null_mut();
/// let path = std::ffi::CString::new(":memory:").unwrap();
/// let r = unsafe { ffi::duckdb_open(path.as_ptr(), &mut db) };
/// assert_eq!(r, ffi::DuckDBSuccess);
/// let raw_db = unsafe { RawDatabase::new(db).unwrap() };
/// let shared = Arc::new(raw_db);
/// // Now `shared` can be cloned and sent between threads
/// ```
pub struct RawDatabase(pub(crate) duckdb_database);
impl RawDatabase {
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
        unsafe {
            if !self.0.is_null() {
                duckdb_close(&mut self.0);
            }
        }
    }
}

/// A low-level connection to a DuckDB database that provides direct access to the database.
///
/// `RawConnection` manages both a connection handle and a reference to the underlying database.
/// It provides methods to execute SQL commands and manage the connection lifecycle.
///
/// # Thread Safety
///
/// While the database handle is shared through an [`Arc`], each connection is unique and
/// should not be shared between threads. Instead, create new connections using [`try_clone`](RawConnection::try_clone)
/// for each thread.
///
/// # Resource Management
///
/// Connections are automatically closed when dropped. The underlying database remains open
/// until all connections are dropped and the last [`Arc`] reference is released.
///
/// # Example
///
/// ```rust
/// use better_duck_core::raw::RawConnection;
/// use std::ffi::CString;
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Open a new in-memory database connection
/// let path = CString::new(":memory:")?;
/// let mut conn = RawConnection::open_with_flags(&path, Default::default())?;
///
/// // Execute a query
/// conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")?;
/// conn.execute("INSERT INTO test VALUES (1, 'example')")?;
///
/// // Create another connection to the same database
/// let mut conn2 = conn.try_clone()?;
/// # Ok(())
/// # }
/// ```
pub struct RawConnection {
    pub db: Arc<RawDatabase>,
    pub con: duckdb_connection,
}
impl RawConnection {
    /// Returns the underlying raw DuckDB connection handle.
    ///
    /// # Safety
    ///
    /// This function exposes the raw FFI connection handle. Improper use of this handle
    /// may lead to undefined behavior. Use with caution.
    ///
    /// # Returns
    ///
    /// The raw `ffi::duckdb_connection` handle associated with this `RawConnection`.
    #[allow(unused)]
    fn raw(&self) -> duckdb_connection {
        self.con
    }

    /// Creates a new `RawConnection` from an existing `RawDatabase`.
    ///
    /// # Safety
    /// This function is unsafe because it dereferences raw pointers and interacts with the FFI.
    /// Callers must ensure that the `RawDatabase` is valid for the duration of the `RawConnection`.
    ///
    /// # Errors
    /// Returns an error if the connection cannot be established.
    ///
    /// # Example
    /// ```rust
    /// use std::sync::Arc;
    /// use better_duck_core::raw::{RawConnection, RawDatabase};
    /// let db = Arc::new(RawDatabase::new(unsafe { std::ptr::null_mut() }).unwrap());
    /// let conn = unsafe { RawConnection::new(db) };
    /// ```
    ///
    /// # Note
    /// This function is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods like `open_with_flags` to create a new connection.
    ///
    #[inline]
    unsafe fn new(db: Arc<RawDatabase>) -> Result<RawConnection> {
        let mut con: duckdb_connection = ptr::null_mut();
        let r = duckdb_connect(db.0, &mut con);
        if r != DuckDBSuccess {
            duckdb_disconnect(&mut con);
            return Err(Error::DuckDBFailure(FFIError::new(r), Some("connect error".to_owned())));
        }
        Ok(RawConnection { db, con })
    }

    /// Opens a new connection to the database with the specified flags.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    ///
    /// # Example
    /// ```rust
    /// use std::ffi::CString;
    /// use better_duck_core::raw::{RawConnection, RawDatabase};
    /// let db = Arc::new(RawDatabase::new(unsafe { std::ptr::null_mut() }).unwrap());
    /// let conn = unsafe { RawConnection::new(db) };
    /// ```
    /// # Note
    /// This function is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `open_with_flags` to create a new connection.
    ///
    pub fn open_with_flags(
        c_path: &CStr,
        config: Config,
    ) -> Result<RawConnection> {
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

    /// Closes the connection to the database.
    ///
    /// This function is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `open_with_flags` to create a new connection.
    /// # Errors
    ///
    /// Returns an error if the connection cannot be closed.
    /// # Example
    /// ```rust
    /// use better_duck_core::raw::RawConnection;
    /// let mut conn = RawConnection::new(...);
    /// conn.close().unwrap();
    /// ```
    pub fn close(&mut self) -> Result<()> {
        if self.con.is_null() {
            return Ok(());
        }
        unsafe {
            duckdb_disconnect(&mut self.con);
            self.con = ptr::null_mut();
        }
        Ok(())
    }

    /// Creates a new connection to the already-opened database.
    ///
    /// This method creates a new connection that shares the same underlying database
    /// handle through an Arc reference. The database will remain open until all
    /// connections are dropped.
    ///
    /// # Errors
    ///
    /// Returns `Error::DuckDBFailure` if the connection cannot be established.
    ///
    /// # Panics
    ///
    /// The [`Clone`] implementation for `RawConnection` uses this method internally
    /// and will panic if the connection creation fails. Use this method directly
    /// instead of [`Clone`] if you want to handle connection errors.
    pub fn try_clone(&self) -> Result<Self> {
        unsafe { RawConnection::new(self.db.clone()) }
    }

    /// Executes a SQL statement.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL statement cannot be executed.
    /// # Example
    /// ```
    /// use better_duck_core::raw::RawConnection;
    /// let mut conn = RawConnection::new(...);
    /// conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    /// ```
    /// # Note
    /// This method is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `execute` to run SQL statements.
    ///
    pub fn execute(
        &mut self,
        sql: &str,
    ) -> Result<()> {
        let c_str = CString::new(sql).unwrap();
        unsafe {
            let out: *mut duckdb_result = mem::zeroed();
            let r = duckdb_query(self.con, c_str.as_ptr() as *const c_char, out);

            result_from_duckdb_result(r, out)?;
            Ok(())
        }
    }

    /// Prepares a SQL statement for execution.
    ///
    /// This method is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `prepare` to create a new statement.
    /// # Errors
    /// Returns an error if the SQL statement cannot be prepared.
    /// # Example
    /// ```rust
    /// use better_duck_core::raw::RawConnection;
    /// let mut conn = RawConnection::new(...);
    /// let stmt = conn.prepare("SELECT * FROM test").unwrap().fetch().unwrap();
    /// ```
    /// # Note
    /// This method is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `prepare` to create a new statement.
    #[allow(unused)]
    pub fn prepare(
        &mut self,
        sql: &str,
    ) -> Result<Statement> {
        Statement::new(self, sql)
    }

    /// Creates a new appender for the specified table and schema.
    /// This method is typically used internally and should not be called directly.
    /// It is recommended to use higher-level methods from [`Connection`](crate::connection::Connection) like `appender` to create a new appender.
    /// # Errors
    /// Returns an error if the appender cannot be created.
    /// # Example
    /// ```rust
    /// use better_duck_core::raw::RawConnection;
    /// let mut conn = RawConnection::new(...);
    /// let appender = conn.appender("test_table", "public").unwrap();
    /// // Use the appender...
    /// ```
    /// # Note
    /// This method is typically used internally and should not be called directly.
    pub fn appender(
        &mut self,
        table: &str,
        schema: &str,
    ) -> Result<Appender> {
        Appender::new(self.clone(), table, schema)
    }
}

impl Clone for RawConnection {
    /// # Warning
    ///
    /// Cloning a `RawConnection` creates a new DuckDB connection to the same underlying database.
    /// This is a potentially expensive operation and may have side effects depending on your usage.
    /// Prefer using a single connection or a connection pool if possible.
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
        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());
    }
    #[test]
    fn test_raw_connection_prepare() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let mut conn = RawConnection::open_with_flags(&path, config).unwrap();

        // Create the table
        let result = conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());
        //Create statement
        let stmt = conn.prepare("SELECT * FROM test");
        assert!(stmt.is_ok(), "{}", stmt.err().unwrap());
    }

    #[test]
    fn test_raw_connection_appender() {
        let path = CString::new(":memory:").unwrap();
        let config = Config::default();
        let mut conn = RawConnection::open_with_flags(&path, config).unwrap();

        // Create the table
        let result = conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(result.is_ok(), "{}", result.err().unwrap());
        // Create appender
        let appender = conn.appender("test_table", "main");
        assert!(appender.is_ok(), "{}", appender.err().unwrap());
    }
}
