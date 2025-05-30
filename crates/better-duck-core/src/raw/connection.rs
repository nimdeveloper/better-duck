#![allow(unused)]
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
    ffi,
    helpers::duck_result::result_from_duckdb_result,
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
pub struct RawDatabase(pub(crate) ffi::duckdb_database);
impl RawDatabase {
    #[inline]
    pub unsafe fn new(db: ffi::duckdb_database) -> Result<RawDatabase> {
        if db.is_null() {
            return Err(Error::DuckDBFailure(
                ffi::Error::new(ffi::DuckDBError),
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
                ffi::duckdb_close(&mut self.0);
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
///
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
    pub con: ffi::duckdb_connection,
}
impl RawConnection {
    #[inline]
    pub unsafe fn new(db: Arc<RawDatabase>) -> Result<RawConnection> {
        let mut con: ffi::duckdb_connection = ptr::null_mut();
        let r = ffi::duckdb_connect(db.0, &mut con);
        if r != ffi::DuckDBSuccess {
            ffi::duckdb_disconnect(&mut con);
            return Err(Error::DuckDBFailure(
                ffi::Error::new(r),
                Some("connect error".to_owned()),
            ));
        }
        Ok(RawConnection { db, con })
    }

    pub fn open_with_flags(c_path: &CStr, config: Config) -> Result<RawConnection> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut c_err = std::ptr::null_mut();
            let r =
                ffi::duckdb_open_ext(c_path.as_ptr(), &mut db, config.duckdb_config(), &mut c_err);
            if r != ffi::DuckDBSuccess {
                let msg = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
                ffi::duckdb_free(c_err as *mut c_void);
                return Err(Error::DuckDBFailure(ffi::Error::new(r), msg));
            }
            RawConnection::new(Arc::new(RawDatabase::new(db)?))
        }
    }

    pub fn close(&mut self) -> Result<()> {
        if self.con.is_null() {
            return Ok(());
        }
        unsafe {
            ffi::duckdb_disconnect(&mut self.con);
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

    pub fn execute(&mut self, sql: &str) -> Result<()> {
        let c_str = CString::new(sql).unwrap();
        unsafe {
            let mut out = mem::zeroed();
            let r = ffi::duckdb_query(self.con, c_str.as_ptr() as *const c_char, &mut out);
            result_from_duckdb_result(r, out)?;
            ffi::duckdb_destroy_result(&mut out);
            Ok(())
        }
    }

    // TODO: Implement `prepare`

    // TODO: Implement `appender`
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
    #[allow(unused_must_use)]
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
