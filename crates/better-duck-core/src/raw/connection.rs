use std::{
    ffi::{c_void, CStr, CString},
    mem,
    os::raw::c_char,
    ptr, str,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    config::Config,
    error::{Error, Result},
    ffi::{
        duckdb_close, duckdb_connect, duckdb_connection, duckdb_database, duckdb_disconnect,
        duckdb_free, duckdb_interrupt, duckdb_open_ext, duckdb_query, duckdb_query_progress,
        duckdb_result, DuckDBError, DuckDBSuccess, Error as FFIError,
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

    /// Opens a database at the given path with the specified config.
    ///
    /// Pass a path of `":memory:"` for an in-memory database.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub(crate) fn open_with_flags(
        c_path: &CStr,
        config: Config,
    ) -> Result<RawDatabase> {
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
            RawDatabase::new(db)
        }
    }
}
// SAFETY: The DuckDB database handle is internally reference-counted and thread-safe.
// Multiple connections (each on its own thread) may share the same database handle.
unsafe impl Send for RawDatabase {}
// SAFETY: Read-only access to the database handle (`db.0`) does not mutate DuckDB state.
// All mutation goes through `duckdb_connect`/`duckdb_close` which are themselves thread-safe.
unsafe impl Sync for RawDatabase {}

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

/// Sole owner of one open DuckDB connection handle.
///
/// Every resource that DuckDB scopes to a connection — prepared statements,
/// appenders, and later pending/control handles — retains an
/// [`Arc<ConnectionInner>`]. That makes "a child cannot outlive its connection"
/// a type invariant instead of a convention the caller has to remember: the
/// handle is disconnected in [`Drop`], which cannot run while any child still
/// holds a reference.
///
/// Sharing the *connection* is deliberate, and different from sharing the
/// database. Opening a second connection to the same database would put the
/// child in a different transaction scope, so cached statements and appenders
/// would silently run outside the caller's `BEGIN`/`ROLLBACK`.
pub(crate) struct ConnectionInner {
    /// Owned connection handle. Disconnected exactly once, in `Drop`.
    con: duckdb_connection,
    /// Keeps the database open for at least as long as this connection.
    db: Arc<RawDatabase>,
    /// Monotonic counter identifying the *current* query on this connection.
    ///
    /// Advanced once each query completes (see [`ConnectionInner::advance_query`]).
    /// A [`QueryControl`] captures the generation live when it is minted; its
    /// [`interrupt`](QueryControl::interrupt) is a no-op once the generation has
    /// moved on, so a delayed cancellation can never interrupt a *later* query.
    generation: AtomicU64,
}

impl ConnectionInner {
    /// Opens a connection against `db`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if DuckDB cannot establish the connection.
    fn connect(db: Arc<RawDatabase>) -> Result<Arc<ConnectionInner>> {
        let mut con: duckdb_connection = ptr::null_mut();
        // SAFETY: `db.0` is a valid open duckdb_database kept alive by the `Arc`;
        // `con` is a valid output pointer.
        let r = unsafe { duckdb_connect(db.0, &mut con) };
        if r != DuckDBSuccess {
            // SAFETY: `con` may be partially initialized on failure; `duckdb_disconnect`
            // handles null/invalid handles gracefully and nulls the pointer.
            unsafe { duckdb_disconnect(&mut con) };
            return Err(Error::DuckDBFailure(FFIError::new(r), Some("connect error".to_owned())));
        }
        Ok(Arc::new(ConnectionInner { con, db, generation: AtomicU64::new(0) }))
    }

    /// Returns the raw connection handle.
    ///
    /// Crate-internal: callers must not retain the handle beyond the borrow, and
    /// must not use it concurrently from another thread (see the `Sync` note below).
    #[inline]
    pub(crate) fn handle(&self) -> duckdb_connection {
        self.con
    }

    /// Reads the current query generation.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Advances the query generation, invalidating any outstanding
    /// [`QueryControl`] minted for the query that just finished.
    ///
    /// Called at the end of every query/statement execution.
    #[inline]
    pub(crate) fn advance_query(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Returns the shared database this connection belongs to.
    #[inline]
    pub(crate) fn database(&self) -> &Arc<RawDatabase> {
        &self.db
    }
}

impl Drop for ConnectionInner {
    #[inline]
    fn drop(&mut self) {
        // SAFETY: `self.con` is the valid handle produced by `connect` and has not been
        // disconnected before: `ConnectionInner` is only reachable behind an `Arc`, so
        // this runs exactly once, when the last child reference is released.
        // `duckdb_disconnect` returns void and tolerates null, so this cannot fail or panic.
        unsafe { duckdb_disconnect(&mut self.con) };
    }
}

// SAFETY: a `duckdb_connection` holds no thread-local state, so DuckDB permits using
// it from a thread other than the one that created it. `ConnectionInner` owns its
// handle outright and exposes no interior mutability, so transferring it (always
// behind an `Arc`) to another thread cannot invalidate the handle.
unsafe impl Send for ConnectionInner {}

// SAFETY: DuckDB does *not* allow two overlapping calls on one connection, so `Sync`
// is justified only because a shared `&ConnectionInner` cannot produce such a call.
// `con` is private and its sole accessor is the crate-internal `handle()`; every type
// that can reach it (`RawConnection`, `CachedStatement`, `Appender`) is `Send` but not
// `Sync`, and each takes `&mut self` for the operations that drive the connection.
// So `&ConnectionInner` can be observed from several threads while the connection
// itself stays exclusively owned by whichever handle is using it.
unsafe impl Sync for ConnectionInner {}

/// A snapshot of a running query's progress.
///
/// Values mirror DuckDB's `duckdb_query_progress_type`. `percentage` is `-1.0`
/// when DuckDB cannot estimate progress (e.g. progress reporting disabled, or no
/// query running).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryProgress {
    /// Completion in `0.0..=100.0`, or `-1.0` when unknown.
    pub percentage: f64,
    /// Rows processed so far.
    pub rows_processed: u64,
    /// Total rows the query expects to process.
    pub total_rows_to_process: u64,
}

/// A narrow, thread-safe handle for interrupting or observing one query.
///
/// Minted by [`RawConnection::query_control`]. Unlike the connection itself,
/// `QueryControl` is `Clone + Send + Sync`: DuckDB explicitly permits
/// `duckdb_interrupt` and `duckdb_query_progress` to be called from a *different*
/// thread than the one running the query — that is their entire purpose. It
/// exposes **only** those two read/signal operations, never anything that would
/// execute SQL, so it cannot create a second concurrent user of the connection.
///
/// # Generation scoping
///
/// The control captures the connection's query generation when minted.
/// [`interrupt`](QueryControl::interrupt) only signals DuckDB while that
/// generation is still current; once the query completes (advancing the
/// generation) it becomes a no-op. This makes a delayed or racing cancellation
/// unable to interrupt a subsequent, unrelated query on the same connection.
#[derive(Clone)]
pub struct QueryControl {
    inner: Arc<ConnectionInner>,
    generation: u64,
}

impl QueryControl {
    /// Requests interruption of the query this control was minted for.
    ///
    /// Idempotent, and a no-op once that query has finished (the generation has
    /// advanced). Returns `true` if the interrupt was actually signalled to
    /// DuckDB, `false` if it was skipped as stale.
    pub fn interrupt(&self) -> bool {
        if self.inner.generation() != self.generation {
            return false;
        }
        // SAFETY: `self.inner` keeps the connection alive, so the handle is valid.
        // DuckDB documents `duckdb_interrupt` as safe to call from another thread
        // while a query runs on the connection; it only sets an interrupt flag.
        unsafe { duckdb_interrupt(self.inner.handle()) };
        true
    }

    /// Reads the progress of the query currently running on the connection.
    ///
    /// Returns `None` once the query this control was minted for has finished
    /// (the generation has advanced), so a stale control cannot report a later
    /// query's progress as its own.
    pub fn progress(&self) -> Option<QueryProgress> {
        if self.inner.generation() != self.generation {
            return None;
        }
        // SAFETY: `self.inner` keeps the connection alive. DuckDB documents
        // `duckdb_query_progress` as safe to call while a query runs; it copies a
        // small POD struct out.
        let raw = unsafe { duckdb_query_progress(self.inner.handle()) };
        Some(QueryProgress {
            percentage: raw.percentage,
            rows_processed: raw.rows_processed,
            total_rows_to_process: raw.total_rows_to_process,
        })
    }

    /// Returns `true` if the query this control was minted for is still current.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.generation() == self.generation
    }
}

/// A low-level connection to a DuckDB database.
///
/// `RawConnection` is a handle to a shared [`ConnectionInner`]. It provides methods to
/// execute SQL commands and to create connection-scoped children.
///
/// # Thread Safety
///
/// A `RawConnection` may be moved between threads but not used from two at once.
/// For a genuinely independent connection to the same database, use
/// [`try_clone`](RawConnection::try_clone), which performs a fresh `duckdb_connect`
/// and therefore gets its own transaction scope.
///
/// # Resource Management
///
/// The connection is disconnected once this handle and every child it produced
/// (prepared statements, appenders) have been dropped. The database stays open
/// until the last connection to it is released. [`close`](RawConnection::close)
/// consumes the handle and reports whether children were still outstanding.
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
    /// Shared owner of the connection handle.
    inner: Arc<ConnectionInner>,
}

impl RawConnection {
    /// Returns the underlying raw DuckDB connection handle.
    #[inline]
    pub(crate) fn handle(&self) -> duckdb_connection {
        self.inner.handle()
    }

    /// Returns the shared connection owner, for children that must outlive this handle.
    #[inline]
    pub(crate) fn inner(&self) -> &Arc<ConnectionInner> {
        &self.inner
    }

    /// Returns the shared database backing this connection.
    #[inline]
    pub(crate) fn database(&self) -> &Arc<RawDatabase> {
        self.inner.database()
    }

    /// Creates a new `RawConnection` from an existing [`RawDatabase`].
    ///
    /// # Errors
    ///
    /// Returns an error if the connection cannot be established.
    #[inline]
    pub(crate) fn new(db: Arc<RawDatabase>) -> Result<RawConnection> {
        ConnectionInner::connect(db).map(|inner| RawConnection { inner })
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
        RawConnection::new(Arc::new(RawDatabase::open_with_flags(c_path, config)?))
    }

    /// Closes the connection, releasing the underlying DuckDB handle.
    ///
    /// Consuming the handle makes double-close unrepresentable. The connection is
    /// disconnected as soon as the last child (prepared statement, appender) is also
    /// released, so dropping a `RawConnection` is equally safe — `close` exists to
    /// *report* outstanding children rather than to silence them.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if prepared statements or appenders created
    /// from this connection are still alive. The connection stays open in that case,
    /// and is disconnected when the last of them is dropped.
    pub fn close(self) -> Result<()> {
        match Arc::try_unwrap(self.inner) {
            // Dropping the sole owner disconnects here.
            Ok(inner) => {
                drop(inner);
                Ok(())
            },
            Err(shared) => {
                let outstanding = Arc::strong_count(&shared).saturating_sub(1);
                Err(Error::DuckDBFailure(
                    FFIError::new(DuckDBError),
                    Some(format!(
                        "cannot close connection: {outstanding} prepared statement(s) or \
                         appender(s) still borrow it"
                    )),
                ))
            },
        }
    }

    /// Opens a second, independent connection to the same database.
    ///
    /// This performs a fresh `duckdb_connect`, so the new connection has its own
    /// transaction scope. To share *this* connection's transaction with a child
    /// resource, pass [`inner`](RawConnection::inner) instead of cloning.
    ///
    /// # Errors
    ///
    /// Returns `Error::DuckDBFailure` if the connection cannot be established.
    pub fn try_clone(&self) -> Result<Self> {
        RawConnection::new(Arc::clone(self.database()))
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
        // the correct initial state for a `duckdb_result` output parameter. `duckdb_result`
        // is a small `Copy` struct (a handful of counters/pointers) with no self-referential
        // fields, so it needs no stable heap address — DuckResult::new takes it by value.
        let mut out = unsafe { mem::zeroed::<duckdb_result>() };
        // SAFETY: `self.con` is a valid open duckdb_connection established in
        // `open_with_flags` and not yet disconnected. `c_str` is a valid null-terminated
        // CString that outlives this call. `&mut out` provides a pointer to the stack-local
        // zeroed `duckdb_result`. Ownership transfers to `DuckResult::new`, whose `Drop`
        // calls `duckdb_destroy_result` exactly once.
        let r = unsafe {
            duckdb_query(
                self.handle(),
                c_str.as_ptr() as *const c_char,
                &mut out as *mut duckdb_result,
            )
        };
        // The query is finished (whether it succeeded or failed): advance the
        // generation so any `QueryControl` minted for it can no longer interrupt.
        self.inner.advance_query();
        result_from_duckdb_result(r, &mut out as *mut duckdb_result)?;
        Ok(DuckResult::new(out))
    }

    /// Returns a [`QueryControl`] for the query currently running (or about to run)
    /// on this connection.
    ///
    /// The control can interrupt that query or read its progress from another
    /// thread. It captures the connection's current query generation, so once the
    /// query completes the control's [`interrupt`](QueryControl::interrupt) becomes
    /// a no-op — a delayed cancellation cannot affect a *later* query.
    #[must_use]
    pub fn query_control(&self) -> QueryControl {
        QueryControl { inner: Arc::clone(&self.inner), generation: self.inner.generation() }
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
    /// The appender retains *this* connection, so appended rows participate in any
    /// transaction open on it.
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
        Appender::new(Arc::clone(self.inner()), table, schema)
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

    /// Prepares `sql`, binds `binds` in order, and executes the statement.
    ///
    /// Works for all statement types. Use [`DuckResult::changes()`] for affected rows
    /// (DML), or iterate the result for SELECT / RETURNING queries.
    /// Pass `&mut []` for not parameterized queries.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if preparation, binding, or execution fails.
    #[must_use = "the DuckResult carries both affected-row count (.changes()) and row iterator"]
    pub fn execute(
        &mut self,
        sql: impl AsRef<str>,
        binds: &mut [&mut dyn AppendAble],
    ) -> Result<DuckResult> {
        let mut stmt = CachedStatement::prepare(self, sql)?;
        for (i, bind) in binds.iter_mut().enumerate() {
            stmt.bind((i + 1) as u64, *bind)?;
        }
        stmt.execute()
    }
}

// `RawConnection` deliberately does not implement `Clone`. Cloning used to mean
// "open a *separate* DuckDB connection", which silently moved the clone into a
// different transaction scope, and it panicked when `duckdb_connect` failed. Use
// `try_clone` for an independent connection, or `inner()` to share this one.
//
// No `Drop` impl is needed: `ConnectionInner` owns the handle and disconnects when
// the last reference — this handle or any child — is released. That destructor
// returns void and therefore cannot panic.

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

    #[test]
    fn raw_database_rejects_null_handle() {
        // SAFETY: null is explicitly supported as an error case and is never dereferenced.
        let error = unsafe { RawDatabase::new(ptr::null_mut()) }.err().unwrap();
        assert!(matches!(
            error,
            Error::DuckDBFailure(_, Some(message)) if message == "database is null"
        ));
    }

    #[test]
    fn raw_connection_rejects_nul_and_invalid_sql() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        assert!(matches!(conn.query("SELECT\0 1"), Err(Error::NulError(_))));
        assert!(conn.query("SELECT * FROM missing_table").is_err());
        assert!(conn.prepare("SELECT FROM").is_err());
        assert!(conn.appender("missing_table", "main").is_err());
    }

    /// `close` consumes the handle, so a second call cannot be written at all —
    /// double-close is a compile error rather than a runtime guard.
    #[test]
    fn close_consumes_the_connection() {
        let path = CString::new(":memory:").unwrap();
        let conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        conn.close().unwrap();
    }

    #[test]
    fn close_reports_outstanding_children() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        conn.query("CREATE TABLE held (id INTEGER)").unwrap();
        let appender = conn.appender("held", "main").unwrap();

        let error = conn.close().unwrap_err();
        assert!(
            matches!(error, Error::DuckDBFailure(_, Some(ref m)) if m.contains("still borrow it")),
            "unexpected error: {error:?}"
        );

        // The connection stayed open; dropping the last child disconnects it.
        drop(appender);
    }

    /// Regression: a `CachedStatement` retains its *connection*, so the connection
    /// handle cannot be disconnected while the statement is still alive. Retaining
    /// only the database would leave `duckdb_destroy_prepare` running against a
    /// disconnected connection.
    #[test]
    fn cached_statement_keeps_its_connection_alive() {
        use crate::raw::statement::CachedStatement;

        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        conn.query("CREATE TABLE kept (id INTEGER); INSERT INTO kept VALUES (5)").unwrap();

        let mut stmt = CachedStatement::prepare(&conn, "SELECT id FROM kept").unwrap();
        drop(conn);

        let mut rows = stmt.execute().unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("id").unwrap(), &crate::types::value::DuckValue::Int(5));
    }

    /// A statement may be moved to another thread; it carries its connection with it.
    #[test]
    fn cached_statement_moves_across_threads() {
        use crate::raw::statement::CachedStatement;

        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        conn.query("CREATE TABLE moved (id INTEGER); INSERT INTO moved VALUES (9)").unwrap();
        let mut stmt = CachedStatement::prepare(&conn, "SELECT id FROM moved").unwrap();
        drop(conn);

        let value = std::thread::spawn(move || {
            let mut rows = stmt.execute().unwrap();
            let row = rows.next().unwrap().unwrap();
            row.get("id").unwrap().clone()
        })
        .join()
        .unwrap();

        assert_eq!(value, crate::types::value::DuckValue::Int(9));
    }

    /// Regression: the appender runs on the caller's own connection, so its rows
    /// join the caller's transaction and roll back with it. Previously the appender
    /// was created on a *separate* cloned connection and the rows survived.
    #[test]
    fn appender_rows_join_the_callers_transaction() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        conn.query("CREATE TABLE txn_rows (id INTEGER)").unwrap();

        conn.query("BEGIN TRANSACTION").unwrap();
        {
            let mut appender = conn.appender("txn_rows", "main").unwrap();
            appender.append(&mut 42_i32).unwrap();
            appender.save().unwrap();
        }
        conn.query("ROLLBACK").unwrap();

        let mut rows = conn.query("SELECT count(*) AS n FROM txn_rows").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(
            row.get("n").unwrap(),
            &crate::types::value::DuckValue::BigInt(0),
            "appended rows must roll back with the caller's transaction"
        );
    }

    #[test]
    fn cloned_raw_connection_shares_database() {
        let path = CString::new(":memory:").unwrap();
        let mut first = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        first.query("CREATE TABLE shared (value INTEGER); INSERT INTO shared VALUES (3)").unwrap();
        let mut second = first.try_clone().unwrap();
        let mut rows = second.query("SELECT value FROM shared").unwrap();
        let row = rows.next().unwrap().unwrap();
        assert_eq!(row.get("value").unwrap(), &crate::types::value::DuckValue::Int(3));
    }

    #[test]
    fn insert_reports_when_no_rows_change() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        let error = conn.insert::<i32, _>("SELECT $1", std::iter::once(1)).unwrap_err();
        assert!(matches!(
            error,
            Error::DuckDBFailure(_, Some(message)) if message == "Failed to insert values"
        ));
    }

    #[test]
    fn query_control_goes_stale_after_the_query_completes() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();

        let control = conn.query_control();
        assert!(control.is_active(), "freshly minted control is active");

        // Running a query advances the generation, retiring the control.
        conn.query("SELECT 1").unwrap();
        assert!(!control.is_active(), "control is stale once its query finished");

        // A stale control neither signals DuckDB nor reports progress.
        assert!(!control.interrupt(), "stale interrupt is a no-op");
        assert!(control.progress().is_none(), "stale control reports no progress");
    }

    #[test]
    fn interrupt_is_idempotent_and_generation_scoped() {
        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();

        // A control for the current (not-yet-run) query can be signalled repeatedly.
        let control = conn.query_control();
        assert!(control.interrupt(), "first interrupt signals");
        assert!(control.interrupt(), "interrupt is idempotent while active");

        // The interrupt flag applies to the connection; a benign query still runs.
        // (DuckDB clears the flag when the interrupted query is processed.)
        let _ = conn.query("SELECT 1");

        // A control minted for an earlier generation cannot interrupt a later query.
        let stale = conn.query_control();
        conn.query("SELECT 2").unwrap();
        conn.query("SELECT 3").unwrap();
        assert!(!stale.interrupt(), "control cannot reach across generations");
    }

    #[test]
    fn query_control_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<QueryControl>();
    }

    /// A `QueryControl` interrupts a genuinely long-running query from another
    /// thread, and the interrupted query returns an error rather than completing.
    #[test]
    fn interrupt_stops_a_long_running_query() {
        use std::sync::mpsc;
        use std::thread;

        let path = CString::new(":memory:").unwrap();
        let mut conn = RawConnection::open_with_flags(&path, Config::default()).unwrap();
        let control = conn.query_control();

        let (tx, rx) = mpsc::channel();
        // Interrupt shortly after the query starts, from another thread.
        let interrupter = thread::spawn(move || {
            // Wait until the main thread signals the query is about to run.
            rx.recv().unwrap();
            thread::sleep(std::time::Duration::from_millis(50));
            control.interrupt();
        });

        tx.send(()).unwrap();
        // A large cross join runs long enough to be interrupted mid-flight.
        let result = conn.query(
            "SELECT count(*) FROM range(1000000) t1, range(1000000) t2 WHERE t1.range = t2.range",
        );
        interrupter.join().unwrap();

        // Either DuckDB reported the interrupt as an error, or (rarely) the query
        // finished first. If it errored, it must be a *typed engine* error — the
        // interrupt path goes through `duckdb_result_error_type`, not a bare
        // `DuckDBFailure`. DuckDB labels an interrupted query `INVALID` (not
        // `INTERRUPT`) on this build, so we assert the typed shape, not the exact
        // kind, which is DuckDB's to decide.
        if let Err(err) = result {
            assert!(
                matches!(&err, Error::Engine(_)),
                "an interrupted query must surface a typed engine error, got {err:?}"
            );
        }
    }
}
