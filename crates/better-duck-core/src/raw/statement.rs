use std::{
    ffi::{c_void, CStr, CString},
    mem, ptr,
};

use crate::ffi::{
    duckdb_bind_parameter_index, duckdb_clear_bindings, duckdb_destroy_prepare,
    duckdb_execute_prepared, duckdb_free, duckdb_nparams, duckdb_parameter_name, duckdb_prepare,
    duckdb_prepared_statement_column_count, duckdb_prepared_statement_column_logical_type,
    duckdb_prepared_statement_column_name, duckdb_prepared_statement_column_type,
    duckdb_prepared_statement_type, duckdb_result, DuckDBSuccess, DUCKDB_TYPE,
};

use crate::{
    chunk::LogicalType,
    error::{Error, Result},
    ffi,
    ffi::duckdb_prepared_statement,
    helpers::duck_result::{result_from_duckdb_prepare, result_from_duckdb_result},
    raw::{
        connection::RawConnection,
        result::{DuckResult, StatementType},
    },
    types::appendable::AppendAble,
};

/// A prepared DuckDB statement that can be executed one or more times.
///
/// After calling [`execute`](Statement::execute), the statement can be reused
/// by calling [`clear_bindings`](Statement::clear_bindings) and re-binding parameters.
pub struct Statement<'a> {
    /// Reference to the underlying DuckDB connection.
    con: &'a RawConnection,
    /// Pointer to the prepared DuckDB statement (FFI resource).
    stmt: duckdb_prepared_statement,
    /// 1-based index of the next parameter to bind (incremented by each `bind` call).
    bind_idx: u64,
}

impl Statement<'_> {
    /// Prepares a new `Statement` from an SQL string.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL cannot be compiled into a prepared statement.
    pub(super) fn new<'a, 'b: 'a>(
        con: &'b RawConnection,
        sql: &str,
    ) -> Result<Statement<'a>> {
        let mut stmt: duckdb_prepared_statement = ptr::null_mut();
        let c_str = std::ffi::CString::new(sql)?;
        // SAFETY: `con.con` is a valid open `duckdb_connection`; `c_str` is a valid
        // null-terminated C string. `stmt` is a valid output pointer.
        let resp = unsafe { duckdb_prepare(con.con, c_str.as_ptr(), &mut stmt) };
        result_from_duckdb_prepare(resp, stmt)?;
        Ok(Statement { con, stmt, bind_idx: 0 })
    }

    /// Returns a reference to the raw prepared-statement pointer.
    #[allow(unused)]
    #[inline]
    fn raw(&self) -> &duckdb_prepared_statement {
        &self.stmt
    }

    /// Returns a reference to the underlying raw connection.
    #[allow(unused)]
    #[inline]
    fn connection(&self) -> &RawConnection {
        self.con
    }
}

// Exposed API
impl Statement<'_> {
    /// Binds a value to the next positional parameter (1-based).
    ///
    /// The first call binds parameter 1, the second call parameter 2, and so on.
    /// Call [`clear_bindings`](Statement::clear_bindings) to reset the counter.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails.
    #[must_use = "bind result should be checked"]
    #[allow(unused)]
    #[inline]
    pub fn bind<T: AppendAble>(
        &mut self,
        binder: &mut T,
    ) -> Result<()> {
        self.bind_idx += 1;
        // Pass the 1-based index directly to stmt_append.
        self.bind_at(binder, self.bind_idx)
    }

    /// Binds a value to the parameter at the given 1-based index.
    ///
    /// # Arguments
    ///
    /// * `binder` - The value to bind, must implement [`AppendAble`].
    /// * `idx` - The 1-based parameter index.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails.
    #[allow(unused)]
    #[inline]
    pub fn bind_at<T: AppendAble>(
        &self,
        binder: &mut T,
        idx: u64,
    ) -> Result<()> {
        binder.stmt_append(idx, self.stmt)
    }

    /// Executes the prepared statement and returns the result.
    ///
    /// The statement can be re-executed after calling [`clear_bindings`](Statement::clear_bindings)
    /// and re-binding parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if execution fails.
    #[must_use = "execute returns the query result; dropping it without reading discards rows"]
    #[allow(unused)]
    pub fn execute(&mut self) -> Result<DuckResult> {
        // SAFETY: `mem::zeroed::<duckdb_result>()` produces an all-zeros value, which is
        // the correct initial state for a `duckdb_result` output parameter. `duckdb_result`
        // is a small `Copy` struct with no self-referential fields, so it needs no stable
        // heap address — DuckResult::new takes it by value.
        let mut out = unsafe { mem::zeroed::<duckdb_result>() };
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut out` provides a pointer
        // to the stack-local zeroed `duckdb_result`. Ownership transfers to `DuckResult::new`,
        // whose `Drop` calls `duckdb_destroy_result` once.
        let resp = unsafe { duckdb_execute_prepared(self.stmt, &mut out as *mut duckdb_result) };
        result_from_duckdb_result(resp, &mut out as *mut duckdb_result)?;
        Ok(DuckResult::new(out))
    }

    /// Returns the number of parameters in the prepared statement.
    #[allow(unused)]
    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        // SAFETY: `self.stmt` is a valid prepared statement.
        unsafe { duckdb_nparams(self.stmt) as usize }
    }

    /// Clears all parameter bindings and resets the bind index to zero.
    ///
    /// After calling this method, subsequent [`bind`](Statement::bind) calls start
    /// from parameter 1 again.
    ///
    /// # Errors
    ///
    /// Returns an error if the DuckDB clear-bindings call fails.
    #[must_use = "clear_bindings result should be checked"]
    #[allow(unused)]
    #[inline]
    pub fn clear_bindings(&mut self) -> Result<()> {
        // SAFETY: `self.stmt` is a valid prepared statement.
        let res = unsafe { duckdb_clear_bindings(self.stmt) };
        if res != DuckDBSuccess {
            Err(Error::DuckDBFailure(
                crate::ffi::Error::new(crate::ffi::DuckDBError),
                Some("Failed to clear bindings".to_owned()),
            ))
        } else {
            self.bind_idx = 0;
            Ok(())
        }
    }

    /// Returns `true` if the prepared statement pointer is null (not initialized).
    #[allow(unused)]
    #[inline]
    pub fn is_null(&self) -> bool {
        self.stmt.is_null()
    }
}

/// Destroys the prepared statement when the `Statement` is dropped.
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.stmt` is a valid duckdb_prepared_statement (or null).
        // `duckdb_destroy_prepare` is idempotent and handles the non-null check itself,
        // but we guard here as a belt-and-suspenders measure.
        unsafe {
            if !self.stmt.is_null() {
                duckdb_destroy_prepare(&mut self.stmt);
            }
        }
    }
}

/// A prepared statement that can be reset and re-executed with different bindings.
///
/// Unlike `Statement`, `CachedStatement` is not tied to a connection's lifetime.
/// It remains valid as long as the underlying database is open.
///
/// This type is used by Diesel statement cache
/// (`StatementCache<DuckDb, CachedStatement>`).
pub struct CachedStatement {
    /// SQL source retained for statement-cache key comparisons.
    ///
    /// Not read within `better-duck-core` itself; consumed by
    /// `StatementCache` implementation in `better-duck-diesel`.
    #[allow(dead_code)]
    pub(crate) sql: Box<str>,
    /// Raw prepared-statement handle.
    stmt: ffi::duckdb_prepared_statement,
}

impl CachedStatement {
    /// Prepares `sql` against the given connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if DuckDB cannot parse or plan the query,
    /// or [`Error::NulError`] if `sql` contains an interior nul byte.
    pub fn prepare(
        conn: &RawConnection,
        sql: impl AsRef<str>,
    ) -> Result<Self> {
        let sql_str = sql.as_ref();
        let mut stmt: ffi::duckdb_prepared_statement = ptr::null_mut();
        let c_str = CString::new(sql_str)?;
        // SAFETY: `conn.con` is a valid open duckdb_connection. `c_str` is a
        // valid null-terminated CString that outlives this call. `&mut stmt` is a
        // valid output pointer. `duckdb_prepare` does not retain either pointer.
        let r = unsafe { ffi::duckdb_prepare(conn.con, c_str.as_ptr(), &mut stmt) };
        result_from_duckdb_prepare(r, stmt)?;
        Ok(CachedStatement { sql: sql_str.into(), stmt })
    }

    /// Resets all parameter bindings so the statement can be re-executed.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if the DuckDB clear-bindings call fails.
    pub fn reset_bindings(&mut self) -> Result<()> {
        // SAFETY: `self.stmt` is a valid prepared statement — the Drop impl enforces this.
        let r = unsafe { ffi::duckdb_clear_bindings(self.stmt) };
        if r == ffi::DuckDBSuccess {
            Ok(())
        } else {
            Err(Error::DuckDBFailure(ffi::Error::new(r), None))
        }
    }

    /// Binds `value` at the given **1-based** parameter index.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails or `idx` is out of range.
    pub fn bind<T: AppendAble + ?Sized>(
        &mut self,
        idx: u64,
        value: &mut T,
    ) -> Result<()> {
        value.stmt_append(idx, self.stmt)
    }

    /// Binds `value` to the named parameter `name` (a SQL `$name`-style
    /// parameter, as opposed to a positional `$1`/`?`).
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidParameterName`] if this statement has no
    /// parameter named `name`, or an error if the underlying DuckDB bind
    /// call fails.
    pub fn bind_named<T: AppendAble + ?Sized>(
        &mut self,
        name: &str,
        value: &mut T,
    ) -> Result<()> {
        let idx = self.parameter_index(name)?;
        self.bind(idx, value)
    }

    /// Resolves `name` to its 1-based parameter index.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidParameterName`] if no parameter is named `name`.
    fn parameter_index(
        &self,
        name: &str,
    ) -> Result<u64> {
        let c_name = CString::new(name)?;
        let mut idx: u64 = 0;
        // SAFETY: `self.stmt` is a valid prepared statement; `c_name` is a
        // valid NUL-terminated C string for the duration of this call;
        // `&mut idx` is a valid output pointer.
        let r = unsafe {
            duckdb_bind_parameter_index(self.stmt, &mut idx as *mut u64, c_name.as_ptr())
        };
        if r == DuckDBSuccess {
            Ok(idx)
        } else {
            Err(Error::InvalidParameterName(name.to_owned()))
        }
    }

    /// Returns the number of parameters this statement accepts.
    #[inline]
    pub fn parameter_count(&self) -> usize {
        // SAFETY: `self.stmt` is a valid prepared statement.
        unsafe { duckdb_nparams(self.stmt) as usize }
    }

    /// Returns the name of the parameter at the given **1-based** index, or
    /// `None` if the parameter is positional (unnamed) or `idx` is out of
    /// range.
    pub fn parameter_name(
        &self,
        idx: u64,
    ) -> Option<String> {
        // SAFETY: `self.stmt` is a valid prepared statement; `idx` need not
        // be pre-validated — DuckDB returns null for an out-of-range index.
        let raw = unsafe { duckdb_parameter_name(self.stmt, idx) };
        if raw.is_null() {
            return None;
        }
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 string,
        // caller-owned — freed via `duckdb_free` exactly once, below.
        let name = unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned();
        // SAFETY: `raw` was allocated by DuckDB and must be freed with
        // `duckdb_free`, per `duckdb_parameter_name`'s documented contract.
        unsafe { duckdb_free(raw.cast_mut().cast::<c_void>()) };
        Some(name)
    }

    /// Returns the DuckDB type of the parameter at the given **1-based** index.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn parameter_type(
        &self,
        idx: u64,
    ) -> Result<DUCKDB_TYPE> {
        // SAFETY: `self.stmt` is a valid prepared statement.
        let ty = unsafe { ffi::duckdb_param_type(self.stmt, idx) };
        if ty == crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID {
            return Err(Error::InvalidColumnIndex(idx as usize));
        }
        Ok(ty)
    }

    /// Returns the kind of SQL statement this is (`SELECT`, `INSERT`, …),
    /// without executing it.
    #[inline]
    pub fn statement_type(&self) -> StatementType {
        // SAFETY: `self.stmt` is a valid prepared statement.
        StatementType::from_raw(unsafe { duckdb_prepared_statement_type(self.stmt) })
    }

    /// Returns the number of columns this statement's result will have,
    /// without executing it.
    #[inline]
    pub fn column_count(&self) -> usize {
        // SAFETY: `self.stmt` is a valid prepared statement.
        unsafe { duckdb_prepared_statement_column_count(self.stmt) as usize }
    }

    /// Returns the name of the result column at `idx`, without executing
    /// this statement.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_name(
        &self,
        idx: usize,
    ) -> Result<String> {
        // SAFETY: `self.stmt` is a valid prepared statement; `idx` need not
        // be pre-validated — DuckDB returns null for an out-of-range index.
        let raw = unsafe { duckdb_prepared_statement_column_name(self.stmt, idx as u64) };
        if raw.is_null() {
            return Err(Error::InvalidColumnIndex(idx));
        }
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 string,
        // caller-owned — freed via `duckdb_free` exactly once, below.
        let name = unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned();
        // SAFETY: `raw` was allocated by DuckDB and must be freed with
        // `duckdb_free`, per this function's documented contract.
        unsafe { duckdb_free(raw.cast_mut().cast::<c_void>()) };
        Ok(name)
    }

    /// Returns the DuckDB type of the result column at `idx`, without
    /// executing this statement.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_type(
        &self,
        idx: usize,
    ) -> Result<DUCKDB_TYPE> {
        // SAFETY: `self.stmt` is a valid prepared statement.
        let ty = unsafe { duckdb_prepared_statement_column_type(self.stmt, idx as u64) };
        if ty == crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID {
            return Err(Error::InvalidColumnIndex(idx));
        }
        Ok(ty)
    }

    /// Returns the full logical type of the result column at `idx`, without
    /// executing this statement.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidColumnIndex`] if `idx` is out of range.
    pub fn column_logical_type(
        &self,
        idx: usize,
    ) -> Result<LogicalType> {
        // SAFETY: `self.stmt` is a valid prepared statement.
        let raw = unsafe { duckdb_prepared_statement_column_logical_type(self.stmt, idx as u64) };
        LogicalType::from_raw(raw).map_err(|_| Error::InvalidColumnIndex(idx))
    }

    /// Executes the prepared statement and returns the result.
    ///
    /// Works for all statement types:
    /// - **SELECT** — iterate rows via the [`Iterator`] impl on [`DuckResult`].
    /// - **INSERT / UPDATE / DELETE** — check [`DuckResult::changes()`] for affected rows.
    /// - **DDL** (`CREATE TABLE` etc.) — `.changes()` returns `0`, no rows to iterate.
    /// - **INSERT … RETURNING** — iterate rows and/or call `.changes()`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuckDBFailure`] if execution fails.
    #[must_use = "the DuckResult carries both affected-row count (.changes()) and row iterator — consume it"]
    pub fn execute(&mut self) -> Result<DuckResult> {
        // SAFETY: `mem::zeroed::<ffi::duckdb_result>()` is the correct initialization for a
        // DuckDB result output parameter. `duckdb_result` is a small `Copy` struct with no
        // self-referential fields, so it needs no stable heap address — DuckResult::new
        // takes it by value.
        let mut out = unsafe { mem::zeroed::<ffi::duckdb_result>() };
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut out` provides a raw
        // pointer to the stack-local zeroed duckdb_result output buffer. Ownership
        // transfers to DuckResult::new; its Drop calls duckdb_destroy_result exactly once.
        let r =
            unsafe { ffi::duckdb_execute_prepared(self.stmt, &mut out as *mut ffi::duckdb_result) };
        result_from_duckdb_result(r, &mut out as *mut ffi::duckdb_result)?;
        Ok(DuckResult::new(out))
    }
}

impl Drop for CachedStatement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            // SAFETY: `self.stmt` is a valid prepared statement not yet destroyed.
            // The null guard ensures this path runs at most once.
            unsafe { ffi::duckdb_destroy_prepare(&mut self.stmt) };
        }
    }
}

// SAFETY: A prepared statement is tied to its database, not to a thread.
// The database handle is reference-counted via Arc<RawDatabase>, making it
// safe to move a CachedStatement to another thread.
unsafe impl Send for CachedStatement {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::helpers::path::path_to_cstring;
    use crate::raw::connection::RawConnection;
    use crate::types::appendable::AppendAble;

    struct DummyAppendAble;
    impl AppendAble for DummyAppendAble {
        fn stmt_append(
            &mut self,
            _idx: u64,
            _stmt: duckdb_prepared_statement,
        ) -> Result<()> {
            Ok(())
        }
        fn appender_append(
            &mut self,
            _appender: crate::ffi::duckdb_appender,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    #[test]
    fn test_new() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let stmt = Statement::new(&con, sql);
        assert!(stmt.is_ok());
    }

    #[test]
    fn test_bind_and_bind_at() {
        let con = get_test_connection();
        let sql = "SELECT ?";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let mut dummy = DummyAppendAble;
        assert!(stmt.bind(&mut dummy).is_ok());
        assert!(stmt.bind_at(&mut dummy, 1).is_ok());
    }

    #[test]
    fn test_raw_and_connection() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let stmt = Statement::new(&con, sql).unwrap();
        let _raw = stmt.raw();
        let _con = stmt.connection();
    }

    #[test]
    fn test_execute() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let result = stmt.execute();
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_can_be_called_multiple_times() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        assert!(stmt.execute().is_ok());
        assert!(stmt.execute().is_ok());
    }

    #[test]
    fn test_clear_bindings_resets_idx() {
        let con = get_test_connection();
        let sql = "SELECT $1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let mut dummy = DummyAppendAble;
        stmt.bind(&mut dummy).unwrap();
        assert_eq!(stmt.bind_idx, 1);
        stmt.clear_bindings().unwrap();
        assert_eq!(stmt.bind_idx, 0);
    }
}
