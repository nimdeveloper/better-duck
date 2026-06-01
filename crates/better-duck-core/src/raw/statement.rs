use std::{ffi::CString, mem, ptr};

use libduckdb_sys::{
    duckdb_clear_bindings, duckdb_destroy_prepare, duckdb_execute_prepared, duckdb_nparams,
    duckdb_prepare, duckdb_result, DuckDBSuccess,
};

use crate::{
    error::{Error, Result},
    ffi,
    ffi::duckdb_prepared_statement,
    helpers::duck_result::{result_from_duckdb_prepare, result_from_duckdb_result},
    raw::{connection::RawConnection, result::DuckResult},
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
        // the correct initial state for a `duckdb_result` output parameter.
        let mut out = Box::new(unsafe { mem::zeroed::<duckdb_result>() });
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut *out` provides a
        // pointer to the heap-allocated zeroed `duckdb_result`. Ownership of `*out`
        // transfers to `DuckResult::new`, whose `Drop` calls `duckdb_destroy_result` once.
        let resp = unsafe { duckdb_execute_prepared(self.stmt, &mut *out as *mut duckdb_result) };
        result_from_duckdb_result(resp, &mut *out as *mut duckdb_result)?;
        Ok(DuckResult::new(*out))
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

    /// Returns `true` if the prepared statement pointer is null (not initialised).
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
/// Unlike [`Statement`], `CachedStatement` is not tied to a connection's lifetime.
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
        // SAFETY: `mem::zeroed::<ffi::duckdb_result>()` is the correct initialisation
        // for a DuckDB result output parameter.
        let mut out = Box::new(unsafe { mem::zeroed::<ffi::duckdb_result>() });
        // SAFETY: `self.stmt` is a valid prepared statement. `&mut *out` provides a
        // raw pointer to the heap-allocated zeroed duckdb_result output buffer.
        // Ownership of `*out` transfers to DuckResult::new; its Drop calls
        // duckdb_destroy_result exactly once.
        let r = unsafe {
            ffi::duckdb_execute_prepared(self.stmt, &mut *out as *mut ffi::duckdb_result)
        };
        result_from_duckdb_result(r, &mut *out as *mut ffi::duckdb_result)?;
        Ok(DuckResult::new(*out))
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
