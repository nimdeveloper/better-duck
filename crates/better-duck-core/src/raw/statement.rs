use std::{ffi::CString, mem, ptr, sync::Arc};

use crate::ffi::{
    duckdb_clear_bindings, duckdb_destroy_prepare, duckdb_execute_prepared, duckdb_nparams,
    duckdb_prepare, duckdb_result, DuckDBSuccess,
};

use crate::{
    error::{Error, Result},
    ffi,
    ffi::duckdb_prepared_statement,
    helpers::duck_result::{result_from_duckdb_prepare, result_from_duckdb_result},
    raw::{
        connection::{ConnectionInner, RawConnection},
        result::DuckResult,
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
        // SAFETY: `con` is a live `RawConnection`, so its handle is a valid open
        // `duckdb_connection`; `c_str` is a valid null-terminated C string. `stmt` is a
        // valid output pointer. The `'a` borrow keeps the connection alive for the
        // statement's whole lifetime.
        let resp = unsafe { duckdb_prepare(con.handle(), c_str.as_ptr(), &mut stmt) };
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
/// The statement is prepared on — and therefore belongs to — the connection
/// passed to [`prepare`](CachedStatement::prepare). DuckDB scopes transactions
/// to a connection, so the statement must never be prepared on a different one:
/// doing so would execute outside any `BEGIN`/`ROLLBACK` the caller has open.
///
/// The statement retains that connection, so it cannot outlive it. Unlike a
/// [`Statement`], it carries no Rust lifetime, which lets it live in a
/// `StatementCache` alongside the connection it was prepared on.
///
/// This type is used by Diesel statement cache
/// (`StatementCache<DuckDb, CachedStatement>`).
pub struct CachedStatement {
    /// Keeps the *connection* this statement was prepared on open for at least as
    /// long as the statement.
    ///
    /// This is deliberately the connection rather than the database. Retaining only
    /// `Arc<RawDatabase>` would keep the database open while allowing the connection
    /// to be disconnected first, leaving `duckdb_destroy_prepare` to run against a
    /// dead connection. Retaining a *cloned* connection would be equally wrong in the
    /// other direction: that opens a separate DuckDB connection, which would silently
    /// run cached statements outside the caller's transaction.
    _connection: Arc<ConnectionInner>,
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
    /// The statement is bound to `conn` and shares its transaction state.
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
        // SAFETY: `conn`'s handle is a valid open duckdb_connection owned by the
        // caller. `c_str` is a valid null-terminated CString that outlives this
        // call, and `&mut stmt` is a valid output pointer. Preparing on the
        // caller's own connection keeps the statement inside that connection's
        // transaction scope.
        let r = unsafe { ffi::duckdb_prepare(conn.handle(), c_str.as_ptr(), &mut stmt) };
        result_from_duckdb_prepare(r, stmt)?;
        Ok(CachedStatement { _connection: Arc::clone(conn.inner()), sql: sql_str.into(), stmt })
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

// SAFETY: the only non-`Send` field is the raw `stmt` pointer; `Arc<ConnectionInner>`
// is already `Send` because `ConnectionInner` is `Send + Sync`. DuckDB permits a
// prepared statement to be used from a different thread than the one that prepared it,
// provided it is not used concurrently — `CachedStatement` takes `&mut self` for every
// operation that touches the handle and is deliberately not `Sync`, so no two threads
// can drive it at once. Retaining the connection means moving the statement to another
// thread also keeps its connection alive, so the handle cannot dangle.
unsafe impl Send for CachedStatement {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::helpers::path::path_to_cstring;
    use crate::raw::connection::RawConnection;
    use crate::types::{appendable::AppendAble, value::DuckValue};

    struct CheckedI32(i32);
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
            _appender: ffi::duckdb_appender,
        ) -> Result<()> {
            unreachable!("DummyAppendAble is only used for statement binding")
        }
    }

    impl AppendAble for CheckedI32 {
        fn stmt_append(
            &mut self,
            idx: u64,
            stmt: duckdb_prepared_statement,
        ) -> Result<()> {
            // SAFETY: `stmt` comes from a live Statement and the scalar value is copied.
            let state = unsafe { ffi::duckdb_bind_int32(stmt, idx, self.0) };
            if state == DuckDBSuccess {
                Ok(())
            } else {
                Err(Error::DuckDBFailure(ffi::Error::new(state), None))
            }
        }

        fn appender_append(
            &mut self,
            _appender: crate::ffi::duckdb_appender,
        ) -> Result<()> {
            unreachable!("CheckedI32 is only used for statement binding")
        }
    }

    fn get_test_connection() -> RawConnection {
        let c_path = path_to_cstring(":memory:".as_ref()).unwrap();
        let config = Config::default().with("duckdb_api", "rust").unwrap();
        RawConnection::open_with_flags(&c_path, config).unwrap()
    }

    fn assert_single_value(
        mut result: DuckResult,
        column: &str,
        expected: DuckValue,
    ) {
        let row = result.next().expect("expected one row").unwrap();
        assert_eq!(row.get(column), Some(&expected));
        assert!(result.next().is_none());
    }

    #[test]
    fn test_new() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let stmt = Statement::new(&con, sql);
        assert!(stmt.is_ok());
    }

    #[test]
    fn test_prepare_rejects_invalid_sql_and_interior_nul() {
        let con = get_test_connection();

        assert!(matches!(Statement::new(&con, "SELEC 1"), Err(Error::DuckDBFailure(..))));
        assert!(matches!(Statement::new(&con, "SELECT \0 1"), Err(Error::NulError(_))));
        assert!(matches!(CachedStatement::prepare(&con, "SELEC 1"), Err(Error::DuckDBFailure(..))));
        assert!(matches!(CachedStatement::prepare(&con, "SELECT \0 1"), Err(Error::NulError(_))));
    }

    #[test]
    fn test_statement_binds_real_values_and_reports_parameter_errors() {
        let con = get_test_connection();
        let mut stmt = Statement::new(&con, "SELECT $1::INTEGER + $2::INTEGER AS total").unwrap();
        assert_eq!(stmt.bind_parameter_count(), 2);

        let mut first = CheckedI32(19);
        let mut second = CheckedI32(23);
        stmt.bind(&mut first).unwrap();
        stmt.bind(&mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "total", DuckValue::Int(42));

        let mut out_of_range = CheckedI32(99);
        assert!(matches!(stmt.bind_at(&mut out_of_range, 3), Err(Error::DuckDBFailure(..))));
        assert_single_value(stmt.execute().unwrap(), "total", DuckValue::Int(42));
    }

    #[test]
    fn test_statement_clear_bindings_resets_and_reuses() {
        let con = get_test_connection();
        let mut stmt = Statement::new(&con, "SELECT $1::INTEGER AS value").unwrap();
        let mut first = CheckedI32(7);
        stmt.bind(&mut first).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(7));

        stmt.clear_bindings().unwrap();
        assert_eq!(stmt.bind_idx, 0);
        let mut second = CheckedI32(11);
        stmt.bind(&mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(11));
    }

    #[test]
    fn test_cached_statement_retains_sql_and_resets_for_reuse() {
        let con = get_test_connection();
        let sql = "SELECT $1::INTEGER AS value";
        let mut stmt = CachedStatement::prepare(&con, sql).unwrap();
        assert_eq!(stmt.sql.as_ref(), sql);

        let mut first = CheckedI32(100);
        stmt.bind(1, &mut first).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(100));

        stmt.reset_bindings().unwrap();
        let mut second = CheckedI32(200);
        stmt.bind(1, &mut second).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(200));
    }

    #[test]
    fn test_cached_statement_recovers_after_invalid_bind_position() {
        let con = get_test_connection();
        let mut stmt = CachedStatement::prepare(&con, "SELECT $1::INTEGER AS value").unwrap();
        let mut invalid = CheckedI32(1);
        assert!(matches!(stmt.bind(0, &mut invalid), Err(Error::DuckDBFailure(..))));

        stmt.reset_bindings().unwrap();
        let mut valid = CheckedI32(55);
        stmt.bind(1, &mut valid).unwrap();
        assert_single_value(stmt.execute().unwrap(), "value", DuckValue::Int(55));
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
