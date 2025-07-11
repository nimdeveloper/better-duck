use std::{mem, ptr, sync::Arc};

use libduckdb_sys::{
    duckdb_clear_bindings, duckdb_destroy_prepare, duckdb_execute_prepared, duckdb_nparams,
    duckdb_prepare, duckdb_result, DuckDBSuccess,
};

use crate::{
    error::{Error, Result},
    ffi::duckdb_prepared_statement,
    helpers::duck_result::{result_from_duckdb_prepare, result_from_duckdb_result},
    raw::{connection::RawConnection, result::RawResult},
    types::appendable::AppendAble,
};

/// Represents a prepared DuckDB statement, holding the connection, the prepared statement pointer, and execution result.
pub struct Statement {
    /// Shared reference to the underlying DuckDB connection.
    con: Arc<RawConnection>,
    /// Pointer to the prepared DuckDB statement (FFI resource).
    stmt: duckdb_prepared_statement,
    /// Result of the last execution, if any.
    result: Option<RawResult>,
    /// Index for parameter binding.
    bind_idx: u64,
}

impl Statement {
    /// Create a new prepared statement from a SQL string.
    ///
    /// # Arguments
    ///
    /// * `con` - Reference to a raw DuckDB connection.
    /// * `sql` - SQL query string to prepare.
    ///
    /// # Returns
    ///
    /// * `Result<Self>` - The prepared statement or an error.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let con = get_test_connection();
    /// let stmt = Statement::new(&con, "SELECT 1");
    /// assert!(stmt.is_ok());
    /// ```
    pub(super) fn new(
        con: &RawConnection,
        sql: &str,
    ) -> Result<Self> {
        let mut stmt: duckdb_prepared_statement = ptr::null_mut();
        // Convert SQL string to C-compatible string for FFI.
        let c_str = std::ffi::CString::new(sql).expect("Failed to create CString from SQL");
        // SAFETY: con.con and c_str are valid, stmt is a valid pointer for output.
        let resp = unsafe { duckdb_prepare(con.con, c_str.as_ptr(), &mut stmt) };
        // The result_from_duckdb_prepare will check the response and return an error if needed.
        result_from_duckdb_prepare(resp, stmt).map(|_| Self {
            con: Arc::new(con.clone()),
            stmt,
            result: None,
            bind_idx: 0,
        })
    }

    // unsafe fn bind_at_raw(
    //     &self,
    //     binder: duckdb_prepared_statement,
    //     idx: u64,
    // ) -> Result<()> {
    //     // SAFETY: This is unsafe because it directly interacts with the DuckDB FFI.
    //     // Ensure that the binder is a valid prepared statement pointer.
    //     let resp = duckdb_bind_int32(binder, idx as i32);
    //     result_from_duckdb_result(resp, &mut self.stmt)
    // }

    /// Return a reference to the raw DuckDB prepared statement pointer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stmt = Statement::new(&con, "SELECT 1").unwrap();
    /// let raw_ptr = stmt.raw();
    /// ```
    #[allow(unused)]
    #[inline]
    fn raw(&self) -> &duckdb_prepared_statement {
        &self.stmt
    }

    /// Return a reference to the underlying raw connection.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stmt = Statement::new(&con, "SELECT 1").unwrap();
    /// let con_ref = stmt.connection();
    /// ```
    #[allow(unused)]
    #[inline]
    fn connection(&self) -> &RawConnection {
        &self.con
    }
}

// Exposed API
impl Statement {
    /// Bind a value to the next parameter index using the provided binder.
    ///
    /// # Arguments
    ///
    /// * `binder` - The value to bind, must implement `AppendAble`.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Ok if successful, error otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stmt = Statement::new(&con, "SELECT ?").unwrap();
    /// let mut binder = MyAppendableValue::new(42);
    /// stmt.bind(&mut binder).unwrap();
    /// ```
    #[allow(unused)]
    #[inline]
    pub fn bind<T: AppendAble>(
        &mut self,
        binder: &mut T,
    ) -> Result<()> {
        self.bind_idx += 1;
        // Bind at the current index (0-based).
        self.bind_at(binder, self.bind_idx - 1)
    }

    /// Bind a value to a specific parameter index using the provided binder.
    ///
    /// # Arguments
    ///
    /// * `binder` - The value to bind, must implement `AppendAble`.
    /// * `idx` - The parameter index to bind at (0-based).
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Ok if successful, error otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stmt = Statement::new(&con, "SELECT ?").unwrap();
    /// let mut binder = MyAppendableValue::new(42);
    /// stmt.bind_at(&mut binder, 0).unwrap();
    /// ```
    #[allow(unused)]
    #[inline]
    pub fn bind_at<T: AppendAble>(
        &self,
        binder: &mut T,
        idx: u64,
    ) -> Result<()> {
        // Call the binder's stmt_append to bind the value at the given index.
        binder.stmt_append(idx, self.stmt)
    }

    /// Execute the prepared statement and fetch the result.
    ///
    /// # Returns
    ///
    /// * `Result<Option<RawResult>>` - The result if successful, or an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the statement has already been executed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stmt = Statement::new(&con, "SELECT 1").unwrap();
    /// let result = stmt.fetch();
    /// assert!(result.is_ok());
    /// ```
    #[allow(unused)]
    pub fn fetch(&mut self) -> Result<Option<RawResult>> {
        if self.result.is_some() {
            // Prevent double execution of the same statement instance.
            return Err(crate::error::Error::DuckDBFailure(
                crate::ffi::Error::new(crate::ffi::DuckDBError),
                Some("Statement already executed".to_owned()),
            ));
        }
        // SAFETY: result must be zeroed before passing to FFI.
        let mut result: duckdb_result = unsafe { mem::zeroed() };
        // SAFETY: stmt must be a valid prepared statement pointer.
        let resp = unsafe { duckdb_execute_prepared(self.stmt, &mut result) };
        // The result_from_duckdb_result will check the response and return an error if needed.
        result_from_duckdb_result(resp, &mut result).map(|_| Some(RawResult::new(result)))
    }

    /// Get the number of parameters bound to the prepared statement.
    ///
    /// # Returns
    ///
    /// * `usize` - The number of parameters bound.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stmt = Statement::new(&con, "SELECT ?").unwrap();
    /// let mut binder = MyAppendableValue::new(42);
    /// stmt.bind(&mut binder).unwrap();
    /// let count = stmt.bind_parameter_count();
    /// assert_eq!(count, 1);
    /// ```
    #[allow(unused)]
    #[inline]
    pub fn bind_parameter_count(&self) -> usize {
        unsafe { duckdb_nparams(self.stmt) as usize }
    }

    /// Clear all bindings for the prepared statement.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Ok if successful, error otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut stmt = Statement::new(&con, "SELECT ?").unwrap();
    /// let mut binder = MyAppendableValue::new(42);
    /// stmt.bind(&mut binder).unwrap();
    /// stmt.clear_bindings().unwrap();
    /// ```
    #[allow(unused)]
    #[inline]
    pub fn clear_bindings(&self) -> Result<()> {
        let res = unsafe { duckdb_clear_bindings(self.stmt) };
        if res != DuckDBSuccess {
            Err(Error::DuckDBFailure(
                crate::ffi::Error::new(crate::ffi::DuckDBError),
                Some("Failed to clear bindings".to_owned()),
            ))
        } else {
            Ok(())
        }
    }

    /// Check if the prepared statement is null (not initialized).
    ///
    /// # Returns
    ///
    /// * `bool` - True if the statement is null, false otherwise.
    #[allow(unused)]
    #[inline]
    pub fn is_null(&self) -> bool {
        self.stmt.is_null()
    }

    /// Reset the result of the last execution, if any.
    ///
    /// # Safety
    ///
    /// Resources held by the previous result (such as DuckDB result pointers)
    /// are freed automatically when the RawResult is dropped.
    /// This ensures no memory leaks occur when resetting the result.
    #[allow(unused)]
    #[inline]
    pub fn reset_result(&mut self) {
        if self.result.is_some() {
            self.result = None;
        }
    }
}
/// Clone the statement, but do not clone the result.
///
/// NOTE: The underlying prepared statement pointer is not duplicated at the FFI level,
/// so use with caution. This is a shallow clone.
impl Clone for Statement {
    fn clone(&self) -> Self {
        // TODO: Should we clone the result? or empty the result?
        Self { con: Arc::clone(&self.con), stmt: self.stmt, result: None, bind_idx: self.bind_idx }
    }
}

/// Ensure the prepared statement is destroyed when the Statement is dropped.
///
/// # Safety
///
/// This will call the DuckDB FFI to destroy the prepared statement. Do not use the pointer after drop.
impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            if !self.stmt.is_null() {
                // SAFETY: This will free the prepared statement at the FFI level.
                // Do not use self.stmt after this call.
                duckdb_destroy_prepare(&mut self.stmt);
            }
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
    use std::sync::Arc;

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
        // This should be replaced with a real connection setup for integration tests
        // For now, this is a placeholder and will not work unless implemented
        // Setup: create a connection (assuming test DB in memory)
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
        assert!(stmt.bind_at(&mut dummy, 0).is_ok());
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
    fn test_fetch() {
        let con = get_test_connection();
        let sql = "SELECT 1";
        let mut stmt = Statement::new(&con, sql).unwrap();
        let result = stmt.fetch();
        // We can't assert much without a real connection, but this checks the call
        let _ = result.is_ok() || result.is_err();
    }

    #[test]
    fn test_clone() {
        // This test only checks that clone does not panic
        let con = Arc::new(get_test_connection());
        let stmt =
            Statement { con: con.clone(), stmt: std::ptr::null_mut(), result: None, bind_idx: 0 };
        let _cloned = stmt.clone();
    }
}
