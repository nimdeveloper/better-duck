// These are public but not re-exported by lib.rs, so only visible within crate.

use std::ffi::CStr;

use crate::ffi::{
    duckdb_appender, duckdb_appender_destroy, duckdb_appender_error, duckdb_arrow,
    duckdb_destroy_arrow, duckdb_destroy_prepare, duckdb_destroy_result, duckdb_prepare_error,
    duckdb_prepared_statement, duckdb_query_arrow_error, duckdb_result, duckdb_result_error,
    duckdb_state, DuckDBSuccess, Error as FFIError,
};

use crate::error::{Error, Result};

/// Converts a DuckDB error code and optional message into a `Result<()>` with a `DuckDBFailure` error.
///
/// # Arguments
///
/// * `code` - The DuckDB error code returned by an FFI call.
/// * `message` - An optional error message to include.
///
/// # Returns
///
/// * `Err(Error::DuckDBFailure)` containing the error code and message.
///
/// # Example
///
/// ```rust
/// let result = error_from_duckdb_code(ffi::DuckDBError, Some("Some error".to_string()));
/// assert!(result.is_err());
/// ```
#[inline]
fn error_from_duckdb_code(
    code: duckdb_state,
    message: Option<String>,
) -> Result<()> {
    Err(Error::DuckDBFailure(FFIError::new(code), message))
}

/// Converts the result of a DuckDB appender operation into a `Result<()>`.
///
/// If the operation was successful, returns `Ok(())`. Otherwise, retrieves the error message
/// from the appender, destroys the appender, and returns an error.
///
/// # Arguments
///
/// * `code` - The DuckDB state code returned by the appender operation.
/// * `appender` - A pointer to the DuckDB appender.
///
/// # Returns
///
/// * `Ok(())` if the operation was successful.
/// * `Err(Error::DuckDBFailure)` with the error message if the operation failed.
///
/// # Example
///
/// ```rust
/// let code = ffi::DuckDBSuccess;
/// let appender: *mut ffi::duckdb_appender = std::ptr::null_mut();
/// let result = result_from_duckdb_appender(code, appender);
/// assert!(result.is_ok());
/// ```
#[cold]
#[inline]
pub fn result_from_duckdb_appender(
    code: duckdb_state,
    appender: *mut duckdb_appender,
) -> Result<()> {
    if code == DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if (*appender).is_null() {
            Some("appender is null".to_string())
        } else {
            let c_err = duckdb_appender_error(*appender);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            duckdb_appender_destroy(appender);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

/// Converts the result of a DuckDB prepared statement operation into a `Result<()>`.
///
/// If the operation was successful, returns `Ok(())`. Otherwise, retrieves the error message
/// from the prepared statement, destroys the prepared statement, and returns an error.
///
/// # Arguments
///
/// * `code` - The DuckDB state code returned by the prepare operation.
/// * `prepare` - The DuckDB prepared statement handle.
///
/// # Returns
///
/// * `Ok(())` if the operation was successful.
/// * `Err(Error::DuckDBFailure)` with the error message if the operation failed.
///
/// # Example
///
/// ```rust
/// let code = ffi::DuckDBSuccess;
/// let prepare: ffi::duckdb_prepared_statement = std::ptr::null_mut();
/// let result = result_from_duckdb_prepare(code, prepare);
/// assert!(result.is_ok());
/// ```
#[cold]
#[inline]
pub fn result_from_duckdb_prepare(
    code: duckdb_state,
    mut prepare: duckdb_prepared_statement,
) -> Result<()> {
    if code == DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if prepare.is_null() {
            Some("prepare is null".to_string())
        } else {
            let c_err = duckdb_prepare_error(prepare);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            duckdb_destroy_prepare(&mut prepare);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

/// Converts the result of a DuckDB Arrow query operation into a `Result<()>`.
///
/// If the operation was successful, returns `Ok(())`. Otherwise, retrieves the error message
/// from the Arrow result, destroys the Arrow result, and returns an error.
///
/// # Arguments
///
/// * `code` - The DuckDB state code returned by the Arrow query operation.
/// * `out` - The DuckDB Arrow result handle.
///
/// # Returns
///
/// * `Ok(())` if the operation was successful.
/// * `Err(Error::DuckDBFailure)` with the error message if the operation failed.
///
/// # Example
///
/// ```rust
/// let code = ffi::DuckDBSuccess;
/// let out: ffi::duckdb_arrow = std::ptr::null_mut();
/// let result = result_from_duckdb_arrow(code, out);
/// assert!(result.is_ok());
/// ```
#[allow(unused)]
#[cold]
#[inline]
pub fn result_from_duckdb_arrow(
    code: duckdb_state,
    mut out: duckdb_arrow,
) -> Result<()> {
    if code == DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if out.is_null() {
            Some("out arrow is null".to_string())
        } else {
            let c_err = duckdb_query_arrow_error(out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            duckdb_destroy_arrow(&mut out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

/// Converts the result of a DuckDB query operation into a `Result<()>`.
///
/// If the operation was successful, returns `Ok(())`. Otherwise, retrieves the error message
/// from the result, destroys the result, and returns an error.
///
/// # Arguments
///
/// * `code` - The DuckDB state code returned by the query operation.
/// * `out` - A pointer to the DuckDB result.
///
/// # Returns
///
/// * `Ok(())` if the operation was successful.
/// * `Err(Error::DuckDBFailure)` with the error message if the operation failed.
///
/// # Example
///
/// ```rust
/// let code = ffi::DuckDBSuccess;
/// let out: *mut ffi::duckdb_result = std::ptr::null_mut();
/// let result = result_from_duckdb_result(code, out);
/// assert!(result.is_ok());
/// ```
#[cold]
#[inline]
pub fn result_from_duckdb_result(
    code: duckdb_state,
    out: *mut duckdb_result,
) -> Result<()> {
    if code == DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = {
            let c_err = duckdb_result_error(out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            duckdb_destroy_result(out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}
