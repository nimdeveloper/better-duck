// These are public but not re-exported by lib.rs, so only visible within crate.

use std::ffi::CStr;

use crate::ffi::{
    duckdb_appender, duckdb_appender_destroy, duckdb_appender_error_data, duckdb_destroy_prepare,
    duckdb_destroy_result, duckdb_prepare_error, duckdb_prepared_statement, duckdb_result,
    duckdb_result_error, duckdb_result_error_type, duckdb_state, DuckDBSuccess, Error as FFIError,
};

use crate::error::{EngineError, EngineErrorKind, Error, Result};
use crate::raw::error_data::ErrorData;

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
/// ```rust,ignore
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
/// ```rust,ignore
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

    if appender.is_null() {
        return error_from_duckdb_code(code, Some("appender pointer is null".to_string()));
    }

    // SAFETY: `appender` is non-null (checked above). The appender handle stored in
    // `*appender` may still be null if creation failed, which we check before use.
    // On failure with a non-null handle we read the *typed* error data (not the
    // deprecated `duckdb_appender_error` string), then destroy the appender.
    unsafe {
        if (*appender).is_null() {
            return error_from_duckdb_code(code, Some("appender is null".to_string()));
        }
        let engine = engine_error_from_appender(*appender);
        duckdb_appender_destroy(appender);
        Err(Error::Engine(engine))
    }
}

/// Maps a failing appender operation into a typed [`Error::Engine`] without
/// destroying the appender, so the caller can keep using or later destroy it.
///
/// # Safety
///
/// `appender` must be a valid, non-null `duckdb_appender`.
#[cold]
#[inline]
pub unsafe fn check_append(
    code: duckdb_state,
    appender: duckdb_appender,
) -> Result<()> {
    if code == DuckDBSuccess {
        return Ok(());
    }
    // SAFETY: `appender` is valid and non-null per this function's contract.
    Err(Error::Engine(unsafe { engine_error_from_appender(appender) }))
}

/// Reads DuckDB's typed error data from an appender, copying it into an owned
/// [`EngineError`]. Falls back to [`EngineError::unavailable`] when the appender
/// exposes no error data.
///
/// # Safety
///
/// `appender` must be a valid, non-null `duckdb_appender`.
#[cold]
#[inline]
unsafe fn engine_error_from_appender(appender: duckdb_appender) -> EngineError {
    // SAFETY: `appender` is valid and non-null. `duckdb_appender_error_data` returns an
    // owned handle (or null); `ErrorData` takes ownership and destroys it, copying every
    // field out first.
    match unsafe { ErrorData::from_raw(duckdb_appender_error_data(appender)) } {
        Some(data) if data.has_error() => data.to_engine_error(),
        _ => EngineError::unavailable(Some(
            "appender reported failure without error data".to_owned(),
        )),
    }
}

/// Converts a bare `duckdb_state` from a bind/value FFI call into a `Result<()>`.
///
/// Bind-family calls (`duckdb_bind_*`, `duckdb_append_value`) expose no per-call
/// error string, so a failure carries only the status code. Use [`check_append`]
/// where a `duckdb_appender` is in hand, since that path can recover typed data.
#[inline]
pub fn check_state(code: duckdb_state) -> Result<()> {
    if code == DuckDBSuccess {
        Ok(())
    } else {
        Err(Error::DuckDBFailure(FFIError::new(code), None))
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
/// ```rust,ignore
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
    // SAFETY: `prepare` is a duckdb_prepared_statement returned by `duckdb_prepare`.
    // If non-null we extract the error message and destroy it. The null check guards
    // against a failed prepare that returned null.
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

/// Converts the result of a DuckDB query operation into a `Result<()>`.
///
/// If the operation was successful, returns `Ok(())`. Otherwise, reads DuckDB's
/// *typed* error classification (`duckdb_result_error_type`) and message, destroys
/// the result, and returns a typed [`Error::Engine`].
///
/// # Arguments
///
/// * `code` - The DuckDB state code returned by the query operation.
/// * `out` - A pointer to the DuckDB result.
///
/// # Returns
///
/// * `Ok(())` if the operation was successful.
/// * `Err(Error::Engine)` carrying DuckDB's classification and message otherwise.
///
/// # Example
///
/// ```rust,ignore
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
    // SAFETY: `out` is a `*mut duckdb_result` that was passed to `duckdb_query` or
    // `duckdb_execute_prepared`. On error DuckDB writes error info into `*out`;
    // `duckdb_result_error_type` and `duckdb_result_error` read from that memory
    // (the string pointer points into the result). We copy both out before
    // `duckdb_destroy_result` frees them.
    unsafe {
        let kind = EngineErrorKind::from_raw(duckdb_result_error_type(out));
        let c_err = duckdb_result_error(out);
        let message =
            (!c_err.is_null()).then(|| CStr::from_ptr(c_err).to_string_lossy().into_owned());
        duckdb_destroy_result(out);
        Err(Error::Engine(EngineError { kind, message }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::DuckDBError;
    use std::{mem, ptr};

    #[test]
    fn converters_accept_success_without_dereferencing_outputs() {
        assert!(result_from_duckdb_appender(DuckDBSuccess, ptr::null_mut()).is_ok());
        assert!(result_from_duckdb_prepare(DuckDBSuccess, ptr::null_mut()).is_ok());
        assert!(result_from_duckdb_result(DuckDBSuccess, ptr::null_mut()).is_ok());
        assert!(check_state(DuckDBSuccess).is_ok());
    }

    #[test]
    fn check_state_reports_failure_as_duckdb_failure() {
        assert!(matches!(check_state(DuckDBError), Err(Error::DuckDBFailure(_, None))));
    }

    #[test]
    fn appender_failure_with_null_output_has_context() {
        let error = result_from_duckdb_appender(DuckDBError, ptr::null_mut()).unwrap_err();
        assert!(matches!(
            error,
            Error::DuckDBFailure(_, Some(message)) if message == "appender pointer is null"
        ));
    }

    #[test]
    fn prepare_failure_with_null_handle_has_context() {
        let error = result_from_duckdb_prepare(DuckDBError, ptr::null_mut()).unwrap_err();
        assert!(matches!(
            error,
            Error::DuckDBFailure(_, Some(message)) if message == "prepare is null"
        ));
    }

    #[test]
    fn failed_query_result_carries_typed_error_and_message() {
        let mut database = ptr::null_mut();
        let mut connection = ptr::null_mut();
        let path = std::ffi::CString::new(":memory:").unwrap();
        let sql = std::ffi::CString::new("SELECT * FROM missing_table").unwrap();

        // SAFETY: all pointers are valid output parameters and successful handles are
        // released below. A zeroed `duckdb_result` is DuckDB's required initial state.
        unsafe {
            assert_eq!(crate::ffi::duckdb_open(path.as_ptr(), &mut database), DuckDBSuccess);
            assert_eq!(crate::ffi::duckdb_connect(database, &mut connection), DuckDBSuccess);
            let mut out = mem::zeroed::<duckdb_result>();
            let code = crate::ffi::duckdb_query(connection, sql.as_ptr(), &mut out);
            let error = result_from_duckdb_result(code, &mut out).unwrap_err();
            let engine = match error {
                Error::Engine(engine) => engine,
                other => panic!("expected a typed engine error, got {other:?}"),
            };
            // A missing table is a catalog error; the driver reports DuckDB's own kind.
            assert_eq!(engine.kind, EngineErrorKind::Catalog);
            assert!(engine.message.is_some_and(|m| m.contains("missing_table")));
            crate::ffi::duckdb_disconnect(&mut connection);
            crate::ffi::duckdb_close(&mut database);
        }
    }
}
