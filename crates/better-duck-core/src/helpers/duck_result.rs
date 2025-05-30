// These are public but not re-exported by lib.rs, so only visible within crate.

use std::ffi::CStr;

use crate::ffi;

use crate::error::{Error, Result};

#[inline]
fn error_from_duckdb_code(code: ffi::duckdb_state, message: Option<String>) -> Result<()> {
    Err(Error::DuckDBFailure(ffi::Error::new(code), message))
}

#[cold]
#[inline]
pub fn result_from_duckdb_appender(
    code: ffi::duckdb_state,
    appender: *mut ffi::duckdb_appender,
) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if (*appender).is_null() {
            Some("appender is null".to_string())
        } else {
            let c_err = ffi::duckdb_appender_error(*appender);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_appender_destroy(appender);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_prepare(
    code: ffi::duckdb_state,
    mut prepare: ffi::duckdb_prepared_statement,
) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if prepare.is_null() {
            Some("prepare is null".to_string())
        } else {
            let c_err = ffi::duckdb_prepare_error(prepare);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_prepare(&mut prepare);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_arrow(code: ffi::duckdb_state, mut out: ffi::duckdb_arrow) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = if out.is_null() {
            Some("out is null".to_string())
        } else {
            let c_err = ffi::duckdb_query_arrow_error(out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_arrow(&mut out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}

#[cold]
#[inline]
pub fn result_from_duckdb_result(
    code: ffi::duckdb_state,
    mut out: ffi::duckdb_result,
) -> Result<()> {
    if code == ffi::DuckDBSuccess {
        return Ok(());
    }
    unsafe {
        let message = {
            let c_err = ffi::duckdb_result_error(&mut out);
            let message = Some(CStr::from_ptr(c_err).to_string_lossy().to_string());
            ffi::duckdb_destroy_result(&mut out);
            message
        };
        error_from_duckdb_code(code, message)
    }
}
