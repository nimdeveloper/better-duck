//! Traits for appending values to DuckDB statements and appenders.
// The trait methods accept raw FFI pointer arguments (`duckdb_prepared_statement`,
// `duckdb_appender`) by design. Implementations are responsible for using them safely.
#![allow(clippy::not_unsafe_ptr_arg_deref)]
//!
//! # Parameter index convention
//! The `idx` argument to [`AppendAble::stmt_append`] is **1-based**, matching
//! the DuckDB C API. The first parameter is `idx = 1`.

use crate::ffi::{
    duckdb_append_value, duckdb_appender, duckdb_bind_value, duckdb_destroy_value,
    duckdb_prepared_statement, duckdb_value,
};

use crate::error::Result;
use crate::helpers::duck_result::{check_append, check_state};

/// Trait implemented by types that can be bound to a DuckDB prepared statement
/// or appended to a DuckDB appender row.
pub trait AppendAble {
    /// Binds this value to a prepared statement parameter.
    ///
    /// # Arguments
    ///
    /// * `idx` - The **1-based** parameter index. The first parameter is `idx = 1`.
    /// * `stmt` - The prepared statement to bind the value to.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the value was successfully bound, or an error if the
    /// operation failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB bind call fails.
    fn stmt_append(
        &mut self,
        _idx: u64,
        _stmt: duckdb_prepared_statement,
    ) -> Result<()>;

    /// Appends this value to an appender row.
    ///
    /// # Arguments
    ///
    /// * `appender` - The appender to append the value to.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the value was successfully appended, or an error if the
    /// operation failed.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying DuckDB append call fails.
    fn appender_append(
        &mut self,
        _appender: duckdb_appender,
    ) -> Result<()>;
}

/// Binds an owned `duckdb_value` to a prepared-statement parameter, then destroys
/// it exactly once and checks the bind status.
///
/// Centralises the `bind → destroy → check` dance used by every value-based
/// [`AppendAble`] implementation so no DuckDB status is silently dropped.
///
/// # Safety
///
/// `stmt` must be a valid prepared statement, `idx` a 1-based parameter index, and
/// `value` an owned `duckdb_value` that no one else destroys.
#[inline]
pub(crate) unsafe fn bind_owned_value(
    stmt: duckdb_prepared_statement,
    idx: u64,
    mut value: duckdb_value,
) -> Result<()> {
    // SAFETY: `stmt`/`idx` are valid per the contract; `value` is a live owned value.
    let rc = unsafe { duckdb_bind_value(stmt, idx, value) };
    // SAFETY: `value` was owned by the caller; destroy exactly once, before returning.
    unsafe { duckdb_destroy_value(&mut value) };
    check_state(rc)
}

/// Appends an owned `duckdb_value` to an appender row, then destroys it exactly
/// once and checks the append status against the appender's typed error data.
///
/// # Safety
///
/// `appender` must be a valid, non-null appender and `value` an owned
/// `duckdb_value` that no one else destroys.
#[inline]
pub(crate) unsafe fn append_owned_value(
    appender: duckdb_appender,
    mut value: duckdb_value,
) -> Result<()> {
    // SAFETY: `appender` is valid per the contract; `value` is a live owned value.
    let rc = unsafe { duckdb_append_value(appender, value) };
    // SAFETY: `value` was owned by the caller; destroy exactly once, before returning.
    unsafe { duckdb_destroy_value(&mut value) };
    // SAFETY: `appender` is valid and non-null per the contract.
    unsafe { check_append(rc, appender) }
}

/// Implements [`AppendAble`] for a type that already implements [`crate::types::DuckDialect`]
/// by going through the `duckdb_value` path: `to_duck()` → `duckdb_bind_value` /
/// `duckdb_append_value` → `duckdb_destroy_value`.
///
/// Use this for types that have no dedicated `duckdb_bind_*` / `duckdb_append_*` FFI
/// function (e.g. `TimestampS`, `TimeTz`, `Decimal`).
#[macro_export]
macro_rules! impl_appendable_via_to_duck_native {
    ($t:ty) => {
        impl AppendAble for $t {
            fn appender_append(
                &mut self,
                appender: $crate::ffi::duckdb_appender,
            ) -> $crate::error::Result<()> {
                let mut dv = self.to_duck().map_err($crate::error::Error::ConversionError)?;
                // SAFETY: `appender` is a valid duckdb_appender; `dv` was created by `to_duck()`.
                let rc = unsafe { $crate::ffi::duckdb_append_value(appender, dv) };
                // SAFETY: `dv` was created above; destroy exactly once, before returning any error.
                unsafe { $crate::ffi::duckdb_destroy_value(&mut dv) };
                // SAFETY: `appender` is valid and non-null.
                unsafe { $crate::helpers::duck_result::check_append(rc, appender) }
            }
            fn stmt_append(
                &mut self,
                idx: u64,
                stmt: $crate::ffi::duckdb_prepared_statement,
            ) -> $crate::error::Result<()> {
                let mut dv = self.to_duck().map_err($crate::error::Error::ConversionError)?;
                // SAFETY: `stmt`/`idx` are valid; `dv` was created by `to_duck()`.
                let rc = unsafe { $crate::ffi::duckdb_bind_value(stmt, idx, dv) };
                // SAFETY: `dv` was created above; destroy exactly once, before returning any error.
                unsafe { $crate::ffi::duckdb_destroy_value(&mut dv) };
                $crate::helpers::duck_result::check_state(rc)
            }
        }
    };
}
