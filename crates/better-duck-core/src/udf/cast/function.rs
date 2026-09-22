//! RAII builder around the `duckdb_cast_function` C API.

use std::ffi::{c_void, CStr, CString};

use crate::{
    error::{Error, Result},
    ffi::{
        duckdb_cast_function, duckdb_cast_function_get_cast_mode,
        duckdb_cast_function_get_extra_info, duckdb_cast_function_set_error,
        duckdb_cast_function_set_extra_info, duckdb_cast_function_set_function,
        duckdb_cast_function_set_implicit_cast_cost, duckdb_cast_function_set_row_error,
        duckdb_cast_function_set_source_type, duckdb_cast_function_set_target_type,
        duckdb_cast_function_t, duckdb_cast_mode, duckdb_connection, duckdb_create_cast_function,
        duckdb_destroy_cast_function, duckdb_function_info, duckdb_register_cast_function,
        duckdb_vector, idx_t,
    },
};

use super::super::LogicalType;

/// An in-progress cast function, built up via its setters and registered.
pub(crate) struct CastFunction {
    ptr: duckdb_cast_function,
}

impl CastFunction {
    pub(crate) fn new() -> Self {
        // SAFETY: always safe to call; returns a freshly allocated, empty function.
        let ptr = unsafe { duckdb_create_cast_function() };
        Self { ptr }
    }

    pub(crate) fn set_source_type(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is valid for the call and copied.
        unsafe { duckdb_cast_function_set_source_type(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_target_type(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is valid for the call and copied.
        unsafe { duckdb_cast_function_set_target_type(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_implicit_cast_cost(
        &self,
        cost: i64,
    ) {
        // SAFETY: `self.ptr` is valid; `cost` is copied by value.
        unsafe { duckdb_cast_function_set_implicit_cast_cost(self.ptr, cost) };
    }

    pub(crate) fn set_function(
        &self,
        f: duckdb_cast_function_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid `extern "C"` cast
        // callback with the expected signature.
        unsafe { duckdb_cast_function_set_function(self.ptr, f) };
    }

    /// Stores `state`, retrievable in the callback via
    /// [`CastFunctionInfo::extra_info`]. Freed when DuckDB drops the entry.
    pub(crate) fn set_extra_info<T: Send + Sync + 'static>(
        &self,
        state: T,
    ) {
        let ptr = Box::into_raw(Box::new(state)).cast::<c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<T>`; `drop_boxed::<T>`
        // frees it with the matching type exactly once when DuckDB drops the entry.
        unsafe {
            duckdb_cast_function_set_extra_info(
                self.ptr,
                ptr,
                Some(super::super::callback::drop_boxed::<T>),
            );
        }
    }

    /// Registers this cast with `con`.
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails (e.g. a conflicting source/target pair).
    pub(crate) fn register(
        &self,
        con: duckdb_connection,
    ) -> Result<()> {
        // SAFETY: `con` is a valid connection; `self.ptr` is a fully configured cast.
        let rc = unsafe { duckdb_register_cast_function(con, self.ptr) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some("failed to register cast function (conflicting source/target?)".to_owned()),
            ));
        }
        Ok(())
    }
}

impl Drop for CastFunction {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null cast function allocated by
            // `duckdb_create_cast_function` and not yet destroyed; destroyed once.
            // Registration copies it, so destroying our handle afterward is correct.
            unsafe { duckdb_destroy_cast_function(&mut self.ptr) };
        }
    }
}

/// The callback-time handle (a `duckdb_function_info`): retrieves the shared state
/// and the cast mode, and reports whole-cast / per-row errors.
pub(crate) struct CastFunctionInfo {
    ptr: duckdb_function_info,
}

impl CastFunctionInfo {
    pub(crate) fn from(ptr: duckdb_function_info) -> Self {
        Self { ptr }
    }

    /// Retrieves the state stored via [`CastFunction::set_extra_info`].
    ///
    /// # Safety
    ///
    /// `T` must be the type stored at registration time.
    pub(crate) unsafe fn extra_info<T>(&self) -> &T {
        // SAFETY: `self.ptr` is valid for the callback; the caller guarantees `T`
        // matches the registered type, which outlives every callback.
        let raw = unsafe { duckdb_cast_function_get_extra_info(self.ptr) };
        // SAFETY: `raw` came from `Box::into_raw::<T>` in `set_extra_info`.
        unsafe { &*raw.cast::<T>() }
    }

    /// The active cast mode (`DUCKDB_CAST_NORMAL` or `DUCKDB_CAST_TRY`).
    pub(crate) fn cast_mode(&self) -> duckdb_cast_mode {
        // SAFETY: `self.ptr` is a valid function-info handle for the callback.
        unsafe { duckdb_cast_function_get_cast_mode(self.ptr) }
    }

    /// Reports a whole-cast error (fails the query).
    pub(crate) fn set_error(
        &self,
        error: &str,
    ) {
        if let Ok(c) = CString::new(error) {
            self.set_error_cstr(&c);
        } else {
            self.set_error_cstr(c"cast error (message contained an interior NUL)");
        }
    }

    fn set_error_cstr(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is a valid function-info handle; `error` is a valid,
        // NUL-terminated C string.
        unsafe { duckdb_cast_function_set_error(self.ptr, error.as_ptr()) };
    }

    /// Reports a per-row error, setting `output[row]` to `NULL` (used under TRY_CAST).
    pub(crate) fn set_row_error(
        &self,
        error: &str,
        row: idx_t,
        output: duckdb_vector,
    ) {
        let c = CString::new(error)
            .unwrap_or_else(|_| c"cast error (message contained an interior NUL)".to_owned());
        // SAFETY: `self.ptr` is a valid function-info handle; `c` is a valid,
        // NUL-terminated C string; `output` is the live output vector DuckDB passed to
        // the cast callback and `row` indexes it.
        unsafe { duckdb_cast_function_set_row_error(self.ptr, c.as_ptr(), row, output) };
    }
}
