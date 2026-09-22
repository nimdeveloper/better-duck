//! RAII builder around the `duckdb_aggregate_function` C API.

use std::ffi::{c_void, CStr};

use crate::{
    error::{Error, Result},
    ffi::{
        duckdb_add_aggregate_function_to_set, duckdb_aggregate_combine_t,
        duckdb_aggregate_finalize_t, duckdb_aggregate_function,
        duckdb_aggregate_function_add_parameter, duckdb_aggregate_function_get_extra_info,
        duckdb_aggregate_function_set, duckdb_aggregate_function_set_error,
        duckdb_aggregate_function_set_extra_info, duckdb_aggregate_function_set_functions,
        duckdb_aggregate_function_set_name, duckdb_aggregate_function_set_return_type,
        duckdb_aggregate_function_set_special_handling, duckdb_aggregate_init_t,
        duckdb_aggregate_state_size, duckdb_aggregate_update_t, duckdb_connection,
        duckdb_create_aggregate_function, duckdb_create_aggregate_function_set,
        duckdb_destroy_aggregate_function, duckdb_destroy_aggregate_function_set,
        duckdb_function_info, duckdb_register_aggregate_function,
        duckdb_register_aggregate_function_set,
    },
};

use super::super::{callback::drop_boxed, callback::CallbackErrorSink, LogicalType};

/// An in-progress aggregate function, built up via its setters and registered.
pub(crate) struct AggregateFunction {
    ptr: duckdb_aggregate_function,
}

impl AggregateFunction {
    pub(crate) fn new(name: &CStr) -> Self {
        // SAFETY: always safe to call; returns a freshly allocated, empty function.
        let ptr = unsafe { duckdb_create_aggregate_function() };
        // SAFETY: `ptr` was just allocated above; `name` is a valid NUL-terminated string.
        unsafe { duckdb_aggregate_function_set_name(ptr, name.as_ptr()) };
        Self { ptr }
    }

    pub(crate) fn add_parameter(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is valid for the call and copied.
        unsafe { duckdb_aggregate_function_add_parameter(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_return_type(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is valid for the call and copied.
        unsafe { duckdb_aggregate_function_set_return_type(self.ptr, ty.as_raw()) };
    }

    /// Installs the five aggregate callbacks (state size / init / update / combine /
    /// finalize).
    pub(crate) fn set_functions(
        &self,
        state_size: duckdb_aggregate_state_size,
        init: duckdb_aggregate_init_t,
        update: duckdb_aggregate_update_t,
        combine: duckdb_aggregate_combine_t,
        finalize: duckdb_aggregate_finalize_t,
    ) {
        // SAFETY: `self.ptr` is valid; each callback (if `Some`) is a valid `extern "C"`
        // function pointer with the expected signature.
        unsafe {
            duckdb_aggregate_function_set_functions(
                self.ptr, state_size, init, update, combine, finalize,
            );
        }
    }

    /// Installs the optional state destructor.
    pub(crate) fn set_destructor(
        &self,
        destroy: crate::ffi::duckdb_aggregate_destroy_t,
    ) {
        // SAFETY: `self.ptr` is valid; `destroy`, if `Some`, is a valid `extern "C"`
        // destructor with the expected signature.
        unsafe { crate::ffi::duckdb_aggregate_function_set_destructor(self.ptr, destroy) };
    }

    pub(crate) fn set_special_handling(&self) {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_aggregate_function_set_special_handling(self.ptr) };
    }

    /// Stores `state`, retrievable in the callbacks via
    /// [`AggregateFunctionInfo::extra_info`]. Freed when DuckDB drops the entry.
    pub(crate) fn set_extra_info<T: Send + Sync + 'static>(
        &self,
        state: T,
    ) {
        let ptr = Box::into_raw(Box::new(state)).cast::<c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<T>`; `drop_boxed::<T>` frees
        // it with the matching type exactly once when DuckDB drops the entry.
        unsafe { duckdb_aggregate_function_set_extra_info(self.ptr, ptr, Some(drop_boxed::<T>)) };
    }

    /// Registers this function with `con`.
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails (e.g. a name conflict or missing
    /// callback).
    pub(crate) fn register(
        &self,
        con: duckdb_connection,
        name: &str,
    ) -> Result<()> {
        // SAFETY: `con` is a valid connection; `self.ptr` is a fully configured function.
        let rc = unsafe { duckdb_register_aggregate_function(con, self.ptr) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(format!(
                    "failed to register aggregate function `{name}` (name conflict, or invalid signature)"
                )),
            ));
        }
        Ok(())
    }

    /// The raw handle (borrowed) for adding this overload to a set.
    pub(crate) fn as_raw(&self) -> duckdb_aggregate_function {
        self.ptr
    }
}

impl Drop for AggregateFunction {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null aggregate function allocated by
            // `duckdb_create_aggregate_function` and not yet destroyed; destroyed once.
            // Registration copies it, so destroying our handle afterward is correct.
            unsafe { duckdb_destroy_aggregate_function(&mut self.ptr) };
        }
    }
}

/// A named collection of aggregate-function overloads, registered together.
pub(crate) struct AggregateFunctionSet {
    ptr: duckdb_aggregate_function_set,
}

impl AggregateFunctionSet {
    pub(crate) fn new(name: &CStr) -> Self {
        // SAFETY: `name` is a valid NUL-terminated C string for the duration of the call.
        let ptr = unsafe { duckdb_create_aggregate_function_set(name.as_ptr()) };
        Self { ptr }
    }

    /// Adds `function` as a new overload.
    ///
    /// # Errors
    ///
    /// Returns an error if the overload conflicts with one already in the set.
    pub(crate) fn add_function(
        &self,
        function: &AggregateFunction,
    ) -> Result<()> {
        // SAFETY: `self.ptr` and `function.as_raw()` are both valid; this copies the
        // function into the set.
        let rc = unsafe { duckdb_add_aggregate_function_to_set(self.ptr, function.as_raw()) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(
                    "failed to add overload to aggregate function set (conflicting signature?)"
                        .to_owned(),
                ),
            ));
        }
        Ok(())
    }

    /// Registers this set with `con`.
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails, e.g. a name conflict.
    pub(crate) fn register(
        &self,
        con: duckdb_connection,
        name: &str,
    ) -> Result<()> {
        // SAFETY: `con` is a valid connection; `self.ptr` is a non-empty set (the caller
        // adds overloads before calling this).
        let rc = unsafe { duckdb_register_aggregate_function_set(con, self.ptr) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(format!(
                    "failed to register aggregate function set `{name}` (name conflict?)"
                )),
            ));
        }
        Ok(())
    }
}

impl Drop for AggregateFunctionSet {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null set allocated by
            // `duckdb_create_aggregate_function_set` and not yet destroyed; destroyed
            // once. Registration copies it, so destroying our handle afterward is correct.
            unsafe { duckdb_destroy_aggregate_function_set(&mut self.ptr) };
        }
    }
}

/// The callback-time handle (a `duckdb_function_info`), used to retrieve the
/// registration state and to report an error back to DuckDB.
pub(crate) struct AggregateFunctionInfo {
    ptr: duckdb_function_info,
}

impl AggregateFunctionInfo {
    pub(crate) fn from(ptr: duckdb_function_info) -> Self {
        Self { ptr }
    }

    /// Retrieves the state stored via [`AggregateFunction::set_extra_info`].
    ///
    /// # Safety
    ///
    /// `T` must be the type stored at registration time.
    pub(crate) unsafe fn extra_info<T>(&self) -> &T {
        // SAFETY: `self.ptr` is valid for the callback; the caller guarantees `T`
        // matches the registered type, which outlives every callback.
        let raw = unsafe { duckdb_aggregate_function_get_extra_info(self.ptr) };
        // SAFETY: `raw` came from `Box::into_raw::<T>` in `set_extra_info`.
        unsafe { &*raw.cast::<T>() }
    }
}

impl CallbackErrorSink for AggregateFunctionInfo {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is a valid function-info handle for the callback; `error`
        // is a valid, NUL-terminated C string.
        unsafe { duckdb_aggregate_function_set_error(self.ptr, error.as_ptr()) };
    }
}
