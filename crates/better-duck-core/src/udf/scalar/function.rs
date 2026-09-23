//! RAII builders around the `duckdb_scalar_function`/`duckdb_scalar_function_set` C API.

use std::ffi::{c_void, CStr};

use crate::{
    error::{Error, Result},
    ffi::{
        duckdb_add_scalar_function_to_set, duckdb_bind_info, duckdb_client_context,
        duckdb_connection, duckdb_create_scalar_function, duckdb_create_scalar_function_set,
        duckdb_destroy_scalar_function, duckdb_destroy_scalar_function_set, duckdb_function_info,
        duckdb_register_scalar_function_set, duckdb_scalar_function,
        duckdb_scalar_function_add_parameter, duckdb_scalar_function_bind_get_argument,
        duckdb_scalar_function_bind_get_argument_count, duckdb_scalar_function_bind_get_extra_info,
        duckdb_scalar_function_bind_set_error, duckdb_scalar_function_bind_t,
        duckdb_scalar_function_get_bind_data, duckdb_scalar_function_get_client_context,
        duckdb_scalar_function_get_extra_info, duckdb_scalar_function_set,
        duckdb_scalar_function_set_bind, duckdb_scalar_function_set_bind_data,
        duckdb_scalar_function_set_error, duckdb_scalar_function_set_extra_info,
        duckdb_scalar_function_set_function, duckdb_scalar_function_set_name,
        duckdb_scalar_function_set_return_type, duckdb_scalar_function_set_special_handling,
        duckdb_scalar_function_set_varargs, duckdb_scalar_function_set_volatile,
        duckdb_scalar_function_t, idx_t,
    },
    raw::{client_context::ClientContext, expression::Expression},
};

use super::super::{
    callback::{drop_boxed, CallbackErrorSink},
    LogicalType,
};

/// An in-progress scalar function overload, built up via its setters and then
/// added to a [`ScalarFunctionSet`].
pub(crate) struct ScalarFunction {
    ptr: duckdb_scalar_function,
}

impl ScalarFunction {
    pub(crate) fn new(name: &CStr) -> Self {
        // SAFETY: always safe to call; returns a freshly allocated, empty function.
        let ptr = unsafe { duckdb_create_scalar_function() };
        // SAFETY: `ptr` was just allocated above and is non-null (DuckDB does not
        // document a null return for this constructor).
        unsafe { duckdb_scalar_function_set_name(ptr, name.as_ptr()) };
        Self { ptr }
    }

    pub(crate) fn add_parameter(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is a valid logical type owned
        // by `ty` for the duration of this call. This function copies it.
        unsafe { duckdb_scalar_function_add_parameter(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_varargs(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is a valid logical type owned
        // by `ty` for the duration of this call. This function copies it.
        unsafe { duckdb_scalar_function_set_varargs(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_return_type(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is a valid logical type owned
        // by `ty` for the duration of this call. This function copies it.
        unsafe { duckdb_scalar_function_set_return_type(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_volatile(&self) {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_scalar_function_set_volatile(self.ptr) };
    }

    pub(crate) fn set_special_handling(&self) {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_scalar_function_set_special_handling(self.ptr) };
    }

    /// Sets the function's execution callback.
    pub(crate) fn set_function(
        &self,
        f: duckdb_scalar_function_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid
        // `extern "C"` function pointer with the expected signature.
        unsafe { duckdb_scalar_function_set_function(self.ptr, f) };
    }

    /// Sets the function's bind callback, invoked once per query that references
    /// the function to inspect argument expressions and produce per-query bind data.
    pub(crate) fn set_bind(
        &self,
        f: duckdb_scalar_function_bind_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid `extern "C"`
        // function pointer with the expected bind signature.
        unsafe { duckdb_scalar_function_set_bind(self.ptr, f) };
    }

    /// Stores `state`, retrievable inside the execution callback via
    /// [`ScalarFunctionInfo::state`]. Freed automatically when DuckDB drops the
    /// catalog entry.
    pub(crate) fn set_extra_info<T: Send + Sync + 'static>(
        &self,
        state: T,
    ) {
        let ptr = Box::into_raw(Box::new(state)).cast::<c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<T>` above;
        // `drop_boxed::<T>` frees it with the matching type, exactly once, when
        // DuckDB calls the delete callback.
        unsafe { duckdb_scalar_function_set_extra_info(self.ptr, ptr, Some(drop_boxed::<T>)) };
    }

    pub(crate) fn as_raw(&self) -> duckdb_scalar_function {
        self.ptr
    }
}

impl Drop for ScalarFunction {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null scalar function allocated by
            // `duckdb_create_scalar_function` and not yet destroyed; destroyed
            // exactly once here. Adding it to a set (`duckdb_add_scalar_function_to_set`)
            // copies it, so destroying our own handle afterward is correct.
            unsafe { duckdb_destroy_scalar_function(&mut self.ptr) };
        }
    }
}

/// A named collection of scalar function overloads, registered together.
pub(crate) struct ScalarFunctionSet {
    ptr: duckdb_scalar_function_set,
}

impl ScalarFunctionSet {
    pub(crate) fn new(name: &CStr) -> Self {
        // SAFETY: `name` is a valid, NUL-terminated C string for the duration of
        // this call.
        let ptr = unsafe { duckdb_create_scalar_function_set(name.as_ptr()) };
        Self { ptr }
    }

    /// Adds `function` as a new overload.
    ///
    /// # Errors
    ///
    /// Returns an error if the overload conflicts with one already in the set.
    pub(crate) fn add_function(
        &self,
        function: &ScalarFunction,
    ) -> Result<()> {
        // SAFETY: `self.ptr` and `function.as_raw()` are both valid; this
        // function copies the scalar function into the set.
        let rc = unsafe { duckdb_add_scalar_function_to_set(self.ptr, function.as_raw()) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(
                    "failed to add overload to scalar function set (conflicting signature?)"
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
        // SAFETY: `con` is a valid connection handle; `self.ptr` is a valid,
        // non-empty function set (the caller adds overloads before calling this).
        let rc = unsafe { duckdb_register_scalar_function_set(con, self.ptr) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(format!(
                    "failed to register scalar function `{name}` (name conflict, or invalid signature)"
                )),
            ));
        }
        Ok(())
    }
}

impl Drop for ScalarFunctionSet {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null function set allocated by
            // `duckdb_create_scalar_function_set` and not yet destroyed; destroyed
            // exactly once here.
            unsafe { duckdb_destroy_scalar_function_set(&mut self.ptr) };
        }
    }
}

/// The execution-time handle passed to [`super::VScalar::invoke`]'s trampoline,
/// used to retrieve `State` and to report an error back to DuckDB.
pub(crate) struct ScalarFunctionInfo {
    ptr: duckdb_function_info,
}

impl ScalarFunctionInfo {
    pub(crate) fn from(ptr: duckdb_function_info) -> Self {
        Self { ptr }
    }

    /// Retrieves the state stored via [`ScalarFunction::set_extra_info`].
    ///
    /// # Safety
    ///
    /// `T` must be the same type that was passed to `set_extra_info` when this
    /// function was registered.
    pub(crate) unsafe fn state<T>(&self) -> &T {
        // SAFETY: `self.ptr` is valid for the duration of the callback; the
        // caller guarantees `T` matches the type stored at registration time,
        // and that stored value outlives every invocation (DuckDB frees it only
        // when the catalog entry itself is dropped).
        let raw = unsafe { duckdb_scalar_function_get_extra_info(self.ptr) };
        // SAFETY: `raw` was produced by `Box::into_raw::<T>` in `set_extra_info`
        // and has not been freed (guaranteed by the caller per the above).
        unsafe { &*raw.cast::<T>() }
    }

    /// Retrieves the per-query bind data stored via
    /// [`ScalarBindInfo::set_bind_data`].
    ///
    /// # Safety
    ///
    /// `T` must be the same type the function's bind callback stored, and the bind
    /// callback must have run for this query (DuckDB guarantees bind precedes
    /// execution).
    pub(crate) unsafe fn bind_data<T>(&self) -> &T {
        // SAFETY: `self.ptr` is valid for the callback; bind ran first and stored a
        // `Box<T>`, which DuckDB keeps alive for every invocation of this query.
        let raw = unsafe { duckdb_scalar_function_get_bind_data(self.ptr) };
        // SAFETY: `raw` came from `Box::into_raw::<T>` in `set_bind_data` and is live.
        unsafe { &*raw.cast::<T>() }
    }
}

/// The bind-time handle passed to [`super::VScalar::bind`]: reads the call's
/// argument expressions and stores per-query bind data.
pub struct ScalarBindInfo {
    ptr: duckdb_bind_info,
}

impl ScalarBindInfo {
    pub(crate) fn from(ptr: duckdb_bind_info) -> Self {
        Self { ptr }
    }

    /// The number of argument expressions passed to this call.
    #[must_use]
    pub fn argument_count(&self) -> u64 {
        // SAFETY: `self.ptr` is a valid bind info for the duration of the callback.
        unsafe { duckdb_scalar_function_bind_get_argument_count(self.ptr) as u64 }
    }

    /// The argument [`Expression`] at `index`, or `None` if out of range.
    #[must_use]
    pub fn argument(
        &self,
        index: u64,
    ) -> Option<Expression> {
        // SAFETY: `self.ptr` is valid; the returned expression is owned (destroy
        // once) and wrapped in RAII. Out-of-range → null → None.
        unsafe {
            Expression::from_raw(duckdb_scalar_function_bind_get_argument(self.ptr, index as idx_t))
        }
    }

    /// The registration-time state stored via `set_extra_info`, readable at bind
    /// time (the same value `ScalarFunctionInfo::state` exposes during execution).
    ///
    /// # Safety
    ///
    /// `T` must match the type stored at registration time (the function's
    /// `VScalar::State`).
    pub unsafe fn extra_info<T>(&self) -> &T {
        // SAFETY: `self.ptr` is valid for the callback; the caller guarantees `T`
        // matches the registered type, which outlives the bind call.
        let raw = unsafe { duckdb_scalar_function_bind_get_extra_info(self.ptr) };
        // SAFETY: `raw` came from `Box::into_raw::<T>` in `set_extra_info`.
        unsafe { &*raw.cast::<T>() }
    }

    /// The client context of the connection binding this call, for folding
    /// constant argument expressions ([`Expression::fold`]). `None` if unavailable.
    #[must_use]
    pub fn client_context(&self) -> Option<ClientContext<'_>> {
        let mut ctx: duckdb_client_context = std::ptr::null_mut();
        // SAFETY: `self.ptr` is valid; `ctx` is a valid out-pointer DuckDB writes an
        // owned client-context handle into.
        unsafe { duckdb_scalar_function_get_client_context(self.ptr, &mut ctx) };
        // SAFETY: `ctx` is null or an owned handle whose owner (this bind call)
        // outlives the returned borrow.
        unsafe { ClientContext::from_raw(ctx) }
    }

    /// Stores per-query bind data, retrievable in the execution callback via
    /// [`ScalarFunctionInfo::bind_data`]. Freed automatically when DuckDB drops the
    /// query's bind data.
    pub(crate) fn set_bind_data<T: Send + Sync + 'static>(
        &self,
        data: T,
    ) {
        let ptr = Box::into_raw(Box::new(data)).cast::<c_void>();
        // SAFETY: `self.ptr` is valid; `ptr` was just created by `Box::into_raw::<T>`,
        // and `drop_boxed::<T>` frees it with the matching type exactly once when
        // DuckDB drops the bind data. Bind data is read-only and shared, so no copy
        // callback (`duckdb_scalar_function_set_bind_data_copy`) is needed.
        unsafe { duckdb_scalar_function_set_bind_data(self.ptr, ptr, Some(drop_boxed::<T>)) };
    }
}

impl CallbackErrorSink for ScalarBindInfo {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is a valid bind-info handle for the duration of the
        // callback; `error` is a valid, NUL-terminated C string.
        unsafe { duckdb_scalar_function_bind_set_error(self.ptr, error.as_ptr()) };
    }
}

impl CallbackErrorSink for ScalarFunctionInfo {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is a valid function-info handle for the duration of
        // the callback; `error` is a valid, NUL-terminated C string.
        unsafe { duckdb_scalar_function_set_error(self.ptr, error.as_ptr()) };
    }
}
