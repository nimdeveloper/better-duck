//! RAII builders and callback-info wrappers around the `duckdb_table_function`/
//! `duckdb_bind_info`/`duckdb_init_info`/`duckdb_function_info` C API.

use std::{
    ffi::{CStr, CString},
    marker::PhantomData,
};

use crate::{
    error::{Error, Result},
    ffi::{
        duckdb_bind_add_result_column, duckdb_bind_get_parameter, duckdb_bind_get_parameter_count,
        duckdb_bind_info, duckdb_bind_set_bind_data, duckdb_bind_set_cardinality,
        duckdb_bind_set_error, duckdb_connection, duckdb_create_table_function,
        duckdb_destroy_table_function, duckdb_destroy_value, duckdb_function_get_bind_data,
        duckdb_function_get_init_data, duckdb_function_info, duckdb_function_set_error,
        duckdb_init_get_bind_data, duckdb_init_info, duckdb_init_set_error,
        duckdb_init_set_init_data, duckdb_init_set_max_threads, duckdb_register_table_function,
        duckdb_table_function, duckdb_table_function_add_parameter, duckdb_table_function_bind_t,
        duckdb_table_function_init_t, duckdb_table_function_set_bind,
        duckdb_table_function_set_extra_info, duckdb_table_function_set_function,
        duckdb_table_function_set_init, duckdb_table_function_set_name, duckdb_table_function_t,
    },
    types::DuckDialect,
};

use super::super::{
    callback::{drop_boxed, CallbackErrorSink},
    LogicalType,
};
use super::VTab;

/// An in-progress table function, built up via its setters and registered.
pub(crate) struct TableFunction {
    ptr: duckdb_table_function,
}

impl TableFunction {
    pub(crate) fn new(name: &CStr) -> Self {
        // SAFETY: always safe to call; returns a freshly allocated, empty function.
        let ptr = unsafe { duckdb_create_table_function() };
        // SAFETY: `ptr` was just allocated above.
        unsafe { duckdb_table_function_set_name(ptr, name.as_ptr()) };
        Self { ptr }
    }

    pub(crate) fn add_parameter(
        &self,
        ty: &LogicalType,
    ) {
        // SAFETY: `self.ptr` is valid; `ty.as_raw()` is a valid logical type owned
        // by `ty` for the duration of this call, which copies it.
        unsafe { duckdb_table_function_add_parameter(self.ptr, ty.as_raw()) };
    }

    pub(crate) fn set_bind(
        &self,
        f: duckdb_table_function_bind_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid `extern "C"`
        // function pointer with the expected signature.
        unsafe { duckdb_table_function_set_bind(self.ptr, f) };
    }

    pub(crate) fn set_init(
        &self,
        f: duckdb_table_function_init_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid `extern "C"`
        // function pointer with the expected signature.
        unsafe { duckdb_table_function_set_init(self.ptr, f) };
    }

    pub(crate) fn set_function(
        &self,
        f: duckdb_table_function_t,
    ) {
        // SAFETY: `self.ptr` is valid; `f`, if `Some`, is a valid `extern "C"`
        // function pointer with the expected signature.
        unsafe { duckdb_table_function_set_function(self.ptr, f) };
    }

    /// Stores `info`, retrievable via [`BindInfo::extra_info`]/
    /// [`InitInfo::extra_info`]/[`TableFunctionInfo::extra_info`]. Freed
    /// automatically when DuckDB drops the catalog entry. Unused in iteration 1
    /// (no caller sets it yet), kept for the `extra_info` accessors below.
    #[allow(dead_code)]
    pub(crate) fn set_extra_info<T: Send + Sync + 'static>(
        &self,
        info: T,
    ) {
        let ptr = Box::into_raw(Box::new(info)).cast::<std::ffi::c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<T>` above;
        // `drop_boxed::<T>` frees it with the matching type, exactly once, when
        // DuckDB calls the delete callback.
        unsafe { duckdb_table_function_set_extra_info(self.ptr, ptr, Some(drop_boxed::<T>)) };
    }

    /// Registers this function with `con`.
    ///
    /// # Errors
    ///
    /// Returns an error if registration fails, e.g. a name conflict, or if the
    /// function is missing a required piece (bind/init/function callback).
    pub(crate) fn register(
        &self,
        con: duckdb_connection,
        name: &str,
    ) -> Result<()> {
        // SAFETY: `con` is a valid connection handle; `self.ptr` is a valid,
        // fully configured table function.
        let rc = unsafe { duckdb_register_table_function(con, self.ptr) };
        if rc != crate::ffi::DuckDBSuccess {
            return Err(Error::DuckDBFailure(
                crate::ffi::Error::new(rc),
                Some(format!(
                    "failed to register table function `{name}` (name conflict, or invalid signature)"
                )),
            ));
        }
        Ok(())
    }
}

impl Drop for TableFunction {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null table function allocated by
            // `duckdb_create_table_function` and not yet destroyed; destroyed
            // exactly once here. `duckdb_register_table_function` copies it, so
            // destroying our own handle afterward is correct.
            unsafe { duckdb_destroy_table_function(&mut self.ptr) };
        }
    }
}

/// An interface to store and retrieve data during the table function's bind stage.
pub struct BindInfo {
    ptr: duckdb_bind_info,
}

impl BindInfo {
    pub(crate) fn from(ptr: duckdb_bind_info) -> Self {
        Self { ptr }
    }

    /// Adds a result column to the output schema of the table function.
    ///
    /// # Errors
    ///
    /// Returns an error if `column_name` contains a NUL byte.
    pub fn add_result_column(
        &self,
        column_name: &str,
        column_type: &LogicalType,
    ) -> Result<()> {
        let c_name = CString::new(column_name)?;
        // SAFETY: `self.ptr` is valid; `c_name` is a valid, NUL-terminated C
        // string for the duration of this call; `column_type.as_raw()` is valid
        // and copied by this function.
        unsafe { duckdb_bind_add_result_column(self.ptr, c_name.as_ptr(), column_type.as_raw()) };
        Ok(())
    }

    /// The number of positional parameters passed to this call of the function.
    pub fn parameter_count(&self) -> u64 {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_bind_get_parameter_count(self.ptr) }
    }

    /// Reads the positional parameter at `index` as `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if `index` is out of range or the parameter cannot be
    /// converted to `T`.
    pub fn get_parameter<T: DuckDialect>(
        &self,
        index: u64,
    ) -> Result<T> {
        // SAFETY: `self.ptr` is valid; `index` is caller-guaranteed to be within
        // `[0, parameter_count())`. The returned value is owned by us and
        // destroyed below.
        let mut value = unsafe { duckdb_bind_get_parameter(self.ptr, index) };
        if value.is_null() {
            return Err(Error::InvalidColumnIndex(index as usize));
        }
        let result = T::from_duck(value).map_err(Error::ConversionError);
        // SAFETY: `value` was allocated by `duckdb_bind_get_parameter` above and
        // is destroyed exactly once here.
        unsafe { duckdb_destroy_value(&mut value) };
        result
    }

    /// Sets the estimated (or exact) number of rows this call will produce, used
    /// by the query optimizer.
    pub fn set_cardinality(
        &self,
        cardinality: u64,
        is_exact: bool,
    ) {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_bind_set_cardinality(self.ptr, cardinality, is_exact) };
    }

    /// Stores `data`, retrievable during init/execution via
    /// [`TableFunctionInfo::bind_data`] and [`InitInfo::bind_data`].
    pub(crate) fn set_bind_data<T: Send + Sync>(
        &self,
        data: T,
    ) {
        let ptr = Box::into_raw(Box::new(data)).cast::<std::ffi::c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<T>` above;
        // `drop_boxed::<T>` frees it with the matching type, exactly once, when
        // DuckDB destroys the bind data (at the end of the query, or on bind
        // failure).
        unsafe { duckdb_bind_set_bind_data(self.ptr, ptr, Some(drop_boxed::<T>)) };
    }
}

impl CallbackErrorSink for BindInfo {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is valid for the duration of the callback; `error`
        // is a valid, NUL-terminated C string.
        unsafe { duckdb_bind_set_error(self.ptr, error.as_ptr()) };
    }
}

/// An interface to store and retrieve data during the table function's init stage.
pub struct InitInfo<V: VTab> {
    ptr: duckdb_init_info,
    _marker: PhantomData<fn() -> V>,
}

impl<V: VTab> InitInfo<V> {
    pub(crate) fn from(ptr: duckdb_init_info) -> Self {
        Self { ptr, _marker: PhantomData }
    }

    /// The bind data produced by [`VTab::bind`] for this call.
    ///
    /// Read-only: for tracking state across calls to the execution callback,
    /// store it in the init data instead.
    pub fn bind_data(&self) -> &V::BindData {
        // SAFETY: `self.ptr` is valid for the duration of the callback.
        let raw = unsafe { duckdb_init_get_bind_data(self.ptr) };
        // SAFETY: `raw` was produced by `BindInfo::set_bind_data::<V::BindData>`
        // for this same registration (the only place that calls it), and has not
        // been freed (guaranteed by DuckDB: it outlives every init/execute call
        // for this query).
        unsafe { &*raw.cast::<V::BindData>() }
    }

    /// Stores `data`, retrievable during execution via [`TableFunctionInfo::init_data`].
    pub(crate) fn set_init_data(
        &self,
        data: V::InitData,
    ) {
        let ptr = Box::into_raw(Box::new(data)).cast::<std::ffi::c_void>();
        // SAFETY: `ptr` was just created by `Box::into_raw::<V::InitData>`
        // above; `drop_boxed::<V::InitData>` frees it with the matching type,
        // exactly once, when DuckDB destroys the init data.
        unsafe { duckdb_init_set_init_data(self.ptr, ptr, Some(drop_boxed::<V::InitData>)) };
    }

    /// Sets how many threads may call the execution callback for this query in
    /// parallel. Defaults to `1` (single-threaded execution).
    pub fn set_max_threads(
        &self,
        max_threads: u64,
    ) {
        // SAFETY: `self.ptr` is valid.
        unsafe { duckdb_init_set_max_threads(self.ptr, max_threads) };
    }
}

impl<V: VTab> CallbackErrorSink for InitInfo<V> {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is valid for the duration of the callback; `error`
        // is a valid, NUL-terminated C string.
        unsafe { duckdb_init_set_error(self.ptr, error.as_ptr()) };
    }
}

/// An interface to store and retrieve data during the table function's
/// execution stage.
pub struct TableFunctionInfo<V: VTab> {
    ptr: duckdb_function_info,
    _marker: PhantomData<fn() -> V>,
}

impl<V: VTab> TableFunctionInfo<V> {
    pub(crate) fn from(ptr: duckdb_function_info) -> Self {
        Self { ptr, _marker: PhantomData }
    }

    /// The bind data produced by [`VTab::bind`] for this call. Read-only.
    pub fn bind_data(&self) -> &V::BindData {
        // SAFETY: `self.ptr` is valid for the duration of the callback.
        let raw = unsafe { duckdb_function_get_bind_data(self.ptr) };
        // SAFETY: `raw` was produced by `BindInfo::set_bind_data::<V::BindData>`
        // for this same registration, and has not been freed (guaranteed by
        // DuckDB: it outlives every execute call for this query).
        unsafe { &*raw.cast::<V::BindData>() }
    }

    /// The init data produced by [`VTab::init`] for this call. Shared across
    /// every worker thread executing this query, so any interior mutation must
    /// be synchronized.
    pub fn init_data(&self) -> &V::InitData {
        // SAFETY: `self.ptr` is valid for the duration of the callback.
        let raw = unsafe { duckdb_function_get_init_data(self.ptr) };
        // SAFETY: `raw` was produced by `InitInfo::set_init_data::<V::InitData>`
        // for this same registration, and has not been freed (guaranteed by
        // DuckDB: it outlives every execute call for this query).
        unsafe { &*raw.cast::<V::InitData>() }
    }
}

impl<V: VTab> CallbackErrorSink for TableFunctionInfo<V> {
    fn set_c_error(
        &self,
        error: &CStr,
    ) {
        // SAFETY: `self.ptr` is valid for the duration of the callback; `error`
        // is a valid, NUL-terminated C string.
        unsafe { duckdb_function_set_error(self.ptr, error.as_ptr()) };
    }
}
