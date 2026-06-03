use std::{
    ffi::{CStr, CString},
    os::raw::{c_char, c_void},
};

use crate::{
    ffi::{duckdb_create_varchar, duckdb_free, duckdb_get_varchar, duckdb_value},
    types::appendable::AppendAble,
};

use super::{DuckDBConversionError, DuckDialect};

impl DuckDialect for String {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of VARCHAR type. `duckdb_get_varchar`
        // returns a heap-allocated null-terminated C string that must be freed with
        // `duckdb_free`. We copy the bytes before freeing.
        unsafe {
            let c_str = duckdb_get_varchar(value);
            if c_str.is_null() {
                return Err(DuckDBConversionError::NullValue);
            }
            let result = CStr::from_ptr(c_str)
                .to_str()
                .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?
                .to_string();
            duckdb_free(c_str as *mut c_void);
            Ok(result)
        }
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let c_str = CString::new(self.as_str())
            .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
        // SAFETY: `c_str` is a valid null-terminated C string. `duckdb_create_varchar`
        // copies the string contents internally.
        Ok(unsafe { duckdb_create_varchar(c_str.as_ptr()) })
    }
}

impl AppendAble for String {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        let bytes = self.as_bytes();
        // SAFETY: `bytes.as_ptr()` is valid UTF-8 data of `bytes.len()` bytes.
        // `duckdb_append_varchar_length` copies the data and does not retain the pointer.
        unsafe {
            crate::ffi::duckdb_append_varchar_length(
                appender,
                bytes.as_ptr() as *const c_char,
                bytes.len() as u64,
            )
        };
        Ok(())
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        let bytes = self.as_bytes();
        // SAFETY: `bytes.as_ptr()` is valid UTF-8 of `bytes.len()` bytes.
        // `duckdb_bind_varchar_length` copies the data and does not retain the pointer.
        unsafe {
            crate::ffi::duckdb_bind_varchar_length(
                stmt,
                idx,
                bytes.as_ptr() as *const c_char,
                bytes.len() as u64,
            )
        };
        Ok(())
    }
}
