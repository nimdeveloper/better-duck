use std::{
    ffi::{CStr, CString},
    os::raw::{c_char, c_void},
};

use crate::{
    ffi::{
        duckdb_create_logical_type, duckdb_create_varchar, duckdb_free, duckdb_get_varchar,
        duckdb_logical_type, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    },
    types::appendable::AppendAble,
};

use super::{value::DuckValue, DuckDBConversionError, DuckDialect, DuckLogicalType};

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

impl DuckLogicalType for String {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR) })
    }
}

impl From<String> for DuckValue {
    fn from(v: String) -> Self {
        DuckValue::Text(v)
    }
}

impl From<&str> for DuckValue {
    fn from(v: &str) -> Self {
        DuckValue::Text(v.to_owned())
    }
}

impl DuckLogicalType for &str {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        String::duck_logical_type()
    }
}

#[cfg(test)]
#[allow(clippy::undocumented_unsafe_blocks)]
mod tests {
    use super::*;
    use crate::ffi::{
        duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_type_id,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    };

    #[test]
    fn string_roundtrip_preserves_empty_unicode_and_multibyte_text() {
        for text in ["", "hello", "Καλημέρα", "duck 🦆"] {
            let value = text.to_owned();
            let mut duck_value = value.to_duck().unwrap();
            assert_eq!(String::from_duck(duck_value).unwrap(), value);
            unsafe { duckdb_destroy_value(&mut duck_value) };
        }
    }

    #[test]
    fn interior_nul_is_a_conversion_error() {
        let err = "before\0after".to_owned().to_duck().unwrap_err();
        assert!(
            matches!(err, DuckDBConversionError::ConversionError(message) if message.contains("nul byte"))
        );
    }

    #[test]
    fn borrowed_and_owned_strings_report_varchar_logical_type() {
        for mut logical_type in
            [String::duck_logical_type().unwrap(), <&str>::duck_logical_type().unwrap()]
        {
            assert_eq!(
                unsafe { duckdb_get_type_id(logical_type) },
                DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
            );
            unsafe { duckdb_destroy_logical_type(&mut logical_type) };
        }
    }

    #[test]
    fn string_conversions_create_text_values() {
        assert_eq!(DuckValue::from("borrowed"), DuckValue::Text("borrowed".to_owned()));
        assert_eq!(DuckValue::from("owned".to_owned()), DuckValue::Text("owned".to_owned()));
    }
}
