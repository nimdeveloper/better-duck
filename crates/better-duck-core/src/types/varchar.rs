use std::{
    ffi::{CStr, CString},
    os::raw::c_void,
};

use crate::ffi::{duckdb_create_varchar, duckdb_free, duckdb_get_varchar, duckdb_value};

use super::{DuckDBConversionError, DuckDialect};

impl DuckDialect for String {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // if type_ != DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR
        //     && type_ != DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL
        // {
        //     return Err(DuckDBConversionError::TypeMismatch {
        //         expected: DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        //         found: type_,
        //     });
        // }
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
        Ok(unsafe { duckdb_create_varchar(c_str.as_ptr()) })
    }
}
