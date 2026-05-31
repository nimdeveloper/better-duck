use super::*;
use crate::ffi::{duckdb_create_blob, duckdb_get_blob};

// Specialized macro for blob types
macro_rules! impl_duck_dialect_blob {
    ($rust_type:ty, $duck_type:expr) => {
        impl DuckDialect for $rust_type {
            fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
                // SAFETY: `value` is a valid duckdb_value of type BLOB.
                // `duckdb_get_blob` reads the blob pointer and size; we copy the bytes
                // immediately into a Vec and do not retain the raw pointer.
                unsafe {
                    let blob = duckdb_get_blob(value);
                    if blob.data.is_null() {
                        return Err(DuckDBConversionError::NullValue);
                    }
                    let slice =
                        std::slice::from_raw_parts(blob.data as *const u8, blob.size as usize);
                    Ok(slice.to_vec())
                }
            }

            fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
                // SAFETY: `self.as_ptr()` is a valid pointer to `self.len()` bytes of
                // initialized data. `duckdb_create_blob` copies the bytes internally.
                Ok(unsafe {
                    duckdb_create_blob(
                        self.as_ptr() as *const u8,
                        self.len() as libduckdb_sys::idx_t,
                    )
                })
            }
        }
    };
}

impl_duck_dialect_blob!(Vec<u8>, DUCKDB_TYPE_DUCKDB_TYPE_BLOB);
