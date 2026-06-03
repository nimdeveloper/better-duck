use super::*;
use crate::{
    ffi::{duckdb_create_blob, duckdb_get_blob},
    types::appendable::AppendAble,
};

/// A DuckDB `BLOB` value — a raw byte sequence of arbitrary length.
///
/// Use `Blob` anywhere you need to read or write a DuckDB `BLOB` column. The inner
/// `Vec<u8>` is always an owned copy of the bytes stored in DuckDB.
///
/// `DuckValue::Blob` still holds `Vec<u8>` directly; this newtype is provided so that
/// a plain `Vec<u8>` is **not** forced to map to `BLOB`, freeing `Vec<T>` for use in
/// a generic `LIST` `From` impl (Phase F).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Blob(pub Vec<u8>);

impl Blob {
    /// Creates a new `Blob` from a byte vector.
    #[inline]
    pub fn new(bytes: Vec<u8>) -> Self {
        Blob(bytes)
    }

    /// Returns a slice of the blob's bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Self {
        Blob(v)
    }
}

impl From<Blob> for Vec<u8> {
    fn from(b: Blob) -> Self {
        b.0
    }
}

impl DuckDialect for Blob {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type BLOB.
        // `duckdb_get_blob` returns a pointer + size; we copy the bytes immediately
        // and do not retain the raw pointer after this block.
        let blob = unsafe { duckdb_get_blob(value) };
        if blob.data.is_null() {
            return Err(DuckDBConversionError::NullValue);
        }
        // SAFETY: `blob.data` is a valid pointer to `blob.size` bytes for the duration
        // of this call; we copy immediately.
        let slice =
            unsafe { std::slice::from_raw_parts(blob.data as *const u8, blob.size as usize) };
        Ok(Blob(slice.to_vec()))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // SAFETY: `self.0.as_ptr()` is valid for `self.0.len()` bytes.
        // `duckdb_create_blob` copies the bytes internally.
        Ok(unsafe { duckdb_create_blob(self.0.as_ptr(), self.0.len() as crate::ffi::idx_t) })
    }
}

impl AppendAble for Blob {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        // SAFETY: `self.0.as_ptr()` is valid for `self.0.len()` bytes; append copies the data.
        unsafe {
            crate::ffi::duckdb_append_blob(
                appender,
                self.0.as_ptr() as *const std::ffi::c_void,
                self.0.len() as u64,
            )
        };
        Ok(())
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        // SAFETY: `self.0.as_ptr()` is valid for `self.0.len()` bytes; bind copies the data.
        unsafe {
            crate::ffi::duckdb_bind_blob(
                stmt,
                idx,
                self.0.as_ptr() as *const std::ffi::c_void,
                self.0.len() as u64,
            )
        };
        Ok(())
    }
}
