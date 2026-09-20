use super::*;
use crate::{
    ffi::{
        duckdb_create_blob, duckdb_create_logical_type, duckdb_get_blob, duckdb_logical_type,
        DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
    },
    types::appendable::AppendAble,
};

/// A DuckDB `BLOB` value — a raw byte sequence of arbitrary length.
///
/// Use `Blob` anywhere you need to read or write a DuckDB `BLOB` column. The inner
/// `Vec<u8>` is always an owned copy of the bytes stored in DuckDB.
///
/// `DuckValue::Blob` still holds `Vec<u8>` directly; this new type is provided so that
/// a plain `Vec<u8>` is **not** forced to map to `BLOB`, freeing `Vec<T>` for use in
/// a generic `LIST` `From` impl
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
        let rc = unsafe {
            crate::ffi::duckdb_append_blob(
                appender,
                self.0.as_ptr() as *const std::ffi::c_void,
                self.0.len() as u64,
            )
        };
        // SAFETY: `appender` is valid and non-null.
        unsafe { crate::helpers::duck_result::check_append(rc, appender) }
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        // SAFETY: `self.0.as_ptr()` is valid for `self.0.len()` bytes; bind copies the data.
        let rc = unsafe {
            crate::ffi::duckdb_bind_blob(
                stmt,
                idx,
                self.0.as_ptr() as *const std::ffi::c_void,
                self.0.len() as u64,
            )
        };
        crate::helpers::duck_result::check_state(rc)
    }
}

impl DuckLogicalType for Blob {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_BLOB is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BLOB) })
    }
}

impl From<Blob> for value::DuckValue {
    fn from(b: Blob) -> Self {
        value::DuckValue::Blob(b)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use super::*;
    use crate::{
        connection::Connection,
        ffi::{
            duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_type_id,
            DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
        },
        types::value::DuckValue,
    };

    fn assert_ffi_round_trip(expected: Blob) {
        let mut raw = expected.to_duck().unwrap();
        let actual = Blob::from_duck(raw);
        // SAFETY: `raw` was created by `to_duck` above and is destroyed exactly once here.
        unsafe { duckdb_destroy_value(&mut raw) };
        assert_eq!(actual.unwrap(), expected);
    }

    #[test]
    fn constructor_access_and_standard_conversions_preserve_bytes() {
        let bytes = vec![0, 1, 0, 255];
        let blob = Blob::new(bytes.clone());
        assert_eq!(blob.as_bytes(), bytes.as_slice());

        let from_vec = Blob::from(bytes.clone());
        let back_to_vec: Vec<u8> = from_vec.into();
        assert_eq!(back_to_vec, bytes);
    }

    #[test]
    fn ordering_and_hash_follow_byte_contents() {
        let first = Blob::new(vec![0]);
        let second = Blob::new(vec![0, 1]);
        let third = Blob::new(vec![1]);

        let ordered = BTreeSet::from([third.clone(), first.clone(), second.clone()]);
        assert_eq!(ordered.into_iter().collect::<Vec<_>>(), vec![first.clone(), second, third]);

        let mut hashed = HashSet::new();
        assert!(hashed.insert(first.clone()));
        assert!(!hashed.insert(first.clone()));
        assert!(hashed.contains(&first));
    }

    #[test]
    fn converts_into_duck_value_blob_variant() {
        let blob = Blob::new(vec![0x00, 0x7f, 0xff]);
        assert_eq!(DuckValue::from(blob.clone()), DuckValue::Blob(blob));
    }

    #[test]
    fn ffi_round_trips_empty_blob() {
        assert_ffi_round_trip(Blob::new(Vec::new()));
    }

    #[test]
    fn ffi_round_trips_binary_blob_with_nul_bytes() {
        assert_ffi_round_trip(Blob::new(vec![0, 0xff, 0, 0x80, 0x41]));
    }

    #[test]
    fn logical_type_is_blob() {
        let mut logical_type = Blob::duck_logical_type().unwrap();
        // SAFETY: `logical_type` is valid until it is destroyed below.
        let type_id = unsafe { duckdb_get_type_id(logical_type) };
        // SAFETY: `logical_type` was created above and is destroyed exactly once here.
        unsafe { duckdb_destroy_logical_type(&mut logical_type) };
        assert_eq!(type_id, DUCKDB_TYPE_DUCKDB_TYPE_BLOB);
    }

    #[test]
    fn prepared_statement_and_appender_preserve_binary_blobs() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE blobs (value BLOB)").unwrap();

        let mut prepared_blob = Blob::new(vec![0, 1, 0, 255]);
        conn.execute_with("INSERT INTO blobs VALUES ($1)", &mut [&mut prepared_blob]).unwrap();

        {
            let mut appender = conn.appender("blobs", "main").unwrap();
            let mut appender_blob = Blob::new(vec![255, 0, 2, 0]);
            appender.append(&mut appender_blob).unwrap();
            appender.save().unwrap();
        }

        let values = conn
            .execute("SELECT value FROM blobs ORDER BY value")
            .unwrap()
            .map(|row| row.unwrap().get("value").unwrap().clone())
            .collect::<Vec<_>>();

        assert_eq!(
            values,
            vec![
                DuckValue::Blob(Blob::new(vec![0, 1, 0, 255])),
                DuckValue::Blob(Blob::new(vec![255, 0, 2, 0])),
            ]
        );
    }
}
