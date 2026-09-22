//! RAII wrapper for a standalone, owned `duckdb_vector`.
//!
//! DuckDB vectors are usually *borrowed* from a data chunk (see the `udf` module's
//! `VectorRef`/`VectorMut`), but `duckdb_create_vector` also builds a standalone,
//! owned vector of a given logical type and capacity. [`OwnedVector`] owns one and
//! destroys it exactly once on drop. Ownership lives here, outside the UDF-only
//! internals, so later vector-mutation helpers can build typed read/write
//! views on top of a single owner; access is through `&mut self`, so a mutable view
//! is exclusive.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::{
    ffi::{
        duckdb_create_vector, duckdb_destroy_vector, duckdb_vector, duckdb_vector_get_column_type,
        idx_t,
    },
    types::LogicalType,
};

/// An owned standalone `duckdb_vector` (destroyed once on drop).
pub struct OwnedVector {
    ptr: duckdb_vector,
}

impl OwnedVector {
    /// Creates a flat vector of logical type `ty` with room for `capacity` rows.
    ///
    /// # Errors
    ///
    /// Returns `None` if DuckDB fails to allocate the vector.
    #[must_use]
    pub fn new(
        ty: &LogicalType,
        capacity: u64,
    ) -> Option<OwnedVector> {
        // SAFETY: `ty.as_raw()` is a valid logical type owned by `ty` for the call;
        // DuckDB copies what it needs. A null return (allocation failure) → None.
        let ptr = unsafe { duckdb_create_vector(ty.as_raw(), capacity as idx_t) };
        if ptr.is_null() {
            return None;
        }
        Some(OwnedVector { ptr })
    }

    /// The vector's column (logical) type.
    #[must_use]
    pub fn column_type(&self) -> Option<LogicalType> {
        // SAFETY: `self.ptr` is a valid vector; the returned logical type is owned
        // (destroy once) and wrapped in RAII. Null → None.
        LogicalType::from_raw(unsafe { duckdb_vector_get_column_type(self.ptr) }).ok()
    }

    /// The raw handle, borrowed exclusively for `&mut self` (for the typed
    /// read/write views built in later vector tasks). The caller must not destroy it.
    #[allow(dead_code)]
    pub(crate) fn raw_mut(&mut self) -> duckdb_vector {
        self.ptr
    }
}

impl Drop for OwnedVector {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null vector created by
            // `duckdb_create_vector` and not yet destroyed; destroyed exactly once here.
            unsafe { duckdb_destroy_vector(&mut self.ptr) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER;

    #[test]
    fn creates_and_reports_its_type() {
        let ty = LogicalType::of::<i32>().unwrap();
        let mut vector = OwnedVector::new(&ty, 16).expect("vector allocation");
        assert_eq!(vector.column_type().unwrap().type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        // The exclusive raw handle is available for typed views.
        assert!(!vector.raw_mut().is_null());
        // Dropped here: the vector is destroyed exactly once (no leak, no double free).
    }
}
