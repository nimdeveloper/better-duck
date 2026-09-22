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
    error::Result,
    ffi::{
        duckdb_create_vector, duckdb_destroy_vector, duckdb_list_vector_get_size,
        duckdb_list_vector_reserve, duckdb_list_vector_set_size, duckdb_validity_set_row_valid,
        duckdb_validity_set_row_validity, duckdb_vector, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_validity, idx_t,
    },
    helpers::duck_result::check_state,
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

    /// The number of elements in a `LIST` vector's child (flattened) buffer.
    ///
    /// Meaningful only for a `LIST` vector.
    #[must_use]
    pub fn list_size(&self) -> u64 {
        // SAFETY: `self.ptr` is a valid vector; the value is meaningful for LIST types.
        unsafe { duckdb_list_vector_get_size(self.ptr) as u64 }
    }

    /// Sets the size of a `LIST` vector's child buffer.
    ///
    /// This does **not** reserve capacity (use [`list_reserve`](OwnedVector::list_reserve)
    /// first); a size may exceed capacity, so callers must reserve before writing.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB rejects the call (e.g. a null/non-list vector).
    pub fn list_set_size(
        &mut self,
        size: u64,
    ) -> Result<()> {
        // SAFETY: `self.ptr` is a valid vector; `size` is copied by value.
        check_state(unsafe { duckdb_list_vector_set_size(self.ptr, size as idx_t) })
    }

    /// Reserves capacity for `required_capacity` elements in a `LIST` vector's child
    /// buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB rejects the call (e.g. a null/non-list vector).
    pub fn list_reserve(
        &mut self,
        required_capacity: u64,
    ) -> Result<()> {
        // SAFETY: `self.ptr` is a valid vector; the capacity is copied by value.
        check_state(unsafe { duckdb_list_vector_reserve(self.ptr, required_capacity as idx_t) })
    }

    /// Sets whether the value at `row` is valid (`true`) or `NULL` (`false`),
    /// ensuring the validity mask is writable first.
    pub fn set_row_validity(
        &mut self,
        row: u64,
        valid: bool,
    ) {
        let validity = self.writable_validity();
        // SAFETY: `validity` is a valid, writable mask (ensured above); `row` indexes it.
        unsafe { duckdb_validity_set_row_validity(validity, row as idx_t, valid) };
    }

    /// Marks the value at `row` valid (the inverse of setting it `NULL`), ensuring
    /// the validity mask is writable first.
    pub fn set_row_valid(
        &mut self,
        row: u64,
    ) {
        let validity = self.writable_validity();
        // SAFETY: `validity` is a valid, writable mask (ensured above); `row` indexes it.
        unsafe { duckdb_validity_set_row_valid(validity, row as idx_t) };
    }

    /// Ensures the validity mask is allocated/writable and returns it.
    fn writable_validity(&mut self) -> *mut u64 {
        // SAFETY: `self.ptr` is valid; this allocates the mask if absent, so the
        // subsequent `get_validity` returns a non-null writable pointer.
        unsafe { duckdb_vector_ensure_validity_writable(self.ptr) };
        // SAFETY: `self.ptr` is valid and its validity mask is now writable.
        unsafe { duckdb_vector_get_validity(self.ptr) }
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
    use crate::ffi::{duckdb_validity_row_is_valid, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER};
    use crate::types::TypeInfo;

    #[test]
    fn creates_and_reports_its_type() {
        let ty = LogicalType::of::<i32>().unwrap();
        let mut vector = OwnedVector::new(&ty, 16).expect("vector allocation");
        assert_eq!(vector.column_type().unwrap().type_id(), DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        // The exclusive raw handle is available for typed views.
        assert!(!vector.raw_mut().is_null());
        // Dropped here: the vector is destroyed exactly once (no leak, no double free).
    }

    #[test]
    fn list_child_size_is_reserved_and_set() {
        // A LIST(INTEGER) vector; manage its flattened child-element count.
        let list_ty = TypeInfo::List(Box::new(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER)))
            .to_logical_type()
            .unwrap();
        let mut v = OwnedVector::new(&list_ty, 8).expect("list vector allocation");
        assert_eq!(v.list_size(), 0);
        v.list_reserve(16).unwrap();
        v.list_set_size(5).unwrap();
        assert_eq!(v.list_size(), 5, "child size reflects the set value");
    }

    #[test]
    fn validity_round_trips_valid_null_valid() {
        let ty = LogicalType::of::<i32>().unwrap();
        let mut v = OwnedVector::new(&ty, 4).expect("vector allocation");
        // Reads `row`'s validity bit from `v`'s (now-writable) mask.
        fn is_valid(
            v: &mut OwnedVector,
            row: u64,
        ) -> bool {
            // SAFETY: `v.raw_mut()` is a valid vector; its validity mask was made
            // writable by the setters before this is called. `duckdb_validity_row_is_valid`
            // treats a null mask as "all valid", so it is safe regardless.
            unsafe { duckdb_validity_row_is_valid(duckdb_vector_get_validity(v.raw_mut()), row) }
        }

        // Mark row 0 NULL, then valid again.
        v.set_row_validity(0, false);
        assert!(!is_valid(&mut v, 0), "row 0 is NULL");
        v.set_row_valid(0);
        assert!(is_valid(&mut v, 0), "row 0 is valid again");
    }
}
