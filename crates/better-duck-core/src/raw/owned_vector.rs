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
    error::{EngineError, Error, Result},
    ffi::{
        duckdb_create_vector, duckdb_destroy_vector, duckdb_list_vector_get_size,
        duckdb_list_vector_reserve, duckdb_list_vector_set_size, duckdb_slice_vector,
        duckdb_validity_set_row_valid, duckdb_validity_set_row_validity, duckdb_vector,
        duckdb_vector_copy_sel, duckdb_vector_ensure_validity_writable,
        duckdb_vector_get_column_type, duckdb_vector_get_validity, idx_t,
    },
    helpers::duck_result::check_state,
    raw::selection_vector::SelectionVector,
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

    /// Destructively re-orders this vector in place so its first `len` logical rows
    /// are `sel`'s selected rows (`duckdb_slice_vector`).
    ///
    /// # Errors
    ///
    /// Returns an error if `len` exceeds `sel`'s length.
    pub fn slice(
        &mut self,
        sel: &SelectionVector,
        len: u64,
    ) -> Result<()> {
        if len > sel.len() {
            return Err(Error::Engine(EngineError::unavailable(Some(format!(
                "slice len {len} exceeds selection length {}",
                sel.len()
            )))));
        }
        // SAFETY: `self.ptr` is a valid vector; `sel.raw()` is a valid selection of at
        // least `len` entries (checked above); `duckdb_slice_vector` remaps in place.
        unsafe { duckdb_slice_vector(self.ptr, sel.raw(), len as idx_t) };
        Ok(())
    }

    /// Copies `src_count` rows from `self` (starting at `src_offset`) into `dst`
    /// (starting at `dst_offset`), picking rows through `sel`
    /// (`duckdb_vector_copy_sel`).
    ///
    /// # Errors
    ///
    /// Returns an error if `sel` holds fewer than `src_count` indices.
    pub fn copy_sel_into(
        &self,
        dst: &mut OwnedVector,
        sel: &SelectionVector,
        src_count: u64,
        src_offset: u64,
        dst_offset: u64,
    ) -> Result<()> {
        if sel.len() < src_count {
            return Err(Error::Engine(EngineError::unavailable(Some(format!(
                "selection length {} is smaller than src_count {src_count}",
                sel.len()
            )))));
        }
        // SAFETY: `self.ptr`/`dst.ptr` are valid vectors of the same type; `sel` has at
        // least `src_count` indices (checked above); the offsets/count are copied by
        // value and DuckDB bounds them against the vectors' own capacities.
        unsafe {
            duckdb_vector_copy_sel(
                self.ptr,
                dst.ptr,
                sel.raw(),
                src_count as idx_t,
                src_offset as idx_t,
                dst_offset as idx_t,
            );
        }
        Ok(())
    }

    /// Makes this vector a constant vector that references `value`
    /// (`duckdb_vector_reference_value`): every logical row reads as `value`,
    /// sharing its storage rather than copying it.
    ///
    /// # Safety
    ///
    /// This creates a zero-copy alias: `value` must outlive every use of this vector,
    /// and `value` must not be mutated or destroyed while the vector still references
    /// it. The wrapper cannot encode that lifetime for two independently-owned
    /// handles, so the invariant is the caller's to uphold.
    pub unsafe fn reference_value(
        &mut self,
        value: &crate::raw::owned_value::OwnedValue,
    ) {
        // SAFETY: `self.ptr` is a valid vector; `value.raw()` is a valid duckdb_value.
        // The caller upholds the outlives/no-mutation contract documented above.
        unsafe { crate::ffi::duckdb_vector_reference_value(self.ptr, value.raw()) };
    }

    /// Makes this vector a zero-copy, read-only alias of `from`'s storage
    /// (`duckdb_vector_reference_vector`).
    ///
    /// # Safety
    ///
    /// `from` must outlive every use of this vector, and neither vector's shared
    /// storage may be mutated while the alias is live (no overlapping mutable views).
    /// The wrapper cannot encode that across two independently-owned vectors, so the
    /// invariant is the caller's to uphold.
    pub unsafe fn reference_vector(
        &mut self,
        from: &OwnedVector,
    ) {
        // SAFETY: both handles are valid vectors of the same type; the caller upholds
        // the outlives / no-aliased-mutation contract documented above.
        unsafe { crate::ffi::duckdb_vector_reference_vector(self.ptr, from.ptr) };
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
    use crate::ffi::{
        duckdb_validity_row_is_valid, duckdb_vector_get_data, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    };
    use crate::types::TypeInfo;

    /// Writes `values` into an `INTEGER` vector's flat data buffer.
    fn write_i32s(
        v: &mut OwnedVector,
        values: &[i32],
    ) {
        // SAFETY: `v` is a valid INTEGER vector with capacity >= values.len(); its data
        // buffer holds `i32` inline, so writing `values.len()` in-bounds `i32`s is sound.
        unsafe {
            let data = duckdb_vector_get_data(v.raw_mut()) as *mut i32;
            for (i, &val) in values.iter().enumerate() {
                *data.add(i) = val;
            }
        }
    }

    /// Reads `n` `i32`s out of a vector's flat data buffer.
    fn read_i32s(
        v: &mut OwnedVector,
        n: usize,
    ) -> Vec<i32> {
        // SAFETY: `v` is a valid INTEGER vector with at least `n` rows written.
        unsafe {
            let data = duckdb_vector_get_data(v.raw_mut()) as *const i32;
            (0..n).map(|i| *data.add(i)).collect()
        }
    }

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

    #[test]
    fn copy_sel_picks_selected_rows_into_the_destination() {
        use crate::raw::selection_vector::SelectionVector;
        let ty = LogicalType::of::<i32>().unwrap();
        let mut src = OwnedVector::new(&ty, 4).unwrap();
        write_i32s(&mut src, &[10, 20, 30, 40]);
        let mut dst = OwnedVector::new(&ty, 4).unwrap();

        // Pick rows 3 and 1 of src into dst[0], dst[1].
        let mut sel = SelectionVector::new(2).unwrap();
        sel.set(0, 3);
        sel.set(1, 1);
        src.copy_sel_into(&mut dst, &sel, 2, 0, 0).unwrap();
        assert_eq!(read_i32s(&mut dst, 2), vec![40, 20]);

        // A selection shorter than src_count is rejected before the FFI call.
        assert!(src.copy_sel_into(&mut dst, &sel, 3, 0, 0).is_err());
    }

    #[test]
    fn slice_reorders_the_vector_in_place() {
        use crate::raw::selection_vector::SelectionVector;
        let ty = LogicalType::of::<i32>().unwrap();
        let mut v = OwnedVector::new(&ty, 4).unwrap();
        write_i32s(&mut v, &[10, 20, 30, 40]);

        // Reverse the four rows: v[i] becomes original[3 - i].
        let mut rev = SelectionVector::new(4).unwrap();
        for i in 0..4u64 {
            rev.set(i, (3 - i) as u32);
        }
        v.slice(&rev, 4).unwrap();

        // The sliced (dictionary) vector's logical values, copied out flat, are reversed.
        let mut flat = OwnedVector::new(&ty, 4).unwrap();
        let mut identity = SelectionVector::new(4).unwrap();
        for i in 0..4u64 {
            identity.set(i, i as u32);
        }
        v.copy_sel_into(&mut flat, &identity, 4, 0, 0).unwrap();
        assert_eq!(read_i32s(&mut flat, 4), vec![40, 30, 20, 10]);

        // `len` beyond the selection is rejected.
        assert!(v.slice(&rev, 5).is_err());
    }

    #[test]
    fn reference_value_makes_a_constant_vector() {
        use crate::types::value::DuckValue;
        let ty = LogicalType::of::<i32>().unwrap();
        let mut v = OwnedVector::new(&ty, 4).unwrap();
        // `value` outlives every use of `v` below (dropped at end of scope, after v).
        let value = DuckValue::Int(99).to_owned_value().unwrap();
        // SAFETY: `value` lives for the rest of this scope, longer than every read of
        // `v`, and is neither mutated nor destroyed while `v` references it.
        unsafe { v.reference_value(&value) };
        // A constant vector reads the referenced value at row 0.
        assert_eq!(read_i32s(&mut v, 1), vec![99]);
    }

    #[test]
    fn reference_vector_aliases_source_storage() {
        let ty = LogicalType::of::<i32>().unwrap();
        let mut src = OwnedVector::new(&ty, 4).unwrap();
        write_i32s(&mut src, &[7, 8]);
        let mut view = OwnedVector::new(&ty, 4).unwrap();
        // SAFETY: `src` outlives `view` (dropped after it) and its storage is not
        // mutated while `view` aliases it.
        unsafe { view.reference_vector(&src) };
        assert_eq!(read_i32s(&mut view, 2), vec![7, 8]);
    }
}
