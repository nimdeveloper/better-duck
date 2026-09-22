//! RAII wrapper for a `duckdb_selection_vector` — an array of row indices used to
//! slice a vector or drive a selected copy.
//!
//! A selection vector holds `size` `u32` indices. [`SelectionVector`] owns one
//! (destroying it exactly once on drop) and exposes bounds-checked
//! [`set`](SelectionVector::set)/[`get`](SelectionVector::get) over its index array.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::ffi::{
    duckdb_create_selection_vector, duckdb_destroy_selection_vector, duckdb_selection_vector,
    duckdb_selection_vector_get_data_ptr, idx_t, sel_t,
};

/// An owned `duckdb_selection_vector` of `size` `u32` indices.
pub struct SelectionVector {
    ptr: duckdb_selection_vector,
    size: u64,
}

impl SelectionVector {
    /// Creates a selection vector holding `size` indices (initially unspecified;
    /// fill them with [`set`](SelectionVector::set)).
    ///
    /// # Errors
    ///
    /// Returns `None` if DuckDB fails to allocate.
    #[must_use]
    pub fn new(size: u64) -> Option<SelectionVector> {
        // SAFETY: always valid to call; a null return (allocation failure) → None.
        let ptr = unsafe { duckdb_create_selection_vector(size as idx_t) };
        if ptr.is_null() {
            return None;
        }
        Some(SelectionVector { ptr, size })
    }

    /// The number of indices this selection vector holds.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.size
    }

    /// Whether the selection vector holds no indices.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Sets the index at position `at` to `row`.
    ///
    /// # Panics
    ///
    /// Panics if `at >= len()`.
    pub fn set(
        &mut self,
        at: u64,
        row: u32,
    ) {
        assert!(at < self.size, "selection index {at} out of range (len {})", self.size);
        // SAFETY: `at < size`, so `.add(at)` is in-bounds for the `size`-element array
        // `duckdb_selection_vector_get_data_ptr` returns; the write is aligned for `u32`.
        unsafe { *duckdb_selection_vector_get_data_ptr(self.ptr).add(at as usize) = row as sel_t };
    }

    /// The index at position `at`, or `None` if `at >= len()`.
    #[must_use]
    pub fn get(
        &self,
        at: u64,
    ) -> Option<u32> {
        if at >= self.size {
            return None;
        }
        // SAFETY: `at < size`, so `.add(at)` is in-bounds and aligned for `u32`.
        Some(unsafe { *duckdb_selection_vector_get_data_ptr(self.ptr).add(at as usize) } as u32)
    }

    /// The raw handle, borrowed for the duration of `&self`. The caller must not
    /// destroy it.
    pub(crate) fn raw(&self) -> duckdb_selection_vector {
        self.ptr
    }
}

impl Drop for SelectionVector {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: `self.ptr` is a valid, non-null selection vector created by
            // `duckdb_create_selection_vector` and not yet destroyed; destroyed once.
            unsafe { duckdb_destroy_selection_vector(self.ptr) };
            self.ptr = std::ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indices_round_trip_and_bounds_are_checked() {
        let mut sel = SelectionVector::new(3).expect("selection vector");
        assert_eq!(sel.len(), 3);
        assert!(!sel.is_empty());
        sel.set(0, 2);
        sel.set(1, 0);
        sel.set(2, 1);
        assert_eq!(sel.get(0), Some(2));
        assert_eq!(sel.get(2), Some(1));
        assert_eq!(sel.get(3), None, "out-of-range read is None");
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn set_out_of_range_panics() {
        let mut sel = SelectionVector::new(2).unwrap();
        sel.set(5, 0);
    }
}
