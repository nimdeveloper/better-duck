//! RAII wrapper for a standalone, owned `duckdb_value`, with introspection.
//!
//! Most values in this driver are read straight out of result vectors into
//! [`DuckValue`], but DuckDB also has a *standalone value* object
//! (`duckdb_value`) — the currency of the scalar-value C API. [`OwnedValue`] owns
//! one (destroying it exactly once on drop) and exposes the introspection the C API
//! offers on any value: whether it is SQL `NULL`, its SQL string rendering, and —
//! for a `STRUCT` value — its child fields by index (each returned child is itself
//! an owned `OwnedValue`, so nothing is leaked or double-freed).
//!
//! Obtain one with [`DuckValue::to_owned_value`](crate::types::value::DuckValue::to_owned_value).
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_void, CStr};

use crate::ffi::{
    duckdb_destroy_value, duckdb_free, duckdb_get_struct_child, duckdb_is_null_value, duckdb_value,
    duckdb_value_to_string, idx_t,
};

/// An owned standalone `duckdb_value` (destroyed once on drop) with introspection.
pub struct OwnedValue {
    value: duckdb_value,
}

impl OwnedValue {
    /// Takes ownership of a raw `duckdb_value`, or `None` if null.
    ///
    /// # Safety
    ///
    /// `value` must be a live `duckdb_value` whose ownership is transferred here
    /// (destroyed on drop).
    pub(crate) unsafe fn from_raw(value: duckdb_value) -> Option<OwnedValue> {
        if value.is_null() {
            return None;
        }
        Some(OwnedValue { value })
    }

    /// Whether the value's type is SQL `NULL`.
    #[must_use]
    pub fn is_null(&self) -> bool {
        // SAFETY: `self.value` is a valid, non-null duckdb_value owned by `self`.
        unsafe { duckdb_is_null_value(self.value) }
    }

    /// The value's SQL string rendering (e.g. `42`, `'text'`), or `None` if DuckDB
    /// produces none.
    #[must_use]
    pub fn to_sql_string(&self) -> Option<String> {
        // SAFETY: `self.value` is valid; `duckdb_value_to_string` returns a heap
        // `char*` (or null) that must be freed with `duckdb_free`.
        let ptr = unsafe { duckdb_value_to_string(self.value) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid, non-null, null-terminated C string.
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
        // SAFETY: `ptr` was allocated by DuckDB and ownership transferred to us.
        unsafe { duckdb_free(ptr as *mut c_void) };
        Some(s)
    }

    /// The child field at `index` of a `STRUCT` value, as an owned [`OwnedValue`],
    /// or `None` if out of range (or the value is not a struct).
    #[must_use]
    pub fn struct_child(
        &self,
        index: u64,
    ) -> Option<OwnedValue> {
        // SAFETY: `self.value` is valid; `duckdb_get_struct_child` returns a newly
        // allocated child value (destroyed by the returned `OwnedValue`) or null.
        unsafe { OwnedValue::from_raw(duckdb_get_struct_child(self.value, index as idx_t)) }
    }
}

impl Drop for OwnedValue {
    fn drop(&mut self) {
        if !self.value.is_null() {
            // SAFETY: `self.value` is a valid, non-null duckdb_value owned by `self`
            // and not yet destroyed; destroyed exactly once here.
            unsafe { duckdb_destroy_value(&mut self.value) };
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::value::DuckValue;
    use std::collections::HashMap;

    #[test]
    fn null_and_scalar_values_report_correctly() {
        assert!(DuckValue::Null.to_owned_value().unwrap().is_null());
        let int = DuckValue::Int(42).to_owned_value().unwrap();
        assert!(!int.is_null());
        assert_eq!(int.to_sql_string().as_deref(), Some("42"));
    }

    #[test]
    fn text_value_renders_as_sql_string() {
        let v = DuckValue::text("hi").to_owned_value().unwrap();
        // DuckDB renders a VARCHAR value quoted.
        assert_eq!(v.to_sql_string().as_deref(), Some("'hi'"));
    }

    #[test]
    fn struct_children_are_owned_and_introspectable() {
        // A single-field struct so child index 0 is deterministic.
        let s = DuckValue::Struct(HashMap::from([("n".to_owned(), DuckValue::Int(7))]));
        let owned = s.to_owned_value().unwrap();
        let child = owned.struct_child(0).expect("struct child 0");
        assert!(!child.is_null());
        assert_eq!(child.to_sql_string().as_deref(), Some("7"));
        // Out-of-range child index yields None.
        assert!(owned.struct_child(5).is_none());
    }
}
