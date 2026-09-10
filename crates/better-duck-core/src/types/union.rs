//! UNION read/write helpers.
//!
//! DuckDB `UNION` is physically a `STRUCT` where child 0 is the tag discriminant
//! and children `1..=N` are the member values.  Only the active member's value is
//! read; the rest are skipped.
// FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::{
    error::DuckDBConversionError,
    ffi::{
        duckdb_create_union_type, duckdb_create_union_value, duckdb_destroy_logical_type,
        duckdb_destroy_value, duckdb_logical_type, duckdb_struct_vector_get_child,
        duckdb_union_type_member_count, duckdb_value, duckdb_vector, duckdb_vector_get_column_type,
        duckdb_vector_get_data, idx_t,
    },
};

use super::value::DuckValue;

// Read path

/// Read a DuckDB `UNION` vector column at `row_idx`.
///
/// Returns `DuckValue::Union(Box::new(active_member_value))`.
///
/// # Safety
/// `val` must be a valid `duckdb_vector` of UNION type; `row_idx` must be within
/// `[0, chunk_size)`.
pub(crate) fn read_union(
    val: duckdb_vector,
    row_idx: u64,
) -> Result<DuckValue, DuckDBConversionError> {
    // SAFETY: `val` is a valid union vector.  The logical type is heap-allocated
    // and must be destroyed exactly once.
    let mut lt = unsafe { duckdb_vector_get_column_type(val) };
    // SAFETY: `lt` is a valid logical type of UNION kind.
    let member_count: idx_t = unsafe { duckdb_union_type_member_count(lt) };

    // Child 0 of the underlying struct layout is the tag vector.
    // SAFETY: `val` is a valid union vector; child 0 is the tag.
    let tag_vec = unsafe { duckdb_struct_vector_get_child(val, 0) };

    // DuckDB uses UTINYINT (u8) for ≤ 255 members, USMALLINT (u16) otherwise.
    // SAFETY: `tag_vec` is a valid data vector; `row_idx` is within [0, chunk_size).
    let tag: idx_t = unsafe {
        let data = duckdb_vector_get_data(tag_vec);
        if member_count <= u8::MAX as idx_t {
            *(data as *const u8).add(row_idx as usize) as idx_t
        } else {
            *(data as *const u16).add(row_idx as usize) as idx_t
        }
    };

    if tag >= member_count {
        // SAFETY: `lt` must be destroyed even on this error path.
        unsafe { duckdb_destroy_logical_type(&mut lt) };
        return Err(DuckDBConversionError::ConversionError(format!(
            "union tag {tag} out of range (member count {member_count})"
        )));
    }

    // The active member sits at child index (tag + 1) in the underlying struct.
    // SAFETY: `val` is a valid union vector; `tag + 1` is within [1, member_count + 1).
    let member_vec = unsafe { duckdb_struct_vector_get_child(val, tag + 1) };
    // SAFETY: `member_vec` is a valid vector for the active member.
    let mut member_lt = unsafe { duckdb_vector_get_column_type(member_vec) };
    // SAFETY: `member_lt` is a valid logical type.
    let member_tid = unsafe { crate::ffi::duckdb_get_type_id(member_lt) };
    // SAFETY: `member_lt` was returned by `duckdb_vector_get_column_type`.
    unsafe { duckdb_destroy_logical_type(&mut member_lt) };

    // Recurse into the active member.
    let inner = DuckValue::from_duckdb_vec(member_vec, member_tid, row_idx);

    // SAFETY: `lt` was returned by `duckdb_vector_get_column_type` and must be destroyed once.
    unsafe { duckdb_destroy_logical_type(&mut lt) };

    inner.map(|v| DuckValue::Union(Box::new(v)))
}

// Write path

/// Build a `duckdb_value` of type `UNION` wrapping the single `inner` member.
///
/// Creates a single-member UNION type with member name `"value"` and tag index 0.
/// The caller is responsible for destroying the returned value.
pub(crate) fn union_to_duck(inner: &DuckValue) -> Result<duckdb_value, DuckDBConversionError> {
    let mut member_lt = DuckValue::logical_type_of(inner)?;
    let c_name = std::ffi::CString::new("value").unwrap();
    let mut name_ptr: *const std::os::raw::c_char = c_name.as_ptr();
    // SAFETY: single-element arrays of valid pointers; create copies both.
    let mut union_lt = unsafe { duckdb_create_union_type(&mut member_lt, &mut name_ptr, 1) };
    // SAFETY: `member_lt` was allocated by `logical_type_of` above.
    unsafe { duckdb_destroy_logical_type(&mut member_lt) };
    let mut member_dv = match inner.to_duck() {
        Ok(v) => v,
        Err(e) => {
            // SAFETY: `union_lt` was allocated above; destroy once.
            unsafe { duckdb_destroy_logical_type(&mut union_lt) };
            return Err(e);
        },
    };
    // SAFETY: `union_lt` valid; tag_index=0 (single-member union); `member_dv` valid.
    let result = unsafe { duckdb_create_union_value(union_lt, 0, member_dv) };
    // SAFETY: `union_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut union_lt) };
    // SAFETY: `member_dv` was created by `to_duck()` above; destroy once.
    unsafe { duckdb_destroy_value(&mut member_dv) };
    Ok(result)
}

// Logical-type path

/// Return a `duckdb_logical_type` for a single-member UNION wrapping `inner`.
pub(crate) fn union_logical_type(
    inner: &DuckValue
) -> Result<duckdb_logical_type, DuckDBConversionError> {
    let mut member_lt = DuckValue::logical_type_of(inner)?;
    let c_name = std::ffi::CString::new("value").unwrap();
    let mut name_ptr: *const std::os::raw::c_char = c_name.as_ptr();
    // SAFETY: single-element arrays of valid pointers; create copies both.
    let lt = unsafe { duckdb_create_union_type(&mut member_lt, &mut name_ptr, 1) };
    // SAFETY: `member_lt` was allocated by `logical_type_of` above.
    unsafe { duckdb_destroy_logical_type(&mut member_lt) };
    Ok(lt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection::Connection,
        ffi::{duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_get_type_id},
    };

    #[test]
    fn union_value_and_logical_type_are_created() {
        let value = DuckValue::Union(Box::new(DuckValue::Int(42)));
        let mut raw = value.to_duck().unwrap();
        assert!(!raw.is_null());
        // SAFETY: `raw` was created by `DuckValue::to_duck` and is destroyed once.
        unsafe { duckdb_destroy_value(&mut raw) };

        let mut logical_type = DuckValue::logical_type_of(&value).unwrap();
        assert_eq!(
            // SAFETY: `logical_type` is valid until it is destroyed below.
            unsafe { duckdb_get_type_id(logical_type) },
            crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_UNION
        );
        // SAFETY: `logical_type` was created by `logical_type_of` and is destroyed once.
        unsafe { duckdb_destroy_logical_type(&mut logical_type) };
    }

    #[test]
    fn union_reads_active_members_and_nulls() {
        let mut conn = Connection::open_in_memory().unwrap();
        let mut result = conn
            .execute(
                "SELECT union_value(i := 7) AS int_value, \
                 union_value(s := 'duck') AS text_value, \
                 union_value(i := NULL::INTEGER) AS null_value",
            )
            .unwrap();
        let row = result.next().unwrap().unwrap();

        assert_eq!(row.get("int_value"), Some(&DuckValue::Union(Box::new(DuckValue::Int(7)))));
        assert_eq!(
            row.get("text_value"),
            Some(&DuckValue::Union(Box::new(DuckValue::text("duck"))))
        );
        assert_eq!(row.get("null_value"), Some(&DuckValue::Union(Box::new(DuckValue::Null))));
    }
}
