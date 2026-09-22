//! UNION read path.
//!
//! DuckDB `UNION` is physically a `STRUCT` where child 0 is the tag discriminant
//! and children `1..=N` are the member values.  Only the active member's value is
//! read; the rest are skipped.  The full member schema (names + types) is recovered
//! from the column's logical type so the value round-trips as the *same* union —
//! the write/logical-type paths live on [`crate::types::duck_union::DuckUnion`].
// FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::sync::Arc;

use crate::{
    error::DuckDBConversionError,
    ffi::{
        duckdb_destroy_logical_type, duckdb_get_type_id, duckdb_struct_vector_get_child,
        duckdb_vector, duckdb_vector_get_column_type, duckdb_vector_get_data, idx_t,
    },
    types::{duck_union::DuckUnion, LogicalType, TypeInfo},
};

use super::value::DuckValue;

/// Read a DuckDB `UNION` vector column at `row_idx`.
///
/// Returns a [`DuckValue::Union`] preserving the full member schema, the selected
/// tag, and the active member's value.
///
/// # Safety
/// `val` must be a valid `duckdb_vector` of UNION type; `row_idx` must be within
/// `[0, chunk_size)`.
pub(crate) fn read_union(
    val: duckdb_vector,
    row_idx: u64,
) -> Result<DuckValue, DuckDBConversionError> {
    // Wrap the column's union logical type in RAII and recover its full member
    // schema; the handle is destroyed exactly once when `union_lt` drops.
    // SAFETY: `val` is a valid union vector; the returned logical type is owned.
    let union_lt =
        LogicalType::from_raw(unsafe { duckdb_vector_get_column_type(val) }).map_err(|_| {
            DuckDBConversionError::ConversionError("null UNION logical type".to_owned())
        })?;
    let members = match union_lt.describe() {
        TypeInfo::Union(members) => members,
        _ => {
            return Err(DuckDBConversionError::ConversionError(
                "expected a UNION logical type".to_owned(),
            ))
        },
    };
    let member_count = members.len();

    // Child 0 of the underlying struct layout is the tag vector.
    // SAFETY: `val` is a valid union vector; child 0 is the tag.
    let tag_vec = unsafe { duckdb_struct_vector_get_child(val, 0) };

    // DuckDB uses UTINYINT (u8) for ≤ 255 members, USMALLINT (u16) otherwise.
    // SAFETY: `tag_vec` is a valid data vector; `row_idx` is within [0, chunk_size).
    let tag: usize = unsafe {
        let data = duckdb_vector_get_data(tag_vec);
        if member_count <= u8::MAX as usize {
            *(data as *const u8).add(row_idx as usize) as usize
        } else {
            *(data as *const u16).add(row_idx as usize) as usize
        }
    };

    if tag >= member_count {
        return Err(DuckDBConversionError::ConversionError(format!(
            "union tag {tag} out of range (member count {member_count})"
        )));
    }

    // The active member sits at child index (tag + 1) in the underlying struct.
    // SAFETY: `val` is a valid union vector; `tag + 1` is within [1, member_count + 1).
    let member_vec = unsafe { duckdb_struct_vector_get_child(val, (tag + 1) as idx_t) };
    // SAFETY: `member_vec` is a valid vector for the active member.
    let mut member_lt = unsafe { duckdb_vector_get_column_type(member_vec) };
    // SAFETY: `member_lt` is a valid logical type.
    let member_tid = unsafe { duckdb_get_type_id(member_lt) };
    // SAFETY: `member_lt` was returned by `duckdb_vector_get_column_type`.
    unsafe { duckdb_destroy_logical_type(&mut member_lt) };

    // Recurse into the active member.
    let inner = DuckValue::from_duckdb_vec(member_vec, member_tid, row_idx)?;

    // `union_lt` (RAII) drops here, destroying the union logical type exactly once.
    let members: Arc<[(String, TypeInfo)]> = Arc::from(members);
    DuckUnion::new(members, tag as u32, inner).map(DuckValue::Union)
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
        // A single-member UNION(v INTEGER) with the active value 42.
        let union = DuckUnion::new(
            Arc::from([(
                "v".to_owned(),
                TypeInfo::Scalar(crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            )]),
            0,
            DuckValue::Int(42),
        )
        .unwrap();
        let value = DuckValue::Union(union);

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

        // Each single-arm union preserves its one member and the active value.
        match row.get("int_value").unwrap() {
            DuckValue::Union(u) => {
                assert_eq!(u.active_name(), "i");
                assert_eq!(u.value(), &DuckValue::Int(7));
            },
            other => panic!("expected Union, got {other:?}"),
        }
        match row.get("text_value").unwrap() {
            DuckValue::Union(u) => {
                assert_eq!(u.active_name(), "s");
                assert_eq!(u.value(), &DuckValue::text("duck"));
            },
            other => panic!("expected Union, got {other:?}"),
        }
        match row.get("null_value").unwrap() {
            DuckValue::Union(u) => {
                assert_eq!(u.active_name(), "i");
                assert_eq!(u.value(), &DuckValue::Null);
            },
            other => panic!("expected Union, got {other:?}"),
        }
    }
}
