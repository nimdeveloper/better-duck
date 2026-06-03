//! STRUCT read/write helpers + [`AppendAble`] impl for `HashMap<String, DuckValue>`.
//!
//! DuckDB `STRUCT` types have a fixed, named field schema.  Each field name is a
//! `String` key; the value is any `DuckValue`.  The read path fetches field names
//! from the column's `duckdb_logical_type` and recursively reads each child vector.
// FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::collections::HashMap;

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        duckdb_append_value, duckdb_bind_value, duckdb_create_struct_type,
        duckdb_create_struct_value, duckdb_destroy_logical_type, duckdb_destroy_value, duckdb_free,
        duckdb_get_type_id, duckdb_logical_type, duckdb_struct_type_child_count,
        duckdb_struct_type_child_name, duckdb_struct_vector_get_child, duckdb_value, duckdb_vector,
        duckdb_vector_get_column_type, idx_t,
    },
    types::appendable::AppendAble,
};

use super::value::DuckValue;

// Read path

/// Read a DuckDB `STRUCT` vector column at `row_idx`.
///
/// Builds a `HashMap<String, DuckValue>` from the field names and child vectors.
///
/// # Safety
/// `val` must be a valid `duckdb_vector` of STRUCT type; `row_idx` must be within
/// `[0, chunk_size)`.
pub(crate) fn read_struct(
    val: duckdb_vector,
    row_idx: u64,
) -> Result<DuckValue, DuckDBConversionError> {
    // SAFETY: `val` is a valid struct vector.  The column logical type is
    // heap-allocated by DuckDB and must be destroyed exactly once with
    // `duckdb_destroy_logical_type`.
    let mut lt = unsafe { duckdb_vector_get_column_type(val) };
    // SAFETY: `lt` is a valid logical type of STRUCT kind.
    let n: idx_t = unsafe { duckdb_struct_type_child_count(lt) };
    let mut pairs: HashMap<String, DuckValue> = HashMap::with_capacity(n as usize);
    let mut read_err: Option<DuckDBConversionError> = None;

    for i in 0..n {
        // SAFETY: `lt` is valid; `i` is within [0, n).
        // The returned C string is heap-allocated by DuckDB and must be freed with `duckdb_free`.
        let name_ptr = unsafe { duckdb_struct_type_child_name(lt, i) };
        let name = if name_ptr.is_null() {
            read_err = Some(DuckDBConversionError::ConversionError(format!(
                "struct child name at index {i} is null"
            )));
            break;
        } else {
            // SAFETY: `name_ptr` is a valid null-terminated C string.
            let s = unsafe { std::ffi::CStr::from_ptr(name_ptr) }.to_str().map(str::to_owned);
            // SAFETY: `name_ptr` was allocated by DuckDB and must be freed with `duckdb_free`.
            unsafe { duckdb_free(name_ptr as *mut std::ffi::c_void) };
            match s {
                Ok(s) => s,
                Err(e) => {
                    read_err = Some(DuckDBConversionError::ConversionError(e.to_string()));
                    break;
                },
            }
        };

        // SAFETY: `val` is a valid struct vector; `i` is within [0, n).
        let child_vec = unsafe { duckdb_struct_vector_get_child(val, i) };
        // SAFETY: `child_vec` is a valid vector; the returned logical type must be destroyed.
        let mut child_lt = unsafe { duckdb_vector_get_column_type(child_vec) };
        // SAFETY: `child_lt` is a valid logical type.
        let child_tid = unsafe { duckdb_get_type_id(child_lt) };
        // SAFETY: `child_lt` was returned by `duckdb_vector_get_column_type`.
        unsafe { duckdb_destroy_logical_type(&mut child_lt) };

        match DuckValue::from_duckdb_vec(child_vec, child_tid, row_idx) {
            Ok(v) => {
                pairs.insert(name, v);
            },
            Err(e) => {
                read_err = Some(e);
                break;
            },
        }
    }

    // SAFETY: `lt` was returned by `duckdb_vector_get_column_type` and must be destroyed
    // exactly once, even on the error path.
    unsafe { duckdb_destroy_logical_type(&mut lt) };

    match read_err {
        Some(e) => Err(e),
        None => Ok(DuckValue::Struct(pairs)),
    }
}

// Write path

/// Build a `duckdb_value` of type `STRUCT` from a `HashMap<String, DuckValue>`.
///
/// Returns an error for empty maps.
/// The caller is responsible for destroying the returned value.
pub(crate) fn struct_to_duck(
    m: &HashMap<String, DuckValue>
) -> Result<duckdb_value, DuckDBConversionError> {
    let entries: Vec<(&String, &DuckValue)> = m.iter().collect();
    let n = entries.len();
    if n == 0 {
        return Err(DuckDBConversionError::ConversionError(
            "cannot convert empty Struct to duckdb_value".into(),
        ));
    }
    let mut member_types: Vec<duckdb_logical_type> = Vec::with_capacity(n);
    let mut c_names: Vec<std::ffi::CString> = Vec::with_capacity(n);
    let mut err: Option<DuckDBConversionError> = None;
    for (k, v) in &entries {
        match DuckValue::logical_type_of(v) {
            Ok(lt) => member_types.push(lt),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
        match std::ffi::CString::new(k.as_str()) {
            Ok(c) => c_names.push(c),
            Err(e) => {
                err = Some(DuckDBConversionError::ConversionError(e.to_string()));
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut lt in member_types {
            // SAFETY: each `lt` was allocated by `logical_type_of` above; destroy once.
            unsafe { duckdb_destroy_logical_type(&mut lt) };
        }
        return Err(e);
    }
    let mut name_ptrs: Vec<*const std::os::raw::c_char> =
        c_names.iter().map(|c| c.as_ptr()).collect();
    // SAFETY: `member_types`/`name_ptrs` valid arrays of `n`; create copies both.
    let mut struct_lt = unsafe {
        duckdb_create_struct_type(member_types.as_mut_ptr(), name_ptrs.as_mut_ptr(), n as idx_t)
    };
    for mut lt in member_types {
        // SAFETY: each `lt` was allocated by `logical_type_of` above; destroy once.
        unsafe { duckdb_destroy_logical_type(&mut lt) };
    }
    let mut member_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
    let mut err: Option<DuckDBConversionError> = None;
    for (_, v) in &entries {
        match v.to_duck() {
            Ok(dv) => member_dvs.push(dv),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut dv in member_dvs {
            // SAFETY: each `dv` was created by `to_duck()` above; destroy once.
            unsafe { duckdb_destroy_value(&mut dv) };
        }
        // SAFETY: `struct_lt` was allocated above; destroy once.
        unsafe { duckdb_destroy_logical_type(&mut struct_lt) };
        return Err(e);
    }
    // SAFETY: `struct_lt` valid; `member_dvs` in schema-declaration order.
    let result = unsafe { duckdb_create_struct_value(struct_lt, member_dvs.as_mut_ptr()) };
    // SAFETY: `struct_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut struct_lt) };
    for mut dv in member_dvs {
        // SAFETY: each `dv` was created by `to_duck()` above; destroy once.
        unsafe { duckdb_destroy_value(&mut dv) };
    }
    Ok(result)
}

// Logical-type path

/// Return a `duckdb_logical_type` for a STRUCT with fields given by `m`.
pub(crate) fn struct_logical_type(
    m: &HashMap<String, DuckValue>
) -> Result<duckdb_logical_type, DuckDBConversionError> {
    let n = m.len();
    if n == 0 {
        return Err(DuckDBConversionError::ConversionError(
            "cannot determine type of empty Struct".into(),
        ));
    }
    let entries: Vec<(&String, &DuckValue)> = m.iter().collect();
    let mut member_types: Vec<duckdb_logical_type> = Vec::with_capacity(n);
    let mut c_names: Vec<std::ffi::CString> = Vec::with_capacity(n);
    let mut err: Option<DuckDBConversionError> = None;

    for (k, v) in &entries {
        match DuckValue::logical_type_of(v) {
            Ok(lt) => member_types.push(lt),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
        match std::ffi::CString::new(k.as_str()) {
            Ok(c) => c_names.push(c),
            Err(e) => {
                err = Some(DuckDBConversionError::ConversionError(e.to_string()));
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut lt in member_types {
            // SAFETY: each `lt` was allocated by `logical_type_of` above; destroy once.
            unsafe { duckdb_destroy_logical_type(&mut lt) };
        }
        return Err(e);
    }
    let mut name_ptrs: Vec<*const std::os::raw::c_char> =
        c_names.iter().map(|c| c.as_ptr()).collect();
    // SAFETY: `member_types` and `name_ptrs` are valid arrays of `n` elements;
    // `duckdb_create_struct_type` copies both.
    let lt = unsafe {
        duckdb_create_struct_type(member_types.as_mut_ptr(), name_ptrs.as_mut_ptr(), n as idx_t)
    };
    for mut mt in member_types {
        // SAFETY: each `mt` was allocated by `logical_type_of` above; destroy once.
        unsafe { duckdb_destroy_logical_type(&mut mt) };
    }
    Ok(lt)
}

// AppendAble impl

// TODO: We need to move this DUckValue itself, if user tries to append Map objects normally we treat it as Map, if user explicitly uses DuckValue::Struct(...) then we will use following code.
/// Bind/append a `HashMap<String, DuckValue>` as a DuckDB `STRUCT`.
impl AppendAble for HashMap<String, DuckValue> {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let mut dv = DuckValue::Struct(self.clone()).to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `stmt`/`idx` are valid; `dv` was created by `to_duck()`.
        unsafe { duckdb_bind_value(stmt, idx, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { duckdb_destroy_value(&mut dv) };
        Ok(())
    }

    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        let mut dv = DuckValue::Struct(self.clone()).to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` was created by `to_duck()`.
        unsafe { duckdb_append_value(appender, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { duckdb_destroy_value(&mut dv) };
        Ok(())
    }
}
