//! LIST and ARRAY read/write helpers + generic [`AppendAble`] impls.
//!
//! Functions here are called by [`DuckValue::from_duckdb_vec`], [`DuckValue::to_duck`],
//! and [`DuckValue::logical_type_of`] so that `value.rs` can be a thin dispatcher.
// The FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ptr;

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        duckdb_append_value, duckdb_array_vector_get_child, duckdb_bind_value,
        duckdb_create_array_type, duckdb_create_array_value, duckdb_create_list_type,
        duckdb_create_list_value, duckdb_destroy_logical_type, duckdb_destroy_value,
        duckdb_get_type_id, duckdb_list_entry, duckdb_list_vector_get_child, duckdb_logical_type,
        duckdb_type, duckdb_validity_row_is_valid, duckdb_value, duckdb_vector,
        duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity, idx_t,
        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_DUCKDB_TYPE_LIST,
    },
    types::appendable::AppendAble,
};

use super::value::DuckValue;

// Read path

/// Read a DuckDB `LIST` or `ARRAY` vector column at `row_idx`.
///
/// Dispatched from [`DuckValue::from_duckdb_vec`] for the `LIST` and `ARRAY` type ids.
///
/// # Safety
/// `val` must be a valid `duckdb_vector` of LIST or ARRAY type; `row_idx` must be
/// within `[0, chunk_size)`.
#[inline(always)]
pub(crate) fn read_list_or_array(
    val: duckdb_vector,
    t: duckdb_type,
    row_idx: u64,
) -> Result<DuckValue, DuckDBConversionError> {
    // SAFETY: `val` is a valid duckdb_vector of LIST/ARRAY type.
    // `duckdb_vector_get_data` returns a pointer to the column's raw data buffer
    // which holds `duckdb_list_entry` values in packed array layout.
    let data_ptr = unsafe { duckdb_vector_get_data(val) as *mut duckdb_list_entry };
    // SAFETY: `row_idx` is within [0, chunk_size).
    let list_data = unsafe { *data_ptr.add(row_idx as usize) };
    // SAFETY: `row_idx` is within [0, chunk_size).
    // let list_data = unsafe { *data_ptr };

    // SAFETY: `val` is a valid duckdb_vector of list/array type.
    let list_child = unsafe {
        if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
            duckdb_list_vector_get_child(val)
        } else {
            duckdb_array_vector_get_child(val)
        }
    };

    // SAFETY: `list_child` is a valid duckdb_vector for the child column.
    let child_validity = unsafe { duckdb_vector_get_validity(list_child) };

    let raw_child_type: duckdb_logical_type = unsafe { duckdb_vector_get_column_type(list_child) };
    let child_type = unsafe { duckdb_get_type_id(raw_child_type) };

    // SAFETY: `list_child` is a valid list-child vector.
    // let list_length = unsafe {
    //     if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
    //         duckdb_list_vector_get_size(list_child)
    //     } else {
    //         duckdb_array_type_array_size(raw_child_type)
    //     }
    // };
    // unsafe { duckdb_destroy_logical_type(&mut raw_child_type) };
    let list_length = list_data.length;

    // TODO: What happens for this var, if the function returns error? (Maybe using https://docs.rs/scopeguard/latest/scopeguard/)
    let mut slice_data: Option<Box<[std::mem::MaybeUninit<DuckValue>]>> = None;
    let mut vec_data: Option<Vec<DuckValue>> = None;
    let iter_ptr: *mut DuckValue;

    if t == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
        let mut tmp = Box::<[DuckValue]>::new_uninit_slice(list_length as usize);
        iter_ptr = tmp.as_mut_ptr() as *mut DuckValue;
        slice_data = Some(tmp);
    } else if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
        let mut tmp = Vec::with_capacity(list_length as usize);
        iter_ptr = tmp.as_mut_ptr();
        vec_data = Some(tmp);
    } else {
        return Err(DuckDBConversionError::ConversionError(
            "invalid type for list/array".to_owned(),
        ));
    }

    // SAFETY: `list_data` is a valid pointer to the list entry for `row_idx`.
    // `(*list_data).offset` gives the count of child elements. `iter_ptr`
    // points to an allocation of at least `list_length` elements.
    unsafe {
        for each in (list_data).offset..((list_data).offset + (list_data).length) {
            let mut elem = DuckValue::Null;
            if duckdb_validity_row_is_valid(child_validity, each) {
                elem = DuckValue::from_duckdb_vec(list_child, child_type, each)?;
            }
            ptr::write(iter_ptr.add(each as usize), elem);
        }
    };

    if t == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
        // SAFETY: every element in `slice_data` was written in the loop above.
        Ok(DuckValue::Array(unsafe { slice_data.unwrap().assume_init() }))
    } else {
        let mut vec_data = vec_data.unwrap();
        // SAFETY: all `list_length` elements were written into `vec_data`'s
        // allocation via `iter_ptr` in the loop above.
        unsafe { vec_data.set_len(list_length as usize) };
        Ok(DuckValue::List(vec_data))
    }
}

// Write path

/// Build a `duckdb_value` of type `LIST` from a Rust slice.
///
/// Returns an error for empty slices (element type cannot be inferred).
/// The caller is responsible for destroying the returned value with `duckdb_destroy_value`.
pub(crate) fn list_to_duck(items: &[DuckValue]) -> Result<duckdb_value, DuckDBConversionError> {
    if items.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "cannot convert empty List to duckdb_value: element type unknown".into(),
        ));
    }
    let mut child_lt = DuckValue::logical_type_of(&items[0])?;
    let mut child_dvs: Vec<duckdb_value> = Vec::with_capacity(items.len());
    let mut err: Option<DuckDBConversionError> = None;
    for item in items {
        match item.to_duck() {
            Ok(v) => child_dvs.push(v),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut v in child_dvs {
            // SAFETY: each `v` was created by `to_duck()` above.
            unsafe { duckdb_destroy_value(&mut v) };
        }
        // SAFETY: `child_lt` was allocated by `logical_type_of` above.
        unsafe { duckdb_destroy_logical_type(&mut child_lt) };
        return Err(e);
    }
    // SAFETY: `child_lt` is valid; `child_dvs` has `len()` elements.
    let result = unsafe {
        duckdb_create_list_value(child_lt, child_dvs.as_mut_ptr(), child_dvs.len() as idx_t)
    };
    // SAFETY: `child_lt` was allocated by `logical_type_of`; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
    for mut v in child_dvs {
        // SAFETY: each `v` was created by `to_duck()` above.
        unsafe { duckdb_destroy_value(&mut v) };
    }
    Ok(result)
}

/// Build a `duckdb_value` of type `ARRAY` from a Rust slice.
///
/// Returns an error for empty slices.
/// The caller is responsible for destroying the returned value.
pub(crate) fn array_to_duck(items: &[DuckValue]) -> Result<duckdb_value, DuckDBConversionError> {
    if items.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "cannot convert empty Array to duckdb_value: element type unknown".into(),
        ));
    }
    let mut child_lt = DuckValue::logical_type_of(&items[0])?;
    let mut child_dvs: Vec<duckdb_value> = Vec::with_capacity(items.len());
    let mut err: Option<DuckDBConversionError> = None;
    for item in items {
        match item.to_duck() {
            Ok(v) => child_dvs.push(v),
            Err(e) => {
                err = Some(e);
                break;
            },
        }
    }
    if let Some(e) = err {
        for mut v in child_dvs {
            // SAFETY: each `v` was created by `to_duck()` above.
            unsafe { duckdb_destroy_value(&mut v) };
        }
        // SAFETY: `child_lt` was allocated by `logical_type_of` above.
        unsafe { duckdb_destroy_logical_type(&mut child_lt) };
        return Err(e);
    }
    // SAFETY: `child_lt` is valid; array_size matches item count.
    let mut arr_lt = unsafe { duckdb_create_array_type(child_lt, child_dvs.len() as idx_t) };
    // SAFETY: `child_lt` was allocated by `logical_type_of`; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
    // SAFETY: `arr_lt` is valid; `child_dvs` has `len()` elements.
    let result = unsafe {
        duckdb_create_array_value(arr_lt, child_dvs.as_mut_ptr(), child_dvs.len() as idx_t)
    };
    // SAFETY: `arr_lt` was allocated above; destroy once.
    unsafe { duckdb_destroy_logical_type(&mut arr_lt) };
    for mut v in child_dvs {
        // SAFETY: each `v` was created by `to_duck()` above.
        unsafe { duckdb_destroy_value(&mut v) };
    }
    Ok(result)
}

// Logical-type path

/// Return a `duckdb_logical_type` for a LIST of the element type of `items[0]`.
pub(crate) fn list_logical_type(
    items: &[DuckValue]
) -> Result<duckdb_logical_type, DuckDBConversionError> {
    if items.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "cannot determine element type of empty List".into(),
        ));
    }
    let mut child_lt = DuckValue::logical_type_of(&items[0])?;
    // SAFETY: `child_lt` is a valid logical type; `duckdb_create_list_type` copies it.
    let lt = unsafe { duckdb_create_list_type(child_lt) };
    // SAFETY: `child_lt` was allocated above and must be freed exactly once.
    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
    Ok(lt)
}

/// Return a `duckdb_logical_type` for an ARRAY of the element type of `items[0]`.
pub(crate) fn array_logical_type(
    items: &[DuckValue]
) -> Result<duckdb_logical_type, DuckDBConversionError> {
    if items.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "cannot determine element type of empty Array".into(),
        ));
    }
    let mut child_lt = DuckValue::logical_type_of(&items[0])?;
    // SAFETY: `child_lt` is valid; `duckdb_create_array_type` copies it.
    let lt = unsafe { duckdb_create_array_type(child_lt, items.len() as idx_t) };
    // SAFETY: `child_lt` was allocated above.
    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
    Ok(lt)
}

// Generic AppendAble impls

/// Bind/append a `Vec<T>` as a DuckDB `LIST`.
///
/// Each element is converted via `T: Into<DuckValue>`, then the entire `DuckValue::List`
/// is serialized to a `duckdb_value` and bound/appended via the value path.
impl<T: Into<DuckValue> + Clone> AppendAble for Vec<T> {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let duck_list: Vec<DuckValue> = self.iter().cloned().map(Into::into).collect();
        let mut dv = DuckValue::List(duck_list).to_duck().map_err(Error::ConversionError)?;
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
        let duck_list: Vec<DuckValue> = self.iter().cloned().map(Into::into).collect();
        let mut dv = DuckValue::List(duck_list).to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` was created by `to_duck()`.
        unsafe { duckdb_append_value(appender, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { duckdb_destroy_value(&mut dv) };
        Ok(())
    }
}

/// Bind/append a `Box<[T]>` as a DuckDB `ARRAY`.
///
/// Each element is converted via `T: Into<DuckValue>`, then the entire `DuckValue::Array`
/// is serialized to a `duckdb_value` and bound/appended via the value path.
impl<T: Into<DuckValue> + Clone> AppendAble for Box<[T]> {
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let duck_arr: Box<[DuckValue]> =
            self.iter().cloned().map(Into::into).collect::<Vec<_>>().into_boxed_slice();
        let mut dv = DuckValue::Array(duck_arr).to_duck().map_err(Error::ConversionError)?;
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
        let duck_arr: Box<[DuckValue]> =
            self.iter().cloned().map(Into::into).collect::<Vec<_>>().into_boxed_slice();
        let mut dv = DuckValue::Array(duck_arr).to_duck().map_err(Error::ConversionError)?;
        // SAFETY: `appender` is valid; `dv` was created by `to_duck()`.
        unsafe { duckdb_append_value(appender, dv) };
        // SAFETY: `dv` was created above; destroy exactly once.
        unsafe { duckdb_destroy_value(&mut dv) };
        Ok(())
    }
}
