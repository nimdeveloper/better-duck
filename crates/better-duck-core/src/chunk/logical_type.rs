//! An RAII wrapper around a `duckdb_logical_type` handle.

use std::ffi::{c_char, c_void, CStr, CString};

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        self, duckdb_create_decimal_type, duckdb_create_enum_type, duckdb_create_list_type,
        duckdb_create_struct_type, duckdb_decimal_scale, duckdb_decimal_width,
        duckdb_destroy_logical_type, duckdb_enum_internal_type, duckdb_free, duckdb_get_type_id,
        duckdb_list_type_child_type, duckdb_logical_type, duckdb_logical_type_get_alias,
        duckdb_logical_type_set_alias, duckdb_struct_type_child_count,
        duckdb_struct_type_child_name, duckdb_struct_type_child_type, DUCKDB_TYPE,
    },
    types::DuckLogicalType,
};

/// An owned DuckDB logical type handle.
///
/// [`DuckLogicalType::duck_logical_type`] returns a raw, caller-destroys pointer;
/// this type is the single adapter that takes that raw form into RAII. Every
/// user-defined-function registration path in [`crate::udf`] uses `LogicalType`
/// rather than the raw pointer, so a `duckdb_logical_type` is never held across
/// more than one statement.
pub struct LogicalType(duckdb_logical_type);

impl LogicalType {
    /// Returns the logical type of the Rust type `T`.
    ///
    /// # Errors
    ///
    /// Returns an error if `T` has no fixed DuckDB representation.
    pub fn of<T: DuckLogicalType>() -> Result<Self> {
        let raw = T::duck_logical_type().map_err(Error::ConversionError)?;
        Self::from_raw(raw)
    }

    /// Builds a `DECIMAL(width, scale)` logical type.
    ///
    /// # Errors
    ///
    /// Returns an error if `width` is outside `1..=38` or `scale > width`.
    pub fn decimal(
        width: u8,
        scale: u8,
    ) -> Result<Self> {
        // SAFETY: always safe to call; returns null if `width`/`scale` are
        // out of range, handled by `from_raw` below.
        let raw = unsafe { duckdb_create_decimal_type(width, scale) };
        Self::from_raw(raw)
    }

    /// Builds an `ENUM` logical type with the given member values, in order.
    ///
    /// # Errors
    ///
    /// Returns an error if any member contains an interior NUL byte.
    pub fn enum_of(members: &[&str]) -> Result<Self> {
        let c_members =
            members.iter().map(|m| CString::new(*m)).collect::<std::result::Result<Vec<_>, _>>()?;
        let mut ptrs: Vec<*const c_char> = c_members.iter().map(|c| c.as_ptr()).collect();
        // SAFETY: `ptrs` is a valid array of `ptrs.len()` NUL-terminated C
        // strings, each owned by `c_members` for the duration of this call.
        let raw = unsafe { duckdb_create_enum_type(ptrs.as_mut_ptr(), ptrs.len() as ffi::idx_t) };
        Self::from_raw(raw)
    }

    /// Builds a `LIST` logical type whose elements are `child`.
    pub fn list_of(child: &LogicalType) -> Self {
        // SAFETY: `child.0` is a valid logical type owned by `child` for the
        // duration of this call; `duckdb_create_list_type` copies it. This
        // constructor is documented to never fail for a valid child type.
        let raw = unsafe { duckdb_create_list_type(child.0) };
        Self::from_raw(raw).expect("duckdb_create_list_type does not fail for a valid child type")
    }

    /// Builds a `STRUCT` logical type with the given `(name, type)` fields,
    /// in order.
    ///
    /// # Errors
    ///
    /// Returns an error if any field name contains an interior NUL byte.
    pub fn struct_of(fields: &[(&str, &LogicalType)]) -> Result<Self> {
        let c_names = fields
            .iter()
            .map(|(name, _)| CString::new(*name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut name_ptrs: Vec<*const c_char> = c_names.iter().map(|c| c.as_ptr()).collect();
        let mut type_ptrs: Vec<duckdb_logical_type> = fields.iter().map(|(_, ty)| ty.0).collect();
        // SAFETY: `type_ptrs`/`name_ptrs` are both valid arrays of
        // `fields.len()` entries; the types are owned by the caller's
        // `&LogicalType` borrows and the names by `c_names`, both live for
        // the duration of this call. `duckdb_create_struct_type` copies both.
        let raw = unsafe {
            duckdb_create_struct_type(
                type_ptrs.as_mut_ptr(),
                name_ptrs.as_mut_ptr(),
                fields.len() as ffi::idx_t,
            )
        };
        Self::from_raw(raw)
    }

    /// Takes ownership of a raw `duckdb_logical_type` handle.
    ///
    /// # Errors
    ///
    /// Returns an error if `raw` is null.
    pub(crate) fn from_raw(raw: duckdb_logical_type) -> Result<Self> {
        if raw.is_null() {
            return Err(Error::ConversionError(
                crate::error::DuckDBConversionError::ConversionError(
                    "duck_logical_type() returned a null logical type".to_owned(),
                ),
            ));
        }
        Ok(Self(raw))
    }

    /// Returns the underlying raw handle, still owned by `self`.
    ///
    /// The handle is valid only for the lifetime of `self`. DuckDB's
    /// `add_parameter`/`set_return_type`/`add_result_column`-style functions all
    /// copy the logical type they are given, so dropping `self` immediately after
    /// passing this to one of them is correct.
    pub(crate) fn as_raw(&self) -> duckdb_logical_type {
        self.0
    }

    /// Returns this type's bare `DUCKDB_TYPE` tag.
    #[inline]
    pub fn type_id(&self) -> DUCKDB_TYPE {
        // SAFETY: `self.0` is a valid logical type for the lifetime of `self`.
        unsafe { duckdb_get_type_id(self.0) }
    }

    fn expect_type_id(
        &self,
        expected: DUCKDB_TYPE,
    ) -> Result<()> {
        let found = self.type_id();
        if found == expected {
            Ok(())
        } else {
            Err(Error::ConversionError(DuckDBConversionError::TypeMismatch { expected, found }))
        }
    }

    /// Returns this DECIMAL type's width.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a DECIMAL type.
    pub fn decimal_width(&self) -> Result<u8> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL)?;
        // SAFETY: `self.0` is a valid DECIMAL logical type, checked above.
        Ok(unsafe { duckdb_decimal_width(self.0) })
    }

    /// Returns this DECIMAL type's scale.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a DECIMAL type.
    pub fn decimal_scale(&self) -> Result<u8> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL)?;
        // SAFETY: `self.0` is a valid DECIMAL logical type, checked above.
        Ok(unsafe { duckdb_decimal_scale(self.0) })
    }

    /// Returns this ENUM type's internal (dictionary-index) storage type.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not an ENUM type.
    pub fn enum_internal_type(&self) -> Result<DUCKDB_TYPE> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM)?;
        // SAFETY: `self.0` is a valid ENUM logical type, checked above.
        Ok(unsafe { duckdb_enum_internal_type(self.0) })
    }

    /// Returns this LIST (or MAP) type's element type.
    ///
    /// # Errors
    ///
    /// Returns an error if this is neither a LIST nor a MAP type.
    pub fn list_child_type(&self) -> Result<Self> {
        let found = self.type_id();
        if found != ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST && found != ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP {
            return Err(Error::ConversionError(DuckDBConversionError::TypeMismatch {
                expected: ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST,
                found,
            }));
        }
        // SAFETY: `self.0` is a valid LIST or MAP logical type, checked above.
        let raw = unsafe { duckdb_list_type_child_type(self.0) };
        Self::from_raw(raw)
    }

    /// Returns this STRUCT type's number of fields.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a STRUCT type.
    pub fn struct_child_count(&self) -> Result<usize> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT)?;
        // SAFETY: `self.0` is a valid STRUCT logical type, checked above.
        Ok(unsafe { duckdb_struct_type_child_count(self.0) as usize })
    }

    /// Returns the name of the STRUCT field at `idx`.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a STRUCT type, or `idx` is out of range.
    pub fn struct_child_name(
        &self,
        idx: usize,
    ) -> Result<String> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT)?;
        // SAFETY: `self.0` is a valid STRUCT logical type, checked above;
        // `idx` need not be pre-validated — DuckDB returns null for an
        // out-of-range index.
        let raw = unsafe { duckdb_struct_type_child_name(self.0, idx as ffi::idx_t) };
        if raw.is_null() {
            return Err(Error::InvalidColumnIndex(idx));
        }
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 string,
        // caller-owned — freed via `duckdb_free` exactly once, below.
        let name = unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned();
        // SAFETY: `raw` was allocated by DuckDB and must be freed with
        // `duckdb_free`, per this function's documented contract.
        unsafe { duckdb_free(raw.cast::<c_void>()) };
        Ok(name)
    }

    /// Returns the type of the STRUCT field at `idx`.
    ///
    /// # Errors
    ///
    /// Returns an error if this is not a STRUCT type, or `idx` is out of range.
    pub fn struct_child_type(
        &self,
        idx: usize,
    ) -> Result<Self> {
        self.expect_type_id(ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT)?;
        // SAFETY: `self.0` is a valid STRUCT logical type, checked above.
        let raw = unsafe { duckdb_struct_type_child_type(self.0, idx as ffi::idx_t) };
        Self::from_raw(raw).map_err(|_| Error::InvalidColumnIndex(idx))
    }

    /// Returns this type's alias, if one has been set.
    pub fn alias(&self) -> Option<String> {
        // SAFETY: `self.0` is a valid logical type for the lifetime of `self`.
        let raw = unsafe { duckdb_logical_type_get_alias(self.0) };
        if raw.is_null() {
            return None;
        }
        // SAFETY: DuckDB guarantees a null-terminated, valid-UTF-8 string,
        // caller-owned — freed via `duckdb_free` exactly once, below.
        let alias = unsafe { CStr::from_ptr(raw) }.to_string_lossy().into_owned();
        // SAFETY: `raw` was allocated by DuckDB and must be freed with
        // `duckdb_free`, per this function's documented contract.
        unsafe { duckdb_free(raw.cast::<c_void>()) };
        Some(alias)
    }

    /// Sets this type's alias.
    ///
    /// # Errors
    ///
    /// Returns an error if `alias` contains an interior NUL byte.
    pub fn set_alias(
        &self,
        alias: &str,
    ) -> Result<()> {
        let c_alias = CString::new(alias)?;
        // SAFETY: `self.0` is a valid logical type; `c_alias` is a valid
        // NUL-terminated C string for the duration of this call, which
        // copies it.
        unsafe { duckdb_logical_type_set_alias(self.0, c_alias.as_ptr()) };
        Ok(())
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is a valid, non-null `duckdb_logical_type` owned by
            // this `LogicalType` and not yet destroyed (guarded by the null check
            // above); `duckdb_destroy_logical_type` is called exactly once here.
            unsafe { duckdb_destroy_logical_type(&mut self.0) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn of_bool_succeeds_and_drops_cleanly() {
        let lt = LogicalType::of::<bool>().unwrap();
        assert!(!lt.as_raw().is_null());
        drop(lt);
    }

    #[test]
    fn of_i32_succeeds() {
        let lt = LogicalType::of::<i32>().unwrap();
        assert!(!lt.as_raw().is_null());
    }

    #[test]
    fn of_string_succeeds() {
        let lt = LogicalType::of::<String>().unwrap();
        assert!(!lt.as_raw().is_null());
    }

    #[test]
    fn decimal_reports_the_width_and_scale_it_was_built_with() {
        let lt = LogicalType::decimal(18, 4).unwrap();
        assert_eq!(lt.decimal_width().unwrap(), 18);
        assert_eq!(lt.decimal_scale().unwrap(), 4);
    }

    #[test]
    fn decimal_out_of_range_width_errors() {
        assert!(LogicalType::decimal(0, 0).is_err());
        assert!(LogicalType::decimal(39, 0).is_err());
    }

    #[test]
    fn decimal_width_on_a_non_decimal_type_errors() {
        let lt = LogicalType::of::<i32>().unwrap();
        assert!(lt.decimal_width().is_err());
    }

    #[test]
    fn enum_of_reports_the_internal_type() {
        let lt = LogicalType::enum_of(&["a", "b", "c"]).unwrap();
        assert_eq!(lt.type_id(), ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM);
        assert!(lt.enum_internal_type().is_ok());
    }

    #[test]
    fn list_of_child_type_round_trips() {
        let child = LogicalType::of::<i32>().unwrap();
        let list = LogicalType::list_of(&child);
        assert_eq!(list.type_id(), ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST);
        let round_tripped = list.list_child_type().unwrap();
        assert_eq!(round_tripped.type_id(), ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
    }

    #[test]
    fn struct_of_fields_round_trip_names_and_types() {
        let a = LogicalType::of::<i32>().unwrap();
        let b = LogicalType::of::<String>().unwrap();
        let s = LogicalType::struct_of(&[("a", &a), ("b", &b)]).unwrap();
        assert_eq!(s.struct_child_count().unwrap(), 2);
        assert_eq!(s.struct_child_name(0).unwrap(), "a");
        assert_eq!(s.struct_child_name(1).unwrap(), "b");
        assert_eq!(s.struct_child_type(0).unwrap().type_id(), ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        assert_eq!(s.struct_child_type(1).unwrap().type_id(), ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    }

    #[test]
    fn alias_round_trips() {
        let lt = LogicalType::of::<i32>().unwrap();
        assert_eq!(lt.alias(), None);
        lt.set_alias("my_int").unwrap();
        assert_eq!(lt.alias().as_deref(), Some("my_int"));
    }
}
