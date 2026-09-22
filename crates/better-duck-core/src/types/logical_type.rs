//! Metadata-preserving [`LogicalType`] — an owned `duckdb_logical_type` handle
//! plus recursive introspection.
//!
//! DuckDB describes a column's type with a `duckdb_logical_type` handle. A scalar
//! type is fully described by its `duckdb_type` id, but the *nested* and
//! *parameterised* types are not: DECIMAL carries width/scale, ENUM a dictionary,
//! LIST/ARRAY a child type (ARRAY also a fixed size), MAP a key and value type,
//! STRUCT an ordered set of named fields, UNION ordered named members, and any
//! type may carry a user alias. This module reads all of that out of the handle,
//! preserving it in an owned [`TypeInfo`] tree — the lossless descriptor the rest
//! of the driver (and, later, result-set schema) builds on.
// FFI pointer arguments are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;

use crate::{
    error::{DuckDBConversionError, Error, Result},
    ffi::{
        duckdb_array_type_array_size, duckdb_array_type_child_type, duckdb_create_array_type,
        duckdb_create_decimal_type, duckdb_create_enum_type, duckdb_create_list_type,
        duckdb_create_logical_type, duckdb_create_map_type, duckdb_create_struct_type,
        duckdb_create_union_type, duckdb_destroy_logical_type, duckdb_enum_dictionary_size,
        duckdb_enum_dictionary_value, duckdb_enum_internal_type, duckdb_free, duckdb_get_type_id,
        duckdb_list_type_child_type, duckdb_logical_type, duckdb_logical_type_get_alias,
        duckdb_map_type_key_type, duckdb_map_type_value_type, duckdb_struct_type_child_count,
        duckdb_struct_type_child_name, duckdb_struct_type_child_type, duckdb_type,
        duckdb_union_type_member_count, duckdb_union_type_member_name,
        duckdb_union_type_member_type, idx_t, DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
        DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP, DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
        DUCKDB_TYPE_DUCKDB_TYPE_UNION,
    },
    types::{
        decimal::{decimal_scale, decimal_width},
        DuckLogicalType,
    },
};

/// A fully materialised, owned description of a DuckDB logical type.
///
/// Produced by [`LogicalType::describe`]. Unlike a bare `duckdb_type` id, this
/// preserves every parameter DuckDB attaches to a type, recursively, so a nested
/// schema (e.g. `STRUCT(a DECIMAL(6,2), b LIST(INTEGER))`) round-trips without
/// losing width/scale, field order, array size, or aliases.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeInfo {
    /// A scalar type fully described by its id (INTEGER, VARCHAR, DATE, …).
    Scalar(duckdb_type),
    /// `DECIMAL(width, scale)`.
    Decimal {
        /// Total significant digits, 1..=38.
        width: u8,
        /// Fractional digits, `<= width`.
        scale: u8,
    },
    /// `ENUM` with its ordered dictionary of labels.
    Enum(Vec<String>),
    /// `LIST(child)`.
    List(Box<TypeInfo>),
    /// `ARRAY(child, size)` — a fixed-length list.
    Array {
        /// Element type.
        child: Box<TypeInfo>,
        /// Fixed element count.
        size: u64,
    },
    /// `MAP(key, value)`.
    Map {
        /// Key type.
        key: Box<TypeInfo>,
        /// Value type.
        value: Box<TypeInfo>,
    },
    /// `STRUCT(name type, …)` — ordered, named fields.
    Struct(Vec<(String, TypeInfo)>),
    /// `UNION(name type, …)` — ordered, named members.
    Union(Vec<(String, TypeInfo)>),
}

impl TypeInfo {
    /// Rebuilds an owned [`LogicalType`] from this descriptor — the inverse of
    /// [`LogicalType::describe`].
    ///
    /// This lets a metadata-preserving value (e.g. a typed `UNION`) reconstruct its
    /// full declared type on write, so a read→write round trip keeps every member's
    /// name/type, DECIMAL precision, ENUM dictionary, and nesting rather than
    /// degrading to a single-arm or scalar approximation. It uses only the DuckDB
    /// type *constructors*, recursively.
    ///
    /// # Errors
    ///
    /// Returns [`DuckDBConversionError::ConversionError`] if DuckDB rejects a
    /// reconstructed type (e.g. an out-of-range DECIMAL, an ENUM label with an
    /// interior NUL, or an empty STRUCT/UNION member set).
    pub(crate) fn to_logical_type(&self) -> Result<LogicalType, DuckDBConversionError> {
        match self {
            TypeInfo::Scalar(id) => {
                // SAFETY: `id` is a duckdb_type constant; the constructor returns an
                // owned handle (or null for a non-primitive id, rejected by from_raw).
                LogicalType::from_raw(unsafe { duckdb_create_logical_type(*id) })
                    .map_err(map_reconstruct_err)
            },
            TypeInfo::Decimal { width, scale } => {
                // SAFETY: width/scale are copied by value; a rejected pair yields null.
                LogicalType::from_raw(unsafe { duckdb_create_decimal_type(*width, *scale) })
                    .map_err(|_| {
                        DuckDBConversionError::ConversionError(format!(
                            "DuckDB rejected DECIMAL({width},{scale})"
                        ))
                    })
            },
            TypeInfo::Enum(dict) => build_enum_type(dict),
            TypeInfo::List(child) => {
                let child_lt = child.to_logical_type()?;
                // SAFETY: `child_lt` is valid; `duckdb_create_list_type` copies it.
                LogicalType::from_raw(unsafe { duckdb_create_list_type(child_lt.as_raw()) })
                    .map_err(map_reconstruct_err)
            },
            TypeInfo::Array { child, size } => {
                let child_lt = child.to_logical_type()?;
                // SAFETY: `child_lt` is valid; the constructor copies it.
                LogicalType::from_raw(unsafe {
                    duckdb_create_array_type(child_lt.as_raw(), *size as idx_t)
                })
                .map_err(map_reconstruct_err)
            },
            TypeInfo::Map { key, value } => {
                let key_lt = key.to_logical_type()?;
                let value_lt = value.to_logical_type()?;
                // SAFETY: both handles are valid; the constructor copies them.
                LogicalType::from_raw(unsafe {
                    duckdb_create_map_type(key_lt.as_raw(), value_lt.as_raw())
                })
                .map_err(map_reconstruct_err)
            },
            TypeInfo::Struct(fields) => build_member_type(fields, MemberKind::Struct),
            TypeInfo::Union(members) => build_member_type(members, MemberKind::Union),
        }
    }
}

/// Whether a named-member type array builds a `STRUCT` or a `UNION`.
enum MemberKind {
    Struct,
    Union,
}

/// Builds a `STRUCT`/`UNION` logical type from ordered `(name, type)` members.
fn build_member_type(
    members: &[(String, TypeInfo)],
    kind: MemberKind,
) -> Result<LogicalType, DuckDBConversionError> {
    if members.is_empty() {
        return Err(DuckDBConversionError::ConversionError(
            "STRUCT/UNION must have at least one member".to_owned(),
        ));
    }
    // Reconstruct each member type first; keep the RAII handles alive until the
    // constructor (which copies them) returns.
    let child_lts: Vec<LogicalType> =
        members.iter().map(|(_, ty)| ty.to_logical_type()).collect::<Result<_, _>>()?;
    let mut child_raw: Vec<duckdb_logical_type> =
        child_lts.iter().map(LogicalType::as_raw).collect();
    let c_names: Vec<CString> = members
        .iter()
        .map(|(name, _)| {
            CString::new(name.as_str())
                .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
        })
        .collect::<Result<_, _>>()?;
    let mut name_ptrs: Vec<*const c_char> = c_names.iter().map(|c| c.as_ptr()).collect();
    let count = members.len() as idx_t;
    // SAFETY: `child_raw`/`name_ptrs` are parallel arrays of `count` valid entries
    // that outlive the call; DuckDB copies both the member types and names.
    let raw = unsafe {
        match kind {
            MemberKind::Struct => {
                duckdb_create_struct_type(child_raw.as_mut_ptr(), name_ptrs.as_mut_ptr(), count)
            },
            MemberKind::Union => {
                duckdb_create_union_type(child_raw.as_mut_ptr(), name_ptrs.as_mut_ptr(), count)
            },
        }
    };
    // The child handles have now been copied by DuckDB; dropping `child_lts` here
    // destroys our originals exactly once.
    drop(child_lts);
    LogicalType::from_raw(raw).map_err(map_reconstruct_err)
}

/// Builds an `ENUM` logical type from an ordered dictionary of labels.
fn build_enum_type(dict: &[String]) -> Result<LogicalType, DuckDBConversionError> {
    let c_labels: Vec<CString> = dict
        .iter()
        .map(|l| {
            CString::new(l.as_str())
                .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
        })
        .collect::<Result<_, _>>()?;
    let mut ptrs: Vec<*const c_char> = c_labels.iter().map(|c| c.as_ptr()).collect();
    // SAFETY: `ptrs` holds `c_labels.len()` valid null-terminated pointers that
    // outlive the call; DuckDB copies the names into the new logical type.
    let raw = unsafe { duckdb_create_enum_type(ptrs.as_mut_ptr(), ptrs.len() as idx_t) };
    LogicalType::from_raw(raw).map_err(map_reconstruct_err)
}

/// Maps a null-handle error from a type constructor to a conversion error.
fn map_reconstruct_err(_: Error) -> DuckDBConversionError {
    DuckDBConversionError::ConversionError(
        "DuckDB rejected a reconstructed logical type".to_owned(),
    )
}

/// An owned DuckDB logical type handle with recursive introspection.
///
/// [`DuckLogicalType::duck_logical_type`] returns a raw, caller-destroys pointer;
/// this is the single adapter that takes that raw form into RAII, and every child
/// type read from it (list/array/map/struct/union member) is itself a
/// `LogicalType`, so no `duckdb_logical_type` is ever leaked or double-freed.
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

    /// Takes ownership of a raw `duckdb_logical_type` handle.
    ///
    /// # Errors
    ///
    /// Returns an error if `raw` is null.
    pub(crate) fn from_raw(raw: duckdb_logical_type) -> Result<Self> {
        if raw.is_null() {
            return Err(Error::ConversionError(DuckDBConversionError::ConversionError(
                "logical type handle is null".to_owned(),
            )));
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

    /// Returns the top-level `duckdb_type` id of this logical type.
    #[must_use]
    pub fn type_id(&self) -> duckdb_type {
        // SAFETY: `self.0` is a valid, non-null logical type.
        unsafe { duckdb_get_type_id(self.0) }
    }

    /// Returns the user-defined alias of this type, if any.
    ///
    /// (`duckdb_logical_type_get_alias`.) Reading the alias is a prerequisite for
    /// preserving custom type names; *setting* aliases is done via set_alias.
    #[must_use]
    pub fn alias(&self) -> Option<String> {
        // SAFETY: `self.0` is valid. `duckdb_logical_type_get_alias` returns a
        // heap `char*` (or null) that the caller must free with `duckdb_free`; we
        // copy it into an owned String first.
        unsafe { owned_c_string(duckdb_logical_type_get_alias(self.0)) }
    }

    /// Sets this type's user-defined alias (`duckdb_logical_type_set_alias`).
    ///
    /// DuckDB copies the name, so it need not outlive the call. Read it back with
    /// [`alias`](LogicalType::alias).
    ///
    /// # Errors
    ///
    /// Returns an error if `alias` contains an interior NUL.
    pub fn set_alias(
        &mut self,
        alias: &str,
    ) -> Result<()> {
        let c_alias = CString::new(alias).map_err(|e| {
            Error::ConversionError(DuckDBConversionError::ConversionError(e.to_string()))
        })?;
        // SAFETY: `self.0` is a valid logical type; `c_alias` is a valid, NUL-terminated
        // C string that outlives the call, and DuckDB copies it.
        unsafe { crate::ffi::duckdb_logical_type_set_alias(self.0, c_alias.as_ptr()) };
        Ok(())
    }

    /// The child type of a `LIST` (also accepts `MAP`).
    #[must_use]
    pub fn list_child(&self) -> Option<LogicalType> {
        // SAFETY: `self.0` is valid; the returned handle is owned (destroy once)
        // and wrapped in RAII. Null (wrong kind) yields None.
        LogicalType::from_raw(unsafe { duckdb_list_type_child_type(self.0) }).ok()
    }

    /// The element type of an `ARRAY`.
    #[must_use]
    pub fn array_child(&self) -> Option<LogicalType> {
        // SAFETY: as above.
        LogicalType::from_raw(unsafe { duckdb_array_type_child_type(self.0) }).ok()
    }

    /// The fixed element count of an `ARRAY`.
    #[must_use]
    pub fn array_size(&self) -> u64 {
        // SAFETY: `self.0` is valid; meaningful only for ARRAY types.
        unsafe { duckdb_array_type_array_size(self.0) as u64 }
    }

    /// The key type of a `MAP`.
    #[must_use]
    pub fn map_key(&self) -> Option<LogicalType> {
        // SAFETY: as above.
        LogicalType::from_raw(unsafe { duckdb_map_type_key_type(self.0) }).ok()
    }

    /// The value type of a `MAP`.
    #[must_use]
    pub fn map_value(&self) -> Option<LogicalType> {
        // SAFETY: as above.
        LogicalType::from_raw(unsafe { duckdb_map_type_value_type(self.0) }).ok()
    }

    /// The number of fields in a `STRUCT`.
    #[must_use]
    pub fn struct_child_count(&self) -> u64 {
        // SAFETY: `self.0` is valid; meaningful only for STRUCT types.
        unsafe { duckdb_struct_type_child_count(self.0) as u64 }
    }

    /// The name of the `STRUCT` field at `index`.
    #[must_use]
    pub fn struct_child_name(
        &self,
        index: u64,
    ) -> Option<String> {
        // SAFETY: `self.0` is valid; the returned `char*` is owned (free with
        // `duckdb_free`) and copied out. Out-of-range indices return null → None.
        unsafe { owned_c_string(duckdb_struct_type_child_name(self.0, index as idx_t)) }
    }

    /// The type of the `STRUCT` field at `index`.
    #[must_use]
    pub fn struct_child(
        &self,
        index: u64,
    ) -> Option<LogicalType> {
        // SAFETY: owned handle wrapped in RAII; null (out of range) → None.
        LogicalType::from_raw(unsafe { duckdb_struct_type_child_type(self.0, index as idx_t) }).ok()
    }

    /// The number of members in a `UNION`.
    #[must_use]
    pub fn union_member_count(&self) -> u64 {
        // SAFETY: `self.0` is valid; meaningful only for UNION types.
        unsafe { duckdb_union_type_member_count(self.0) as u64 }
    }

    /// The name of the `UNION` member at `index`.
    #[must_use]
    pub fn union_member_name(
        &self,
        index: u64,
    ) -> Option<String> {
        // SAFETY: owned `char*` (free with `duckdb_free`), copied out; null → None.
        unsafe { owned_c_string(duckdb_union_type_member_name(self.0, index as idx_t)) }
    }

    /// The type of the `UNION` member at `index`.
    #[must_use]
    pub fn union_member(
        &self,
        index: u64,
    ) -> Option<LogicalType> {
        // SAFETY: owned handle wrapped in RAII; null (out of range) → None.
        LogicalType::from_raw(unsafe { duckdb_union_type_member_type(self.0, index as idx_t) }).ok()
    }

    /// Reads the ENUM dictionary as an ordered list of labels.
    #[must_use]
    pub fn enum_dictionary(&self) -> Vec<String> {
        // SAFETY: `self.0` is a valid logical type owned by `self`.
        unsafe { enum_dictionary_of(self.0) }
    }

    /// The physical storage type DuckDB uses for this ENUM's index
    /// (`UTINYINT`/`USMALLINT`/`UINTEGER`), which is authoritative — it is not
    /// always the narrowest type the dictionary size would allow.
    #[must_use]
    pub fn enum_internal_type(&self) -> duckdb_type {
        // SAFETY: `self.0` is a valid logical type; meaningful only for ENUM types.
        unsafe { duckdb_enum_internal_type(self.0) }
    }

    /// Reads the ENUM dictionary from a logical-type handle that this
    /// `LogicalType` does **not** own — e.g. the borrowed handle
    /// `duckdb_get_value_type` returns, which must not be destroyed.
    ///
    /// Copies every label out; the handle itself is neither retained nor freed.
    #[must_use]
    pub(crate) fn enum_dictionary_of_borrowed(borrowed: duckdb_logical_type) -> Vec<String> {
        if borrowed.is_null() {
            return Vec::new();
        }
        // SAFETY: `borrowed` is a valid (non-null) logical type; we only read from it
        // and never destroy it, honouring `duckdb_get_value_type`'s ownership rule.
        unsafe { enum_dictionary_of(borrowed) }
    }

    /// Materialises the full, recursive [`TypeInfo`] descriptor for this type.
    ///
    /// Preserves DECIMAL width/scale, ENUM dictionary, LIST/ARRAY child types,
    /// ARRAY size, MAP key/value, and ordered STRUCT/UNION fields — recursively,
    /// so an arbitrarily nested schema is described without loss.
    #[must_use]
    pub fn describe(&self) -> TypeInfo {
        let id = self.type_id();
        match id {
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                // SAFETY: `self.0` is a DECIMAL logical type.
                let width = unsafe { decimal_width(self.0) };
                // SAFETY: as above.
                let scale = unsafe { decimal_scale(self.0) };
                TypeInfo::Decimal { width, scale }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_ENUM => TypeInfo::Enum(self.enum_dictionary()),
            DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
                TypeInfo::List(Box::new(describe_or_scalar(self.list_child(), id)))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => TypeInfo::Array {
                child: Box::new(describe_or_scalar(self.array_child(), id)),
                size: self.array_size(),
            },
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => TypeInfo::Map {
                key: Box::new(describe_or_scalar(self.map_key(), id)),
                value: Box::new(describe_or_scalar(self.map_value(), id)),
            },
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                let n = self.struct_child_count();
                let mut fields = Vec::with_capacity(n as usize);
                for i in 0..n {
                    fields.push((
                        self.struct_child_name(i).unwrap_or_default(),
                        describe_or_scalar(self.struct_child(i), id),
                    ));
                }
                TypeInfo::Struct(fields)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UNION => {
                let n = self.union_member_count();
                let mut members = Vec::with_capacity(n as usize);
                for i in 0..n {
                    members.push((
                        self.union_member_name(i).unwrap_or_default(),
                        describe_or_scalar(self.union_member(i), id),
                    ));
                }
                TypeInfo::Union(members)
            },
            scalar => TypeInfo::Scalar(scalar),
        }
    }
}

/// Describes an optional child type, falling back to a scalar of `parent_id` when
/// DuckDB unexpectedly returns no child (malformed/unknown nested type).
fn describe_or_scalar(
    child: Option<LogicalType>,
    parent_id: duckdb_type,
) -> TypeInfo {
    child.map_or(TypeInfo::Scalar(parent_id), |c| c.describe())
}

/// Reads an ENUM logical type's ordered dictionary of labels.
///
/// # Safety
///
/// `lt` must be a valid ENUM `duckdb_logical_type`. The handle is only read, never
/// destroyed, so it is valid for both owned and borrowed handles.
unsafe fn enum_dictionary_of(lt: duckdb_logical_type) -> Vec<String> {
    // SAFETY: `lt` is a valid ENUM logical type per the contract.
    let size = unsafe { duckdb_enum_dictionary_size(lt) } as u64;
    let mut labels = Vec::with_capacity(size as usize);
    for i in 0..size {
        // SAFETY: `i` is within [0, size); `duckdb_enum_dictionary_value` returns an
        // owned `char*` (free with `duckdb_free`) that `owned_c_string` copies + frees.
        let label = unsafe { owned_c_string(duckdb_enum_dictionary_value(lt, i as idx_t)) };
        labels.push(label.unwrap_or_default());
    }
    labels
}

/// Copies a DuckDB-owned `char*` into an owned `String`, freeing the original
/// with `duckdb_free`. Returns `None` for a null pointer.
///
/// # Safety
///
/// `ptr` must be either null or a `char*` that DuckDB allocated and that the
/// caller is responsible for freeing with `duckdb_free`.
unsafe fn owned_c_string(ptr: *mut std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is a valid, non-null null-terminated C string per the contract.
    let owned = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    // SAFETY: `ptr` was allocated by DuckDB and ownership was transferred to us.
    unsafe { duckdb_free(ptr as *mut c_void) };
    Some(owned)
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: `self.0` is a valid, non-null `duckdb_logical_type` owned by
            // this `LogicalType` and not yet destroyed (guarded above);
            // `duckdb_destroy_logical_type` is called exactly once here.
            unsafe { duckdb_destroy_logical_type(&mut self.0) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::{
        duckdb_create_array_type, duckdb_create_list_type, duckdb_create_logical_type,
        duckdb_create_map_type, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    };
    use crate::types::decimal::DuckDecimal;

    /// A `LogicalType` for a fixed scalar id, via the FFI constructor.
    fn scalar(id: duckdb_type) -> LogicalType {
        // SAFETY: `id` is a valid duckdb_type constant; the constructor returns an
        // owned handle wrapped in RAII.
        LogicalType::from_raw(unsafe { duckdb_create_logical_type(id) }).unwrap()
    }

    #[test]
    fn scalar_describe_reports_the_type_id() {
        let lt = LogicalType::of::<i32>().unwrap();
        assert_eq!(lt.describe(), TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER));
    }

    #[test]
    fn null_handle_is_rejected() {
        assert!(LogicalType::from_raw(std::ptr::null_mut()).is_err());
    }

    #[test]
    fn set_alias_round_trips_and_rejects_interior_nul() {
        let mut lt = LogicalType::of::<i32>().unwrap();
        assert_eq!(lt.alias(), None, "a fresh scalar type has no alias");
        lt.set_alias("my_int").unwrap();
        assert_eq!(lt.alias().as_deref(), Some("my_int"));
        // An interior NUL is rejected before the FFI call.
        assert!(lt.set_alias("bad\0alias").is_err());
    }

    #[test]
    fn decimal_describe_preserves_width_and_scale() {
        let lt = LogicalType::from_raw(DuckDecimal::new(0, 18, 4).unwrap().logical_type().unwrap())
            .unwrap();
        assert_eq!(lt.describe(), TypeInfo::Decimal { width: 18, scale: 4 });
    }

    #[test]
    fn list_describe_recurses_into_child() {
        // LIST(INTEGER) built via the FFI constructor (consumes the child handle).
        let child = LogicalType::of::<i32>().unwrap();
        // SAFETY: `child` is a valid logical type; create_list_type copies it.
        let list =
            LogicalType::from_raw(unsafe { duckdb_create_list_type(child.as_raw()) }).unwrap();
        drop(child);
        assert_eq!(
            list.describe(),
            TypeInfo::List(Box::new(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER)))
        );
    }

    #[test]
    fn array_describe_preserves_child_and_size() {
        let child = scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER);
        // SAFETY: `child` valid; create_array_type copies it; size 5.
        let arr =
            LogicalType::from_raw(unsafe { duckdb_create_array_type(child.as_raw(), 5) }).unwrap();
        assert_eq!(
            arr.describe(),
            TypeInfo::Array {
                child: Box::new(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER)),
                size: 5,
            }
        );
    }

    #[test]
    fn map_describe_preserves_key_and_value() {
        let key = scalar(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        let val = LogicalType::from_raw(DuckDecimal::new(0, 6, 2).unwrap().logical_type().unwrap())
            .unwrap();
        // SAFETY: both valid; create_map_type copies them.
        let map =
            LogicalType::from_raw(unsafe { duckdb_create_map_type(key.as_raw(), val.as_raw()) })
                .unwrap();
        assert_eq!(
            map.describe(),
            TypeInfo::Map {
                key: Box::new(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)),
                // The nested DECIMAL keeps its declared precision through the tree.
                value: Box::new(TypeInfo::Decimal { width: 6, scale: 2 }),
            }
        );
    }

    #[test]
    fn nested_struct_and_union_round_trip_through_real_duckdb() {
        use crate::connection::Connection;
        let mut conn = Connection::open_in_memory().unwrap();
        // A STRUCT with a nested DECIMAL and a LIST, plus a UNION column: build the
        // columns, then read their logical types back off the result and describe.
        conn.execute_batch(
            "CREATE TABLE t AS SELECT \
               {'amount': CAST(1.50 AS DECIMAL(6,2)), 'tags': ['a','b']} AS s, \
               union_value(num := 7) AS u",
        )
        .unwrap();

        // Pull column logical types straight off a fresh result via the raw FFI
        // (the safe wrapper for this is column_logical_type; here we exercise describe()).
        let sql = std::ffi::CString::new("SELECT s, u FROM t").unwrap();
        // SAFETY: valid open connection; zeroed result is the correct init state.
        unsafe {
            let mut result = std::mem::zeroed::<crate::ffi::duckdb_result>();
            assert_eq!(
                crate::ffi::duckdb_query(conn.db().handle(), sql.as_ptr(), &mut result),
                crate::ffi::DuckDBSuccess
            );
            let s_ty =
                LogicalType::from_raw(crate::ffi::duckdb_column_logical_type(&mut result, 0))
                    .unwrap();
            let u_ty =
                LogicalType::from_raw(crate::ffi::duckdb_column_logical_type(&mut result, 1))
                    .unwrap();

            match s_ty.describe() {
                TypeInfo::Struct(fields) => {
                    let map: std::collections::HashMap<_, _> = fields.into_iter().collect();
                    assert_eq!(map["amount"], TypeInfo::Decimal { width: 6, scale: 2 });
                    assert_eq!(
                        map["tags"],
                        TypeInfo::List(Box::new(TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)))
                    );
                },
                other => panic!("expected STRUCT, got {other:?}"),
            }
            assert!(
                matches!(u_ty.describe(), TypeInfo::Union(members) if !members.is_empty()),
                "expected a non-empty UNION description"
            );
            crate::ffi::duckdb_destroy_result(&mut result);
        }
    }
}
