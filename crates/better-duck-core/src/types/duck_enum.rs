//! Metadata-preserving `ENUM` value: [`DuckEnum`].
//!
//! DuckDB's `ENUM` is a dictionary type: the column's logical type carries an
//! ordered list of string labels, and each row stores a small integer *index*
//! into that dictionary. Representing an enum value as a bare `String` (the old
//! `DuckValue::Enum(String)`) throws the dictionary away, so a read→write round
//! trip could only produce a `VARCHAR`, silently changing the column type.
//!
//! [`DuckEnum`] keeps both the ordered dictionary and the selected index, so it
//! round-trips as a real `ENUM` via `duckdb_create_enum_type` +
//! `duckdb_create_enum_value`.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::sync::Arc;

use crate::{
    error::{DuckDBConversionError, Result},
    ffi::{
        duckdb_create_enum_type, duckdb_create_enum_value, duckdb_destroy_logical_type,
        duckdb_get_enum_value, duckdb_logical_type, duckdb_value, idx_t,
    },
    types::{appendable::AppendAble, DuckDialect, DuckLogicalType},
};

use super::value::DuckValue;

/// A DuckDB `ENUM` value: an ordered dictionary of labels plus the selected index.
///
/// The numeric value is `index`, naming `dict[index]`. The dictionary is shared
/// (`Arc`) so cloning a value — or many rows of the same enum column — does not
/// re-allocate the labels.
#[derive(Debug, Clone)]
pub struct DuckEnum {
    /// Ordered dictionary of labels; `dict[i]` is the label for index `i`.
    dict: Arc<[String]>,
    /// Selected member: an index into `dict`.
    index: u64,
}

impl DuckEnum {
    /// Creates a `DuckEnum` from an ordered dictionary and a selected index.
    ///
    /// # Errors
    ///
    /// [`DuckDBConversionError::ConversionError`] if `dict` is empty or `index` is
    /// out of range for it.
    pub fn new(
        dict: Arc<[String]>,
        index: u64,
    ) -> Result<DuckEnum, DuckDBConversionError> {
        if dict.is_empty() {
            return Err(DuckDBConversionError::ConversionError(
                "ENUM dictionary must not be empty".to_owned(),
            ));
        }
        if index as usize >= dict.len() {
            return Err(DuckDBConversionError::ConversionError(format!(
                "ENUM index {index} out of range (dictionary size {})",
                dict.len()
            )));
        }
        Ok(DuckEnum { dict, index })
    }

    /// Builds a `DuckEnum` from an ordered list of labels and a label to select.
    ///
    /// # Errors
    ///
    /// Returns an error if `dict` is empty or does not contain `label`.
    pub fn from_label(
        dict: Arc<[String]>,
        label: &str,
    ) -> Result<DuckEnum, DuckDBConversionError> {
        let index = dict.iter().position(|l| l == label).ok_or_else(|| {
            DuckDBConversionError::ConversionError(format!("'{label}' is not a member of the ENUM"))
        })?;
        DuckEnum::new(dict, index as u64)
    }

    /// The selected label.
    #[must_use]
    pub fn label(&self) -> &str {
        // `index` is validated in range on construction.
        &self.dict[self.index as usize]
    }

    /// The selected index into the dictionary.
    #[must_use]
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The ordered dictionary of labels.
    #[must_use]
    pub fn dictionary(&self) -> &[String] {
        &self.dict
    }

    /// Builds the DuckDB `ENUM` logical type for this value's dictionary.
    ///
    /// The caller must destroy the returned type with `duckdb_destroy_logical_type`.
    ///
    /// # Errors
    ///
    /// Returns an error if a label contains an interior nul, or DuckDB rejects the
    /// dictionary.
    pub(crate) fn logical_type(&self) -> Result<duckdb_logical_type, DuckDBConversionError> {
        // DuckDB copies the names, but needs an array of `*const c_char` pointing at
        // null-terminated strings. Build owned CStrings, then a pointer array over them.
        let c_labels: Vec<std::ffi::CString> = self
            .dict
            .iter()
            .map(|l| {
                std::ffi::CString::new(l.as_str())
                    .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let mut ptrs: Vec<*const std::os::raw::c_char> =
            c_labels.iter().map(|c| c.as_ptr()).collect();
        // SAFETY: `ptrs` holds `c_labels.len()` valid null-terminated pointers that
        // outlive the call; DuckDB copies the names into the new logical type.
        let lt = unsafe { duckdb_create_enum_type(ptrs.as_mut_ptr(), ptrs.len() as idx_t) };
        if lt.is_null() {
            return Err(DuckDBConversionError::ConversionError(
                "DuckDB rejected the ENUM dictionary".to_owned(),
            ));
        }
        Ok(lt)
    }
}

// Equality/hash are by (dictionary, index) — two enum values are equal only if
// they share the same dictionary and select the same member.
impl PartialEq for DuckEnum {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.index == other.index && self.dict == other.dict
    }
}
impl Eq for DuckEnum {}
impl std::hash::Hash for DuckEnum {
    fn hash<H: std::hash::Hasher>(
        &self,
        state: &mut H,
    ) {
        self.dict.hash(state);
        self.index.hash(state);
    }
}

impl DuckDialect for DuckEnum {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // A standalone `duckdb_value` of ENUM type carries its index; the dictionary
        // comes from the value's logical type. Read both.
        // SAFETY: `value` is a valid duckdb_value of ENUM type.
        let index = unsafe { duckdb_get_enum_value(value) };
        // SAFETY: `value` is valid; `duckdb_get_value_type` returns an owned logical
        // type wrapped in RAII and described below.
        let dict = read_dictionary_of_value(value)?;
        DuckEnum::new(dict, index)
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let mut lt = self.logical_type()?;
        // SAFETY: `lt` is a valid ENUM logical type; `self.index` is in range. DuckDB
        // copies what it needs, so we destroy `lt` afterwards.
        let value = unsafe { duckdb_create_enum_value(lt, self.index) };
        // SAFETY: `lt` was allocated by `logical_type`; destroy exactly once.
        unsafe { duckdb_destroy_logical_type(&mut lt) };
        if value.is_null() {
            return Err(DuckDBConversionError::ConversionError(
                "DuckDB rejected the ENUM value".to_owned(),
            ));
        }
        Ok(value)
    }
}

impl DuckLogicalType for DuckEnum {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // A bare `DuckEnum` type has no fixed dictionary; a specific value's type
        // comes from `DuckEnum::logical_type`. There is no meaningful default ENUM,
        // so this is an error (used only by empty typed collections, which an ENUM
        // element should not hit in practice).
        Err(DuckDBConversionError::ConversionError(
            "ENUM has no fixed logical type without a dictionary".to_owned(),
        ))
    }
}

impl AppendAble for DuckEnum {
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> Result<()> {
        let dv = self.to_duck().map_err(crate::error::Error::ConversionError)?;
        // SAFETY: `appender` valid; `dv` is an owned value appended and destroyed here.
        unsafe { crate::types::appendable::append_owned_value(appender, dv) }
    }

    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> Result<()> {
        let dv = self.to_duck().map_err(crate::error::Error::ConversionError)?;
        // SAFETY: `stmt`/`idx` valid; `dv` is an owned value bound and destroyed here.
        unsafe { crate::types::appendable::bind_owned_value(stmt, idx, dv) }
    }
}

impl From<DuckEnum> for DuckValue {
    fn from(e: DuckEnum) -> Self {
        DuckValue::Enum(e)
    }
}

/// Reads the ordered dictionary from a standalone ENUM `duckdb_value`'s type.
fn read_dictionary_of_value(value: duckdb_value) -> Result<Arc<[String]>, DuckDBConversionError> {
    use crate::ffi::duckdb_get_value_type;
    use crate::types::LogicalType;
    // SAFETY: `value` is a valid duckdb_value; `duckdb_get_value_type` returns a
    // *borrowed* logical type owned by `value` — must NOT be destroyed. We read the
    // dictionary out and do not take ownership.
    let borrowed = unsafe { duckdb_get_value_type(value) };
    if borrowed.is_null() {
        return Err(DuckDBConversionError::ConversionError(
            "ENUM value has no logical type".to_owned(),
        ));
    }
    // `LogicalType::describe`/`enum_dictionary` operate through the RAII wrapper,
    // which would destroy the handle on drop — but this handle is borrowed. So read
    // the dictionary via a non-owning helper instead.
    let dict = LogicalType::enum_dictionary_of_borrowed(borrowed);
    Ok(Arc::from(dict))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::types::value::DuckValue;

    fn mood() -> Arc<[String]> {
        Arc::from(["sad".to_owned(), "ok".to_owned(), "happy".to_owned()])
    }

    #[test]
    fn new_validates_dictionary_and_index() {
        assert!(DuckEnum::new(Arc::from([]), 0).is_err(), "empty dict rejected");
        assert!(DuckEnum::new(mood(), 3).is_err(), "out-of-range index rejected");
        let e = DuckEnum::new(mood(), 2).unwrap();
        assert_eq!(e.label(), "happy");
        assert_eq!(e.index(), 2);
        assert_eq!(e.dictionary().len(), 3);
    }

    #[test]
    fn from_label_resolves_index_or_errors() {
        assert_eq!(DuckEnum::from_label(mood(), "ok").unwrap().index(), 1);
        assert!(DuckEnum::from_label(mood(), "nope").is_err());
    }

    #[test]
    fn equality_is_by_dictionary_and_index() {
        assert_eq!(DuckEnum::new(mood(), 1).unwrap(), DuckEnum::new(mood(), 1).unwrap());
        assert_ne!(DuckEnum::new(mood(), 1).unwrap(), DuckEnum::new(mood(), 2).unwrap());
    }

    /// A read of an ENUM column preserves the whole dictionary + selected index,
    /// and a write of that value round-trips as a real ENUM (not VARCHAR).
    #[test]
    fn enum_column_round_trips_as_a_real_enum() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TYPE mood AS ENUM ('sad','ok','happy')").unwrap();
        conn.execute_batch("CREATE TABLE t (m mood)").unwrap();
        conn.execute_batch("INSERT INTO t VALUES ('happy'), ('sad')").unwrap();

        let mut rows = conn.execute("SELECT m FROM t ORDER BY m").unwrap();
        // ORDER BY on an ENUM orders by dictionary index: sad(0) then happy(2).
        let first = rows.next().unwrap().unwrap();
        match first.get("m").unwrap() {
            DuckValue::Enum(e) => {
                assert_eq!(e.label(), "sad");
                assert_eq!(e.index(), 0);
                assert_eq!(e.dictionary(), &["sad", "ok", "happy"]);
            },
            other => panic!("expected Enum, got {other:?}"),
        }
        drop(rows);

        // Write a DuckEnum value back into an ENUM column via the appender.
        conn.execute_batch("CREATE TABLE t2 (m mood)").unwrap();
        {
            let mut app = conn.appender("t2", "main").unwrap();
            app.append(&mut DuckValue::Enum(DuckEnum::new(mood(), 2).unwrap())).unwrap();
            app.save().unwrap();
        }
        let mut back = conn.execute("SELECT m FROM t2").unwrap();
        match back.next().unwrap().unwrap().get("m").unwrap() {
            DuckValue::Enum(e) => assert_eq!(e.label(), "happy"),
            other => panic!("expected Enum, got {other:?}"),
        }
    }

    #[test]
    fn enum_write_paths_conversions_and_logical_types() {
        use crate::connection::Connection;
        use std::hash::{Hash, Hasher};

        let e = DuckEnum::from_label(mood(), "happy").unwrap();
        // From<DuckEnum> for DuckValue + accessors.
        assert!(matches!(DuckValue::from(e.clone()), DuckValue::Enum(_)));
        assert_eq!(e.index(), 2);
        assert_eq!(e.dictionary().len(), 3);
        // Hash.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        e.hash(&mut h);
        let _ = h.finish();
        // Instance logical type builds a real ENUM type; the type-level impl errors
        // (a bare `DuckEnum` has no dictionary to build one from).
        assert!(e.logical_type().is_ok());
        assert!(<DuckEnum as DuckLogicalType>::duck_logical_type().is_err());

        let mut conn = Connection::open_in_memory().unwrap();
        // stmt_append (bind) → to_duck.
        let _ = conn.execute_with("SELECT $1", &mut [&mut e.clone()]);
        // appender_append into a real ENUM column.
        conn.execute_batch(
            "CREATE TYPE mood AS ENUM ('sad','ok','happy'); CREATE TABLE t (v mood)",
        )
        .unwrap();
        {
            let mut app = conn.appender("t", "main").unwrap();
            app.append(&mut e.clone()).unwrap();
            app.save().unwrap();
        }
    }
}
