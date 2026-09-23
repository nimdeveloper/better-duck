//! Metadata-preserving `UNION` value: [`DuckUnion`].
//!
//! DuckDB's `UNION` is a tagged sum type: the column's logical type carries an
//! ordered set of named members (each with its own type), and each row stores a
//! small integer *tag* selecting the active member plus that member's value.
//! Representing a union value as a bare `Box<DuckValue>` (the old
//! `DuckValue::Union(Box<DuckValue>)`) threw the schema away, so a read→write
//! round trip could only produce a single-arm `UNION(value <active type>)`,
//! silently changing the column type and losing every inactive member and the
//! member names.
//!
//! [`DuckUnion`] keeps the full ordered member schema, the selected tag, and the
//! active value, so it round-trips as the *same* `UNION` via
//! [`TypeInfo::to_logical_type`] + `duckdb_create_union_value`.
// FFI pointer args are used safely inside `unsafe` blocks.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use std::sync::Arc;

use crate::{
    error::DuckDBConversionError,
    ffi::{
        duckdb_create_union_value, duckdb_destroy_value, duckdb_logical_type, duckdb_value, idx_t,
    },
    types::{DuckDialect, TypeInfo},
};

use super::value::DuckValue;

/// A DuckDB `UNION` value: the full ordered member schema, the selected tag, and
/// the active member's value.
///
/// The active member is `members[tag]`; its declared type is `members[tag].1` and
/// its name `members[tag].0`. The schema is shared (`Arc`) so cloning a value — or
/// many rows of the same union column — does not re-allocate the member list.
#[derive(Debug, Clone)]
pub struct DuckUnion {
    /// Ordered member schema; `members[i]` is the `(name, type)` of member `i`.
    members: Arc<[(String, TypeInfo)]>,
    /// The active member: an index into `members`.
    tag: u32,
    /// The active member's value.
    value: Box<DuckValue>,
}

impl DuckUnion {
    /// Creates a `DuckUnion` from a member schema, a selected tag, and the active
    /// value.
    ///
    /// # Errors
    ///
    /// [`DuckDBConversionError::ConversionError`] if `members` is empty or `tag` is
    /// out of range for it.
    pub fn new(
        members: Arc<[(String, TypeInfo)]>,
        tag: u32,
        value: DuckValue,
    ) -> Result<DuckUnion, DuckDBConversionError> {
        if members.is_empty() {
            return Err(DuckDBConversionError::ConversionError(
                "UNION must have at least one member".to_owned(),
            ));
        }
        if tag as usize >= members.len() {
            return Err(DuckDBConversionError::ConversionError(format!(
                "UNION tag {tag} out of range (member count {})",
                members.len()
            )));
        }
        Ok(DuckUnion { members, tag, value: Box::new(value) })
    }

    /// Builds a single-member `UNION(value <type>)` around `value`, deriving the
    /// member type from the value itself.
    ///
    /// Used where only the active value is available and there is no full schema to
    /// preserve — e.g. binding a bare [`DuckValue`] through Diesel. A value read from
    /// a real union column keeps its full schema via [`DuckUnion::new`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` has no DuckDB logical type.
    pub fn single(value: DuckValue) -> Result<DuckUnion, DuckDBConversionError> {
        let raw = DuckValue::logical_type_of(&value)?;
        // SAFETY: `raw` is an owned logical type handle; the RAII wrapper destroys it
        // exactly once on drop.
        let lt = crate::types::LogicalType::from_raw(raw).map_err(|_| {
            DuckDBConversionError::ConversionError("null member logical type".to_owned())
        })?;
        let ty = lt.describe();
        DuckUnion::new(Arc::from([("value".to_owned(), ty)]), 0, value)
    }

    /// The ordered member schema.
    #[must_use]
    pub fn members(&self) -> &[(String, TypeInfo)] {
        &self.members
    }

    /// The active member's tag (index into [`members`](Self::members)).
    #[must_use]
    pub fn tag(&self) -> u32 {
        self.tag
    }

    /// The active member's name.
    #[must_use]
    pub fn active_name(&self) -> &str {
        // `tag` is validated in range on construction.
        &self.members[self.tag as usize].0
    }

    /// The active member's value.
    #[must_use]
    pub fn value(&self) -> &DuckValue {
        &self.value
    }

    /// Builds the DuckDB `UNION` logical type for this value's full schema.
    ///
    /// The caller must destroy the returned handle with `duckdb_destroy_logical_type`.
    ///
    /// # Errors
    ///
    /// Returns an error if DuckDB rejects the reconstructed member types (see
    /// [`TypeInfo::to_logical_type`]).
    pub(crate) fn logical_type(&self) -> Result<duckdb_logical_type, DuckDBConversionError> {
        let lt = TypeInfo::Union(self.members.to_vec()).to_logical_type()?;
        // Transfer ownership of the raw handle to the caller: `forget` skips the RAII
        // destructor so the handle is not freed twice.
        let raw = lt.as_raw();
        std::mem::forget(lt);
        Ok(raw)
    }
}

// Equality/hash are by (schema, tag, value) — two union values are equal only if
// they share the same member schema, select the same member, and carry an equal
// active value.
impl PartialEq for DuckUnion {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        self.tag == other.tag && self.value == other.value && self.members == other.members
    }
}
impl Eq for DuckUnion {}
impl std::hash::Hash for DuckUnion {
    fn hash<H: std::hash::Hasher>(
        &self,
        state: &mut H,
    ) {
        self.members.hash(state);
        self.tag.hash(state);
        self.value.hash(state);
    }
}

impl DuckDialect for DuckUnion {
    fn from_duck(_value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // A standalone `duckdb_value` of UNION type would need the owned-value
        // introspection to recover its member schema; the column read
        // path (which has the vector's logical type) builds a `DuckUnion` directly in
        // `types::union::read_union` instead.
        Err(DuckDBConversionError::ConversionError(
            "reconstructing a standalone UNION value requires owned-value introspection".to_owned(),
        ))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // Rebuild the *full* union type (every member name + type), so the value is
        // written back as the same UNION rather than a single-arm approximation.
        // `union_lt` is RAII: an early `?` return below drops (destroys) it.
        let union_lt = TypeInfo::Union(self.members.to_vec()).to_logical_type()?;
        let mut member_dv = self.value.to_duck()?;
        // SAFETY: `union_lt` is a valid UNION type; `self.tag` is in range; `member_dv`
        // is a valid value. `duckdb_create_union_value` copies the value, so we destroy
        // `member_dv` ourselves afterwards.
        let value =
            unsafe { duckdb_create_union_value(union_lt.as_raw(), self.tag as idx_t, member_dv) };
        // SAFETY: `member_dv` was created by `to_duck` above; destroy exactly once.
        unsafe { duckdb_destroy_value(&mut member_dv) };
        // `union_lt` (RAII) is dropped here, destroying the type exactly once.
        drop(union_lt);
        if value.is_null() {
            return Err(DuckDBConversionError::ConversionError(
                "DuckDB rejected the UNION value".to_owned(),
            ));
        }
        Ok(value)
    }
}

impl From<DuckUnion> for DuckValue {
    fn from(u: DuckUnion) -> Self {
        DuckValue::Union(u)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER;
    use crate::types::value::DuckValue;

    fn schema() -> Arc<[(String, TypeInfo)]> {
        Arc::from([
            ("n".to_owned(), TypeInfo::Scalar(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER)),
            ("s".to_owned(), TypeInfo::Scalar(crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)),
        ])
    }

    #[test]
    fn new_validates_schema_and_tag() {
        assert!(DuckUnion::new(Arc::from([]), 0, DuckValue::Int(1)).is_err(), "empty schema");
        assert!(DuckUnion::new(schema(), 2, DuckValue::Int(1)).is_err(), "tag out of range");
        let u = DuckUnion::new(schema(), 1, DuckValue::text("hi")).unwrap();
        assert_eq!(u.tag(), 1);
        assert_eq!(u.active_name(), "s");
        assert_eq!(u.value(), &DuckValue::text("hi"));
        assert_eq!(u.members().len(), 2);
    }

    #[test]
    fn equality_is_by_schema_tag_and_value() {
        let a = DuckUnion::new(schema(), 0, DuckValue::Int(7)).unwrap();
        let b = DuckUnion::new(schema(), 0, DuckValue::Int(7)).unwrap();
        let c = DuckUnion::new(schema(), 1, DuckValue::text("x")).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// A read of a multi-member UNION column preserves the whole member schema +
    /// selected tag, and a write of that value round-trips as the same UNION (not a
    /// single-arm approximation).
    #[test]
    fn union_column_round_trips_with_full_schema() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (u UNION(n INTEGER, s VARCHAR))").unwrap();
        conn.execute_batch("INSERT INTO t VALUES (union_value(s := 'duck'))").unwrap();

        let mut rows = conn.execute("SELECT u FROM t").unwrap();
        let first = rows.next().unwrap().unwrap();
        let read = match first.get("u").unwrap() {
            DuckValue::Union(u) => u.clone(),
            other => panic!("expected Union, got {other:?}"),
        };
        // Full schema preserved: both members, in order, with names + types.
        assert_eq!(read.members().len(), 2);
        assert_eq!(read.members()[0].0, "n");
        assert_eq!(read.members()[1].0, "s");
        assert_eq!(read.active_name(), "s");
        assert_eq!(read.value(), &DuckValue::text("duck"));
        drop(rows);

        // Write the DuckUnion back into a fresh UNION column via the appender; it must
        // be accepted as the same 2-member union and read back identically.
        conn.execute_batch("CREATE TABLE t2 (u UNION(n INTEGER, s VARCHAR))").unwrap();
        {
            let mut app = conn.appender("t2", "main").unwrap();
            app.append(&mut DuckValue::Union(read)).unwrap();
            app.save().unwrap();
        }
        let mut back = conn.execute("SELECT u FROM t2").unwrap();
        match back.next().unwrap().unwrap().get("u").unwrap() {
            DuckValue::Union(u) => {
                assert_eq!(u.active_name(), "s");
                assert_eq!(u.value(), &DuckValue::text("duck"));
                assert_eq!(u.members().len(), 2);
            },
            other => panic!("expected Union, got {other:?}"),
        }
    }
}
