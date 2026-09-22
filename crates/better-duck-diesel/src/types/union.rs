//! `FromSql`/`ToSql` implementations for DuckDB `UNION` / Diesel `DuckUnion`.
//!
//! Two Rust mirrors are supported:
//!
//! * [`better_duck_core::types::DuckUnion`] — the full, metadata-preserving value:
//!   the whole member schema, the selected tag, and the active value. A value read
//!   from a union column and bound back reproduces the *same* multi-arm union.
//! * [`Box<DuckValue>`] — the active member's value only, kept for ergonomics. On
//!   read it yields the active value; on write it builds a single-member
//!   `UNION(value <type>)`, since a bare value carries no schema to preserve.

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef, DuckUnion as CoreUnion};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckUnion;

// FromSql — full value

/// Deserialize a DuckDB `UNION` column into a metadata-preserving
/// [`CoreUnion`](better_duck_core::types::DuckUnion).
impl FromSql<DuckUnion, DuckDb> for CoreUnion {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Union(u) => Ok(u.into_owned()),
            other => Err(format!("expected Union, got {other:?}").into()),
        }
    }
}

// ToSql — full value

/// Serialize a metadata-preserving [`CoreUnion`](better_duck_core::types::DuckUnion)
/// as a DuckDB `UNION` bind parameter, reproducing its full member schema.
impl ToSql<DuckUnion, DuckDb> for CoreUnion {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Union(std::borrow::Cow::Owned(self.clone())));
        Ok(IsNull::No)
    }
}

// FromSql — active member only

/// Deserialize a DuckDB `UNION` column into a [`Box<DuckValue>`] holding the
/// active member's value.
impl FromSql<DuckUnion, DuckDb> for Box<DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Union(u) => Ok(Box::new(u.value().clone())),
            other => Err(format!("expected Union, got {other:?}").into()),
        }
    }
}

// ToSql — active member only

/// Serialize a [`Box<DuckValue>`] as a DuckDB `UNION` bind parameter. With only the
/// active value available, this builds a single-member `UNION(value <type>)`.
impl ToSql<DuckUnion, DuckDb> for Box<DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        let union = CoreUnion::single((**self).clone()).map_err(|e| format!("{e:?}"))?;
        out.set_value(DuckValueRef::Union(std::borrow::Cow::Owned(union)));
        Ok(IsNull::No)
    }
}
