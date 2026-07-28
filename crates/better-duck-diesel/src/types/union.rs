//! `FromSql`/`ToSql` implementations for DuckDB `UNION` / Diesel `DuckUnion`.
//!
//! The Rust mirror type is `Box<DuckValue>` — the active member's value — matching
//! `better_duck_core::types::value::DuckValue::Union` exactly. The member name and
//! tag index are not currently surfaced (see the core-level roadmap for multi-arm
//! union support).

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckUnion;

// FromSql

/// Deserialize a DuckDB `UNION` column into a [`Box<DuckValue>`] holding the
/// active member's value.
impl FromSql<DuckUnion, DuckDb> for Box<DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Union(inner) => Ok(Box::new(DuckValue::from(inner.as_ref()))),
            other => Err(format!("expected Union, got {other:?}").into()),
        }
    }
}

// ToSql

/// Serialize a [`Box<DuckValue>`] as a DuckDB `UNION` bind parameter.
impl ToSql<DuckUnion, DuckDb> for Box<DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        let inner = DuckValueRef::from((**self).clone());
        out.set_value(DuckValueRef::Union(Box::new(inner)));
        Ok(IsNull::No)
    }
}
