//! `FromSql`/`ToSql` implementations for DuckDB `ARRAY` / Diesel `DuckArray`.
//!
//! The Rust mirror type is `Vec<DuckValue>`, matching `list.rs`'s `DuckList` pattern.
//! `ARRAY` and `LIST` share this mirror type but are distinct Diesel SQL types
//! (`DuckArray` vs. `DuckList`), so both impls coexist without collision.

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckArray;

// FromSql

/// Deserialize a DuckDB `ARRAY` column into a [`Vec<DuckValue>`].
impl FromSql<DuckArray, DuckDb> for Vec<DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Array(items) => Ok(items.iter().map(DuckValue::from).collect()),
            other => Err(format!("expected Array, got {other:?}").into()),
        }
    }
}

// ToSql

/// Serialize a [`Vec<DuckValue>`] as a DuckDB `ARRAY` bind parameter.
impl ToSql<DuckArray, DuckDb> for Vec<DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        let items: Box<[DuckValueRef<'b>]> = self.iter().cloned().map(DuckValueRef::from).collect();
        out.set_value(DuckValueRef::Array(items));
        Ok(IsNull::No)
    }
}
