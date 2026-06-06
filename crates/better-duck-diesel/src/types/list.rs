//! `FromSql`/`ToSql` implementations for DuckDB `LIST` / Diesel `DuckList`.
//!
//! The Rust mirror type is `Vec<DuckValue>`, which lets callers work with any
//! element type that DuckDB supports without needing separate monomorphisations
//! for every element variety. Element conversion is handled by
//! [`DuckValue::from`] / [`DuckValueRef::from`].

use better_duck_core::types::{value::DuckValue, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckList;

// FromSql

/// Deserialize a DuckDB `LIST` column into a [`Vec<DuckValue>`].
///
/// Each element in the list is converted to its owned [`DuckValue`]
/// representation, preserving `NULL` elements as [`DuckValue::Null`].
impl FromSql<DuckList, DuckDb> for Vec<DuckValue> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            // `From<&DuckValueRef<'_>> for DuckValue` converts each element.
            DuckValueRef::List(items) => Ok(items.iter().map(DuckValue::from).collect()),
            other => Err(format!("expected List, got {other:?}").into()),
        }
    }
}

// ToSql

/// Serialize a [`Vec<DuckValue>`] as a DuckDB `LIST` bind parameter.
///
/// Each element is converted to a zero-copy (where possible) `DuckValueRef`
/// for the lifetime `'b` tied to the `self` borrow.
impl ToSql<DuckList, DuckDb> for Vec<DuckValue> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        // Clone each element into an owned DuckValue, then convert to a fully-owned
        // DuckValueRef (all Cow branches become Cow::Owned, so the lifetime is 'static
        // and satisfies the 'b bound on the output).
        let items: Vec<DuckValueRef<'b>> = self.iter().cloned().map(DuckValueRef::from).collect();
        out.set_value(DuckValueRef::List(items));
        Ok(IsNull::No)
    }
}
