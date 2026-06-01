//! `FromSql`/`ToSql` implementations for DuckDB `BLOB` / Diesel `Binary`.

use std::borrow::Cow;

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::Binary,
};

use crate::backend::DuckDb;

/// Deserialize a DuckDB `BLOB` column into a [`Vec<u8>`].
impl FromSql<Binary, DuckDb> for Vec<u8> {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Blob(b) => Ok(b.into_owned()),
            other => Err(format!("expected Blob, got {other:?}").into()),
        }
    }
}

/// Serialise a `[u8]` slice as a DuckDB `BLOB` bind parameter.
impl ToSql<Binary, DuckDb> for [u8] {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Blob(Cow::Borrowed(self)));
        Ok(IsNull::No)
    }
}
