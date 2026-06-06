use std::borrow::Cow;

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::Text,
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckEnum;

/// Deserialize a DuckDB `VARCHAR` column into a [`String`].
///
/// DuckDB may represent the result of a `SELECT DISTINCT` (or other aggregation)
/// over a `VARCHAR` column using dictionary encoding, in which case the physical
/// type reported by the C API is `ENUM` rather than `VARCHAR`.  Both variants are
/// accepted here so that the standard Diesel `Text` SQL type works transparently
/// regardless of whether DuckDB chose to dictionary-encode the result.
impl FromSql<Text, DuckDb> for String {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Text(v) => Ok(v.into_owned()),
            // DuckDB uses dictionary encoding (ENUM physical type) for DISTINCT /
            // aggregate results on VARCHAR columns.
            DuckValueRef::Enum(v) => Ok(v.into_owned()),
            _ => Err("Unexpected data for string type".into()),
        }
    }
}

/// Serialize a `str` string slice as a DuckDB `VARCHAR` bind parameter.
///
/// Diesel's blanket `impl<A, T: ToSql<A, DB> + ?Sized, DB: Backend> ToSql<A, DB> for &T`
/// derives `&str: ToSql<Text, DuckDb>` from this impl automatically.
/// Similarly, Diesel's `impl<DB> ToSql<Text, DB> for String where str: ToSql<Text, DB>`
/// derives `String: ToSql<Text, DuckDb>`.
/// Having only the `str` impl avoids conflicting with those blankets.
impl ToSql<Text, DuckDb> for str {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Text(Cow::Borrowed(self)));
        Ok(IsNull::No)
    }
}

// ENUM

/// Deserialize a DuckDB `ENUM` column into a [`String`].
///
/// DuckDB stores enum values as dictionary-encoded strings. The decoded
/// label is exposed via `DuckValueRef::Enum(Cow<str>)`.
impl FromSql<DuckEnum, DuckDb> for String {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Enum(v) => Ok(v.into_owned()),
            _ => Err("Unexpected data for String (ENUM) type".into()),
        }
    }
}

/// Serialize a `str` slice as a DuckDB `ENUM` bind parameter.
///
/// Diesel's `&str` and `String` blanket impls derive from this `str` impl automatically.
impl ToSql<DuckEnum, DuckDb> for str {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Enum(Cow::Borrowed(self)));
        Ok(IsNull::No)
    }
}
