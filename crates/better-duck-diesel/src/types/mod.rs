//! DuckDB-specific SQL types and `FromSql`/`ToSql` implementations.

pub mod duckdb_types;
pub use duckdb_types::*;

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::deserialize::{self, FromSql};
pub use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::sql_types::Bool;

use crate::backend::DuckDb;

/// Chrono `FromSql`/`ToSql` implementations for date/time types.
#[cfg(feature = "chrono")]
pub mod date_chrono;

// TODO: enable date_native for non-chrono date/time FromSql/ToSql (currently requires chrono)
// #[cfg(not(feature = "chrono"))]
// pub mod date_native;

/// `FromSql`/`ToSql` implementations for `BLOB` / `Binary`.
pub mod binary;
/// `FromSql`/`ToSql` implementations for DuckDB `LIST`.
pub mod list;
/// `FromSql`/`ToSql` implementations for all numeric types.
pub mod numeric;
/// `FromSql`/`ToSql` implementations for `VARCHAR` / `Text`.
pub mod varchar;

/// Implementation of `FromSql` for bool values.
impl FromSql<Bool, DuckDb> for bool {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Boolean(v) => Ok(v),
            _ => Err("Unexpected data for boolean type".into()),
        }
    }
}

/// Implementation of `ToSql` for bool values.
impl ToSql<Bool, DuckDb> for bool {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Boolean(*self));
        Ok(IsNull::No)
    }
}
