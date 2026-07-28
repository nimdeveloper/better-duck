//! `FromSql`/`ToSql` implementation for DuckDB `UUID` / Diesel `DuckUuid`.

use better_duck_core::types::{uuid::DuckUuid as CoreUuid, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckUuid;

/// Deserialize a DuckDB `UUID` column into a [`CoreUuid`].
impl FromSql<DuckUuid, DuckDb> for CoreUuid {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Uuid(v) => Ok(v),
            other => Err(format!("expected Uuid, got {other:?}").into()),
        }
    }
}

/// Serialize a [`CoreUuid`] as a DuckDB `UUID` bind parameter.
impl ToSql<DuckUuid, DuckDb> for CoreUuid {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Uuid(*self));
        Ok(IsNull::No)
    }
}
