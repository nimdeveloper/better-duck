//! `FromSql`/`ToSql` implementation for DuckDB `BIT` / Diesel `DuckBit`.

use better_duck_core::types::{bit::DuckBit as CoreBit, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckBit;

/// Deserialize a DuckDB `BIT` column into a [`CoreBit`].
impl FromSql<DuckBit, DuckDb> for CoreBit {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Bit(v) => Ok(v),
            other => Err(format!("expected Bit, got {other:?}").into()),
        }
    }
}

/// Serialize a [`CoreBit`] as a DuckDB `BIT` bind parameter.
impl ToSql<DuckBit, DuckDb> for CoreBit {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Bit(self.clone()));
        Ok(IsNull::No)
    }
}
