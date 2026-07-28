//! `FromSql`/`ToSql` implementation for DuckDB `BIGNUM` / Diesel `DuckBignum`.

use better_duck_core::types::{bignum::DuckBignum as CoreBignum, value_ref::DuckValueRef};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckBignum;

/// Deserialize a DuckDB `BIGNUM` column into a [`CoreBignum`].
impl FromSql<DuckBignum, DuckDb> for CoreBignum {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Bignum(v) => Ok(v),
            other => Err(format!("expected Bignum, got {other:?}").into()),
        }
    }
}

/// Serialize a [`CoreBignum`] as a DuckDB `BIGNUM` bind parameter.
impl ToSql<DuckBignum, DuckDb> for CoreBignum {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Bignum(self.clone()));
        Ok(IsNull::No)
    }
}
