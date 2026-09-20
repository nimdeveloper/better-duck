use crate::backend::DuckDb;
use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{BigInt, Double, Float, Integer, SmallInt},
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use crate::types::duckdb_types::{
    DuckHugeInt, DuckTinyInt, DuckUBigInt, DuckUHugeInt, DuckUInt, DuckUSmallInt, DuckUTinyInt,
};

macro_rules! impl_numeric_sql {
    ($rust_ty:ty, $sql_ty:ty, $duck_enum:ident) => {
        /// Implementation of `FromSql` for numeric values.
        impl FromSql<$sql_ty, DuckDb> for $rust_ty {
            fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
                match val {
                    DuckValueRef::$duck_enum(v) => Ok(v),
                    _ => Err("Unexpected data for numeric type".into()),
                }
            }
        }
        /// Implementation of `ToSql` for numeric values.
        impl ToSql<$sql_ty, DuckDb> for $rust_ty {
            fn to_sql<'b>(
                &'b self,
                out: &mut Output<'b, '_, DuckDb>,
            ) -> serialize::Result {
                out.set_value(DuckValueRef::$duck_enum(*self));
                Ok(IsNull::No)
            }
        }
    };
}

impl_numeric_sql!(i8, DuckTinyInt, TinyInt);
impl_numeric_sql!(u8, DuckUTinyInt, UTinyInt);
impl_numeric_sql!(i16, SmallInt, SmallInt);
impl_numeric_sql!(u16, DuckUSmallInt, USmallInt);
impl_numeric_sql!(i32, Integer, Int);
impl_numeric_sql!(u32, DuckUInt, UInt);
impl_numeric_sql!(i64, BigInt, BigInt);
impl_numeric_sql!(u64, DuckUBigInt, UBigInt);
impl_numeric_sql!(f32, Float, Float);
impl_numeric_sql!(f64, Double, Double);
impl_numeric_sql!(i128, DuckHugeInt, HugeInt);
impl_numeric_sql!(u128, DuckUHugeInt, UHugeInt);

#[cfg(feature = "decimal")]
use diesel::sql_types::Numeric;

// DECIMAL cannot use `impl_numeric_sql!`: the wire value is a metadata-preserving
// `DuckDecimal`, not a bare `rust_decimal::Decimal`, so the two directions convert
// explicitly. A DuckDB scale beyond rust_decimal's 28 surfaces as a deserialization
// error rather than a silent truncation.
#[cfg(feature = "decimal")]
impl FromSql<Numeric, DuckDb> for Decimal {
    fn from_sql(val: DuckValueRef<'_>) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Decimal(d) => Decimal::try_from(d)
                .map_err(|e| better_duck_core::error::Error::ConversionError(e).to_string().into()),
            _ => Err("Unexpected data for DECIMAL type".into()),
        }
    }
}

#[cfg(feature = "decimal")]
impl ToSql<Numeric, DuckDb> for Decimal {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Decimal((*self).into()));
        Ok(IsNull::No)
    }
}
