//! Local newtype wrappers for binding and loading DuckDB-specific-typed values
//! through the Diesel DSL.
//!
//! Diesel's blanket `impl<T: Expression> AsExpression<T::SqlType> for T` makes it
//! impossible for a *third-party* crate to implement `AsExpression` for a foreign
//! value type (e.g. `i128`) at a custom SQL type — coherence rejects it (see the
//! crate's design notes). These thin **local** newtypes sidestep that: because they
//! are defined here, `#[derive(AsExpression, FromSqlRow)]` applies, so wrapping a
//! value lets it be used as an `.eq(…)` / `.values(…)` / function argument and
//! loaded back through the standard `Queryable` path.
//!
//! Only the widths that map to a *custom* `Duck*` SQL type need a wrapper — the
//! standard-typed widths (`i16`/`i32`/`i64`/`f32`/`f64`) already work directly.
//!
//! The `#[derive]` + `#[diesel(…)]` must sit directly on each struct: a derive's
//! `#[diesel]` helper attribute is not recognised when generated from inside a
//! `macro_rules!`, so only the `From`/`ToSql`/`FromSql` impls are macro-generated.

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::deserialize::{self, FromSql, FromSqlRow};
use diesel::expression::AsExpression;
use diesel::serialize::{self, Output, ToSql};

use crate::backend::DuckDb;
use crate::sql_types;

/// Generates the `From` conversions and `ToSql`/`FromSql` delegation for a value
/// newtype (the derives sit on the struct itself, above the invocation).
macro_rules! impl_value_newtype {
    ($name:ident($inner:ty) => $sql:ty) => {
        impl ::core::convert::From<$inner> for $name {
            fn from(value: $inner) -> Self {
                $name(value)
            }
        }
        impl ::core::convert::From<$name> for $inner {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl ToSql<$sql, DuckDb> for $name {
            fn to_sql<'b>(
                &'b self,
                out: &mut Output<'b, '_, DuckDb>,
            ) -> serialize::Result {
                <$inner as ToSql<$sql, DuckDb>>::to_sql(&self.0, out)
            }
        }
        impl FromSql<$sql, DuckDb> for $name {
            fn from_sql(value: DuckValueRef<'_>) -> deserialize::Result<Self> {
                <$inner as FromSql<$sql, DuckDb>>::from_sql(value).map($name)
            }
        }
    };
}

/// A `TINYINT` value (`i8`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckTinyInt)]
pub struct TinyInt(pub i8);
impl_value_newtype!(TinyInt(i8) => sql_types::DuckTinyInt);

/// A `UTINYINT` value (`u8`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckUTinyInt)]
pub struct UTinyInt(pub u8);
impl_value_newtype!(UTinyInt(u8) => sql_types::DuckUTinyInt);

/// A `USMALLINT` value (`u16`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckUSmallInt)]
pub struct USmallInt(pub u16);
impl_value_newtype!(USmallInt(u16) => sql_types::DuckUSmallInt);

/// A `UINTEGER` value (`u32`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckUInt)]
pub struct UInt(pub u32);
impl_value_newtype!(UInt(u32) => sql_types::DuckUInt);

/// A `UBIGINT` value (`u64`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckUBigInt)]
pub struct UBigInt(pub u64);
impl_value_newtype!(UBigInt(u64) => sql_types::DuckUBigInt);

/// A `HUGEINT` value (`i128`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckHugeInt)]
pub struct HugeInt(pub i128);
impl_value_newtype!(HugeInt(i128) => sql_types::DuckHugeInt);

/// A `UHUGEINT` value (`u128`) usable in the Diesel DSL.
#[derive(AsExpression, FromSqlRow, Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[diesel(sql_type = sql_types::DuckUHugeInt)]
pub struct UHugeInt(pub u128);
impl_value_newtype!(UHugeInt(u128) => sql_types::DuckUHugeInt);
