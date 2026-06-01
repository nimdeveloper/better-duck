use std::borrow::Cow;

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::Text,
};

use crate::backend::DuckDb;

/// Implementation of `FromSql` for string values
impl FromSql<Text, DuckDb> for String {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Text(v) => Ok(v.to_string()),
            // DuckValueRef::Varchar(v) => Ok(v.to_owned()),
            _ => Err("Unexpected data for string type".into()),
        }
    }
}

/// Implementation of `ToSql` for string values
impl ToSql<Text, DuckDb> for &str {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Text(Cow::Borrowed(self)));
        Ok(IsNull::No)
    }
}

/// Implementation of `ToSql` for string values
impl ToSql<Text, DuckDb> for String {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Text(Cow::Owned(self.clone())));
        Ok(IsNull::No)
    }
}
