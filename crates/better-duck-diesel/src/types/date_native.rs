use std::time::SystemTime;

use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::ToSql,
    sql_types::Date,
};

use crate::backend::DuckDb;

impl FromSql<Date, DuckDb> for SystemTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Date(v) => Ok(v),
            _ => Err("Unexpected data for date type".into()),
        }
    }
}
impl ToSql<Date, DuckDb> for SystemTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, DuckDb>,
    ) -> diesel::serialize::Result {
        out.set_value(DuckValueRef::Date(*self));
        Ok(diesel::serialize::IsNull::No)
    }
}

// StdDuration
// SystemTime
