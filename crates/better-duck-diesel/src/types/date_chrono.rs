// Chrono implementations (feature-gated)

use better_duck_core::types::value_ref::DuckValueRef;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{Date, Interval, Time, Timestamp},
};

use crate::backend::DuckDb;

/// Implementation of `FromSql` for `NaiveDate`
impl FromSql<Date, DuckDb> for NaiveDate {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Date(v) => Ok(v),
            _ => Err("Unexpected data for NaiveDate type".into()),
        }
    }
}
/// Implementation of `ToSql` for `NaiveDate`
impl ToSql<Date, DuckDb> for NaiveDate {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Date(*self));
        Ok(IsNull::No)
    }
}

/// Implementation of `FromSql` for `NaiveTime`
impl FromSql<Time, DuckDb> for NaiveTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Time(v) => Ok(v),
            _ => Err("Unexpected data for NaiveTime type".into()),
        }
    }
}
/// Implementation of `ToSql` for `NaiveTime`
impl ToSql<Time, DuckDb> for NaiveTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Time(*self));
        Ok(IsNull::No)
    }
}
/// Implementation of `FromSql` for `NaiveDateTime`
impl FromSql<Timestamp, DuckDb> for NaiveDateTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Timestamp(v) => Ok(v),
            _ => Err("Unexpected data for NaiveDateTime type".into()),
        }
    }
}
/// Implementation of `ToSql` for `NaiveDateTime`
impl ToSql<Timestamp, DuckDb> for NaiveDateTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Timestamp(*self));
        Ok(IsNull::No)
    }
}

/// Implementation of `FromSql` for `Duration`
impl FromSql<Interval, DuckDb> for Duration {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Interval(v) => Ok(v),
            _ => Err("Unexpected data for Duration type".into()),
        }
    }
}
/// Implementation of `ToSql` for `Duration`
impl ToSql<Interval, DuckDb> for Duration {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Interval(*self));
        Ok(IsNull::No)
    }
}
