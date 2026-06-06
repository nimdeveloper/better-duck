// Chrono implementations (feature-gated)

use better_duck_core::types::date_chrono::TimeTz as CoreTimeTz;
use better_duck_core::types::value_ref::DuckValueRef;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{Date, Interval, Time, Timestamp},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::{DuckTimeNs, DuckTimeTz, DuckTimestamptz};

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

// TIMESTAMPTZ

/// Deserialize a DuckDB `TIMESTAMP_TZ` column into a [`DateTime<Utc>`].
impl FromSql<DuckTimestamptz, DuckDb> for DateTime<Utc> {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimestampTz(v) => Ok(v),
            _ => Err("Unexpected data for DateTime<Utc> (TIMESTAMPTZ) type".into()),
        }
    }
}

/// Serialize a [`DateTime<Utc>`] as a DuckDB `TIMESTAMP_TZ` bind parameter.
impl ToSql<DuckTimestamptz, DuckDb> for DateTime<Utc> {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimestampTz(*self));
        Ok(IsNull::No)
    }
}

// TIME_TZ

/// Deserialize a DuckDB `TIME_TZ` column into a [`CoreTimeTz`].
impl FromSql<DuckTimeTz, DuckDb> for CoreTimeTz {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimeTz(v) => Ok(v),
            _ => Err("Unexpected data for TimeTz (TIME_TZ) type".into()),
        }
    }
}

/// Serialize a [`CoreTimeTz`] as a DuckDB `TIME_TZ` bind parameter.
impl ToSql<DuckTimeTz, DuckDb> for CoreTimeTz {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimeTz(*self));
        Ok(IsNull::No)
    }
}

// TIME_NS

/// Deserialize a DuckDB `TIME_NS` column into a [`NaiveTime`].
///
/// `TIME_NS` has nanosecond precision; `NaiveTime` stores sub-second time as
/// nanoseconds internally, so no precision is lost in the round-trip.
impl FromSql<DuckTimeNs, DuckDb> for NaiveTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimeNs(v) => Ok(v),
            _ => Err("Unexpected data for NaiveTime (TIME_NS) type".into()),
        }
    }
}

/// Serialize a [`NaiveTime`] as a DuckDB `TIME_NS` bind parameter.
impl ToSql<DuckTimeNs, DuckDb> for NaiveTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimeNs(*self));
        Ok(IsNull::No)
    }
}
