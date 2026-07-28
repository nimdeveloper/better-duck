//! Non-chrono `FromSql`/`ToSql` implementations for date/time types.
//!
//! Mirrors every pair in `date_chrono.rs`, but against
//! `better_duck_core::types::date_native`'s plain structs and `std::time` types
//! instead of `chrono`. Enabled when the `chrono` feature is off.

use std::time::{Duration, SystemTime};

use better_duck_core::types::date_native::{DuckDate, DuckTime, DuckTimeNs, DuckTimeTz};
use better_duck_core::types::value_ref::DuckValueRef;
use diesel::{
    deserialize::{self, FromSql},
    serialize::{self, IsNull, Output, ToSql},
    sql_types::{Date, Interval, Time, Timestamp},
};

use crate::backend::DuckDb;
use crate::types::duckdb_types::DuckTimestamptz;
use crate::types::duckdb_types::{DuckTimeNs as DuckTimeNsTy, DuckTimeTz as DuckTimeTzTy};

/// Deserialize a DuckDB `DATE` column into a [`DuckDate`].
impl FromSql<Date, DuckDb> for DuckDate {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Date(v) => Ok(v),
            _ => Err("Unexpected data for DuckDate type".into()),
        }
    }
}
/// Serialize a [`DuckDate`] as a DuckDB `DATE` bind parameter.
impl ToSql<Date, DuckDb> for DuckDate {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Date(*self));
        Ok(IsNull::No)
    }
}

/// Deserialize a DuckDB `TIME` column into a [`DuckTime`].
impl FromSql<Time, DuckDb> for DuckTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Time(v) => Ok(v),
            _ => Err("Unexpected data for DuckTime type".into()),
        }
    }
}
/// Serialize a [`DuckTime`] as a DuckDB `TIME` bind parameter.
impl ToSql<Time, DuckDb> for DuckTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Time(*self));
        Ok(IsNull::No)
    }
}

/// Deserialize a DuckDB `TIMESTAMP` column into a [`SystemTime`].
impl FromSql<Timestamp, DuckDb> for SystemTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Timestamp(v) => Ok(v),
            _ => Err("Unexpected data for SystemTime type".into()),
        }
    }
}
/// Serialize a [`SystemTime`] as a DuckDB `TIMESTAMP` bind parameter.
impl ToSql<Timestamp, DuckDb> for SystemTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::Timestamp(*self));
        Ok(IsNull::No)
    }
}

/// Deserialize a DuckDB `INTERVAL` column into a [`Duration`].
impl FromSql<Interval, DuckDb> for Duration {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::Interval(v) => Ok(v),
            _ => Err("Unexpected data for Duration type".into()),
        }
    }
}
/// Serialize a [`Duration`] as a DuckDB `INTERVAL` bind parameter.
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

/// Deserialize a DuckDB `TIMESTAMP_TZ` column into a [`SystemTime`].
impl FromSql<DuckTimestamptz, DuckDb> for SystemTime {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimestampTz(v) => Ok(v),
            _ => Err("Unexpected data for SystemTime (TIMESTAMPTZ) type".into()),
        }
    }
}
/// Serialize a [`SystemTime`] as a DuckDB `TIMESTAMP_TZ` bind parameter.
impl ToSql<DuckTimestamptz, DuckDb> for SystemTime {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimestampTz(*self));
        Ok(IsNull::No)
    }
}

// TIME_TZ

/// Deserialize a DuckDB `TIME_TZ` column into a [`DuckTimeTz`].
impl FromSql<DuckTimeTzTy, DuckDb> for DuckTimeTz {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimeTz(v) => Ok(v),
            _ => Err("Unexpected data for DuckTimeTz (TIME_TZ) type".into()),
        }
    }
}
/// Serialize a [`DuckTimeTz`] as a DuckDB `TIME_TZ` bind parameter.
impl ToSql<DuckTimeTzTy, DuckDb> for DuckTimeTz {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimeTz(*self));
        Ok(IsNull::No)
    }
}

// TIME_NS

/// Deserialize a DuckDB `TIME_NS` column into a [`DuckTimeNs`].
impl FromSql<DuckTimeNsTy, DuckDb> for DuckTimeNs {
    fn from_sql(val: DuckValueRef) -> deserialize::Result<Self> {
        match val {
            DuckValueRef::TimeNs(v) => Ok(v),
            _ => Err("Unexpected data for DuckTimeNs (TIME_NS) type".into()),
        }
    }
}
/// Serialize a [`DuckTimeNs`] as a DuckDB `TIME_NS` bind parameter.
impl ToSql<DuckTimeNsTy, DuckDb> for DuckTimeNs {
    fn to_sql<'b>(
        &'b self,
        out: &mut Output<'b, '_, DuckDb>,
    ) -> serialize::Result {
        out.set_value(DuckValueRef::TimeNs(*self));
        Ok(IsNull::No)
    }
}
