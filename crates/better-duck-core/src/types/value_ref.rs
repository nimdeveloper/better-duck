#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use std::borrow::Cow;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use std::collections::HashMap;

use crate::types::Blob;

use super::value::DuckValue;

/// A reference-based version of DuckValue that can store either owned or borrowed data.
/// This is useful for cases where you want to avoid cloning data or need to work with references.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DuckValueRef<'a> {
    /// The value is a `NULL` value.
    Null,
    /// The value is a boolean.
    Boolean(bool),
    /// The value is a signed tiny integer.
    TinyInt(i8),
    /// The value is a signed small integer.
    SmallInt(i16),
    /// The value is a signed integer.
    Int(i32),
    /// The value is a signed big integer.
    BigInt(i64),
    /// The value is a signed huge integer.
    HugeInt(i128),
    /// The value is a unsigned tiny integer.
    UTinyInt(u8),
    /// The value is a unsigned small integer.
    USmallInt(u16),
    /// The value is a unsigned integer.
    UInt(u32),
    /// The value is a unsigned big integer.
    UBigInt(u64),
    /// The value is a unsigned huge integer.
    UHugeInt(u128),
    /// The value is a f32.
    Float(f32),
    /// The value is a f64.
    Double(f64),
    /// The value is a microsecond-precision timestamp (`TIMESTAMP`).
    #[cfg(feature = "chrono")]
    Timestamp(NaiveDateTime),
    /// The value is a microsecond-precision timestamp (`TIMESTAMP`).
    #[cfg(not(feature = "chrono"))]
    Timestamp(SystemTime),

    /// The value is a second-precision timestamp (`TIMESTAMP_S`).
    #[cfg(feature = "chrono")]
    TimestampS(NaiveDateTime),
    /// The value is a second-precision timestamp (`TIMESTAMP_S`).
    #[cfg(not(feature = "chrono"))]
    TimestampS(SystemTime),

    /// The value is a millisecond-precision timestamp (`TIMESTAMP_MS`).
    #[cfg(feature = "chrono")]
    TimestampMs(NaiveDateTime),
    /// The value is a millisecond-precision timestamp (`TIMESTAMP_MS`).
    #[cfg(not(feature = "chrono"))]
    TimestampMs(SystemTime),

    /// The value is a nanosecond-precision timestamp (`TIMESTAMP_NS`).
    #[cfg(feature = "chrono")]
    TimestampNs(NaiveDateTime),
    /// The value is a nanosecond-precision timestamp (`TIMESTAMP_NS`).
    #[cfg(not(feature = "chrono"))]
    TimestampNs(SystemTime),

    /// The value is a UTC timestamp with timezone (`TIMESTAMP_TZ`).
    #[cfg(feature = "chrono")]
    TimestampTz(chrono::DateTime<chrono::Utc>),
    /// The value is a UTC timestamp with timezone (`TIMESTAMP_TZ`).
    #[cfg(not(feature = "chrono"))]
    TimestampTz(SystemTime),

    /// The value is a date.
    #[cfg(feature = "chrono")]
    Date(NaiveDate),
    /// The value is a date.
    #[cfg(not(feature = "chrono"))]
    Date(crate::types::date_native::DuckDate),

    /// The value is a time.
    #[cfg(feature = "chrono")]
    Time(NaiveTime),
    /// The value is a time.
    #[cfg(not(feature = "chrono"))]
    Time(crate::types::date_native::DuckTime),

    /// The value is an interval.
    #[cfg(feature = "chrono")]
    Interval(Duration),
    /// The value is an interval.
    #[cfg(not(feature = "chrono"))]
    Interval(Duration),

    /// The value is a microsecond-precision time with timezone (`TIME_TZ`).
    #[cfg(feature = "chrono")]
    TimeTz(crate::types::date_chrono::TimeTz),
    /// The value is a microsecond-precision time with timezone (`TIME_TZ`).
    #[cfg(not(feature = "chrono"))]
    TimeTz(crate::types::date_native::DuckTimeTz),

    /// The value is a nanosecond-precision time (`TIME_NS`).
    #[cfg(feature = "chrono")]
    TimeNs(chrono::NaiveTime),
    /// The value is a nanosecond-precision time (`TIME_NS`).
    #[cfg(not(feature = "chrono"))]
    TimeNs(crate::types::date_native::DuckTimeNs),

    /// The value is a text string, using Cow for zero-copy when possible
    Text(Cow<'a, str>),
    #[cfg(feature = "decimal")]
    /// The value is a Decimal.
    Decimal(Decimal),
    /// The value is a blob of data, using Cow for zero-copy when possible
    Blob(Cow<'a, [u8]>),
    /// The value is a list
    List(Vec<DuckValueRef<'a>>),
    /// The value is an enum
    Enum(Cow<'a, str>),
    /// The value is a struct (string-keyed field map with a fixed schema).
    Struct(HashMap<String, DuckValueRef<'a>>),
    /// The value is an array with fixed length
    Array(Box<[DuckValueRef<'a>]>),
    /// The value is a map (string-keyed value map with a dynamic schema).
    Map(HashMap<String, DuckValueRef<'a>>),
    /// The value is a union (tagged sum type; holds the active member value).
    Union(Box<DuckValueRef<'a>>),
}

// Implement From<DuckValue> for DuckValueRef
impl<'a> From<&'a DuckValue> for DuckValueRef<'a> {
    /// Creates a `DuckValueRef` from a `&DuckValue`, borrowing `Text`/`Enum`/`Blob` data
    /// for genuine zero-copy.
    fn from(value: &'a DuckValue) -> Self {
        match value {
            DuckValue::Null => DuckValueRef::Null,
            DuckValue::Boolean(b) => DuckValueRef::Boolean(*b),
            DuckValue::TinyInt(n) => DuckValueRef::TinyInt(*n),
            DuckValue::SmallInt(n) => DuckValueRef::SmallInt(*n),
            DuckValue::Int(n) => DuckValueRef::Int(*n),
            DuckValue::BigInt(n) => DuckValueRef::BigInt(*n),
            DuckValue::HugeInt(n) => DuckValueRef::HugeInt(*n),
            DuckValue::UTinyInt(n) => DuckValueRef::UTinyInt(*n),
            DuckValue::USmallInt(n) => DuckValueRef::USmallInt(*n),
            DuckValue::UInt(n) => DuckValueRef::UInt(*n),
            DuckValue::UBigInt(n) => DuckValueRef::UBigInt(*n),
            DuckValue::UHugeInt(n) => DuckValueRef::UHugeInt(*n),
            DuckValue::Float(n) => DuckValueRef::Float(*n),
            DuckValue::Double(n) => DuckValueRef::Double(*n),
            #[cfg(feature = "chrono")]
            DuckValue::Timestamp(t) => DuckValueRef::Timestamp(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Timestamp(t) => DuckValueRef::Timestamp(*t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampS(t) => DuckValueRef::TimestampS(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampS(t) => DuckValueRef::TimestampS(*t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampMs(t) => DuckValueRef::TimestampMs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampMs(t) => DuckValueRef::TimestampMs(*t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampNs(t) => DuckValueRef::TimestampNs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampNs(t) => DuckValueRef::TimestampNs(*t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampTz(t) => DuckValueRef::TimestampTz(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampTz(t) => DuckValueRef::TimestampTz(*t),
            #[cfg(feature = "chrono")]
            DuckValue::Date(d) => DuckValueRef::Date(*d),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Date(d) => DuckValueRef::Date(*d),
            #[cfg(feature = "chrono")]
            DuckValue::Time(t) => DuckValueRef::Time(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Time(t) => DuckValueRef::Time(*t),
            #[cfg(feature = "chrono")]
            DuckValue::Interval(i) => DuckValueRef::Interval(*i),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Interval(i) => DuckValueRef::Interval(*i),
            #[cfg(feature = "chrono")]
            DuckValue::TimeTz(t) => DuckValueRef::TimeTz(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeTz(t) => DuckValueRef::TimeTz(*t),
            #[cfg(feature = "chrono")]
            DuckValue::TimeNs(t) => DuckValueRef::TimeNs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeNs(t) => DuckValueRef::TimeNs(*t),
            DuckValue::Text(s) => DuckValueRef::Text(Cow::Owned(s.clone())),
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => DuckValueRef::Decimal(*d),
            DuckValue::Blob(b) => DuckValueRef::Blob(Cow::Owned(b.clone())),
            DuckValue::List(l) => DuckValueRef::List(l.iter().map(DuckValueRef::from).collect()),
            DuckValue::Enum(e) => DuckValueRef::Enum(Cow::Owned(e.clone())),
            DuckValue::Struct(m) => DuckValueRef::Struct(
                m.iter().map(|(k, v)| (k.clone(), DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Array(a) => DuckValueRef::Array(
                a.iter().map(DuckValueRef::from).collect::<Vec<_>>().into_boxed_slice(),
            ),
            DuckValue::Map(m) => DuckValueRef::Map(
                m.iter().map(|(k, v)| (k.clone(), DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Union(u) => DuckValueRef::Union(Box::new(DuckValueRef::from(u.as_ref()))),
        }
    }
}

impl<'a> DuckValueRef<'a> {
    /// Converts an owned [`DuckValue`] into a fully-owned `DuckValueRef<'a>`.
    ///
    /// All borrowed slots (`Text`, `Blob`, `Enum`) use [`Cow::Owned`]; scalars are
    /// copied; composites are converted recursively. Because no external data is
    /// borrowed, the caller may choose **any** lifetime `'a` — Rust will infer it
    /// from the call context. This sidesteps the invariance issue that arises when
    /// extending a `Vec<DuckValueRef<'a>>` with `DuckValueRef<'static>` items.
    pub fn from_value(v: DuckValue) -> DuckValueRef<'a> {
        match v {
            DuckValue::Null => DuckValueRef::Null,
            DuckValue::Boolean(b) => DuckValueRef::Boolean(b),
            DuckValue::TinyInt(n) => DuckValueRef::TinyInt(n),
            DuckValue::SmallInt(n) => DuckValueRef::SmallInt(n),
            DuckValue::Int(n) => DuckValueRef::Int(n),
            DuckValue::BigInt(n) => DuckValueRef::BigInt(n),
            DuckValue::HugeInt(n) => DuckValueRef::HugeInt(n),
            DuckValue::UTinyInt(n) => DuckValueRef::UTinyInt(n),
            DuckValue::USmallInt(n) => DuckValueRef::USmallInt(n),
            DuckValue::UInt(n) => DuckValueRef::UInt(n),
            DuckValue::UBigInt(n) => DuckValueRef::UBigInt(n),
            DuckValue::UHugeInt(n) => DuckValueRef::UHugeInt(n),
            DuckValue::Float(f) => DuckValueRef::Float(f),
            DuckValue::Double(d) => DuckValueRef::Double(d),
            #[cfg(feature = "chrono")]
            DuckValue::Timestamp(t) => DuckValueRef::Timestamp(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Timestamp(t) => DuckValueRef::Timestamp(t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampS(t) => DuckValueRef::TimestampS(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampS(t) => DuckValueRef::TimestampS(t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampMs(t) => DuckValueRef::TimestampMs(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampMs(t) => DuckValueRef::TimestampMs(t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampNs(t) => DuckValueRef::TimestampNs(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampNs(t) => DuckValueRef::TimestampNs(t),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampTz(t) => DuckValueRef::TimestampTz(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampTz(t) => DuckValueRef::TimestampTz(t),
            #[cfg(feature = "chrono")]
            DuckValue::Date(d) => DuckValueRef::Date(d),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Date(d) => DuckValueRef::Date(d),
            #[cfg(feature = "chrono")]
            DuckValue::Time(t) => DuckValueRef::Time(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Time(t) => DuckValueRef::Time(t),
            #[cfg(feature = "chrono")]
            DuckValue::Interval(i) => DuckValueRef::Interval(i),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Interval(i) => DuckValueRef::Interval(i),
            #[cfg(feature = "chrono")]
            DuckValue::TimeTz(t) => DuckValueRef::TimeTz(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeTz(t) => DuckValueRef::TimeTz(t),
            #[cfg(feature = "chrono")]
            DuckValue::TimeNs(t) => DuckValueRef::TimeNs(t),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeNs(t) => DuckValueRef::TimeNs(t),
            DuckValue::Text(s) => DuckValueRef::Text(Cow::Owned(s)),
            DuckValue::Enum(s) => DuckValueRef::Enum(Cow::Owned(s)),
            DuckValue::Blob(b) => DuckValueRef::Blob(Cow::Owned(b)),
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => DuckValueRef::Decimal(d),
            DuckValue::List(items) => {
                DuckValueRef::List(items.into_iter().map(DuckValueRef::from_value).collect())
            },
            DuckValue::Array(items) => DuckValueRef::Array(
                items
                    .into_vec()
                    .into_iter()
                    .map(DuckValueRef::from_value)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            DuckValue::Struct(m) => DuckValueRef::Struct(
                m.into_iter().map(|(k, v)| (k, DuckValueRef::from_value(v))).collect(),
            ),
            DuckValue::Map(m) => DuckValueRef::Map(
                m.into_iter().map(|(k, v)| (k, DuckValueRef::from_value(v))).collect(),
            ),
            DuckValue::Union(b) => DuckValueRef::Union(Box::new(DuckValueRef::from_value(*b))),
        }
    }
}

// Common conversions for primitive types
impl<'a> From<DuckValueRef<'a>> for String {
    fn from(val: DuckValueRef<'_>) -> Self {
        match val {
            DuckValueRef::Text(s) => s.into_owned(),
            DuckValueRef::Null => String::new(),
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}

impl<'a> From<DuckValueRef<'a>> for i64 {
    fn from(val: DuckValueRef<'_>) -> Self {
        match val {
            DuckValueRef::BigInt(v) => v,
            DuckValueRef::Int(v) => v as i64,
            DuckValueRef::SmallInt(v) => v as i64,
            DuckValueRef::TinyInt(v) => v as i64,
            DuckValueRef::Null => 0,
            _ => panic!("Cannot convert {:?} to i64", val),
        }
    }
}

impl<'a> From<DuckValueRef<'a>> for i32 {
    fn from(val: DuckValueRef<'_>) -> Self {
        match val {
            DuckValueRef::Int(v) => v,
            DuckValueRef::SmallInt(v) => v as i32,
            DuckValueRef::TinyInt(v) => v as i32,
            DuckValueRef::Null => 0,
            _ => panic!("Cannot convert {:?} to i32", val),
        }
    }
}

// AppendAble for DuckValueRef

impl crate::types::appendable::AppendAble for DuckValueRef<'_> {
    /// Binds this value to a prepared-statement parameter at 1-based index `idx`.
    ///
    /// Supports all scalar DuckDB types. Composite types (`List`, `Array`, `Union`,
    /// `Enum`) return `Err` because the DuckDB C API does not expose bind functions
    /// for them at this level.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::ConversionError`] for composite variants.
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        use crate::error::{DuckDBConversionError, Error};
        use crate::ffi;
        match self {
            DuckValueRef::Null => {
                // SAFETY: `stmt` is a valid prepared statement; `idx` is 1-based.
                unsafe { ffi::duckdb_bind_null(stmt, idx) };
                Ok(())
            },
            DuckValueRef::Boolean(v) => v.stmt_append(idx, stmt),
            DuckValueRef::TinyInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::SmallInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::BigInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::HugeInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UTinyInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::USmallInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UBigInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Float(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Double(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Text(s) => s.into_owned().stmt_append(idx, stmt),
            DuckValueRef::Blob(b) => Blob::new(b.into_owned()).stmt_append(idx, stmt),
            #[cfg(feature = "chrono")]
            DuckValueRef::Date(d) => d.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Date(d) => d.stmt_append(idx, stmt),
            #[cfg(feature = "chrono")]
            DuckValueRef::Time(t) => t.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Time(t) => t.stmt_append(idx, stmt),
            // All four timestamp variants bind as TIMESTAMP (microseconds since epoch).
            // DuckDB handles implicit narrowing/widening at the column level.
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(dt)
            | DuckValueRef::TimestampS(dt)
            | DuckValueRef::TimestampMs(dt)
            | DuckValueRef::TimestampNs(dt) => st.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(st)
            | DuckValueRef::TimestampS(st)
            | DuckValueRef::TimestampMs(st)
            | DuckValueRef::TimestampNs(st) => st.stmt_append(idx, stmt),
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(d) => d.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(d) => d.stmt_append(idx, stmt),
            // Bind TIMESTAMP_TZ as UTC microseconds via duckdb_bind_timestamp_tz.
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(dt) => {
                let raw = ffi::duckdb_timestamp { micros: dt.timestamp_micros() };
                // SAFETY: `raw` is a valid duckdb_timestamp (UTC microseconds).
                unsafe { ffi::duckdb_bind_timestamp_tz(stmt, idx, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampTz(st) => st.stmt_append(idx, stmt),
            // Bind TIME_TZ and TIME_NS via duckdb_bind_value (no dedicated bind API).
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeTz(tz) => tz.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeTz(tz) => tz.stmt_append(idx, stmt),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeNs(t) => t.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeNs(t) => t.stmt_append(idx, stmt),
            #[cfg(feature = "decimal")]
            DuckValueRef::Decimal(v) => v.stmt_append(idx, stmt),
            DuckValueRef::List(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Array(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Struct(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Map(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Union(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Enum(v) => v.into_owned().stmt_append(idx, stmt),
            DuckValueRef::UHugeInt(_) => panic!("Not implemented yet!"),
        }
    }

    /// Appends this value to a DuckDB appender row.
    ///
    /// All scalar types use their dedicated `duckdb_append_*` functions. Composite
    /// types (`List`, `Array`, `Struct`, `Map`, `Union`) and `TimeTz`/`TimeNs`/`Decimal`
    /// are converted to a `duckdb_value` via `DuckValue::to_duck()` and then appended
    /// with `duckdb_append_value`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::ConversionError`] if `to_duck()` fails for a
    /// composite/special type.
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        use crate::error::Error;
        use crate::ffi;
        use crate::types::value::DuckValue;

        // Helper: convert self to DuckValue, call to_duck(), then append via value path.
        macro_rules! append_via_to_duck {
            () => {{
                let owned: DuckValue = DuckValue::from(&*self);
                let mut dv = owned.to_duck().map_err(Error::ConversionError)?;
                // SAFETY: `appender` is valid; `dv` was just created by `to_duck()`.
                unsafe { ffi::duckdb_append_value(appender, dv) };
                // SAFETY: `dv` was created above; destroy exactly once.
                unsafe { ffi::duckdb_destroy_value(&mut dv) };
            }};
        }

        match self {
            DuckValueRef::Null => {
                // SAFETY: `appender` is a valid duckdb_appender inside a begin/end_row pair.
                unsafe { ffi::duckdb_append_null(appender) };
            },
            DuckValueRef::Boolean(v) => {
                // SAFETY: `*v` is a valid bool.
                unsafe { ffi::duckdb_append_bool(appender, *v) };
            },
            DuckValueRef::TinyInt(v) => {
                // SAFETY: `*v` is a valid i8.
                unsafe { crate::ffi::duckdb_append_int8(appender, *v) };
            },
            DuckValueRef::SmallInt(v) => {
                // SAFETY: `*v` is a valid i16.
                unsafe { crate::ffi::duckdb_append_int16(appender, *v) };
            },
            DuckValueRef::Int(v) => {
                // SAFETY: `*v` is a valid i32.
                unsafe { crate::ffi::duckdb_append_int32(appender, *v) };
            },
            DuckValueRef::BigInt(v) => {
                // SAFETY: `*v` is a valid i64.
                unsafe { crate::ffi::duckdb_append_int64(appender, *v) };
            },
            DuckValueRef::HugeInt(v) => {
                let val = *v;
                let neg = val < 0;
                let x = if neg { -val } else { val };
                let m = u64::MAX as i128;
                let mut h = ffi::duckdb_hugeint { upper: (x / m) as i64, lower: (x % m) as u64 };
                if neg {
                    h.lower = u64::MAX - h.lower;
                    h.upper = (!h.upper).wrapping_add((h.lower == 0) as i64);
                }
                // SAFETY: `h` is a valid duckdb_hugeint computed above.
                unsafe { crate::ffi::duckdb_append_hugeint(appender, h) };
            },
            DuckValueRef::UTinyInt(v) => {
                // SAFETY: `*v` is a valid u8.
                unsafe { crate::ffi::duckdb_append_uint8(appender, *v) };
            },
            DuckValueRef::USmallInt(v) => {
                // SAFETY: `*v` is a valid u16.
                unsafe { crate::ffi::duckdb_append_uint16(appender, *v) };
            },
            DuckValueRef::UInt(v) => {
                // SAFETY: `*v` is a valid u32.
                unsafe { crate::ffi::duckdb_append_uint32(appender, *v) };
            },
            DuckValueRef::UBigInt(v) => {
                // SAFETY: `*v` is a valid u64.
                unsafe { crate::ffi::duckdb_append_uint64(appender, *v) };
            },
            DuckValueRef::UHugeInt(v) => {
                let uhi = ffi::duckdb_uhugeint { lower: *v as u64, upper: (*v >> 64) as u64 };
                // SAFETY: `uhi` is a valid duckdb_uhugeint.
                unsafe { crate::ffi::duckdb_append_uhugeint(appender, uhi) };
            },
            DuckValueRef::Float(v) => {
                // SAFETY: `*v` is a valid f32.
                unsafe { crate::ffi::duckdb_append_float(appender, *v) };
            },
            DuckValueRef::Double(v) => {
                // SAFETY: `*v` is a valid f64.
                unsafe { crate::ffi::duckdb_append_double(appender, *v) };
            },
            DuckValueRef::Text(s) => {
                let bytes = s.as_bytes();
                // SAFETY: `bytes.as_ptr()` is valid UTF-8; append copies the data.
                unsafe {
                    crate::ffi::duckdb_append_varchar_length(
                        appender,
                        bytes.as_ptr() as *const std::os::raw::c_char,
                        bytes.len() as u64,
                    )
                };
            },
            DuckValueRef::Blob(b) => {
                // SAFETY: `b.as_ptr()` is valid for `b.len()` bytes; append copies the data.
                unsafe {
                    ffi::duckdb_append_blob(
                        appender,
                        b.as_ptr() as *const std::ffi::c_void,
                        b.len() as u64,
                    )
                };
            },
            #[cfg(feature = "chrono")]
            DuckValueRef::Date(d) => {
                let raw = ffi::duckdb_date { days: d.num_days_from_ce() - 719_163 };
                // SAFETY: `raw` is a valid duckdb_date.
                unsafe { ffi::duckdb_append_date(appender, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Date(d) => {
                let ds = ffi::duckdb_date_struct {
                    year: d.year,
                    month: d.month as i8,
                    day: d.day as i8,
                };
                // SAFETY: `duckdb_to_date` is a pure arithmetic conversion.
                let raw = unsafe { ffi::duckdb_to_date(ds) };
                // SAFETY: `raw` is a valid duckdb_date.
                unsafe { ffi::duckdb_append_date(appender, raw) };
            },
            #[cfg(feature = "chrono")]
            DuckValueRef::Time(t) => {
                let micros = (t.num_seconds_from_midnight() as i64) * 1_000_000
                    + (t.nanosecond() as i64) / 1_000;
                let raw = ffi::duckdb_time { micros };
                // SAFETY: `raw` is a valid duckdb_time.
                unsafe { ffi::duckdb_append_time(appender, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Time(t) => {
                let ts = ffi::duckdb_time_struct {
                    hour: t.hour as i8,
                    min: t.min as i8,
                    sec: t.sec as i8,
                    micros: t.micros as i32,
                };
                // SAFETY: `duckdb_to_time` is a pure arithmetic conversion.
                let raw = unsafe { ffi::duckdb_to_time(ts) };
                // SAFETY: `raw` is a valid duckdb_time.
                unsafe { ffi::duckdb_append_time(appender, raw) };
            },
            // All four timestamp variants append as TIMESTAMP (microseconds since epoch).
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(dt)
            | DuckValueRef::TimestampS(dt)
            | DuckValueRef::TimestampMs(dt)
            | DuckValueRef::TimestampNs(dt) => {
                let micros = dt.and_utc().timestamp() * 1_000_000
                    + dt.and_utc().timestamp_subsec_micros() as i64;
                let raw = ffi::duckdb_timestamp { micros };
                // SAFETY: `raw` is a valid duckdb_timestamp.
                unsafe { ffi::duckdb_append_timestamp(appender, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(st)
            | DuckValueRef::TimestampS(st)
            | DuckValueRef::TimestampMs(st)
            | DuckValueRef::TimestampNs(st) => {
                let dur = st.duration_since(UNIX_EPOCH).unwrap_or_default();
                let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
                let raw = ffi::duckdb_timestamp { micros };
                // SAFETY: `raw` is a valid duckdb_timestamp.
                unsafe { ffi::duckdb_append_timestamp(appender, raw) };
            },
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(dt) => {
                let raw = ffi::duckdb_timestamp { micros: dt.timestamp_micros() };
                // SAFETY: `raw` is a valid duckdb_timestamp (UTC microseconds).
                unsafe { ffi::duckdb_append_timestamp(appender, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampTz(st) => {
                let dur = st.duration_since(UNIX_EPOCH).unwrap_or_default();
                let micros = dur.as_secs() as i64 * 1_000_000 + dur.subsec_micros() as i64;
                let raw = ffi::duckdb_timestamp { micros };
                // SAFETY: `raw` is a valid duckdb_timestamp (UTC microseconds).
                unsafe { ffi::duckdb_append_timestamp(appender, raw) };
            },
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(d) => {
                let micros = d.num_microseconds().unwrap_or(0);
                let raw = ffi::duckdb_interval { months: 0, days: 0, micros };
                // SAFETY: `raw` is a valid duckdb_interval.
                unsafe { ffi::duckdb_append_interval(appender, raw) };
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(d) => {
                let micros = d.as_micros().min(i64::MAX as u128) as i64;
                let raw = ffi::duckdb_interval { months: 0, days: 0, micros };
                // SAFETY: `raw` is a valid duckdb_interval.
                unsafe { ffi::duckdb_append_interval(appender, raw) };
            },
            // TimeTz, TimeNs, Decimal, and all composite types go through the value path.
            DuckValueRef::TimeTz(_)
            | DuckValueRef::TimeNs(_)
            | DuckValueRef::Enum(_)
            | DuckValueRef::List(_)
            | DuckValueRef::Array(_)
            | DuckValueRef::Struct(_)
            | DuckValueRef::Map(_)
            | DuckValueRef::Union(_) => {
                // SAFETY: `appender` is valid; `dv` is created by `DuckValue::to_duck()`
                // and destroyed immediately after. The conversion is infallible for these types
                // when the inner values are well-formed.
                append_via_to_duck!();
            },
            #[cfg(feature = "decimal")]
            DuckValueRef::Decimal(_) => {
                // SAFETY: same as above; `Decimal::to_duck()` returns a valid duckdb_value.
                append_via_to_duck!();
            },
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_ref_conversion() {
        // Test simple value
        let value = DuckValue::Int(42);
        let value_ref = DuckValueRef::from(&value);
        assert!(matches!(value_ref, DuckValueRef::Int(42)));

        // Test string value
        let value = DuckValue::Text("hello".to_string());
        let value_ref = DuckValueRef::from(&value);
        match value_ref {
            DuckValueRef::Text(text) => assert_eq!(text, "hello"),
            _ => panic!("Wrong variant"),
        }

        // Test list value
        let value = DuckValue::List(vec![DuckValue::Int(1), DuckValue::Text("test".to_string())]);
        let value_ref = DuckValueRef::from(&value);
        match value_ref {
            DuckValueRef::List(list) => {
                assert_eq!(list.len(), 2);
                assert!(matches!(list[0], DuckValueRef::Int(1)));
                match &list[1] {
                    DuckValueRef::Text(text) => assert_eq!(text, "test"),
                    _ => panic!("Wrong variant"),
                }
            },
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_value_ref_into_owned() {
        // Test converting back to owned
        let original =
            DuckValue::List(vec![DuckValue::Int(1), DuckValue::Text("test".to_string())]);
        let value_ref = DuckValueRef::from(&original);
        let back_to_owned = DuckValue::from(&value_ref);

        match &back_to_owned {
            DuckValue::List(list) => {
                assert_eq!(list.len(), 2);
                assert!(matches!(list[0], DuckValue::Int(1)));
                match &list[1] {
                    DuckValue::Text(text) => assert_eq!(text, "test"),
                    _ => panic!("Wrong variant"),
                }
            },
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_primitive_conversions() {
        // Test i32 conversion
        let value_ref = DuckValueRef::Int(42);
        let i32_val: i32 = value_ref.clone().into();
        assert_eq!(i32_val, 42);

        // Test i64 conversion
        let i64_val: i64 = value_ref.clone().into();
        assert_eq!(i64_val, 42);

        // Test string conversion
        let value_ref = DuckValueRef::Text(Cow::Borrowed("hello"));
        let string_val: String = value_ref.into();
        assert_eq!(string_val, "hello");
    }
}
