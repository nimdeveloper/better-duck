#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use crate::types::Blob;

use super::map;
use super::value::DuckValue;
use crate::types::cmp::{canonical_f32, canonical_f64};

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
    /// The value is a map (arbitrary key → value pairs with a dynamic schema).
    Map(HashMap<DuckValueRef<'a>, DuckValueRef<'a>>),
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
                m.iter().map(|(k, v)| (DuckValueRef::from(k), DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Union(u) => DuckValueRef::Union(Box::new(DuckValueRef::from(u.as_ref()))),
        }
    }
}

// From<DuckValue> — owned conversion, any lifetime 'a
//
// This replaces the former `DuckValueRef::from_value` associated method.
// Because all borrowed slots use `Cow::Owned`, no external data is borrowed, so
// Rust can infer any lifetime `'a` from the call context — in particular, the
// diesel bind-collector's `Vec<DuckValueRef<'a>>` context. This avoids the
// `&mut Vec<DuckValueRef<'a>>` invariance issue that `from_value` was originally
// introduced to solve.

impl<'a> From<DuckValue> for DuckValueRef<'a> {
    /// Converts an owned [`DuckValue`] into a fully-owned `DuckValueRef<'a>`.
    ///
    /// All borrowed slots (`Text`, `Blob`, `Enum`) use [`Cow::Owned`]; scalars are
    /// copied; composites are converted recursively. Because no external data is
    /// borrowed, the caller may choose **any** lifetime `'a`.
    fn from(v: DuckValue) -> DuckValueRef<'a> {
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
            DuckValue::Blob(b) => DuckValueRef::Blob(Cow::Owned(b.0)),
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => DuckValueRef::Decimal(d),
            DuckValue::List(items) => {
                DuckValueRef::List(items.into_iter().map(DuckValueRef::from).collect())
            },
            DuckValue::Array(items) => DuckValueRef::Array(
                items
                    .into_vec()
                    .into_iter()
                    .map(DuckValueRef::from)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            DuckValue::Struct(m) => DuckValueRef::Struct(
                m.into_iter().map(|(k, v)| (k, DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Map(m) => DuckValueRef::Map(
                m.into_iter()
                    .map(|(k, v)| (DuckValueRef::from(k), DuckValueRef::from(v)))
                    .collect(),
            ),
            DuckValue::Union(b) => DuckValueRef::Union(Box::new(DuckValueRef::from(*b))),
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

// AppendAble — pure delegation

impl crate::types::appendable::AppendAble for DuckValueRef<'_> {
    /// Binds this value to a prepared-statement parameter at 1-based index `idx`.
    ///
    /// Scalar and temporal variants delegate to each inner type's own [`AppendAble`]
    /// impl.  Composite variants (`List`, `Array`, `Struct`, `Map`, `Union`, `Enum`,
    /// `TimeTz`, `TimeNs`, `Decimal`) convert to [`DuckValue`] via [`DuckValue::from`]
    /// then go through `DuckValue::to_duck()` + `duckdb_bind_value`.
    ///
    /// [`AppendAble`]: crate::types::appendable::AppendAble
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        use crate::error::Error;
        use crate::ffi;

        /// Convert `self` to DuckValue, call to_duck(), then bind via value path.
        macro_rules! bind_via_to_duck {
            () => {{
                let owned = DuckValue::from(&*self);
                let mut dv = owned.to_duck().map_err(Error::ConversionError)?;
                // SAFETY: `stmt`/`idx` are valid; `dv` was created by `to_duck()`.
                unsafe { ffi::duckdb_bind_value(stmt, idx, dv) };
                // SAFETY: `dv` was created above; destroy exactly once.
                unsafe { ffi::duckdb_destroy_value(&mut dv) };
                return Ok(());
            }};
        }

        match self {
            DuckValueRef::Null => {
                // SAFETY: `stmt` is a valid prepared statement; `idx` is 1-based.
                unsafe { ffi::duckdb_bind_null(stmt, idx) };
                Ok(())
            },
            DuckValueRef::Boolean(v) => v.stmt_append(idx, stmt),
            DuckValueRef::TinyInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::SmallInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Int(v) => v.stmt_append(idx, stmt),
            DuckValueRef::BigInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::HugeInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UTinyInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::USmallInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UBigInt(v) => v.stmt_append(idx, stmt),
            DuckValueRef::UHugeInt(v) => {
                // No generic u128 AppendAble; inline the bind.
                let uhi = ffi::duckdb_uhugeint { lower: *v as u64, upper: (*v >> 64) as u64 };
                // SAFETY: `uhi` is a valid duckdb_uhugeint; `stmt`/`idx` are valid.
                unsafe { ffi::duckdb_bind_uhugeint(stmt, idx, uhi) };
                Ok(())
            },
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
            // All four TIMESTAMP variants bind as TIMESTAMP (microseconds since epoch).
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(dt)
            | DuckValueRef::TimestampS(dt)
            | DuckValueRef::TimestampMs(dt)
            | DuckValueRef::TimestampNs(dt) => dt.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(st)
            | DuckValueRef::TimestampS(st)
            | DuckValueRef::TimestampMs(st)
            | DuckValueRef::TimestampNs(st) => st.stmt_append(idx, stmt),
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(d) => d.stmt_append(idx, stmt),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(d) => d.stmt_append(idx, stmt),
            // TIMESTAMP_TZ: delegate to TimestampTz wrapper which uses duckdb_bind_timestamp_tz.
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(dt) => {
                crate::types::date_chrono::TimestampTz(*dt).stmt_append(idx, stmt)
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
            DuckValueRef::Decimal(d) => d.stmt_append(idx, stmt),

            // Remaining types go through the value path.
            DuckValueRef::List(_)
            | DuckValueRef::Array(_)
            | DuckValueRef::Struct(_)
            | DuckValueRef::Map(_)
            | DuckValueRef::Union(_) => {
                bind_via_to_duck!();
            },
            DuckValueRef::Enum(v) => v.clone().into_owned().stmt_append(idx, stmt),
        }
    }

    /// Appends this value to a DuckDB appender row.
    ///
    /// Scalar and temporal variants delegate to each inner type's own [`AppendAble`]
    /// impl.  Composite variants (`List`, `Array`, `Struct`, `Map`, `Union`, `Enum`,
    /// `TimeTz`, `TimeNs`, `Decimal`) convert to [`DuckValue`] and go through
    /// `DuckValue::to_duck()` + `duckdb_append_value`.
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        use crate::error::Error;
        use crate::ffi;

        /// Convert `self` to DuckValue, call to_duck(), then append via value path.
        macro_rules! append_via_to_duck {
            () => {{
                let owned = DuckValue::from(&*self);
                let mut dv = owned.to_duck().map_err(Error::ConversionError)?;
                // SAFETY: `appender` is valid; `dv` was created by `to_duck()`.
                unsafe { ffi::duckdb_append_value(appender, dv) };
                // SAFETY: `dv` was created above; destroy exactly once.
                unsafe { ffi::duckdb_destroy_value(&mut dv) };
                return Ok(());
            }};
        }

        match self {
            DuckValueRef::Null => {
                // SAFETY: `appender` is a valid duckdb_appender.
                unsafe { ffi::duckdb_append_null(appender) };
                Ok(())
            },
            DuckValueRef::Boolean(v) => v.appender_append(appender),
            DuckValueRef::TinyInt(v) => v.appender_append(appender),
            DuckValueRef::SmallInt(v) => v.appender_append(appender),
            DuckValueRef::Int(v) => v.appender_append(appender),
            DuckValueRef::BigInt(v) => v.appender_append(appender),
            DuckValueRef::HugeInt(v) => v.appender_append(appender),
            DuckValueRef::UTinyInt(v) => v.appender_append(appender),
            DuckValueRef::USmallInt(v) => v.appender_append(appender),
            DuckValueRef::UInt(v) => v.appender_append(appender),
            DuckValueRef::UBigInt(v) => v.appender_append(appender),
            DuckValueRef::UHugeInt(v) => {
                let uhi = ffi::duckdb_uhugeint { lower: *v as u64, upper: (*v >> 64) as u64 };
                // SAFETY: `uhi` is a valid duckdb_uhugeint; `appender` is valid.
                unsafe { ffi::duckdb_append_uhugeint(appender, uhi) };
                Ok(())
            },
            DuckValueRef::Float(v) => v.appender_append(appender),
            DuckValueRef::Double(v) => v.appender_append(appender),
            DuckValueRef::Text(s) => {
                let bytes = s.as_bytes();
                // SAFETY: `bytes.as_ptr()` is valid UTF-8; append copies the data.
                unsafe {
                    ffi::duckdb_append_varchar_length(
                        appender,
                        bytes.as_ptr() as *const std::os::raw::c_char,
                        bytes.len() as u64,
                    )
                };
                Ok(())
            },
            DuckValueRef::Blob(b) => Blob::new(b.to_vec()).appender_append(appender),
            #[cfg(feature = "chrono")]
            DuckValueRef::Date(d) => d.appender_append(appender),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Date(d) => d.appender_append(appender),
            #[cfg(feature = "chrono")]
            DuckValueRef::Time(t) => t.appender_append(appender),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Time(t) => t.appender_append(appender),
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(dt)
            | DuckValueRef::TimestampS(dt)
            | DuckValueRef::TimestampMs(dt)
            | DuckValueRef::TimestampNs(dt) => dt.appender_append(appender),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(st)
            | DuckValueRef::TimestampS(st)
            | DuckValueRef::TimestampMs(st)
            | DuckValueRef::TimestampNs(st) => st.appender_append(appender),
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(d) => d.appender_append(appender),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(d) => d.appender_append(appender),
            // TIMESTAMP_TZ: delegate to TimestampTz wrapper (value path inside it).
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(dt) => {
                crate::types::date_chrono::TimestampTz(*dt).appender_append(appender)
            },
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampTz(st) => st.appender_append(appender),
            // Remaining types go through the value path.
            DuckValueRef::TimeTz(_)
            | DuckValueRef::TimeNs(_)
            | DuckValueRef::Enum(_)
            | DuckValueRef::List(_)
            | DuckValueRef::Array(_)
            | DuckValueRef::Struct(_)
            | DuckValueRef::Map(_)
            | DuckValueRef::Union(_) => {
                append_via_to_duck!();
            },
            #[cfg(feature = "decimal")]
            DuckValueRef::Decimal(d) => d.appender_append(appender),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_ref_conversion() {
        let value = DuckValue::Int(42);
        let value_ref = DuckValueRef::from(&value);
        assert!(matches!(value_ref, DuckValueRef::Int(42)));

        let value = DuckValue::Text("hello".to_string());
        let value_ref = DuckValueRef::from(&value);
        match value_ref {
            DuckValueRef::Text(text) => assert_eq!(text, "hello"),
            _ => panic!("Wrong variant"),
        }

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
        let value_ref = DuckValueRef::Int(42);
        let i32_val: i32 = value_ref.clone().into();
        assert_eq!(i32_val, 42);

        let i64_val: i64 = value_ref.clone().into();
        assert_eq!(i64_val, 42);

        let value_ref = DuckValueRef::Text(Cow::Borrowed("hello"));
        let string_val: String = value_ref.into();
        assert_eq!(string_val, "hello");
    }
}
