#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use libduckdb_sys::duckdb_hugeint;
use std::collections::HashMap;
use std::ffi::CStr;
use std::hash::{Hash, Hasher};
use std::mem;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

use crate::{
    ffi::{
        duckdb_create_logical_type, duckdb_create_null_value, duckdb_create_uhugeint, duckdb_date,
        duckdb_destroy_logical_type, duckdb_enum_dictionary_size, duckdb_enum_dictionary_value,
        duckdb_free, duckdb_interval, duckdb_logical_type, duckdb_string_t, duckdb_string_t_data,
        duckdb_string_t_length, duckdb_time, duckdb_time_ns, duckdb_time_tz, duckdb_timestamp,
        duckdb_timestamp_ms, duckdb_timestamp_ns, duckdb_timestamp_s, duckdb_type, duckdb_uhugeint,
        duckdb_validity_row_is_valid, duckdb_value, duckdb_vector, duckdb_vector_get_column_type,
        duckdb_vector_get_data, duckdb_vector_get_validity, idx_t, DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
        DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
        DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        DUCKDB_TYPE_DUCKDB_TYPE_ENUM, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, DUCKDB_TYPE_DUCKDB_TYPE_INVALID,
        DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP,
        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL,
        DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL, DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
        DUCKDB_TYPE_DUCKDB_TYPE_UNION, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    },
    types::value_ref::DuckValueRef,
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use super::*;
use crate::types::appendable::AppendAble;
use crate::types::cmp::{canonical_f32, canonical_f64};

/// Represents any value that can be stored in a DuckDB column.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum DuckValue {
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
    TimestampTz(DateTime<Utc>),
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

    /// The value is an interval (months, days, microseconds).
    #[cfg(feature = "chrono")]
    Interval(Duration),
    /// The value is an interval (months, days, microseconds).
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
    TimeNs(NaiveTime),
    /// The value is a nanosecond-precision time (`TIME_NS`).
    #[cfg(not(feature = "chrono"))]
    TimeNs(crate::types::date_native::DuckTimeNs),

    /// The value is a text string.
    Text(String),
    #[cfg(feature = "decimal")]
    /// The value is a Decimal.
    Decimal(Decimal),
    /// The value is a blob of data (raw byte sequence).
    Blob(Blob),
    /// The value is a list
    List(Vec<DuckValue>),
    /// The value is an enum
    Enum(String),
    /// The value is a struct (string-keyed field map with a fixed schema).
    Struct(HashMap<String, DuckValue>),
    /// The value is an array with fixed length
    Array(Box<[DuckValue]>),
    /// The value is a map (arbitrary key → value pairs with a dynamic schema).
    Map(HashMap<DuckValue, DuckValue>),
    /// The value is a union (tagged sum type; holds the active member value).
    Union(Box<DuckValue>),
}

// PartialEq / Eq / Hash
//
// DuckValue contains f32/f64 (no Eq/Hash on IEEE floats) and HashMap fields (no Hash
// on HashMap).  We hand-implement all three so that:
//
//   • Float/Double: normalized via canonical_f32/canonical_f64 (NaN == NaN, -0 == +0).
//   • Map/Struct:   hashed order-independently (wrapping-add of per-entry XOR hashes)
//                   so that HashMap iteration order doesn't affect the result.
//   • SystemTime (no-chrono timestamps): hashed via duration_since(UNIX_EPOCH) because
//     std::time::SystemTime does not implement Hash.

impl PartialEq for DuckValue {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        use DuckValue::*;
        match (self, other) {
            (Null, Null) => true,
            (Boolean(a), Boolean(b)) => a == b,
            (TinyInt(a), TinyInt(b)) => a == b,
            (SmallInt(a), SmallInt(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (BigInt(a), BigInt(b)) => a == b,
            (HugeInt(a), HugeInt(b)) => a == b,
            (UTinyInt(a), UTinyInt(b)) => a == b,
            (USmallInt(a), USmallInt(b)) => a == b,
            (UInt(a), UInt(b)) => a == b,
            (UBigInt(a), UBigInt(b)) => a == b,
            (UHugeInt(a), UHugeInt(b)) => a == b,
            // Canonical float comparison: NaN == NaN, -0 == +0.
            (Float(a), Float(b)) => canonical_f32(*a) == canonical_f32(*b),
            (Double(a), Double(b)) => canonical_f64(*a) == canonical_f64(*b),
            #[cfg(feature = "chrono")]
            (Timestamp(a), Timestamp(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (Timestamp(a), Timestamp(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimestampS(a), TimestampS(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimestampS(a), TimestampS(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimestampMs(a), TimestampMs(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimestampMs(a), TimestampMs(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimestampNs(a), TimestampNs(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimestampNs(a), TimestampNs(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimestampTz(a), TimestampTz(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimestampTz(a), TimestampTz(b)) => a == b,
            #[cfg(feature = "chrono")]
            (Date(a), Date(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (Date(a), Date(b)) => a == b,
            #[cfg(feature = "chrono")]
            (Time(a), Time(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (Time(a), Time(b)) => a == b,
            #[cfg(feature = "chrono")]
            (Interval(a), Interval(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (Interval(a), Interval(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimeTz(a), TimeTz(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimeTz(a), TimeTz(b)) => a == b,
            #[cfg(feature = "chrono")]
            (TimeNs(a), TimeNs(b)) => a == b,
            #[cfg(not(feature = "chrono"))]
            (TimeNs(a), TimeNs(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Enum(a), Enum(b)) => a == b,
            #[cfg(feature = "decimal")]
            (Decimal(a), Decimal(b)) => a == b,
            (Blob(a), Blob(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Array(a), Array(b)) => a == b,
            (Struct(a), Struct(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            (Union(a), Union(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for DuckValue {}

impl Hash for DuckValue {
    fn hash<H: Hasher>(
        &self,
        state: &mut H,
    ) {
        mem::discriminant(self).hash(state);
        match self {
            DuckValue::Null => {},
            DuckValue::Boolean(v) => v.hash(state),
            DuckValue::TinyInt(v) => v.hash(state),
            DuckValue::SmallInt(v) => v.hash(state),
            DuckValue::Int(v) => v.hash(state),
            DuckValue::BigInt(v) => v.hash(state),
            DuckValue::HugeInt(v) => v.hash(state),
            DuckValue::UTinyInt(v) => v.hash(state),
            DuckValue::USmallInt(v) => v.hash(state),
            DuckValue::UInt(v) => v.hash(state),
            DuckValue::UBigInt(v) => v.hash(state),
            DuckValue::UHugeInt(v) => v.hash(state),
            DuckValue::Float(f) => canonical_f32(*f).hash(state),
            DuckValue::Double(d) => canonical_f64(*d).hash(state),
            #[cfg(feature = "chrono")]
            DuckValue::Timestamp(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Timestamp(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampS(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampS(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampMs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampMs(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampNs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampNs(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampTz(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimestampTz(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValue::Date(d) => d.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Date(d) => d.hash(state),
            #[cfg(feature = "chrono")]
            DuckValue::Time(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Time(t) => t.hash(state),
            #[cfg(feature = "chrono")]
            DuckValue::Interval(i) => i.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Interval(i) => i.hash(state),
            #[cfg(feature = "chrono")]
            DuckValue::TimeTz(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeTz(t) => t.hash(state),
            #[cfg(feature = "chrono")]
            DuckValue::TimeNs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeNs(t) => t.hash(state),
            DuckValue::Text(s) => s.hash(state),
            DuckValue::Enum(s) => s.hash(state),
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => d.hash(state),
            DuckValue::Blob(b) => b.hash(state),
            DuckValue::List(items) => items.hash(state),
            DuckValue::Array(items) => items.hash(state),
            // Order-independent hash for Map entries.
            DuckValue::Map(m) => {
                map::map_entries_hash(m.iter(), m.len(), state);
            },
            // Order-independent hash for Struct entries (String keys, deterministic order).
            DuckValue::Struct(m) => {
                map::map_entries_hash(m.iter(), m.len(), state);
            },
            DuckValue::Union(u) => u.hash(state),
        }
    }
}

// From<&DuckValueRef>

impl<'a> From<&DuckValueRef<'a>> for DuckValue {
    /// Converts this DuckValueRef into a DuckValue, cloning data where necessary
    fn from(value: &DuckValueRef<'a>) -> Self {
        match value {
            DuckValueRef::Null => DuckValue::Null,
            DuckValueRef::Boolean(b) => DuckValue::Boolean(*b),
            DuckValueRef::TinyInt(n) => DuckValue::TinyInt(*n),
            DuckValueRef::SmallInt(n) => DuckValue::SmallInt(*n),
            DuckValueRef::Int(n) => DuckValue::Int(*n),
            DuckValueRef::BigInt(n) => DuckValue::BigInt(*n),
            DuckValueRef::HugeInt(n) => DuckValue::HugeInt(*n),
            DuckValueRef::UTinyInt(n) => DuckValue::UTinyInt(*n),
            DuckValueRef::USmallInt(n) => DuckValue::USmallInt(*n),
            DuckValueRef::UInt(n) => DuckValue::UInt(*n),
            DuckValueRef::UBigInt(n) => DuckValue::UBigInt(*n),
            DuckValueRef::UHugeInt(n) => DuckValue::UHugeInt(*n),
            DuckValueRef::Float(n) => DuckValue::Float(*n),
            DuckValueRef::Double(n) => DuckValue::Double(*n),
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(t) => DuckValue::Timestamp(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(t) => DuckValue::Timestamp(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampS(t) => DuckValue::TimestampS(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampS(t) => DuckValue::TimestampS(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampMs(t) => DuckValue::TimestampMs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampMs(t) => DuckValue::TimestampMs(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampNs(t) => DuckValue::TimestampNs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampNs(t) => DuckValue::TimestampNs(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(t) => DuckValue::TimestampTz(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampTz(t) => DuckValue::TimestampTz(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::Date(d) => DuckValue::Date(*d),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Date(d) => DuckValue::Date(*d),
            #[cfg(feature = "chrono")]
            DuckValueRef::Time(t) => DuckValue::Time(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Time(t) => DuckValue::Time(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(i) => DuckValue::Interval(*i),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(i) => DuckValue::Interval(*i),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeTz(t) => DuckValue::TimeTz(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeTz(t) => DuckValue::TimeTz(*t),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeNs(t) => DuckValue::TimeNs(*t),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeNs(t) => DuckValue::TimeNs(*t),
            DuckValueRef::Text(s) => DuckValue::Text(s.to_string()),
            #[cfg(feature = "decimal")]
            DuckValueRef::Decimal(d) => DuckValue::Decimal(*d),
            DuckValueRef::Blob(b) => DuckValue::Blob(b.clone()),
            DuckValueRef::List(l) => DuckValue::List(l.iter().map(DuckValue::from).collect()),
            DuckValueRef::Enum(e) => DuckValue::Enum(e.to_string()),
            DuckValueRef::Struct(m) => {
                DuckValue::Struct(m.iter().map(|(k, v)| (k.clone(), DuckValue::from(v))).collect())
            },
            DuckValueRef::Array(a) => DuckValue::Array(
                a.iter()
                    .map(|v| DuckValue::from(&v.clone()))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            DuckValueRef::Map(m) => DuckValue::Map(
                m.iter().map(|(k, v)| (DuckValue::from(k), DuckValue::from(v))).collect(),
            ),
            DuckValueRef::Union(u) => DuckValue::Union(Box::new(DuckValue::from(u.as_ref()))),
        }
    }
}

// Macro to implement DuckDialect for types
macro_rules! simple_type_conversion {
    ($row_index:expr, $vector_ptr:expr, $rust_type:expr, $duck_primitive_type:ty) => {{
        // SAFETY: `$vector_ptr` is a valid duckdb_vector obtained from
        // `duckdb_data_chunk_get_vector`. `duckdb_vector_get_data` returns a pointer to the
        // column's raw data buffer which is valid for at least the chunk's row count entries.
        let data_ptr = unsafe { duckdb_vector_get_data($vector_ptr) };
        let values: *mut $duck_primitive_type = data_ptr as *mut $duck_primitive_type;
        // SAFETY: `$row_index` is within [0, chunk row count), so `values.add($row_index)`
        // is within the allocated column buffer for this type.
        let primitive_value = unsafe { *values.add($row_index as usize) as $duck_primitive_type };
        Ok($rust_type(primitive_value))
    }};
}

/// Reads a fixed-width `#[repr(C)]` FFI struct (e.g. `duckdb_date`, `duckdb_timestamp`, …)
/// directly from the packed chunk vector, then converts it via `DuckDialect::from_duck`.
///
/// This avoids the invalid integer→pointer cast that plagued the old temporal read arms
/// and reads at the correct width for each type (e.g. 4 bytes for DATE, 8 for TIMESTAMP).
macro_rules! read_packed {
    ($val:expr, $row_idx:expr, $raw_ty:ty, $target:ty) => {{
        let raw: $raw_ty = {
            // SAFETY: the column stores `$raw_ty` inline in packed layout; `$row_idx` is within
            // [0, chunk_size), so `.add($row_idx)` is in-bounds and correctly aligned for
            // `$raw_ty` (all temporal FFI structs are `#[repr(C)]` with natural alignment).
            unsafe { *(duckdb_vector_get_data($val) as *const $raw_ty).add($row_idx as usize) }
        };
        <$target as DuckDialect<$raw_ty>>::from_duck(raw)
    }};
}

impl DuckValue {
    pub(crate) fn from_duckdb_vec(
        val: duckdb_vector,
        t: duckdb_type,
        row_idx: u64,
    ) -> Result<DuckValue, DuckDBConversionError> {
        // SAFETY: `val` is a valid duckdb_vector; the validity bitmap is valid for at
        // least the chunk's row count.
        let validity_ptr = unsafe { duckdb_vector_get_validity(val) };
        // SAFETY: `row_idx` is within [0, chunk row count).
        let is_valid = unsafe { duckdb_validity_row_is_valid(validity_ptr, row_idx) };

        if !is_valid {
            return Ok(DuckValue::Null);
        }

        match t {
            DUCKDB_TYPE_DUCKDB_TYPE_INVALID => {
                Err(DuckDBConversionError::ConversionError(String::from("invalid type")))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => Ok(DuckValue::Null),
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
                simple_type_conversion!(row_idx, val, DuckValue::Boolean, bool)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => {
                simple_type_conversion!(row_idx, val, DuckValue::TinyInt, i8)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                simple_type_conversion!(row_idx, val, DuckValue::SmallInt, i16)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                simple_type_conversion!(row_idx, val, DuckValue::Int, i32)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                simple_type_conversion!(row_idx, val, DuckValue::BigInt, i64)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UTinyInt, u8)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                simple_type_conversion!(row_idx, val, DuckValue::USmallInt, u16)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                simple_type_conversion!(row_idx, val, DuckValue::UInt, u32)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UBigInt, u64)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                simple_type_conversion!(row_idx, val, DuckValue::Float, f32)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
                simple_type_conversion!(row_idx, val, DuckValue::Double, f64)
            },

            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UHugeInt, u128)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                read_packed!(val, row_idx, duckdb_hugeint, i128).map(DuckValue::HugeInt)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(val, row_idx, duckdb_date, chrono::NaiveDate).map(DuckValue::Date)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    read_packed!(val, row_idx, duckdb_date, crate::types::date_native::DuckDate)
                        .map(DuckValue::Date)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(val, row_idx, duckdb_time, chrono::NaiveTime).map(DuckValue::Time)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    read_packed!(val, row_idx, duckdb_time, crate::types::date_native::DuckTime)
                        .map(DuckValue::Time)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(val, row_idx, duckdb_timestamp, chrono::NaiveDateTime)
                        .map(DuckValue::Timestamp)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    read_packed!(val, row_idx, duckdb_timestamp, std::time::SystemTime)
                        .map(DuckValue::Timestamp)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(val, row_idx, duckdb_interval, chrono::Duration)
                        .map(DuckValue::Interval)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    read_packed!(val, row_idx, duckdb_interval, std::time::Duration)
                        .map(DuckValue::Interval)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_timestamp_s,
                        crate::types::date_chrono::TimestampS
                    )
                    .map(|t| DuckValue::TimestampS(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use std::time::UNIX_EPOCH;
                    let secs = unsafe {
                        (*(duckdb_vector_get_data(val) as *const duckdb_timestamp_s)
                            .add(row_idx as usize))
                        .seconds
                    };
                    let abs = secs.unsigned_abs();
                    Ok(DuckValue::TimestampS(if secs >= 0 {
                        UNIX_EPOCH + std::time::Duration::from_secs(abs)
                    } else {
                        UNIX_EPOCH - std::time::Duration::from_secs(abs)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_timestamp_ms,
                        crate::types::date_chrono::TimestampMs
                    )
                    .map(|t| DuckValue::TimestampMs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use std::time::UNIX_EPOCH;
                    let millis = unsafe {
                        (*(duckdb_vector_get_data(val) as *const duckdb_timestamp_ms)
                            .add(row_idx as usize))
                        .millis
                    };
                    let abs = millis.unsigned_abs();
                    Ok(DuckValue::TimestampMs(if millis >= 0 {
                        UNIX_EPOCH + std::time::Duration::from_millis(abs)
                    } else {
                        UNIX_EPOCH - std::time::Duration::from_millis(abs)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_timestamp_ns,
                        crate::types::date_chrono::TimestampNs
                    )
                    .map(|t| DuckValue::TimestampNs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use std::time::UNIX_EPOCH;
                    let nanos = unsafe {
                        (*(duckdb_vector_get_data(val) as *const duckdb_timestamp_ns)
                            .add(row_idx as usize))
                        .nanos
                    };
                    let abs = nanos.unsigned_abs();
                    Ok(DuckValue::TimestampNs(if nanos >= 0 {
                        UNIX_EPOCH + std::time::Duration::from_nanos(abs)
                    } else {
                        UNIX_EPOCH - std::time::Duration::from_nanos(abs)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR | DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL => {
                // SAFETY: VARCHAR columns store an array of `duckdb_string_t`.
                // We copy into an owned `String` before returning.
                unsafe {
                    let values = duckdb_vector_get_data(val) as *mut duckdb_string_t;
                    let mut duck_string_t = *values.add(row_idx as usize);
                    let char_ptr = duckdb_string_t_data(&mut duck_string_t);
                    let len = duckdb_string_t_length(duck_string_t);

                    Ok(DuckValue::Text(
                        String::from_utf8_lossy(std::slice::from_raw_parts(
                            char_ptr as *const u8,
                            len as usize,
                        ))
                        .into_owned(),
                    ))
                    // let rust_str = CStr::from_ptr(char_ptr)
                    //     .to_str()
                    //     .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
                    // Ok(DuckValue::Text(rust_str.to_string()))
                    // String::from_duck(rust_string).map(DuckValue::Text)

                    // let c_str_ptr = duckdb_string_t_data(duck_string);
                    // let rust_string =
                    //     std::ffi::CStr::from_ptr(c_str_ptr).to_string_lossy().into_owned();
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
                // SAFETY: BLOB columns use the same `duckdb_string_t` layout as VARCHAR.
                let bytes = unsafe {
                    // TODO: use duckdb_get_blob(value)
                    let values = duckdb_vector_get_data(val) as *mut duckdb_string_t;
                    let mut s = *values.add(row_idx as usize);
                    let ptr = duckdb_string_t_data(&mut s);
                    let len = duckdb_string_t_length(s) as usize;
                    std::slice::from_raw_parts(ptr as *const u8, len).to_vec()
                };
                Ok(DuckValue::Blob(Blob::new(bytes)))
            },
            #[cfg(feature = "decimal")]
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                // SAFETY: `val` is a valid duckdb_vector; the data pointer is valid for the
                // chunk's row count. We read the raw i64 at `row_idx` as a decimal.
                let data_ptr = unsafe { duckdb_vector_get_data(val) as *mut i64 };
                // SAFETY: `row_idx` is within [0, chunk_size).
                let value = unsafe { *data_ptr.add(row_idx as usize) as crate::ffi::duckdb_value };
                Decimal::from_duck(value).map(DuckValue::Decimal)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {
                // SAFETY: `val` is a valid duckdb_vector from an active DuckDB result.
                // `duckdb_vector_get_column_type` returns a new logical type that the caller
                // must destroy with `duckdb_destroy_logical_type`.
                let mut logical_type = unsafe { duckdb_vector_get_column_type(val) };
                // SAFETY: `logical_type` is a valid duckdb_logical_type of ENUM kind.
                let dict_size = unsafe { duckdb_enum_dictionary_size(logical_type) };
                // SAFETY: The dictionary size determines storage width per the DuckDB spec.
                // `row_idx` is within [0, chunk_size).
                let raw_index: u32 = unsafe {
                    let data = duckdb_vector_get_data(val);
                    if dict_size <= u8::MAX as u32 {
                        *(data as *const u8).add(row_idx as usize) as u32
                    } else if dict_size <= u16::MAX as u32 {
                        *(data as *const u16).add(row_idx as usize) as u32
                    } else {
                        *(data as *const u32).add(row_idx as usize)
                    }
                };
                // SAFETY: `raw_index` is within [0, dict_size). The returned C string is a
                // heap-allocated null-terminated UTF-8 string that we must free with `duckdb_free`.
                let c_str_ptr =
                    unsafe { duckdb_enum_dictionary_value(logical_type, raw_index as idx_t) };
                let name = if c_str_ptr.is_null() {
                    // SAFETY: `logical_type` was obtained from `duckdb_vector_get_column_type`.
                    unsafe { duckdb_destroy_logical_type(&mut logical_type) };
                    return Err(DuckDBConversionError::ConversionError(format!(
                        "enum index {raw_index} out of range (dict size {dict_size})"
                    )));
                } else {
                    // SAFETY: `duckdb_enum_dictionary_value` returns valid null-terminated UTF-8.
                    let s = unsafe { CStr::from_ptr(c_str_ptr) }
                        .to_str()
                        .map(|s| s.to_owned())
                        .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
                    // SAFETY: `c_str_ptr` was allocated by DuckDB and must be freed via `duckdb_free`.
                    unsafe { duckdb_free(c_str_ptr as *mut std::ffi::c_void) };
                    s
                };
                // SAFETY: `logical_type` was obtained from `duckdb_vector_get_column_type`
                // and must be destroyed exactly once.
                unsafe { duckdb_destroy_logical_type(&mut logical_type) };
                Ok(DuckValue::Enum(name))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                crate::types::array::read_list_or_array(val, t, row_idx)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
                // TIMESTAMP_TZ uses the same duckdb_timestamp wire format as TIMESTAMP.
                #[cfg(feature = "chrono")]
                {
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_timestamp,
                        crate::types::date_chrono::TimestampTz
                    )
                    .map(|t| DuckValue::TimestampTz(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use std::time::UNIX_EPOCH;
                    let micros = unsafe {
                        (*(duckdb_vector_get_data(val) as *const duckdb_timestamp)
                            .add(row_idx as usize))
                        .micros
                    };
                    let secs = micros / 1_000_000;
                    let sub_micros = (micros % 1_000_000).unsigned_abs() as u32;
                    let abs_secs = secs.unsigned_abs();
                    Ok(DuckValue::TimestampTz(if secs >= 0 {
                        UNIX_EPOCH + std::time::Duration::new(abs_secs, sub_micros * 1_000)
                    } else {
                        UNIX_EPOCH - std::time::Duration::new(abs_secs, sub_micros * 1_000)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                #[cfg(feature = "chrono")]
                {
                    // TODO: We need to use timezone here, but how?
                    read_packed!(val, row_idx, duckdb_time_tz, crate::types::date_chrono::TimeTz)
                        .map(DuckValue::TimeTz)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO: We need to use timezone here, but how?
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_time_tz,
                        crate::types::date_native::DuckTimeTz
                    )
                    .map(DuckValue::TimeTz)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => {
                #[cfg(feature = "chrono")]
                {
                    read_packed!(val, row_idx, duckdb_time_ns, crate::types::date_chrono::TimeNs)
                        .map(|t| DuckValue::TimeNs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    read_packed!(
                        val,
                        row_idx,
                        duckdb_time_ns,
                        crate::types::date_native::DuckTimeNs
                    )
                    .map(DuckValue::TimeNs)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => crate::types::duck_struct::read_struct(val, row_idx),
            DUCKDB_TYPE_DUCKDB_TYPE_UNION => crate::types::union::read_union(val, row_idx),
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => crate::types::map::read_map(val, row_idx),
            _ => {
                todo!()
            },
        }
    }
}

impl DuckValue {
    /// Creates a [`duckdb_value`] heap object from this value.
    ///
    /// The returned value must be destroyed with `duckdb_destroy_value`.
    ///
    /// # Errors
    ///
    /// Returns [`DuckDBConversionError`] for empty collections whose element type
    /// cannot be inferred.
    pub fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        match self {
            DuckValue::Null => {
                // SAFETY: duckdb_create_null_value always succeeds.
                Ok(unsafe { duckdb_create_null_value() })
            },
            DuckValue::Boolean(b) => b.to_duck(),
            DuckValue::TinyInt(n) => n.to_duck(),
            DuckValue::SmallInt(n) => n.to_duck(),
            DuckValue::Int(n) => n.to_duck(),
            DuckValue::BigInt(n) => n.to_duck(),
            DuckValue::HugeInt(n) => n.to_duck(),
            DuckValue::UTinyInt(n) => n.to_duck(),
            DuckValue::USmallInt(n) => n.to_duck(),
            DuckValue::UInt(n) => n.to_duck(),
            DuckValue::UBigInt(n) => n.to_duck(),
            DuckValue::UHugeInt(n) => {
                let uhi = duckdb_uhugeint { lower: *n as u64, upper: (*n >> 64) as u64 };
                // SAFETY: `uhi` is a valid duckdb_uhugeint computed from a u128 value.
                Ok(unsafe { duckdb_create_uhugeint(uhi) })
            },
            DuckValue::Float(f) => f.to_duck(),
            DuckValue::Double(d) => d.to_duck(),

            #[cfg(feature = "chrono")]
            DuckValue::Timestamp(dt) => dt.to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampS(dt) => crate::types::date_chrono::TimestampS(*dt).to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampMs(dt) => crate::types::date_chrono::TimestampMs(*dt).to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampNs(dt) => crate::types::date_chrono::TimestampNs(*dt).to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimestampTz(dt) => crate::types::date_chrono::TimestampTz(*dt).to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::Date(d) => d.to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::Time(t) => t.to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::Interval(d) => d.to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimeTz(tz) => tz.to_duck(),
            #[cfg(feature = "chrono")]
            DuckValue::TimeNs(t) => crate::types::date_chrono::TimeNs(*t).to_duck(),

            #[cfg(not(feature = "chrono"))]
            DuckValue::Timestamp(st)
            | DuckValue::TimestampS(st)
            | DuckValue::TimestampMs(st)
            | DuckValue::TimestampNs(st)
            | DuckValue::TimestampTz(st) => st.to_duck(),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Date(d) => d.to_duck(),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Time(t) => t.to_duck(),
            #[cfg(not(feature = "chrono"))]
            DuckValue::Interval(d) => d.to_duck(),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeTz(tz) => tz.to_duck(),
            #[cfg(not(feature = "chrono"))]
            DuckValue::TimeNs(t) => t.to_duck(),

            DuckValue::Text(s) | DuckValue::Enum(s) => s.to_duck(),
            DuckValue::Blob(b) => b.to_duck(),

            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => d.to_duck(),

            DuckValue::List(items) => crate::types::array::list_to_duck(items),
            DuckValue::Array(items) => crate::types::array::array_to_duck(items),
            DuckValue::Struct(m) => crate::types::duck_struct::struct_to_duck(m),
            DuckValue::Map(m) => crate::types::map::map_to_duck(m),
            DuckValue::Union(inner) => crate::types::union::union_to_duck(inner),
        }
    }

    /// Returns a newly-allocated [`duckdb_logical_type`] that describes `val`.
    ///
    /// The caller is responsible for destroying the returned type with
    /// `duckdb_destroy_logical_type`.
    ///
    /// # Errors
    ///
    /// Returns [`DuckDBConversionError`] if the type cannot be determined (empty
    /// `List`, `Array`, `Struct`, or `Map`).
    pub fn logical_type_of(val: &DuckValue) -> Result<duckdb_logical_type, DuckDBConversionError> {
        macro_rules! scalar_lt {
            ($t:expr) => {{
                // SAFETY: scalar type constants are always valid duckdb_type values.
                Ok(unsafe { duckdb_create_logical_type($t) })
            }};
        }
        match val {
            DuckValue::Null => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL),
            DuckValue::Boolean(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN),
            DuckValue::TinyInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TINYINT),
            DuckValue::SmallInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT),
            DuckValue::Int(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            DuckValue::BigInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT),
            DuckValue::HugeInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT),
            DuckValue::UTinyInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT),
            DuckValue::USmallInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT),
            DuckValue::UInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER),
            DuckValue::UBigInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT),
            DuckValue::UHugeInt(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT),
            DuckValue::Float(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_FLOAT),
            DuckValue::Double(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE),
            DuckValue::Timestamp(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP),
            DuckValue::TimestampS(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S),
            DuckValue::TimestampMs(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS),
            DuckValue::TimestampNs(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS),
            DuckValue::TimestampTz(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ),
            DuckValue::Date(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_DATE),
            DuckValue::Time(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIME),
            DuckValue::Interval(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL),
            DuckValue::TimeTz(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ),
            DuckValue::TimeNs(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS),
            DuckValue::Text(_) | DuckValue::Enum(_) => {
                scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR)
            },
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL),
            DuckValue::Blob(_) => scalar_lt!(DUCKDB_TYPE_DUCKDB_TYPE_BLOB),

            DuckValue::List(items) => crate::types::array::list_logical_type(items),
            DuckValue::Array(items) => crate::types::array::array_logical_type(items),
            DuckValue::Struct(m) => crate::types::duck_struct::struct_logical_type(m),
            DuckValue::Map(m) => crate::types::map::map_logical_type(m),
            DuckValue::Union(inner) => crate::types::union::union_logical_type(inner),
        }
    }
}

impl DuckValue {
    /// Creates a `Text` value from any string-like type.
    ///
    /// Accepts `&str`, `String`, or anything else that converts into `String`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use better_duck_core::types::value::DuckValue;
    ///
    /// let a = DuckValue::text("hello");
    /// let b = DuckValue::text(String::from("hello"));
    /// assert_eq!(a, b);
    /// ```
    #[inline]
    pub fn text(s: impl Into<String>) -> DuckValue {
        DuckValue::Text(s.into())
    }

    /// Looks up a value in a `Map` variant by any key convertible into [`DuckValue`].
    ///
    /// Returns `None` for non-`Map` variants or a missing key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use better_duck_core::types::value::DuckValue;
    /// use std::collections::HashMap;
    ///
    /// let m: DuckValue = HashMap::from([
    ///     (DuckValue::Int(1), DuckValue::text("one")),
    /// ]).into();
    /// assert_eq!(m.get(1i32), Some(&DuckValue::text("one")));
    /// assert_eq!(m.get(99i32), None);
    /// ```
    #[inline]
    pub fn get(
        &self,
        key: impl Into<DuckValue>,
    ) -> Option<&DuckValue> {
        match self {
            DuckValue::Map(m) => m.get(&key.into()),
            _ => None,
        }
    }

    /// Returns a mutable reference to the value for the given key in a `Map` variant.
    ///
    /// Returns `None` for non-`Map` variants or missing keys.
    #[inline]
    pub fn get_mut(
        &mut self,
        key: impl Into<DuckValue>,
    ) -> Option<&mut DuckValue> {
        match self {
            DuckValue::Map(m) => m.get_mut(&key.into()),
            _ => None,
        }
    }

    /// Returns `true` if the `Map` variant contains the given key.
    ///
    /// Always returns `false` for non-`Map` variants.
    #[inline]
    pub fn contains_key(
        &self,
        key: impl Into<DuckValue>,
    ) -> bool {
        match self {
            DuckValue::Map(m) => m.contains_key(&key.into()),
            _ => false,
        }
    }
}

impl AppendAble for DuckValue {
    /// Binds this value to a prepared-statement parameter at the 1-based index `idx`.
    ///
    /// Delegates to the [`DuckValueRef`] implementation to avoid duplicating per-variant logic.
    fn stmt_append(
        &mut self,
        idx: u64,
        stmt: crate::ffi::duckdb_prepared_statement,
    ) -> crate::error::Result<()> {
        DuckValueRef::from(&*self).stmt_append(idx, stmt)
    }

    /// Appends this value to a DuckDB appender row.
    ///
    /// Delegates to the [`DuckValueRef`] implementation to avoid duplicating per-variant logic.
    fn appender_append(
        &mut self,
        appender: crate::ffi::duckdb_appender,
    ) -> crate::error::Result<()> {
        DuckValueRef::from(&*self).appender_append(appender)
    }
}

impl From<DuckValue> for String {
    fn from(val: DuckValue) -> Self {
        match val {
            DuckValue::Text(ref s) => s.clone(),
            DuckValue::Null => String::new(),
            _ => panic!("Cannot convert {:?} to String", val),
        }
    }
}
impl From<DuckValue> for i64 {
    fn from(val: DuckValue) -> Self {
        match val {
            DuckValue::BigInt(v) => v,
            DuckValue::Int(v) => v as i64,
            DuckValue::SmallInt(v) => v as i64,
            DuckValue::TinyInt(v) => v as i64,
            DuckValue::Null => 0,
            _ => panic!("Cannot convert {:?} to i64", val),
        }
    }
}
impl From<DuckValue> for i32 {
    fn from(val: DuckValue) -> Self {
        match val {
            DuckValue::Int(v) => v,
            DuckValue::SmallInt(v) => v as i32,
            DuckValue::TinyInt(v) => v as i32,
            DuckValue::Null => 0,
            _ => panic!("Cannot convert {:?} to i32", val),
        }
    }
}
