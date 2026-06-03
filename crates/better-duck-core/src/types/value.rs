#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use std::collections::HashMap;
use std::ffi::CStr;
use std::hash::{Hash, Hasher};
use std::mem;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

use crate::{
    ffi::{
        duckdb_create_array_type, duckdb_create_array_value, duckdb_create_blob,
        duckdb_create_list_type, duckdb_create_list_value, duckdb_create_logical_type,
        duckdb_create_map_type, duckdb_create_map_value, duckdb_create_null_value,
        duckdb_create_struct_type, duckdb_create_struct_value, duckdb_create_uhugeint,
        duckdb_create_union_type, duckdb_create_union_value, duckdb_destroy_logical_type,
        duckdb_destroy_value, duckdb_enum_dictionary_size, duckdb_enum_dictionary_value,
        duckdb_free, duckdb_get_type_id, duckdb_list_entry, duckdb_list_vector_get_child,
        duckdb_list_vector_get_size, duckdb_logical_type, duckdb_string_t, duckdb_string_t_data,
        duckdb_string_t_length, duckdb_struct_type_child_count, duckdb_struct_type_child_name,
        duckdb_struct_vector_get_child, duckdb_type, duckdb_uhugeint,
        duckdb_union_type_member_count, duckdb_validity_row_is_valid, duckdb_vector,
        duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity, idx_t,
        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_BLOB, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
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
use crate::types::cmp::{canonical_f32, canonical_f64};

/// Represents any value that can be stored in a DuckDB column.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
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
            DuckValueRef::Blob(b) => DuckValue::Blob(Blob::new(b.to_vec())),
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
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                simple_type_conversion!(row_idx, val, DuckValue::HugeInt, i128)
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
            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UHugeInt, u128)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                simple_type_conversion!(row_idx, val, DuckValue::Float, f32)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
                simple_type_conversion!(row_idx, val, DuckValue::Double, f64)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveDate::from_duck(value).map(DuckValue::Date)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    crate::types::date_native::DuckDate::from_duck(value).map(DuckValue::Date)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveTime::from_duck(value).map(DuckValue::Time)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    crate::types::date_native::DuckTime::from_duck(value).map(DuckValue::Time)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveDateTime::from_duck(value).map(DuckValue::Timestamp)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    std::time::SystemTime::from_duck(value).map(DuckValue::Timestamp)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    chrono::Duration::from_duck(value).map(DuckValue::Interval)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    std::time::Duration::from_duck(value).map(DuckValue::Interval)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    crate::types::date_chrono::TimestampS::from_duck(value)
                        .map(|t| DuckValue::TimestampS(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use crate::ffi::duckdb_get_timestamp_s;
                    use std::time::UNIX_EPOCH;
                    // SAFETY: `value` was cast from a packed-array read; valid as TIMESTAMP_S.
                    let secs = unsafe { duckdb_get_timestamp_s(value) }.seconds;
                    let abs = secs.unsigned_abs();
                    Ok(DuckValue::TimestampS(if secs >= 0 {
                        UNIX_EPOCH + std::time::Duration::from_secs(abs)
                    } else {
                        UNIX_EPOCH - std::time::Duration::from_secs(abs)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    crate::types::date_chrono::TimestampMs::from_duck(value)
                        .map(|t| DuckValue::TimestampMs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use crate::ffi::duckdb_get_timestamp_ms;
                    use std::time::UNIX_EPOCH;
                    // SAFETY: `value` was cast from a packed-array read; valid as TIMESTAMP_MS.
                    let millis = unsafe { duckdb_get_timestamp_ms(value) }.millis;
                    let abs = millis.unsigned_abs();
                    Ok(DuckValue::TimestampMs(if millis >= 0 {
                        UNIX_EPOCH + std::time::Duration::from_millis(abs)
                    } else {
                        UNIX_EPOCH - std::time::Duration::from_millis(abs)
                    }))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    crate::types::date_chrono::TimestampNs::from_duck(value)
                        .map(|t| DuckValue::TimestampNs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    use crate::ffi::duckdb_get_timestamp_ns;
                    use std::time::UNIX_EPOCH;
                    // SAFETY: `value` was cast from a packed-array read; valid as TIMESTAMP_NS.
                    let nanos = unsafe { duckdb_get_timestamp_ns(value) }.nanos;
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
                    let rust_string = CStr::from_ptr(char_ptr).to_string_lossy().into_owned();
                    Ok(DuckValue::Text(rust_string))
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
                // SAFETY: `val` is a valid duckdb_vector of LIST/ARRAY type.
                // `duckdb_vector_get_data` returns a pointer to the column's raw data buffer
                // which holds `duckdb_list_entry` values in packed array layout.
                let data_ptr = unsafe { duckdb_vector_get_data(val) as *mut i64 };
                // SAFETY: `row_idx` is within [0, chunk_size), so `data_ptr.add(row_idx)`
                // is within the allocated column buffer. The reinterpret-cast to
                // `*mut duckdb_list_entry` is valid because list/array vectors store that
                // struct at each slot.
                let list_data =
                    unsafe { *data_ptr.add(row_idx as usize) as *mut duckdb_list_entry };
                // SAFETY: `val` is a valid duckdb_vector of list/array type.
                let list_child = unsafe { duckdb_list_vector_get_child(val) as duckdb_vector };
                // SAFETY: `list_child` is a valid duckdb_vector for the child column.
                let child_validity = unsafe { duckdb_vector_get_validity(list_child) };
                // SAFETY: `list_child` is a valid list-child vector.
                let list_length = unsafe { duckdb_list_vector_get_size(list_child) };
                // TODO: What happens for this var, if the function returns error? (Maybe using https://docs.rs/scopeguard/latest/scopeguard/)
                let mut slice_data: Option<Box<[std::mem::MaybeUninit<DuckValue>]>> = None;
                let mut vec_data: Option<Vec<DuckValue>> = None;
                let iter_ptr: *mut DuckValue;
                if t == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
                    let mut tmp = Box::<[DuckValue]>::new_uninit_slice(list_length as usize);
                    iter_ptr = tmp.as_mut_ptr() as *mut DuckValue;
                    slice_data = Some(tmp);
                } else if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                    let mut tmp = Vec::with_capacity(list_length as usize);
                    iter_ptr = tmp.as_mut_ptr();
                    vec_data = Some(tmp);
                } else {
                    return Err(DuckDBConversionError::ConversionError(String::from(
                        "invalid type for list/array",
                    )));
                }

                // SAFETY: `list_data` is a valid pointer to the list entry for `row_idx`.
                // `(*list_data).offset` gives the count of child elements. `iter_ptr`
                // points to an allocation of at least `list_length` elements. `each` is
                // within that bound, so `iter_ptr.add(each)` is in bounds.
                unsafe {
                    for each in 0..(*list_data).offset {
                        let mut elem = DuckValue::Null;
                        if duckdb_validity_row_is_valid(child_validity, each) {
                            let mut raw_child_type: duckdb_logical_type =
                                duckdb_vector_get_column_type(list_child);
                            let child_type = duckdb_get_type_id(raw_child_type);
                            duckdb_destroy_logical_type(&mut raw_child_type);
                            elem = DuckValue::from_duckdb_vec(list_child, child_type, each)?;
                        }
                        ptr::write(iter_ptr.add(each as usize), elem);
                    }
                };

                if t == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
                    // SAFETY: every element in `slice_data` was written in the loop above.
                    Ok(DuckValue::Array(unsafe { slice_data.unwrap().assume_init() }))
                } else if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                    let mut vec_data = vec_data.unwrap();
                    // SAFETY: all `list_length` elements were written into `vec_data`'s
                    // allocation via `iter_ptr` in the loop above.
                    unsafe { vec_data.set_len(list_length as usize) };
                    Ok(DuckValue::List(vec_data))
                } else {
                    Err(DuckDBConversionError::ConversionError(String::from(
                        "invalid type for list/array",
                    )))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    // TODO: We need to use timezone here, but how?
                    crate::types::date_chrono::TimestampTz::from_duck(value)
                        .map(|t| DuckValue::TimestampTz(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO: We need to use timezone here, but how?
                    use std::time::UNIX_EPOCH;
                    // SAFETY: `value` was cast from a packed-array read; valid as TIMESTAMP_TZ.
                    let micros = unsafe {
                        use crate::ffi::duckdb_get_timestamp_tz;
                        duckdb_get_timestamp_tz(value)
                    }
                    .micros;
                    let secs = micros / 1_000_000;
                    let sub_micros = (micros % 1_000_000).unsigned_abs() as u32;
                    let abs_secs = secs.unsigned_abs();
                    let st = if secs >= 0 {
                        UNIX_EPOCH + std::time::Duration::new(abs_secs, sub_micros * 1_000)
                    } else {
                        UNIX_EPOCH - std::time::Duration::new(abs_secs, sub_micros * 1_000)
                    };
                    Ok(DuckValue::TimestampTz(st))
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    // TODO: We need to use timezone here, but how?
                    crate::types::date_chrono::TimeTz::from_duck(value).map(DuckValue::TimeTz)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO: We need to use timezone here, but how?
                    crate::types::date_native::DuckTimeTz::from_duck(value).map(DuckValue::TimeTz)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS => {
                // SAFETY: The temporal type stores its raw value in packed array layout.
                // `row_idx` is within [0, chunk_size), so the offset is in-bounds.
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    crate::types::date_chrono::TimeNs::from_duck(value)
                        .map(|t| DuckValue::TimeNs(t.0))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    crate::types::date_native::DuckTimeNs::from_duck(value).map(DuckValue::TimeNs)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
                // SAFETY: `val` is a valid struct vector.  The column logical type is
                // heap-allocated by DuckDB and must be destroyed exactly once with
                // `duckdb_destroy_logical_type`.
                let mut lt = unsafe { duckdb_vector_get_column_type(val) };
                // SAFETY: `lt` is a valid logical type of STRUCT kind.
                let n: idx_t = unsafe { duckdb_struct_type_child_count(lt) };
                let mut pairs: HashMap<String, DuckValue> = HashMap::with_capacity(n as usize);
                let mut read_err: Option<DuckDBConversionError> = None;

                for i in 0..n {
                    // SAFETY: `lt` is valid; `i` is within [0, n).
                    // The returned C string is heap-allocated by DuckDB and must be
                    // freed with `duckdb_free`.
                    let name_ptr = unsafe { duckdb_struct_type_child_name(lt, i) };
                    let name = if name_ptr.is_null() {
                        read_err = Some(DuckDBConversionError::ConversionError(format!(
                            "struct child name at index {i} is null"
                        )));
                        break;
                    } else {
                        // SAFETY: `name_ptr` is a valid null-terminated C string.
                        let s = unsafe { std::ffi::CStr::from_ptr(name_ptr) }
                            .to_str()
                            .map(str::to_owned);
                        // SAFETY: `name_ptr` was allocated by DuckDB and must be freed
                        // with `duckdb_free`.
                        unsafe { duckdb_free(name_ptr as *mut std::ffi::c_void) };
                        match s {
                            Ok(s) => s,
                            Err(e) => {
                                read_err =
                                    Some(DuckDBConversionError::ConversionError(e.to_string()));
                                break;
                            },
                        }
                    };

                    // SAFETY: `val` is a valid struct vector; `i` is within [0, n).
                    // The child vector shares the parent's lifetime and must not be
                    // independently freed.
                    let child_vec = unsafe { duckdb_struct_vector_get_child(val, i) };
                    // SAFETY: `child_vec` is a valid vector for this struct child.
                    // The returned logical type must be destroyed with
                    // `duckdb_destroy_logical_type`.
                    let mut child_lt = unsafe { duckdb_vector_get_column_type(child_vec) };
                    // SAFETY: `child_lt` is a valid logical type.
                    let child_tid = unsafe { duckdb_get_type_id(child_lt) };
                    // SAFETY: `child_lt` was returned by `duckdb_vector_get_column_type`
                    // and must be destroyed exactly once.
                    unsafe { duckdb_destroy_logical_type(&mut child_lt) };

                    // Recurse: validity is checked inside `from_duckdb_vec`; a null
                    // child field comes back as `DuckValue::Null`.
                    match DuckValue::from_duckdb_vec(child_vec, child_tid, row_idx) {
                        Ok(v) => {
                            pairs.insert(name, v);
                        },
                        Err(e) => {
                            read_err = Some(e);
                            break;
                        },
                    }
                }

                // SAFETY: `lt` was returned by `duckdb_vector_get_column_type` and
                // must be destroyed exactly once, even on the error path.
                unsafe { duckdb_destroy_logical_type(&mut lt) };

                match read_err {
                    Some(e) => Err(e),
                    None => Ok(DuckValue::Struct(pairs)),
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UNION => {
                // DuckDB UNION is physically a STRUCT where:
                //   child 0 = union tag discriminant (UTINYINT or USMALLINT)
                //   child 1..=N = member values (only the active member is valid)
                //
                // SAFETY: `val` is a valid union vector.  The logical type is
                // heap-allocated and must be destroyed exactly once.
                let mut lt = unsafe { duckdb_vector_get_column_type(val) };
                // SAFETY: `lt` is a valid logical type of UNION kind.
                let member_count: idx_t = unsafe { duckdb_union_type_member_count(lt) };

                // SAFETY: Child 0 of the underlying struct layout is the tag vector.
                let tag_vec = unsafe { duckdb_struct_vector_get_child(val, 0) };

                // Read the discriminant. DuckDB uses UTINYINT (u8) for ≤ 255 members
                // and USMALLINT (u16) otherwise.
                // SAFETY: `tag_vec` is a valid data vector; `row_idx` is within
                // [0, chunk_size).
                let tag: idx_t = unsafe {
                    let data = duckdb_vector_get_data(tag_vec);
                    if member_count <= u8::MAX as idx_t {
                        *(data as *const u8).add(row_idx as usize) as idx_t
                    } else {
                        *(data as *const u16).add(row_idx as usize) as idx_t
                    }
                };

                if tag >= member_count {
                    // SAFETY: `lt` must be destroyed even on this error path.
                    unsafe { duckdb_destroy_logical_type(&mut lt) };
                    return Err(DuckDBConversionError::ConversionError(format!(
                        "union tag {tag} out of range (member count {member_count})"
                    )));
                }

                // The active member sits at child index (tag + 1) in the underlying struct.
                // SAFETY: `val` is a valid union vector; `tag + 1` is within
                // [1, member_count + 1).
                let member_vec = unsafe { duckdb_struct_vector_get_child(val, tag + 1) };
                // SAFETY: `member_vec` is a valid vector for the active member.
                let mut member_lt = unsafe { duckdb_vector_get_column_type(member_vec) };
                // SAFETY: `member_lt` is a valid logical type.
                let member_tid = unsafe { duckdb_get_type_id(member_lt) };
                // SAFETY: `member_lt` was returned by `duckdb_vector_get_column_type`.
                unsafe { duckdb_destroy_logical_type(&mut member_lt) };

                // Recurse into the active member.
                let inner = DuckValue::from_duckdb_vec(member_vec, member_tid, row_idx);

                // SAFETY: `lt` was returned by `duckdb_vector_get_column_type` and
                // must be destroyed exactly once.
                unsafe { duckdb_destroy_logical_type(&mut lt) };

                inner.map(|v| DuckValue::Union(Box::new(v)))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                // MAP is physically stored as LIST<STRUCT(key, value)> in DuckDB.
                // Read it like a LIST: fetch the list entry for `row_idx`, then iterate
                // the flat STRUCT child vector using key child 0 and value child 1.
                //
                // SAFETY: `val` is a valid MAP vector.  MAP data is laid out identically
                // to LIST: each row slot holds a `duckdb_list_entry { offset, length }`.
                let data_ptr = unsafe { duckdb_vector_get_data(val) as *const duckdb_list_entry };
                // SAFETY: `row_idx` is within [0, chunk_size).
                let entry: duckdb_list_entry = unsafe { *data_ptr.add(row_idx as usize) };

                // `entries` is the flat STRUCT(key, value) child vector.
                // SAFETY: `val` is a valid MAP/LIST vector.
                let entries_vec = unsafe { duckdb_list_vector_get_child(val) };
                // SAFETY: `entries_vec` is a valid STRUCT vector; child 0 = keys.
                let key_vec = unsafe { duckdb_struct_vector_get_child(entries_vec, 0) };
                // SAFETY: `entries_vec` is a valid STRUCT vector; child 1 = values.
                let val_vec_map = unsafe { duckdb_struct_vector_get_child(entries_vec, 1) };

                // SAFETY: `key_vec` is a valid vector.
                let mut key_lt = unsafe { duckdb_vector_get_column_type(key_vec) };
                // SAFETY: `key_lt` was returned by `duckdb_vector_get_column_type`.
                let key_tid = unsafe { duckdb_get_type_id(key_lt) };
                // SAFETY: `key_lt` was allocated by `duckdb_vector_get_column_type` above.
                unsafe { duckdb_destroy_logical_type(&mut key_lt) };

                // SAFETY: `val_vec_map` is a valid vector.
                let mut vlt = unsafe { duckdb_vector_get_column_type(val_vec_map) };
                // SAFETY: `vlt` was returned by `duckdb_vector_get_column_type`.
                let val_tid = unsafe { duckdb_get_type_id(vlt) };
                // SAFETY: `vlt` was allocated by `duckdb_vector_get_column_type` above.
                unsafe { duckdb_destroy_logical_type(&mut vlt) };

                let mut pairs: HashMap<String, DuckValue> =
                    HashMap::with_capacity(entry.length as usize);
                let mut read_err: Option<DuckDBConversionError> = None;

                for j in entry.offset..entry.offset + entry.length {
                    let k = match DuckValue::from_duckdb_vec(key_vec, key_tid, j) {
                        Ok(v) => v,
                        Err(e) => {
                            read_err = Some(e);
                            break;
                        },
                    };
                    let v = match DuckValue::from_duckdb_vec(val_vec_map, val_tid, j) {
                        Ok(v) => v,
                        Err(e) => {
                            read_err = Some(e);
                            break;
                        },
                    };
                    pairs.insert(k.to_map_key(), v);
                }

                match read_err {
                    Some(e) => Err(e),
                    None => Ok(DuckValue::Map(pairs)),
                }
            },
            _ => {
                todo!()
            },
        }
    }

    /// Converts this value to a string key suitable for use as a MAP entry key.
    ///
    /// - `Text` and `Enum` variants yield the string directly (zero-copy clone).
    /// - Scalar variants use their standard display representation.
    /// - Complex values (`List`, `Struct`, `Map`, temporal, …) fall back to `Debug` format.
    pub fn to_map_key(&self) -> String {
        match self {
            DuckValue::Text(s) | DuckValue::Enum(s) => s.clone(),
            DuckValue::Null => "null".to_owned(),
            DuckValue::Boolean(b) => b.to_string(),
            DuckValue::TinyInt(n) => n.to_string(),
            DuckValue::SmallInt(n) => n.to_string(),
            DuckValue::Int(n) => n.to_string(),
            DuckValue::BigInt(n) => n.to_string(),
            DuckValue::HugeInt(n) => n.to_string(),
            DuckValue::UTinyInt(n) => n.to_string(),
            DuckValue::USmallInt(n) => n.to_string(),
            DuckValue::UInt(n) => n.to_string(),
            DuckValue::UBigInt(n) => n.to_string(),
            DuckValue::UHugeInt(n) => n.to_string(),
            DuckValue::Float(f) => f.to_string(),
            DuckValue::Double(d) => d.to_string(),
            other => format!("{other:?}"),
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

            // Collections
            DuckValue::List(items) => {
                if items.is_empty() {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot convert empty List to duckdb_value: element type unknown".into(),
                    ));
                }
                let mut child_lt = DuckValue::logical_type_of(&items[0])?;
                let mut child_dvs: Vec<duckdb_value> = Vec::with_capacity(items.len());
                let mut err: Option<DuckDBConversionError> = None;
                for item in items {
                    match item.to_duck() {
                        Ok(v) => child_dvs.push(v),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut v in child_dvs {
                        // SAFETY: each `v` was created by `to_duck()` above.
                        unsafe { duckdb_destroy_value(&mut v) };
                    }
                    // SAFETY: `child_lt` was allocated by `logical_type_of` above.
                    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                    return Err(e);
                }
                // SAFETY: `child_lt` is valid; `child_dvs` has `len()` elements.
                let result = unsafe {
                    duckdb_create_list_value(
                        child_lt,
                        child_dvs.as_mut_ptr(),
                        child_dvs.len() as idx_t,
                    )
                };
                // SAFETY: `child_lt` was allocated by `logical_type_of`; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                for mut v in child_dvs {
                    // SAFETY: each `v` was created by `to_duck()` above.
                    unsafe { duckdb_destroy_value(&mut v) };
                }
                Ok(result)
            },

            DuckValue::Array(items) => {
                if items.is_empty() {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot convert empty Array to duckdb_value: element type unknown".into(),
                    ));
                }
                let mut child_lt = DuckValue::logical_type_of(&items[0])?;
                let mut child_dvs: Vec<duckdb_value> = Vec::with_capacity(items.len());
                let mut err: Option<DuckDBConversionError> = None;
                for item in items.iter() {
                    match item.to_duck() {
                        Ok(v) => child_dvs.push(v),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut v in child_dvs {
                        // SAFETY: each `v` was created by `to_duck()` above.
                        unsafe { duckdb_destroy_value(&mut v) };
                    }
                    // SAFETY: `child_lt` was allocated by `logical_type_of` above.
                    unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                    return Err(e);
                }
                // SAFETY: `child_lt` is valid; array_size matches item count.
                let mut arr_lt =
                    unsafe { duckdb_create_array_type(child_lt, child_dvs.len() as idx_t) };
                // SAFETY: `child_lt` was allocated by `logical_type_of`; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                // SAFETY: `arr_lt` is valid; `child_dvs` has `len()` elements.
                let result = unsafe {
                    duckdb_create_array_value(
                        arr_lt,
                        child_dvs.as_mut_ptr(),
                        child_dvs.len() as idx_t,
                    )
                };
                // SAFETY: `arr_lt` was allocated above; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut arr_lt) };
                for mut v in child_dvs {
                    // SAFETY: each `v` was created by `to_duck()` above.
                    unsafe { duckdb_destroy_value(&mut v) };
                }
                Ok(result)
            },

            DuckValue::Struct(m) => {
                let entries: Vec<(&String, &DuckValue)> = m.iter().collect();
                let n = entries.len();
                if n == 0 {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot convert empty Struct to duckdb_value".into(),
                    ));
                }
                let mut member_types: Vec<duckdb_logical_type> = Vec::with_capacity(n);
                let mut c_names: Vec<std::ffi::CString> = Vec::with_capacity(n);
                let mut err: Option<DuckDBConversionError> = None;
                for (k, v) in &entries {
                    match DuckValue::logical_type_of(v) {
                        Ok(lt) => member_types.push(lt),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                    match std::ffi::CString::new(k.as_str()) {
                        Ok(c) => c_names.push(c),
                        Err(e) => {
                            err = Some(DuckDBConversionError::ConversionError(e.to_string()));
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut lt in member_types {
                        // SAFETY: each `lt` was allocated by `logical_type_of` above.
                        unsafe { duckdb_destroy_logical_type(&mut lt) };
                    }
                    return Err(e);
                }
                let mut name_ptrs: Vec<*const std::os::raw::c_char> =
                    c_names.iter().map(|c| c.as_ptr()).collect();
                // SAFETY: `member_types`/`name_ptrs` valid arrays of `n`; create copies both.
                let mut struct_lt = unsafe {
                    duckdb_create_struct_type(
                        member_types.as_mut_ptr(),
                        name_ptrs.as_mut_ptr(),
                        n as idx_t,
                    )
                };
                for mut lt in member_types {
                    // SAFETY: each `lt` was allocated by `logical_type_of` above.
                    unsafe { duckdb_destroy_logical_type(&mut lt) };
                }
                let mut member_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
                let mut err: Option<DuckDBConversionError> = None;
                for (_, v) in &entries {
                    match v.to_duck() {
                        Ok(dv) => member_dvs.push(dv),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut dv in member_dvs {
                        // SAFETY: each `dv` was created by `to_duck()` above.
                        unsafe { duckdb_destroy_value(&mut dv) };
                    }
                    // SAFETY: `struct_lt` was allocated above; destroy once.
                    unsafe { duckdb_destroy_logical_type(&mut struct_lt) };
                    return Err(e);
                }
                // SAFETY: `struct_lt` valid; `member_dvs` in schema-declaration order.
                let result =
                    unsafe { duckdb_create_struct_value(struct_lt, member_dvs.as_mut_ptr()) };
                // SAFETY: `struct_lt` was allocated above; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut struct_lt) };
                for mut dv in member_dvs {
                    // SAFETY: each `dv` was created by `to_duck()` above.
                    unsafe { duckdb_destroy_value(&mut dv) };
                }
                Ok(result)
            },

            DuckValue::Map(m) => {
                let n = m.len();
                if n == 0 {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot convert empty Map to duckdb_value: value type unknown".into(),
                    ));
                }
                let pairs: Vec<(&String, &DuckValue)> = m.iter().collect();
                // SAFETY: the type constant is always a valid duckdb_type.
                let mut key_lt =
                    unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR) };
                let mut val_lt = match DuckValue::logical_type_of(pairs[0].1) {
                    Ok(lt) => lt,
                    Err(e) => {
                        // SAFETY: `key_lt` was allocated above.
                        unsafe { duckdb_destroy_logical_type(&mut key_lt) };
                        return Err(e);
                    },
                };
                // SAFETY: both types are valid; `duckdb_create_map_type` copies them.
                let mut map_lt = unsafe { duckdb_create_map_type(key_lt, val_lt) };
                // SAFETY: `key_lt` was allocated above; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut key_lt) };
                // SAFETY: `val_lt` was allocated by `logical_type_of` above.
                unsafe { duckdb_destroy_logical_type(&mut val_lt) };

                let mut key_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
                let mut val_dvs: Vec<duckdb_value> = Vec::with_capacity(n);
                let mut err: Option<DuckDBConversionError> = None;
                for (k, v) in &pairs {
                    match k.to_duck() {
                        Ok(kv) => key_dvs.push(kv),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                    match v.to_duck() {
                        Ok(vv) => val_dvs.push(vv),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut kv in key_dvs {
                        // SAFETY: each `kv` was created by `to_duck()` above.
                        unsafe { duckdb_destroy_value(&mut kv) };
                    }
                    for mut vv in val_dvs {
                        // SAFETY: each `vv` was created by `to_duck()` above.
                        unsafe { duckdb_destroy_value(&mut vv) };
                    }
                    // SAFETY: `map_lt` was allocated above; destroy once.
                    unsafe { duckdb_destroy_logical_type(&mut map_lt) };
                    return Err(e);
                }
                // SAFETY: `map_lt` valid; key/val arrays have `n` elements each.
                let result = unsafe {
                    duckdb_create_map_value(
                        map_lt,
                        key_dvs.as_mut_ptr(),
                        val_dvs.as_mut_ptr(),
                        n as idx_t,
                    )
                };
                // SAFETY: `map_lt` was allocated above; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut map_lt) };
                for mut kv in key_dvs {
                    // SAFETY: each `kv` was created by `to_duck()` above.
                    unsafe { duckdb_destroy_value(&mut kv) };
                }
                for mut vv in val_dvs {
                    // SAFETY: each `vv` was created by `to_duck()` above.
                    unsafe { duckdb_destroy_value(&mut vv) };
                }
                Ok(result)
            },

            DuckValue::Union(inner) => {
                let mut member_lt = DuckValue::logical_type_of(inner)?;
                let c_name = std::ffi::CString::new("value").unwrap();
                let mut name_ptr: *const std::os::raw::c_char = c_name.as_ptr();
                // SAFETY: single-element arrays of valid pointers; create copies both.
                let mut union_lt =
                    unsafe { duckdb_create_union_type(&mut member_lt, &mut name_ptr, 1) };
                // SAFETY: `member_lt` was allocated by `logical_type_of` above.
                unsafe { duckdb_destroy_logical_type(&mut member_lt) };
                let mut member_dv = match inner.to_duck() {
                    Ok(v) => v,
                    Err(e) => {
                        // SAFETY: `union_lt` was allocated above; destroy once.
                        unsafe { duckdb_destroy_logical_type(&mut union_lt) };
                        return Err(e);
                    },
                };
                // SAFETY: `union_lt` valid; tag_index=0 (single-member union); `member_dv` valid.
                let result = unsafe { duckdb_create_union_value(union_lt, 0, member_dv) };
                // SAFETY: `union_lt` was allocated above; destroy once.
                unsafe { duckdb_destroy_logical_type(&mut union_lt) };
                // SAFETY: `member_dv` was created by `to_duck()` above; destroy once.
                unsafe { duckdb_destroy_value(&mut member_dv) };
                Ok(result)
            },
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

            DuckValue::List(items) => {
                if items.is_empty() {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot determine element type of empty List".into(),
                    ));
                }
                let mut child_lt = DuckValue::logical_type_of(&items[0])?;
                // SAFETY: `child_lt` is a valid logical type; `duckdb_create_list_type` copies it.
                let lt = unsafe { duckdb_create_list_type(child_lt) };
                // SAFETY: `child_lt` was allocated above and must be freed exactly once.
                unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                Ok(lt)
            },

            DuckValue::Array(items) => {
                if items.is_empty() {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot determine element type of empty Array".into(),
                    ));
                }
                let mut child_lt = DuckValue::logical_type_of(&items[0])?;
                // SAFETY: `child_lt` is valid; `duckdb_create_array_type` copies it.
                let lt = unsafe { duckdb_create_array_type(child_lt, items.len() as idx_t) };
                // SAFETY: `child_lt` was allocated above.
                unsafe { duckdb_destroy_logical_type(&mut child_lt) };
                Ok(lt)
            },

            DuckValue::Struct(m) => {
                let n = m.len();
                if n == 0 {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot determine type of empty Struct".into(),
                    ));
                }
                let entries: Vec<(&String, &DuckValue)> = m.iter().collect();
                let mut member_types: Vec<duckdb_logical_type> = Vec::with_capacity(n);
                let mut c_names: Vec<std::ffi::CString> = Vec::with_capacity(n);
                let mut err: Option<DuckDBConversionError> = None;

                for (k, v) in &entries {
                    match DuckValue::logical_type_of(v) {
                        Ok(lt) => member_types.push(lt),
                        Err(e) => {
                            err = Some(e);
                            break;
                        },
                    }
                    match std::ffi::CString::new(k.as_str()) {
                        Ok(c) => c_names.push(c),
                        Err(e) => {
                            err = Some(DuckDBConversionError::ConversionError(e.to_string()));
                            break;
                        },
                    }
                }
                if let Some(e) = err {
                    for mut lt in member_types {
                        // SAFETY: each `lt` was allocated by `logical_type_of` above.
                        unsafe { duckdb_destroy_logical_type(&mut lt) };
                    }
                    return Err(e);
                }
                let mut name_ptrs: Vec<*const std::os::raw::c_char> =
                    c_names.iter().map(|c| c.as_ptr()).collect();
                // SAFETY: `member_types` and `name_ptrs` are valid arrays of `n` elements;
                // `duckdb_create_struct_type` copies both.
                let lt = unsafe {
                    duckdb_create_struct_type(
                        member_types.as_mut_ptr(),
                        name_ptrs.as_mut_ptr(),
                        n as idx_t,
                    )
                };
                for mut mt in member_types {
                    // SAFETY: each `mt` was allocated by `logical_type_of` above.
                    unsafe { duckdb_destroy_logical_type(&mut mt) };
                }
                Ok(lt)
            },

            DuckValue::Map(m) => {
                if m.is_empty() {
                    return Err(DuckDBConversionError::ConversionError(
                        "cannot determine value type of empty Map".into(),
                    ));
                }
                // Keys are always VARCHAR (HashMap<String, _> keys).
                // SAFETY: the type constant is always a valid duckdb_type.
                let mut key_lt =
                    unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR) };
                let first_val = m.values().next().unwrap();
                let mut val_lt = match DuckValue::logical_type_of(first_val) {
                    Ok(lt) => lt,
                    Err(e) => {
                        // SAFETY: `key_lt` was allocated above.
                        unsafe { duckdb_destroy_logical_type(&mut key_lt) };
                        return Err(e);
                    },
                };
                // SAFETY: both types are valid; `duckdb_create_map_type` copies them.
                let lt = unsafe { duckdb_create_map_type(key_lt, val_lt) };
                // SAFETY: `key_lt` was allocated above; destroy exactly once.
                unsafe { duckdb_destroy_logical_type(&mut key_lt) };
                // SAFETY: `val_lt` was allocated by `logical_type_of` above.
                unsafe { duckdb_destroy_logical_type(&mut val_lt) };
                Ok(lt)
            },

            DuckValue::Union(inner) => {
                let mut member_lt = DuckValue::logical_type_of(inner)?;
                let c_name = std::ffi::CString::new("value").unwrap();
                let mut name_ptr: *const std::os::raw::c_char = c_name.as_ptr();
                // SAFETY: single-element arrays of valid pointers; create copies both.
                let lt = unsafe { duckdb_create_union_type(&mut member_lt, &mut name_ptr, 1) };
                // SAFETY: `member_lt` was allocated by `logical_type_of` above.
                unsafe { duckdb_destroy_logical_type(&mut member_lt) };
                Ok(lt)
            },
        }
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
