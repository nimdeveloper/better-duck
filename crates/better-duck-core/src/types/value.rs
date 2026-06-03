#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use std::ffi::CStr;
use std::ptr;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

use crate::{
    ffi::{
        duckdb_destroy_logical_type, duckdb_enum_dictionary_size, duckdb_enum_dictionary_value,
        duckdb_free, duckdb_get_type_id, duckdb_list_entry, duckdb_list_vector_get_child,
        duckdb_list_vector_get_size, duckdb_logical_type, duckdb_string_t, duckdb_string_t_data,
        duckdb_string_t_length, duckdb_type, duckdb_validity_row_is_valid, duckdb_vector,
        duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity, idx_t,
        DUCKDB_TYPE_DUCKDB_TYPE_ARRAY, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_BLOB, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        DUCKDB_TYPE_DUCKDB_TYPE_ENUM, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, DUCKDB_TYPE_DUCKDB_TYPE_INVALID,
        DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP,
        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ,
        DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
        DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    },
    types::value_ref::DuckValueRef,
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use super::*;

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
    /// The value is a blob of data
    Blob(Vec<u8>),
    /// The value is a list
    List(Vec<DuckValue>),
    /// The value is an enum
    Enum(String),
    /// The value is a struct
    // Struct(OrderedMap<String, Value>), // TODO: We need to complete this
    /// The value is an array with fixed length
    Array(Box<[DuckValue]>),
    /// The value is a map
    // Map(OrderedMap<Value, Value>), // TODO: We need to complete this
    /// The value is a union
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
            DuckValueRef::Blob(b) => DuckValue::Blob(b.to_vec()),
            DuckValueRef::List(l) => DuckValue::List(l.iter().map(DuckValue::from).collect()),
            DuckValueRef::Enum(e) => DuckValue::Enum(e.to_string()),
            DuckValueRef::Array(a) => DuckValue::Array(
                a.iter()
                    .map(|v| DuckValue::from(&v.clone()))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
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
    // Converts the current value to a DuckDB-compatible format.
    // fn from_duck() -> ();
    // Converts a DuckDB-compatible format to the current value.
    // fn to_duck() -> ();
    pub(crate) fn from_duckdb_vec(
        val: duckdb_vector,
        t: duckdb_type,
        row_idx: u64,
    ) -> Result<DuckValue, DuckDBConversionError> {
        // SAFETY: `val` is a valid duckdb_vector; the validity bitmap is valid for at
        // least the chunk's row count.
        let validity_ptr = unsafe { duckdb_vector_get_validity(val) };
        // SAFETY: `row_idx` is within [0, chunk row count), so the validity bitmap access
        // is in bounds.
        let is_valid = unsafe { duckdb_validity_row_is_valid(validity_ptr, row_idx) };

        if !is_valid {
            return Ok(DuckValue::Null);
        }

        match t {
            DUCKDB_TYPE_DUCKDB_TYPE_INVALID => {
                Err(DuckDBConversionError::ConversionError(String::from("invalid type")))
            },
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
                // SAFETY: DATE stores i32 days-since-epoch in packed array layout.
                // `row_idx` is within [0, chunk row count), so the offset is in-bounds.
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
                // SAFETY: TIME stores i64 microseconds-since-midnight in packed array layout.
                // `row_idx` is within [0, chunk row count), so the offset is in-bounds.
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
                // SAFETY: TIMESTAMP stores i64 microseconds-since-epoch in packed array layout.
                // `row_idx` is within [0, chunk row count), so the offset is in-bounds.
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
                // SAFETY: INTERVAL stores duckdb_interval { months: i32, days: i32, micros: i64 }
                // (16 bytes) in packed array layout. `row_idx` is within [0, chunk row count).
                let value =
                    unsafe { *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize) }
                        as duckdb_value;
                #[cfg(feature = "chrono")]
                {
                    return chrono::Duration::from_duck(value).map(DuckValue::Interval);
                }
                #[cfg(not(feature = "chrono"))]
                {
                    std::time::Duration::from_duck(value).map(DuckValue::Interval)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                // SAFETY: TIMESTAMP_S stores i64 seconds-since-epoch in packed array layout.
                // `row_idx` is within [0, chunk_size).
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
                    // SAFETY: `value` was cast from an i32 read at `row_idx`; valid for
                    // the lifetime of the result set.
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
                // SAFETY: TIMESTAMP_MS stores i64 milliseconds-since-epoch in packed array layout.
                // `row_idx` is within [0, chunk_size).
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
                    // SAFETY: `value` was cast from an i32 read at `row_idx`; valid for
                    // the lifetime of the result set.
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
                // SAFETY: TIMESTAMP_NS stores i64 nanoseconds-since-epoch in packed array layout.
                // `row_idx` is within [0, chunk_size). Full nanosecond precision is preserved.
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
                    // SAFETY: `value` was cast from an i32 read at `row_idx`; valid for
                    // the lifetime of the result set.
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
                // SAFETY: VARCHAR columns store an array of `duckdb_string_t`. Each element
                // contains either an inlined short string or a pointer to a heap string owned
                // by DuckDB. `duckdb_string_t_data` returns a valid null-terminated UTF-8
                // pointer for the lifetime of the result. We copy into an owned `String`
                // before returning; no raw pointer escapes this block.
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
            #[cfg(feature = "decimal")]
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                // SAFETY: `val` is a valid duckdb_vector; the data pointer is valid for
                // the chunk's row count. We read the raw i64 at `row_idx` as a decimal.
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
                // SAFETY: `logical_type` is a valid duckdb_logical_type of ENUM kind,
                // as returned by `duckdb_vector_get_column_type` above.
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
                // heap-allocated null-terminated UTF-8 string that we must free with
                // `duckdb_free`.
                let c_str_ptr =
                    unsafe { duckdb_enum_dictionary_value(logical_type, raw_index as idx_t) };

                let name = if c_str_ptr.is_null() {
                    // Clean up before returning the error.
                    // SAFETY: `logical_type` was obtained from `duckdb_vector_get_column_type`.
                    unsafe { duckdb_destroy_logical_type(&mut logical_type) };
                    return Err(DuckDBConversionError::ConversionError(format!(
                        "enum index {raw_index} out of range (dict size {dict_size})"
                    )));
                } else {
                    // SAFETY: `duckdb_enum_dictionary_value` returns valid null-terminated
                    // UTF-8 when the index is in range.
                    let s = unsafe { CStr::from_ptr(c_str_ptr) }
                        .to_str()
                        .map(|s| s.to_owned())
                        .map_err(|e| DuckDBConversionError::ConversionError(e.to_string()))?;
                    // SAFETY: `c_str_ptr` was allocated by DuckDB and must be freed via
                    // `duckdb_free`.
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
                // SAFETY: TIMESTAMP_TZ stores i64 UTC microseconds-since-epoch in packed array
                // layout (same wire format as TIMESTAMP). `row_idx` is within [0, chunk_size).
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
                    // SAFETY: `value` was cast from an i32 read at `row_idx`; valid for
                    // the lifetime of the result set.
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
                // SAFETY: TIME_TZ stores duckdb_time_tz { bits: u64 } (8 bytes) in packed array
                // layout. `row_idx` is within [0, chunk_size).
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
                // SAFETY: TIME_NS stores i64 nanoseconds-since-midnight in packed array layout.
                // `row_idx` is within [0, chunk_size). Full nanosecond precision is preserved.
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
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                // TODO: We need to move the functionality outside!
                //  as we need to handle the type and access the column itself (Also we need to destroy each Item after inserting them in rust data types!)
                todo!()
            },
            _ => {
                todo!()
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
