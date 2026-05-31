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
        duckdb_free, duckdb_get_type_id, duckdb_interval, duckdb_list_entry,
        duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_logical_type,
        duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length, duckdb_type,
        duckdb_validity_row_is_valid, duckdb_vector, duckdb_vector_get_column_type,
        duckdb_vector_get_data, duckdb_vector_get_validity, idx_t, DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
        DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_BLOB, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
        DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
        DUCKDB_TYPE_DUCKDB_TYPE_ENUM, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
        DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
        DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL, DUCKDB_TYPE_DUCKDB_TYPE_INVALID,
        DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP,
        DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL,
        DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
        DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S, DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT, DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
        DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
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
    /// The value is a timestamp.
    #[cfg(feature = "chrono")]
    Timestamp(NaiveDateTime),
    #[cfg(not(feature = "chrono"))]
    Timestamp(SystemTime),

    /// The value is a date
    #[cfg(feature = "chrono")]
    Date(NaiveDate),
    #[cfg(not(feature = "chrono"))]
    Date(NaiveDate), // TODO: We need a type for this!

    /// The value is a time
    #[cfg(feature = "chrono")]
    Time(NaiveTime),
    #[cfg(not(feature = "chrono"))]
    Time(NaiveTime), // TODO: We need a type for this!

    /// The value is an interval (month, day, nano)
    #[cfg(feature = "chrono")]
    Interval(Duration),
    #[cfg(not(feature = "chrono"))]
    Interval(Duration),

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

/// Converts microseconds since the Unix epoch to a `NaiveDateTime`.
///
/// Used by all TIMESTAMP variant chunk-read paths.
#[cfg(feature = "chrono")]
fn micros_to_naive_datetime(micros: i64) -> Result<NaiveDateTime, DuckDBConversionError> {
    DateTime::<Utc>::from_timestamp(
        micros / 1_000_000,
        ((micros % 1_000_000).unsigned_abs() * 1_000) as u32,
    )
    .map(|dt| dt.naive_utc())
    .ok_or_else(|| {
        DuckDBConversionError::ConversionError(format!("timestamp {micros}µs out of range"))
    })
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
                let value = unsafe {
                    *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize)
                        as duckdb_value
                };
                #[cfg(feature = "chrono")]
                {
                    return chrono::NaiveDate::from_duck(value).map(DuckValue::Date);
                    // return NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    //     .map(DuckValue::Date)
                    //     .ok_or_else(|| {
                    //         DuckDBConversionError::ConversionError(format!(
                    //             "date value {days} out of representable range"
                    //         ))
                    //     });
                }
                #[cfg(not(feature = "chrono"))]
                todo!()
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => {
                let value = unsafe {
                    *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize)
                        as duckdb_value
                };
                #[cfg(feature = "chrono")]
                {
                    return chrono::NaiveTime::from_duck(value).map(DuckValue::Time);
                    // let secs = (micros / 1_000_000) as u32;
                    // let nano = ((micros % 1_000_000) * 1_000) as u32;
                    // NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                    //     .map(DuckValue::Time)
                    //     .ok_or_else(|| {
                    //         DuckDBConversionError::ConversionError(format!(
                    //             "time {micros}µs out of range"
                    //         ))
                    //     })
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO
                    todo!()
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                let value = unsafe {
                    *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize)
                        as duckdb_value
                };
                #[cfg(feature = "chrono")]
                {
                    return chrono::NaiveDateTime::from_duck(value).map(DuckValue::Timestamp);
                    // return micros_to_naive_datetime(micros).map(DuckValue::Timestamp);
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO
                    todo!()
                    // use std::time::UNIX_EPOCH;
                    // let secs = (micros / 1_000_000) as u64;
                    // let sub_micros = (micros % 1_000_000) as u32;
                    // return Ok(DuckValue::Timestamp(
                    //     UNIX_EPOCH + std::time::Duration::new(secs, sub_micros * 1_000),
                    // ));
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                let value = unsafe {
                    *(duckdb_vector_get_data(val) as *const i32).add(row_idx as usize)
                        as duckdb_value
                };
                #[cfg(feature = "chrono")]
                {
                    return chrono::Duration::from_duck(value).map(DuckValue::Interval);
                    // let total_micros = (iv.months as i64)
                    //     .saturating_mul(30 * 86_400 * 1_000_000)
                    //     .saturating_add((iv.days as i64).saturating_mul(86_400 * 1_000_000))
                    //     .saturating_add(iv.micros);
                    // Ok(DuckValue::Interval(Duration::microseconds(total_micros)))
                }
                #[cfg(not(feature = "chrono"))]
                {
                    return std::time::Duration::from_duck(val).map(DuckValue::Interval);
                    // let total_micros = (iv.months as u64)
                    //     .saturating_mul(30 * 86_400 * 1_000_000)
                    //     .saturating_add((iv.days as u64).saturating_mul(86_400 * 1_000_000))
                    //     .saturating_add(iv.micros as u64);
                    // return Ok(DuckValue::Interval(std::time::Duration::from_micros(total_micros)));
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
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
                // SAFETY: `val` is a valid duckdb_vector; the data pointer is valid for
                // the chunk's row count. We read the raw i64 at `row_idx` as a decimal.
                let data_ptr = unsafe { duckdb_vector_get_data(val) as *mut i64 };
                // SAFETY: `row_idx` is within [0, chunk_size).
                let value = unsafe { *data_ptr.add(row_idx as usize) as crate::ffi::duckdb_value };
                Decimal::from_duck(value).map(DuckValue::Decimal)
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
            // DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            // let list_data =
            //     unsafe { *data_ptr.add(row_idx as usize) as *mut duckdb_list_entry };
            // let list_child = unsafe { duckdb_list_vector_get_child(val) as duckdb_vector };
            // let child_validity = unsafe { duckdb_vector_get_validity(list_child) };
            // let list_length = unsafe { duckdb_list_vector_get_size(list_child) };
            // // TODO: What happens for this var, if the function returns error? (Maybe using https://docs.rs/scopeguard/latest/scopeguard/)

            // unsafe {
            //     for each in 0..(*list_data).offset {
            //         let val = DuckValue::Null;
            //         if duckdb_validity_row_is_valid(child_validity, each) {
            //             let mut raw_child_type: duckdb_logical_type =
            //                 duckdb_vector_get_column_type(list_child);
            //             let child_type = duckdb_get_type_id(raw_child_type);
            //             duckdb_destroy_logical_type(&mut raw_child_type);
            //             val = DuckValue::from_duckdb_vec(list_child, child_type, each)?;
            //         } // otherwise it's NULL value
            //         unsafe { ptr::write(vec_ptr.add(each as usize), val) };
            //     }
            // };
            // Ok(DuckValue::Array(unsafe { vec_data.assume_init() }))
            // TODO: We need to move the functionality outside!
            //  as we need to handle the type and access the column itself (Also we need to destroy each Item after inserting them in rust data types!)
            // },
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => {
                // TODO: We need to move the functionality outside!
                //  as we need to handle the type and access the column itself (Also we need to destroy each Item after inserting them in rust data types!)
                todo!()
            },
            _ => {
                todo!()
            }, // DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_ENUM => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_UUID => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_UNION => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_BIT => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_TIME_TZ => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_ANY => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_VARINT => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_SQLNULL => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL => {},
               // DUCKDB_TYPE_DUCKDB_TYPE_INTEGER_LITERAL => {},
        }
    }
}

impl Drop for DuckValue {
    fn drop(&mut self) {}
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
