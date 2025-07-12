#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::ptr;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

use crate::ffi::{
    duckdb_destroy_logical_type, duckdb_get_type_id, duckdb_list_entry,
    duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_logical_type,
    duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length, duckdb_type,
    duckdb_validity_row_is_valid, duckdb_vector, duckdb_vector_get_column_type,
    duckdb_vector_get_data, duckdb_vector_get_validity, DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
    DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_DATE, DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    DUCKDB_TYPE_DUCKDB_TYPE_INVALID, DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP,
    DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL,
    DUCKDB_TYPE_DUCKDB_TYPE_TIME, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
    DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT, DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
    DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use super::*;

#[derive(Debug)]
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

// Macro to implement DuckDialect for types
macro_rules! simple_type_conversion {
    // This macro expects `val` (duckdb_vector pointer) and `row_idx` (usize)
    // to be in scope where it is called.
    // It also expects `Ok` to be a valid return type for the surrounding function.
    ($row_index:expr, $vector_ptr:expr, $rust_type:expr, $duck_primitive_type:ty) => {{
        // Get the raw data pointer from the DuckDB vector
        let data_ptr = unsafe { duckdb_vector_get_data($vector_ptr) };
        // Cast the raw pointer to a pointer of the expected DuckDB primitive type
        let values: *mut $duck_primitive_type = data_ptr as *mut $duck_primitive_type;
        // Dereference the value at the specific row index
        let primitive_value = unsafe { *values.add($row_index as usize) as $duck_primitive_type };
        // Wrap the primitive value in your Rust type and return Ok
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
        // Get data as i64 slice
        let data_ptr = unsafe { duckdb_vector_get_data(val) as *mut i64 };
        let validity_ptr = unsafe { duckdb_vector_get_validity(val) };
        // Check validity
        let is_valid = unsafe { duckdb_validity_row_is_valid(validity_ptr, row_idx) };

        // Access data if valid
        if !is_valid {
            return Ok(DuckValue::Null);
        }

        let value = unsafe { *data_ptr.add(row_idx as usize) as duckdb_value };

        match t {
            DUCKDB_TYPE_DUCKDB_TYPE_INVALID => {
                Err(DuckDBConversionError::ConversionError(String::from("Invalid type!")))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
                // Ok(DuckValue::Boolean(bool::from_duck(value)?))
                simple_type_conversion!(row_idx, val, DuckValue::Boolean, bool)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => {
                // Ok(DuckValue::TinyInt(i8::from_duck(value)?))
                simple_type_conversion!(row_idx, val, DuckValue::TinyInt, i8)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                // Ok(DuckValue::SmallInt(i16::from_duck(value)?))
                simple_type_conversion!(row_idx, val, DuckValue::SmallInt, i16)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                // Ok(DuckValue::Int(i32::from_duck(value)?))
                simple_type_conversion!(row_idx, val, DuckValue::Int, i32)
            },
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
                simple_type_conversion!(row_idx, val, DuckValue::BigInt, i64)
                // Ok(DuckValue::BigInt(i64::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
                simple_type_conversion!(row_idx, val, DuckValue::HugeInt, i128)
                // Ok(DuckValue::HugeInt(i128::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UTinyInt, u8)
                // Ok(DuckValue::UTinyInt(u8::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
                simple_type_conversion!(row_idx, val, DuckValue::USmallInt, u16)
                // Ok(DuckValue::USmallInt(u16::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
                simple_type_conversion!(row_idx, val, DuckValue::UInt, u32)
                // Ok(DuckValue::UInt(u32::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UBigInt, u64)
                // Ok(DuckValue::UBigInt(u64::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
                simple_type_conversion!(row_idx, val, DuckValue::UHugeInt, u128)
                // Ok(DuckValue::UHugeInt(u128::from_duck(val, t)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
                simple_type_conversion!(row_idx, val, DuckValue::Float, f32)
                // Ok(DuckValue::Float(f32::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
                simple_type_conversion!(row_idx, val, DuckValue::Double, f64)
                // Ok(DuckValue::Double(f64::from_duck(value)?))
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => {
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveDateTime::from_duck(value).map(DuckValue::Timestamp)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    SystemTime::from_duck(value).map(DuckValue::Timestamp)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveDate::from_duck(value).map(DuckValue::Date)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO
                    todo!()
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => {
                #[cfg(feature = "chrono")]
                {
                    chrono::NaiveTime::from_duck(value).map(DuckValue::Time)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    // TODO
                    todo!()
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => {
                #[cfg(feature = "chrono")]
                {
                    chrono::Duration::from_duck(value).map(DuckValue::Interval)
                }
                #[cfg(not(feature = "chrono"))]
                {
                    Duration::from_duck(val).map(DuckValue::Interval)
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR | DUCKDB_TYPE_DUCKDB_TYPE_STRING_LITERAL => {
                unsafe {
                    let data_ptr = duckdb_vector_get_data(val);
                    // Cast the raw pointer to a pointer of the expected DuckDB primitive type
                    let values: *mut duckdb_string_t = data_ptr as *mut duckdb_string_t;

                    // Dereference the value at the specific row index
                    let mut duck_string_t: duckdb_string_t = *values.add(row_idx as usize);
                    // Wrap the primitive value in your Rust type and return Ok

                    // Use duckdb_string_t_data to get the raw C char pointer
                    // NOTE: duckdb_string_t_data expects *mut duckdb_string_t.
                    // We're passing a mutable pointer here.
                    let char_ptr = duckdb_string_t_data(&mut duck_string_t);

                    // Use duckdb_string_t_length to get the length
                    let _length = duckdb_string_t_length(duck_string_t); // !NOTICE

                    // Create a Rust slice from the raw pointer and length
                    // The pointer might not be null-terminated for non-inlined strings,
                    // so using CStr::from_ptr is not always safe without the length.
                    // slice::from_raw_parts is the correct way here.
                    // let byte_slice = slice::from_raw_parts(char_ptr as *const u8, length as usize);

                    // Convert the byte slice to a Rust String
                    // Assuming UTF-8, use String::from_utf8_lossy for safety
                    let rust_string =
                        std::ffi::CStr::from_ptr(char_ptr).to_string_lossy().into_owned();

                    // let rust_string = String::from_utf8_lossy(byte_slice).into_owned();
                    Ok(DuckValue::Text(rust_string))
                    // String::from_duck(rust_string).map(DuckValue::Text)

                    // let c_str_ptr = duckdb_string_t_data(duck_string);
                    // let rust_string =
                    //     std::ffi::CStr::from_ptr(c_str_ptr).to_string_lossy().into_owned();
                }
            },
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Decimal::from_duck(value).map(DuckValue::Decimal),
            DUCKDB_TYPE_DUCKDB_TYPE_LIST | DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
                let list_data =
                    unsafe { *data_ptr.add(row_idx as usize) as *mut duckdb_list_entry };
                let list_child = unsafe { duckdb_list_vector_get_child(val) as duckdb_vector };
                let child_validity = unsafe { duckdb_vector_get_validity(list_child) };
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
                        "Invalid type for List/Array!",
                    )));
                }

                unsafe {
                    for each in 0..(*list_data).offset {
                        let mut val = DuckValue::Null;
                        if duckdb_validity_row_is_valid(child_validity, each) {
                            let mut raw_child_type: duckdb_logical_type =
                                duckdb_vector_get_column_type(list_child);
                            let child_type = duckdb_get_type_id(raw_child_type);
                            duckdb_destroy_logical_type(&mut raw_child_type);
                            val = DuckValue::from_duckdb_vec(list_child, child_type, each)?;
                        } // otherwise it's NULL value
                        ptr::write(iter_ptr.add(each as usize), val);
                    }
                };

                if t == DUCKDB_TYPE_DUCKDB_TYPE_ARRAY {
                    Ok(DuckValue::Array(unsafe { slice_data.unwrap().assume_init() }))
                } else if t == DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                    let mut vec_data = vec_data.unwrap();
                    unsafe { vec_data.set_len(list_length as usize) };
                    Ok(DuckValue::List(vec_data))
                } else {
                    Err(DuckDBConversionError::ConversionError(String::from(
                        "Invalid type for List/Array!",
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
