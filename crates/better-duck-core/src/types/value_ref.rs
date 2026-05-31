#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::borrow::Cow;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

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
    /// The value is a timestamp.
    #[cfg(feature = "chrono")]
    Timestamp(NaiveDateTime),
    #[cfg(not(feature = "chrono"))]
    Timestamp(SystemTime),

    /// The value is a date
    #[cfg(feature = "chrono")]
    Date(NaiveDate),
    #[cfg(not(feature = "chrono"))]
    Date(NaiveDate),

    /// The value is a time
    #[cfg(feature = "chrono")]
    Time(NaiveTime),
    #[cfg(not(feature = "chrono"))]
    Time(NaiveTime),

    /// The value is an interval
    #[cfg(feature = "chrono")]
    Interval(Duration),
    #[cfg(not(feature = "chrono"))]
    Interval(Duration),

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
    /// The value is an array with fixed length
    Array(Box<[DuckValueRef<'a>]>),
    /// The value is a union
    Union(Box<DuckValueRef<'a>>),
}

// Implement From<DuckValue> for DuckValueRef
impl From<&DuckValue> for DuckValueRef<'_> {
    /// Creates a new DuckValueRef from a DuckValue, borrowing where possible
    fn from(value: &DuckValue) -> Self {
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
            DuckValue::Text(s) => DuckValueRef::Text(Cow::Owned(s.clone())),
            #[cfg(feature = "decimal")]
            DuckValue::Decimal(d) => DuckValueRef::Decimal(*d),
            DuckValue::Blob(b) => DuckValueRef::Blob(Cow::Owned(b.clone())),
            DuckValue::List(l) => DuckValueRef::List(l.iter().map(DuckValueRef::from).collect()),
            DuckValue::Enum(e) => DuckValueRef::Enum(Cow::Owned(e.clone())),
            DuckValue::Array(a) => DuckValueRef::Array(
                a.iter().map(DuckValueRef::from).collect::<Vec<_>>().into_boxed_slice(),
            ),
            DuckValue::Union(u) => DuckValueRef::Union(Box::new(DuckValueRef::from(u.as_ref()))),
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
