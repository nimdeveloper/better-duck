//! Fallible conversion from a [`DuckValue`](crate::types::value::DuckValue) into a
//! native Rust type.
//!
//! [`FromDuckValue`] is the read-direction counterpart to
//! [`AppendAble`](crate::types::appendable::AppendAble): it turns a borrowed
//! [`DuckValue`](crate::types::value::DuckValue) (as produced by
//! [`DuckRow`](crate::raw::row::DuckRow) or
//! [`ResultSet`](crate::result_set::ResultSet)) into a concrete Rust value,
//! returning a [`DuckDBConversionError`](crate::error::DuckDBConversionError) instead
//! of panicking on a type or nullability mismatch. It is the foundation the
//! `#[derive(FromRow)]` macro builds on, and the safe alternative to the legacy
//! panicking `From<DuckValue>` impls (which now delegate here).

use std::collections::HashMap;

use crate::error::DuckDBConversionError;
use crate::types::value::DuckValue;

/// Converts a borrowed [`DuckValue`] into `Self`, or reports why it cannot.
pub trait FromDuckValue: Sized {
    /// Reads `value` as `Self`.
    ///
    /// # Errors
    ///
    /// Returns [`DuckDBConversionError::NullValue`] if `value` is
    /// [`DuckValue::Null`] and `Self` is not an [`Option`], or
    /// [`DuckDBConversionError::ConversionError`] if the stored variant cannot
    /// be read as `Self`.
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError>;
}

/// Builds the error for a variant that does not match the target type, mapping
/// `NULL` to the dedicated [`DuckDBConversionError::NullValue`].
fn mismatch(
    value: &DuckValue,
    target: &str,
) -> DuckDBConversionError {
    match value {
        DuckValue::Null => DuckDBConversionError::NullValue,
        other => {
            DuckDBConversionError::ConversionError(format!("cannot read {other:?} as {target}"))
        },
    }
}

/// `NULL` reads as `None`; any other value delegates to `T`.
impl<T: FromDuckValue> FromDuckValue for Option<T> {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match value {
            DuckValue::Null => Ok(None),
            other => T::from_duck_value(other).map(Some),
        }
    }
}

/// Identity: a [`DuckValue`] field reads as itself (cloned).
impl FromDuckValue for DuckValue {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        Ok(value.clone())
    }
}

/// Generates a [`FromDuckValue`] impl accepting any of the listed integer
/// variants — every arm is a lossless widening into `$target`, expressed via
/// [`From`] (which also covers the reflexive same-width arm) so neither
/// `clippy::unnecessary_cast` nor `clippy::cast_lossless` fires.
macro_rules! impl_from_duck_int {
    ($target:ty, $name:literal, $($variant:ident),+ $(,)?) => {
        impl FromDuckValue for $target {
            fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
                match *value {
                    $( DuckValue::$variant(v) => Ok(<$target>::from(v)), )+
                    ref other => Err(mismatch(other, $name)),
                }
            }
        }
    };
}

impl_from_duck_int!(i8, "i8 (TINYINT)", TinyInt);
impl_from_duck_int!(i16, "i16 (SMALLINT)", SmallInt, TinyInt);
impl_from_duck_int!(i32, "i32 (INTEGER)", Int, SmallInt, TinyInt);
impl_from_duck_int!(i64, "i64 (BIGINT)", BigInt, Int, SmallInt, TinyInt);
impl_from_duck_int!(i128, "i128 (HUGEINT)", HugeInt, BigInt, Int, SmallInt, TinyInt);
impl_from_duck_int!(u8, "u8 (UTINYINT)", UTinyInt);
impl_from_duck_int!(u16, "u16 (USMALLINT)", USmallInt, UTinyInt);
impl_from_duck_int!(u32, "u32 (UINTEGER)", UInt, USmallInt, UTinyInt);
impl_from_duck_int!(u64, "u64 (UBIGINT)", UBigInt, UInt, USmallInt, UTinyInt);
impl_from_duck_int!(u128, "u128 (UHUGEINT)", UHugeInt, UBigInt, UInt, USmallInt, UTinyInt);

impl FromDuckValue for bool {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match *value {
            DuckValue::Boolean(v) => Ok(v),
            ref other => Err(mismatch(other, "bool (BOOLEAN)")),
        }
    }
}

impl FromDuckValue for f32 {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match *value {
            DuckValue::Float(v) => Ok(v),
            ref other => Err(mismatch(other, "f32 (FLOAT)")),
        }
    }
}

impl FromDuckValue for f64 {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match *value {
            DuckValue::Double(v) => Ok(v),
            DuckValue::Float(v) => Ok(f64::from(v)),
            ref other => Err(mismatch(other, "f64 (DOUBLE)")),
        }
    }
}

impl FromDuckValue for String {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match value {
            DuckValue::Text(s) => Ok(s.clone()),
            other => Err(mismatch(other, "String (VARCHAR)")),
        }
    }
}

/// Copy-wrapper impl: the variant's payload is `Copy`, so bind it out directly
/// (avoids a `clippy::clone_on_copy` warning).
macro_rules! impl_from_duck_copy {
    ($target:ty, $name:literal, $variant:ident) => {
        impl FromDuckValue for $target {
            fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
                match *value {
                    DuckValue::$variant(v) => Ok(v),
                    ref other => Err(mismatch(other, $name)),
                }
            }
        }
    };
}

/// Clone-wrapper impl: the variant's payload owns heap data, so clone it.
macro_rules! impl_from_duck_clone {
    ($target:ty, $name:literal, $variant:ident) => {
        impl FromDuckValue for $target {
            fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
                match value {
                    DuckValue::$variant(v) => Ok(v.clone()),
                    other => Err(mismatch(other, $name)),
                }
            }
        }
    };
}

impl_from_duck_copy!(crate::types::DuckDecimal, "DuckDecimal (DECIMAL)", Decimal);
impl_from_duck_copy!(crate::types::DuckUuid, "DuckUuid (UUID)", Uuid);
impl_from_duck_clone!(crate::types::Blob, "Blob (BLOB)", Blob);
impl_from_duck_clone!(crate::types::DuckBit, "DuckBit (BIT)", Bit);
impl_from_duck_clone!(crate::types::DuckBignum, "DuckBignum (BIGNUM)", Bignum);
impl_from_duck_clone!(crate::types::DuckEnum, "DuckEnum (ENUM)", Enum);
impl_from_duck_clone!(crate::types::DuckUnion, "DuckUnion (UNION)", Union);

/// A `LIST` or fixed-size `ARRAY` reads element-by-element into `Vec<T>`.
impl<T: FromDuckValue> FromDuckValue for Vec<T> {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match value {
            DuckValue::List(items) => items.iter().map(T::from_duck_value).collect(),
            DuckValue::Array(items) => items.iter().map(T::from_duck_value).collect(),
            other => Err(mismatch(other, "Vec<T> (LIST/ARRAY)")),
        }
    }
}

/// A `STRUCT` reads into a `HashMap<String, V>` (field name → value).
impl<V: FromDuckValue> FromDuckValue for HashMap<String, V> {
    fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
        match value {
            DuckValue::Struct(fields) => {
                fields.iter().map(|(k, v)| V::from_duck_value(v).map(|v| (k.clone(), v))).collect()
            },
            other => Err(mismatch(other, "HashMap<String, V> (STRUCT)")),
        }
    }
}

/// Temporal `FromDuckValue` impls for the `chrono` feature (all payloads `Copy`).
#[cfg(feature = "chrono")]
mod chrono_impls {
    use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};

    use super::{mismatch, DuckValue, FromDuckValue};
    use crate::error::DuckDBConversionError;

    impl FromDuckValue for NaiveDateTime {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Timestamp(v)
                | DuckValue::TimestampS(v)
                | DuckValue::TimestampMs(v)
                | DuckValue::TimestampNs(v) => Ok(v),
                ref other => Err(mismatch(other, "NaiveDateTime (TIMESTAMP)")),
            }
        }
    }

    impl FromDuckValue for DateTime<Utc> {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::TimestampTz(v) => Ok(v),
                ref other => Err(mismatch(other, "DateTime<Utc> (TIMESTAMP_TZ)")),
            }
        }
    }

    impl FromDuckValue for NaiveDate {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Date(v) => Ok(v),
                ref other => Err(mismatch(other, "NaiveDate (DATE)")),
            }
        }
    }

    impl FromDuckValue for NaiveTime {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Time(v) | DuckValue::TimeNs(v) => Ok(v),
                ref other => Err(mismatch(other, "NaiveTime (TIME/TIME_NS)")),
            }
        }
    }

    impl FromDuckValue for Duration {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Interval(v) => Ok(v),
                ref other => Err(mismatch(other, "chrono::Duration (INTERVAL)")),
            }
        }
    }

    impl FromDuckValue for crate::types::date_chrono::TimeTz {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::TimeTz(v) => Ok(v),
                ref other => Err(mismatch(other, "TimeTz (TIME_TZ)")),
            }
        }
    }
}

/// Temporal `FromDuckValue` impls for the no-chrono native path (all payloads `Copy`).
#[cfg(not(feature = "chrono"))]
mod native_impls {
    use std::time::{Duration, SystemTime};

    use super::{mismatch, DuckValue, FromDuckValue};
    use crate::error::DuckDBConversionError;
    use crate::types::date_native::{DuckDate, DuckTime, DuckTimeNs, DuckTimeTz};

    impl FromDuckValue for SystemTime {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Timestamp(v)
                | DuckValue::TimestampS(v)
                | DuckValue::TimestampMs(v)
                | DuckValue::TimestampNs(v)
                | DuckValue::TimestampTz(v) => Ok(v),
                ref other => Err(mismatch(other, "SystemTime (TIMESTAMP)")),
            }
        }
    }

    impl FromDuckValue for DuckDate {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Date(v) => Ok(v),
                ref other => Err(mismatch(other, "DuckDate (DATE)")),
            }
        }
    }

    impl FromDuckValue for DuckTime {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Time(v) => Ok(v),
                ref other => Err(mismatch(other, "DuckTime (TIME)")),
            }
        }
    }

    impl FromDuckValue for DuckTimeNs {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::TimeNs(v) => Ok(v),
                ref other => Err(mismatch(other, "DuckTimeNs (TIME_NS)")),
            }
        }
    }

    impl FromDuckValue for DuckTimeTz {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::TimeTz(v) => Ok(v),
                ref other => Err(mismatch(other, "DuckTimeTz (TIME_TZ)")),
            }
        }
    }

    impl FromDuckValue for Duration {
        fn from_duck_value(value: &DuckValue) -> Result<Self, DuckDBConversionError> {
            match *value {
                DuckValue::Interval(v) => Ok(v),
                ref other => Err(mismatch(other, "Duration (INTERVAL)")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_exact_and_widened_ints() {
        assert_eq!(i32::from_duck_value(&DuckValue::Int(7)).unwrap(), 7);
        // BIGINT target accepts any smaller signed integer (lossless widening).
        assert_eq!(i64::from_duck_value(&DuckValue::Int(7)).unwrap(), 7);
        assert_eq!(i128::from_duck_value(&DuckValue::BigInt(9)).unwrap(), 9);
        assert_eq!(u32::from_duck_value(&DuckValue::UTinyInt(3)).unwrap(), 3);
    }

    #[test]
    fn reads_scalars_and_strings() {
        assert!(bool::from_duck_value(&DuckValue::Boolean(true)).unwrap());
        let got = f64::from_duck_value(&DuckValue::Float(1.5)).unwrap();
        assert!((got - 1.5).abs() < 1e-9);
        assert_eq!(String::from_duck_value(&DuckValue::Text("hi".to_owned())).unwrap(), "hi");
    }

    #[test]
    fn null_maps_to_none_but_errors_for_non_option() {
        assert_eq!(Option::<i32>::from_duck_value(&DuckValue::Null).unwrap(), None);
        assert_eq!(Option::<i32>::from_duck_value(&DuckValue::Int(5)).unwrap(), Some(5));
        assert!(matches!(
            i32::from_duck_value(&DuckValue::Null),
            Err(DuckDBConversionError::NullValue)
        ));
    }

    #[test]
    fn type_mismatch_is_reported() {
        assert!(matches!(
            i32::from_duck_value(&DuckValue::Text("x".to_owned())),
            Err(DuckDBConversionError::ConversionError(_))
        ));
    }

    #[test]
    fn reads_list_into_typed_vec() {
        let list = DuckValue::List(vec![DuckValue::Int(1), DuckValue::Int(2)]);
        assert_eq!(Vec::<i32>::from_duck_value(&list).unwrap(), vec![1, 2]);
    }

    #[test]
    fn reads_every_integer_width_and_float32_and_identity() {
        assert_eq!(i8::from_duck_value(&DuckValue::TinyInt(-1)).unwrap(), -1);
        assert_eq!(i16::from_duck_value(&DuckValue::SmallInt(-2)).unwrap(), -2);
        assert_eq!(u8::from_duck_value(&DuckValue::UTinyInt(1)).unwrap(), 1);
        assert_eq!(u16::from_duck_value(&DuckValue::USmallInt(2)).unwrap(), 2);
        assert_eq!(u64::from_duck_value(&DuckValue::UBigInt(3)).unwrap(), 3);
        assert_eq!(u128::from_duck_value(&DuckValue::UHugeInt(4)).unwrap(), 4);
        let f = f32::from_duck_value(&DuckValue::Float(2.5)).unwrap();
        assert!((f - 2.5).abs() < 1e-6);
        // Identity: DuckValue reads as a clone of itself.
        assert_eq!(DuckValue::from_duck_value(&DuckValue::Int(9)).unwrap(), DuckValue::Int(9));
    }

    #[test]
    fn reads_copy_and_clone_wrapper_types() {
        use crate::types::{Blob, DuckBignum, DuckBit, DuckDecimal, DuckEnum, DuckUnion, DuckUuid};

        let dec = DuckDecimal::new(123, 5, 2).unwrap();
        assert_eq!(DuckDecimal::from_duck_value(&DuckValue::Decimal(dec)).unwrap(), dec);
        assert_eq!(
            DuckUuid::from_duck_value(&DuckValue::Uuid(DuckUuid(42))).unwrap(),
            DuckUuid(42)
        );
        let blob = Blob(vec![1, 2, 3]);
        assert_eq!(Blob::from_duck_value(&DuckValue::Blob(blob.clone())).unwrap(), blob);
        let bit = DuckBit(vec![0, 1]);
        assert_eq!(DuckBit::from_duck_value(&DuckValue::Bit(bit.clone())).unwrap(), bit);
        let bn = DuckBignum::new(vec![7], false);
        assert_eq!(DuckBignum::from_duck_value(&DuckValue::Bignum(bn.clone())).unwrap(), bn);

        let en = DuckEnum::from_label(std::sync::Arc::from(vec!["A".to_owned()]), "A").unwrap();
        // `DuckEnum`/`DuckUnion` don't derive `PartialEq`; check the read succeeds + round-trips a field.
        let read_en = DuckEnum::from_duck_value(&DuckValue::Enum(en.clone())).unwrap();
        assert_eq!(read_en.label(), en.label());
        let un = DuckUnion::single(DuckValue::Int(1)).unwrap();
        assert!(DuckUnion::from_duck_value(&DuckValue::Union(un.clone())).is_ok());

        // Wrong-variant error arm for a wrapper type.
        assert!(Blob::from_duck_value(&DuckValue::Int(0)).is_err());
    }

    #[test]
    fn reads_array_into_vec_and_struct_into_map() {
        let arr = DuckValue::Array(vec![DuckValue::Int(3), DuckValue::Int(4)].into_boxed_slice());
        assert_eq!(Vec::<i32>::from_duck_value(&arr).unwrap(), vec![3, 4]);
        assert!(Vec::<i32>::from_duck_value(&DuckValue::Int(0)).is_err());

        let mut fields = std::collections::HashMap::new();
        fields.insert("a".to_owned(), DuckValue::Int(1));
        let map: std::collections::HashMap<String, i32> =
            FromDuckValue::from_duck_value(&DuckValue::Struct(fields)).unwrap();
        assert_eq!(map.get("a"), Some(&1));
        assert!(
            std::collections::HashMap::<String, i32>::from_duck_value(&DuckValue::Int(0)).is_err()
        );
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn reads_chrono_temporals() {
        use crate::types::date_chrono::TimeTz;
        use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

        let d = NaiveDate::from_ymd_opt(2020, 1, 2).unwrap();
        let t = NaiveTime::from_hms_opt(3, 4, 5).unwrap();
        let dt = d.and_time(t);
        assert_eq!(NaiveDate::from_duck_value(&DuckValue::Date(d)).unwrap(), d);
        assert_eq!(NaiveTime::from_duck_value(&DuckValue::Time(t)).unwrap(), t);
        assert_eq!(chrono::NaiveDateTime::from_duck_value(&DuckValue::Timestamp(dt)).unwrap(), dt);
        let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
        assert_eq!(DateTime::<Utc>::from_duck_value(&DuckValue::TimestampTz(utc)).unwrap(), utc);
        let dur = Duration::seconds(90);
        assert_eq!(Duration::from_duck_value(&DuckValue::Interval(dur)).unwrap(), dur);
        let tz = TimeTz { time: t, offset_secs: 3600 };
        assert_eq!(TimeTz::from_duck_value(&DuckValue::TimeTz(tz)).unwrap(), tz);
    }
}
