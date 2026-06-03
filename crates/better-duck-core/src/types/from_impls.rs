//! Ergonomic `From<T>` conversions into [`DuckValue`].
//!
//! These impls let you write `DuckValue::from(42i32)` or `"hello".into()` instead
//! of constructing the enum variant by hand.
//!
//! ## Note on the read path
//!
//! `from_duckdb_vec` is **not** expressed as a `From` impl because it requires three
//! inputs (`vector`, `type_id`, `row_idx`) and performs packed-array reads with
//! validity-bitmap checks — a single-input `From` cannot express it.

use std::collections::HashMap;

use super::value::DuckValue;

// Primitives

impl From<bool> for DuckValue {
    fn from(v: bool) -> Self {
        DuckValue::Boolean(v)
    }
}

impl From<i8> for DuckValue {
    fn from(v: i8) -> Self {
        DuckValue::TinyInt(v)
    }
}

impl From<i16> for DuckValue {
    fn from(v: i16) -> Self {
        DuckValue::SmallInt(v)
    }
}

impl From<i32> for DuckValue {
    fn from(v: i32) -> Self {
        DuckValue::Int(v)
    }
}

impl From<i64> for DuckValue {
    fn from(v: i64) -> Self {
        DuckValue::BigInt(v)
    }
}

impl From<i128> for DuckValue {
    fn from(v: i128) -> Self {
        DuckValue::HugeInt(v)
    }
}

impl From<u8> for DuckValue {
    fn from(v: u8) -> Self {
        DuckValue::UTinyInt(v)
    }
}

impl From<u16> for DuckValue {
    fn from(v: u16) -> Self {
        DuckValue::USmallInt(v)
    }
}

impl From<u32> for DuckValue {
    fn from(v: u32) -> Self {
        DuckValue::UInt(v)
    }
}

impl From<u64> for DuckValue {
    fn from(v: u64) -> Self {
        DuckValue::UBigInt(v)
    }
}

impl From<u128> for DuckValue {
    fn from(v: u128) -> Self {
        DuckValue::UHugeInt(v)
    }
}

impl From<f32> for DuckValue {
    fn from(v: f32) -> Self {
        DuckValue::Float(v)
    }
}

impl From<f64> for DuckValue {
    fn from(v: f64) -> Self {
        DuckValue::Double(v)
    }
}

// Strings

impl From<String> for DuckValue {
    fn from(v: String) -> Self {
        DuckValue::Text(v)
    }
}

impl From<&str> for DuckValue {
    fn from(v: &str) -> Self {
        DuckValue::Text(v.to_owned())
    }
}

// Blob

impl From<super::blob::Blob> for DuckValue {
    fn from(b: super::blob::Blob) -> Self {
        DuckValue::Blob(b)
    }
}

// Collections

/// Converts a `Vec<DuckValue>` into `DuckValue::List`.
///
/// For a `Vec<T>` where `T: Into<DuckValue>`, call `.into_iter().map(Into::into).collect()`
/// first, then convert:
/// ```rust
/// # use better_duck_core::types::value::DuckValue;
/// let list: DuckValue = vec![1i32, 2, 3]
///     .into_iter()
///     .map(DuckValue::from)
///     .collect::<Vec<_>>()
///     .into();
/// ```
impl From<Vec<DuckValue>> for DuckValue {
    fn from(v: Vec<DuckValue>) -> Self {
        DuckValue::List(v)
    }
}

/// Converts a `Box<[DuckValue]>` into `DuckValue::Array`.
impl From<Box<[DuckValue]>> for DuckValue {
    fn from(a: Box<[DuckValue]>) -> Self {
        DuckValue::Array(a)
    }
}

// Map / Struct

/// Converts a `HashMap<DuckValue, DuckValue>` directly into `DuckValue::Map`.
impl From<HashMap<DuckValue, DuckValue>> for DuckValue {
    fn from(h: HashMap<DuckValue, DuckValue>) -> Self {
        DuckValue::Map(h)
    }
}

/// Converts a `HashMap<String, DuckValue>` into `DuckValue::Map` (keys become
/// `DuckValue::Text`).
impl From<HashMap<String, DuckValue>> for DuckValue {
    fn from(h: HashMap<String, DuckValue>) -> Self {
        DuckValue::Map(h.into_iter().map(|(k, v)| (DuckValue::Text(k), v)).collect())
    }
}

/// Converts a `Vec<(String, DuckValue)>` into `DuckValue::Map` (keys become
/// `DuckValue::Text`).
impl From<Vec<(String, DuckValue)>> for DuckValue {
    fn from(v: Vec<(String, DuckValue)>) -> Self {
        DuckValue::Map(v.into_iter().map(|(k, v)| (DuckValue::Text(k), v)).collect())
    }
}

// Option

impl<T: Into<DuckValue>> From<Option<T>> for DuckValue {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => DuckValue::Null,
        }
    }
}
