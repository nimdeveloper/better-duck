#![allow(non_snake_case)]
#[cfg(feature = "chrono")]
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::mem;
#[cfg(not(feature = "chrono"))]
use std::time::{Duration, SystemTime};

use crate::types::decimal::DuckDecimal;

#[cfg(not(feature = "chrono"))]
use crate::types::date_native;
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

    /// The value is a `DATE`/`TIMESTAMP` ±infinity sentinel — see
    /// [`DuckValue::TemporalInfinity`].
    /// Feature-independent (`Copy`, no chrono type).
    TemporalInfinity {
        /// Which temporal family this infinity belongs to.
        kind: crate::types::temporal::TemporalKind,
        /// `+infinity` or `-infinity`.
        sign: crate::types::temporal::Sign,
    },

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
    /// The value is a `DECIMAL(width, scale)`, preserving its declared precision.
    Decimal(DuckDecimal),
    /// The value is a blob of data.
    Blob(Blob),
    /// The value is a list
    List(Vec<DuckValueRef<'a>>),
    /// The value is an `ENUM`, borrowing its dictionary + selected index.
    Enum(Cow<'a, crate::types::duck_enum::DuckEnum>),
    /// The value is a struct (string-keyed field map with a fixed schema).
    Struct(HashMap<String, DuckValueRef<'a>>),
    /// The value is an array with fixed length
    Array(Box<[DuckValueRef<'a>]>),
    /// The value is a map (arbitrary key → value pairs with a dynamic schema).
    Map(HashMap<DuckValueRef<'a>, DuckValueRef<'a>>),
    /// The value is a `UNION` (tagged sum type), borrowing its full member schema,
    /// selected tag, and active member value.
    Union(Cow<'a, crate::types::duck_union::DuckUnion>),
    /// The value is a UUID.
    Uuid(crate::types::uuid::DuckUuid),
    /// The value is a bitstring (`BIT`).
    Bit(crate::types::bit::DuckBit),
    /// The value is an arbitrary-precision integer (`BIGNUM`).
    Bignum(crate::types::bignum::DuckBignum),
}

// PartialEq / Eq / Hash
//
// DuckValueRef contains f32/f64 and HashMap fields.  We hand-implement all three
// using the same canonicalization strategy as DuckValue.
// `Cow<'a, str>`: already implements PartialEq/Hash as `&str`.
// `SystemTime`: hashed via duration_since(UNIX_EPOCH) (no std Hash impl).

impl<'a> PartialEq for DuckValueRef<'a> {
    fn eq(
        &self,
        other: &Self,
    ) -> bool {
        use DuckValueRef::*;
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
            (Decimal(a), Decimal(b)) => a == b,
            (Blob(a), Blob(b)) => a == b,
            (List(a), List(b)) => a == b,
            (Array(a), Array(b)) => a == b,
            (Struct(a), Struct(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            (Union(a), Union(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            (Bit(a), Bit(b)) => a == b,
            (Bignum(a), Bignum(b)) => a == b,
            (TemporalInfinity { kind: ka, sign: sa }, TemporalInfinity { kind: kb, sign: sb }) => {
                ka == kb && sa == sb
            },
            _ => false,
        }
    }
}

impl<'a> Eq for DuckValueRef<'a> {}

impl<'a> Hash for DuckValueRef<'a> {
    fn hash<H: Hasher>(
        &self,
        state: &mut H,
    ) {
        mem::discriminant(self).hash(state);
        match self {
            DuckValueRef::Null => {},
            DuckValueRef::Boolean(v) => v.hash(state),
            DuckValueRef::TinyInt(v) => v.hash(state),
            DuckValueRef::SmallInt(v) => v.hash(state),
            DuckValueRef::Int(v) => v.hash(state),
            DuckValueRef::BigInt(v) => v.hash(state),
            DuckValueRef::HugeInt(v) => v.hash(state),
            DuckValueRef::UTinyInt(v) => v.hash(state),
            DuckValueRef::USmallInt(v) => v.hash(state),
            DuckValueRef::UInt(v) => v.hash(state),
            DuckValueRef::UBigInt(v) => v.hash(state),
            DuckValueRef::UHugeInt(v) => v.hash(state),
            DuckValueRef::Float(f) => canonical_f32(*f).hash(state),
            DuckValueRef::Double(d) => canonical_f64(*d).hash(state),
            #[cfg(feature = "chrono")]
            DuckValueRef::Timestamp(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Timestamp(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampS(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampS(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampMs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampMs(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampNs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampNs(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimestampTz(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimestampTz(t) => date_native::hash_system_time(t, state),
            #[cfg(feature = "chrono")]
            DuckValueRef::Date(d) => d.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Date(d) => d.hash(state),
            DuckValueRef::TemporalInfinity { kind, sign } => {
                kind.hash(state);
                sign.hash(state);
            },
            #[cfg(feature = "chrono")]
            DuckValueRef::Time(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Time(t) => t.hash(state),
            #[cfg(feature = "chrono")]
            DuckValueRef::Interval(i) => i.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::Interval(i) => i.hash(state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeTz(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeTz(t) => t.hash(state),
            #[cfg(feature = "chrono")]
            DuckValueRef::TimeNs(t) => t.hash(state),
            #[cfg(not(feature = "chrono"))]
            DuckValueRef::TimeNs(t) => t.hash(state),
            DuckValueRef::Text(s) => s.hash(state),
            DuckValueRef::Enum(s) => s.hash(state),
            DuckValueRef::Decimal(d) => d.hash(state),
            DuckValueRef::Blob(b) => b.hash(state),
            DuckValueRef::List(items) => items.hash(state),
            DuckValueRef::Array(items) => items.hash(state),
            DuckValueRef::Map(m) => {
                map::map_entries_hash_ref(m.iter(), m.len(), state);
            },
            DuckValueRef::Struct(m) => {
                // String keys — use order-independent combine for consistency.
                m.len().hash(state);
                let xor_fold: u64 = m
                    .iter()
                    .map(|(k, v)| {
                        let mut h = DefaultHasher::new();
                        k.hash(&mut h);
                        v.hash(&mut h);
                        h.finish()
                    })
                    .fold(0u64, |acc, x| acc ^ x);
                xor_fold.hash(state);
            },
            DuckValueRef::Union(u) => u.hash(state),
            DuckValueRef::Uuid(u) => u.hash(state),
            DuckValueRef::Bit(b) => b.hash(state),
            DuckValueRef::Bignum(b) => b.hash(state),
        }
    }
}

// From<&'a DuckValue> — borrows for Text/Enum/Blob

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
            DuckValue::TemporalInfinity { kind, sign } => {
                DuckValueRef::TemporalInfinity { kind: *kind, sign: *sign }
            },
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
            // Zero-copy borrows for text/enum; Blob is always cloned (owned).
            DuckValue::Text(s) => DuckValueRef::Text(Cow::Borrowed(s.as_str())),
            DuckValue::Decimal(d) => DuckValueRef::Decimal(*d),
            DuckValue::Blob(b) => DuckValueRef::Blob(b.clone()),
            DuckValue::List(l) => DuckValueRef::List(l.iter().map(DuckValueRef::from).collect()),
            DuckValue::Enum(e) => DuckValueRef::Enum(Cow::Borrowed(e)),
            DuckValue::Struct(m) => DuckValueRef::Struct(
                m.iter().map(|(k, v)| (k.clone(), DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Array(a) => DuckValueRef::Array(
                a.iter().map(DuckValueRef::from).collect::<Vec<_>>().into_boxed_slice(),
            ),
            DuckValue::Map(m) => DuckValueRef::Map(
                m.iter().map(|(k, v)| (DuckValueRef::from(k), DuckValueRef::from(v))).collect(),
            ),
            DuckValue::Union(u) => DuckValueRef::Union(Cow::Borrowed(u)),
            DuckValue::Uuid(u) => DuckValueRef::Uuid(*u),
            DuckValue::Bit(b) => DuckValueRef::Bit(b.clone()),
            DuckValue::Bignum(b) => DuckValueRef::Bignum(b.clone()),
        }
    }
}

// From<DuckValue> — owned conversion, any lifetime 'a
//
// All borrowed slots (`Text`, `Blob`, `Enum`) use `Cow::Owned`, so no external
// data is borrowed.  This lets Rust infer any `'a` from the call context — useful
// when the caller holds a `Vec<DuckValueRef<'a>>` and needs a concrete lifetime.

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
            DuckValue::TemporalInfinity { kind, sign } => {
                DuckValueRef::TemporalInfinity { kind, sign }
            },
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
            DuckValue::Blob(b) => DuckValueRef::Blob(b),
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
            DuckValue::Union(u) => DuckValueRef::Union(Cow::Owned(u)),
            DuckValue::Uuid(u) => DuckValueRef::Uuid(u),
            DuckValue::Bit(b) => DuckValueRef::Bit(b),
            DuckValue::Bignum(b) => DuckValueRef::Bignum(b),
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
    /// Scalar and temporal variants delegate to each inner type's own [`crate::types::appendable::AppendAble`]
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
                let dv = owned.to_duck().map_err(Error::ConversionError)?;
                // SAFETY: `stmt`/`idx` are valid; `dv` is an owned value bound and destroyed here.
                return unsafe { crate::types::appendable::bind_owned_value(stmt, idx, dv) };
            }};
        }

        match self {
            DuckValueRef::Null => {
                // SAFETY: `stmt` is a valid prepared statement; `idx` is 1-based.
                let rc = unsafe { ffi::duckdb_bind_null(stmt, idx) };
                crate::helpers::duck_result::check_state(rc)
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
                let rc = unsafe { ffi::duckdb_bind_uhugeint(stmt, idx, uhi) };
                crate::helpers::duck_result::check_state(rc)
            },
            DuckValueRef::Float(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Double(v) => v.stmt_append(idx, stmt),
            DuckValueRef::Text(s) => {
                let mut owned: String = s.as_ref().to_owned();
                owned.stmt_append(idx, stmt)
            },
            DuckValueRef::Blob(b) => b.stmt_append(idx, stmt),
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
            DuckValueRef::Decimal(d) => d.stmt_append(idx, stmt),

            // Remaining types go through the value path.
            DuckValueRef::List(_)
            | DuckValueRef::Array(_)
            | DuckValueRef::Struct(_)
            | DuckValueRef::Map(_)
            | DuckValueRef::Union(_)
            | DuckValueRef::TemporalInfinity { .. }
            | DuckValueRef::Uuid(_)
            | DuckValueRef::Bit(_)
            | DuckValueRef::Bignum(_) => {
                bind_via_to_duck!();
            },
            DuckValueRef::Enum(v) => v.clone().into_owned().stmt_append(idx, stmt),
        }
    }

    /// Appends this value to a DuckDB appender row.
    ///
    /// Scalar and temporal variants delegate to each inner type's own [`crate::types::appendable::AppendAble`]
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
                let dv = owned.to_duck().map_err(Error::ConversionError)?;
                // SAFETY: `appender` is valid; `dv` is an owned value appended and destroyed here.
                return unsafe { crate::types::appendable::append_owned_value(appender, dv) };
            }};
        }

        match self {
            DuckValueRef::Null => {
                // SAFETY: `appender` is a valid duckdb_appender.
                let rc = unsafe { ffi::duckdb_append_null(appender) };
                // SAFETY: `appender` is valid and non-null.
                unsafe { crate::helpers::duck_result::check_append(rc, appender) }
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
                let rc = unsafe { ffi::duckdb_append_uhugeint(appender, uhi) };
                // SAFETY: `appender` is valid and non-null.
                unsafe { crate::helpers::duck_result::check_append(rc, appender) }
            },
            DuckValueRef::Float(v) => v.appender_append(appender),
            DuckValueRef::Double(v) => v.appender_append(appender),
            DuckValueRef::Text(s) => {
                let bytes = s.as_bytes();
                // SAFETY: `bytes.as_ptr()` is valid UTF-8; append copies the data.
                let rc = unsafe {
                    ffi::duckdb_append_varchar_length(
                        appender,
                        bytes.as_ptr() as *const std::os::raw::c_char,
                        bytes.len() as u64,
                    )
                };
                // SAFETY: `appender` is valid and non-null.
                unsafe { crate::helpers::duck_result::check_append(rc, appender) }
            },
            DuckValueRef::Blob(b) => b.appender_append(appender),
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
            | DuckValueRef::Union(_)
            | DuckValueRef::TemporalInfinity { .. }
            | DuckValueRef::Uuid(_)
            | DuckValueRef::Bit(_)
            | DuckValueRef::Bignum(_) => {
                append_via_to_duck!();
            },
            DuckValueRef::Decimal(d) => d.appender_append(appender),
        }
    }
}

//
// From<primitive> for DuckValueRef — mirrors the per-type `From<T> for DuckValue`
// impls scattered across numeric.rs/varchar.rs/blob.rs/etc.
// These allow callers to pass raw Rust primitives where a DuckValueRef is expected.
//

impl<'a> From<bool> for DuckValueRef<'a> {
    fn from(v: bool) -> Self {
        DuckValueRef::Boolean(v)
    }
}
impl<'a> From<i8> for DuckValueRef<'a> {
    fn from(v: i8) -> Self {
        DuckValueRef::TinyInt(v)
    }
}
impl<'a> From<i16> for DuckValueRef<'a> {
    fn from(v: i16) -> Self {
        DuckValueRef::SmallInt(v)
    }
}
impl<'a> From<i32> for DuckValueRef<'a> {
    fn from(v: i32) -> Self {
        DuckValueRef::Int(v)
    }
}
impl<'a> From<i64> for DuckValueRef<'a> {
    fn from(v: i64) -> Self {
        DuckValueRef::BigInt(v)
    }
}
impl<'a> From<i128> for DuckValueRef<'a> {
    fn from(v: i128) -> Self {
        DuckValueRef::HugeInt(v)
    }
}
impl<'a> From<u8> for DuckValueRef<'a> {
    fn from(v: u8) -> Self {
        DuckValueRef::UTinyInt(v)
    }
}
impl<'a> From<u16> for DuckValueRef<'a> {
    fn from(v: u16) -> Self {
        DuckValueRef::USmallInt(v)
    }
}
impl<'a> From<u32> for DuckValueRef<'a> {
    fn from(v: u32) -> Self {
        DuckValueRef::UInt(v)
    }
}
impl<'a> From<u64> for DuckValueRef<'a> {
    fn from(v: u64) -> Self {
        DuckValueRef::UBigInt(v)
    }
}
impl<'a> From<u128> for DuckValueRef<'a> {
    fn from(v: u128) -> Self {
        DuckValueRef::UHugeInt(v)
    }
}
impl<'a> From<f32> for DuckValueRef<'a> {
    fn from(v: f32) -> Self {
        DuckValueRef::Float(v)
    }
}
impl<'a> From<f64> for DuckValueRef<'a> {
    fn from(v: f64) -> Self {
        DuckValueRef::Double(v)
    }
}

/// Borrows the string slice (zero-copy).
impl<'a> From<&'a str> for DuckValueRef<'a> {
    fn from(v: &'a str) -> Self {
        DuckValueRef::Text(Cow::Borrowed(v))
    }
}

/// Owns the string.
impl<'a> From<String> for DuckValueRef<'a> {
    fn from(v: String) -> Self {
        DuckValueRef::Text(Cow::Owned(v))
    }
}

impl<'a> From<Blob> for DuckValueRef<'a> {
    fn from(b: Blob) -> Self {
        DuckValueRef::Blob(b)
    }
}

/// Converts a `Vec<u8>` into a `Blob` value.
impl<'a> From<Vec<u8>> for DuckValueRef<'a> {
    fn from(v: Vec<u8>) -> Self {
        DuckValueRef::Blob(Blob::new(v))
    }
}

impl<'a, T: Into<DuckValueRef<'a>>> From<Option<T>> for DuckValueRef<'a> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => DuckValueRef::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::{Hash, Hasher};

    /// A single-member (`INTEGER`) `DuckUnion` wrapping `value`, for round-trip tests.
    fn union1(value: DuckValue) -> crate::types::duck_union::DuckUnion {
        crate::types::duck_union::DuckUnion::new(
            std::sync::Arc::from([(
                "v".to_owned(),
                crate::types::TypeInfo::Scalar(crate::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER),
            )]),
            0,
            value,
        )
        .unwrap()
    }

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

    #[test]
    fn test_from_duck_value_owned() {
        // From<DuckValue> should produce any lifetime.
        let v = DuckValue::Text("world".to_string());
        let r: DuckValueRef<'_> = DuckValueRef::from(v);
        match r {
            DuckValueRef::Text(s) => assert_eq!(s, "world"),
            _ => panic!("Wrong variant"),
        }
    }

    #[test]
    fn test_eq_and_hash() {
        use std::collections::HashMap;
        let mut map: HashMap<DuckValueRef<'_>, i32> = HashMap::new();
        map.insert(DuckValueRef::Int(1), 10);
        map.insert(DuckValueRef::Text(Cow::Borrowed("key")), 20);
        assert_eq!(map.get(&DuckValueRef::Int(1)), Some(&10));
        assert_eq!(map.get(&DuckValueRef::Text(Cow::Borrowed("key"))), Some(&20));
    }

    #[test]
    fn composite_refs_roundtrip_borrowed_and_owned() {
        let value = DuckValue::Struct(HashMap::from([
            (
                "list".to_owned(),
                DuckValue::List(vec![DuckValue::text("borrowed"), DuckValue::Null]),
            ),
            (
                "map".to_owned(),
                DuckValue::Map(HashMap::from([(
                    DuckValue::Int(1),
                    DuckValue::Array(vec![DuckValue::Boolean(true)].into_boxed_slice()),
                )])),
            ),
            ("union".to_owned(), DuckValue::Union(union1(DuckValue::Int(9)))),
        ]));

        let borrowed = DuckValueRef::from(&value);
        assert_eq!(DuckValue::from(&borrowed), value);

        let owned: DuckValueRef<'static> = DuckValueRef::from(value.clone());
        assert_eq!(DuckValue::from(&owned), value);
    }

    #[test]
    fn composite_ref_hashes_ignore_map_and_struct_insertion_order() {
        use std::collections::hash_map::DefaultHasher;

        let first = DuckValueRef::Struct(HashMap::from([
            ("a".to_owned(), DuckValueRef::Int(1)),
            ("b".to_owned(), DuckValueRef::Int(2)),
        ]));
        let second = DuckValueRef::Struct(HashMap::from([
            ("b".to_owned(), DuckValueRef::Int(2)),
            ("a".to_owned(), DuckValueRef::Int(1)),
        ]));
        let hash = |value: &DuckValueRef<'_>| {
            let mut hasher = DefaultHasher::new();
            value.hash(&mut hasher);
            hasher.finish()
        };

        assert_eq!(first, second);
        assert_eq!(hash(&first), hash(&second));
        assert_ne!(DuckValueRef::List(vec![]), DuckValueRef::Array(Box::new([])));
        assert_ne!(
            DuckValueRef::Union(std::borrow::Cow::Owned(union1(DuckValue::Int(1)))),
            DuckValueRef::Int(1)
        );
    }

    #[test]
    fn primitive_ref_conversions_cover_all_integer_paths_and_nulls() {
        assert_eq!(String::from(DuckValueRef::Text(Cow::Borrowed("duck"))), "duck");
        assert_eq!(String::from(DuckValueRef::Null), "");
        assert_eq!(i64::from(DuckValueRef::BigInt(9)), 9);
        assert_eq!(i64::from(DuckValueRef::Int(8)), 8);
        assert_eq!(i64::from(DuckValueRef::SmallInt(7)), 7);
        assert_eq!(i64::from(DuckValueRef::TinyInt(6)), 6);
        assert_eq!(i64::from(DuckValueRef::Null), 0);
        assert_eq!(i32::from(DuckValueRef::Int(5)), 5);
        assert_eq!(i32::from(DuckValueRef::SmallInt(4)), 4);
        assert_eq!(i32::from(DuckValueRef::TinyInt(3)), 3);
        assert_eq!(i32::from(DuckValueRef::Null), 0);
    }

    #[cfg(feature = "chrono")]
    fn owned_variants() -> Vec<DuckValue> {
        use crate::types::{Blob, DuckBignum, DuckBit, DuckDecimal, DuckEnum, DuckUuid};
        let nd = chrono::NaiveDate::from_ymd_opt(2021, 6, 15).unwrap();
        let nt = chrono::NaiveTime::from_hms_opt(1, 2, 3).unwrap();
        let ndt = nd.and_time(nt);
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
        let mut st = std::collections::HashMap::new();
        st.insert("a".to_owned(), DuckValue::Int(1));
        let mut mp = std::collections::HashMap::new();
        mp.insert(DuckValue::Int(1), DuckValue::Int(2));
        vec![
            DuckValue::Boolean(true), DuckValue::TinyInt(1), DuckValue::SmallInt(1),
            DuckValue::Int(1), DuckValue::BigInt(1), DuckValue::HugeInt(1), DuckValue::UTinyInt(1),
            DuckValue::USmallInt(1), DuckValue::UInt(1), DuckValue::UBigInt(1),
            DuckValue::UHugeInt(1), DuckValue::Float(1.5), DuckValue::Double(1.5),
            DuckValue::Timestamp(ndt), DuckValue::TimestampTz(dt), DuckValue::Date(nd),
            DuckValue::Time(nt), DuckValue::Interval(chrono::Duration::seconds(5)),
            DuckValue::Text("x".to_owned()), DuckValue::Decimal(DuckDecimal::new(100, 5, 2).unwrap()),
            DuckValue::Blob(Blob(vec![1, 2])), DuckValue::List(vec![DuckValue::Int(1)]),
            DuckValue::Array(vec![DuckValue::Int(1)].into_boxed_slice()), DuckValue::Struct(st),
            DuckValue::Map(mp),
            DuckValue::Enum(DuckEnum::from_label(std::sync::Arc::from(vec!["A".to_owned()]), "A").unwrap()),
            DuckValue::Union(union1(DuckValue::Int(1))), DuckValue::Uuid(DuckUuid(1)),
            DuckValue::Bit(DuckBit(vec![0, 1])), DuckValue::Bignum(DuckBignum::new(vec![1], false)),
            DuckValue::Null,
        ]
    }

    #[test]
    fn primitive_from_conversions_into_ref() {
        use crate::types::blob::Blob;
        let _ = DuckValueRef::from(true);
        let _ = DuckValueRef::from(1i8);
        let _ = DuckValueRef::from(1i16);
        let _ = DuckValueRef::from(1i32);
        let _ = DuckValueRef::from(1i64);
        let _ = DuckValueRef::from(1i128);
        let _ = DuckValueRef::from(1u8);
        let _ = DuckValueRef::from(1u16);
        let _ = DuckValueRef::from(1u32);
        let _ = DuckValueRef::from(1u64);
        let _ = DuckValueRef::from(1u128);
        let _ = DuckValueRef::from(1.5f32);
        let _ = DuckValueRef::from(1.5f64);
        let _ = DuckValueRef::from("x");
        let _ = DuckValueRef::from(String::from("x"));
        let _ = DuckValueRef::from(Blob(vec![1u8]));
        let _ = DuckValueRef::from(vec![1u8, 2]);
        let _ = DuckValueRef::from(Some(1i32));
        let _ = DuckValueRef::from(None::<i32>);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn owned_borrowed_eq_hash_and_bind_all_variants() {
        use std::hash::{Hash, Hasher};
        let mut conn = crate::connection::Connection::open_in_memory().unwrap();
        for v in owned_variants() {
            // Borrowed conversion + bind via a prepared statement (AppendAble::stmt_append).
            {
                let mut r = DuckValueRef::from(&v);
                let _ = conn.execute_with("SELECT $1", &mut [&mut r]);
            }
            // Owned conversion, equality, and hashing.
            let owned = DuckValueRef::from(v);
            assert_eq!(owned, owned.clone());
            let mut h = std::collections::hash_map::DefaultHasher::new();
            owned.hash(&mut h);
            let _ = h.finish();
        }
    }
}
