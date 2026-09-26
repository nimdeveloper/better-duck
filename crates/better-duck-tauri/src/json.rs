//! `DuckValue` ⇄ `serde_json::Value` bridge.
//!
//! Precision-sensitive scalars (`HUGEINT`/`DECIMAL`/`UUID`/`BIT`/`BIGNUM`) serialize as
//! strings to survive JS `number` limits; `BLOB` as a byte array; composites (`LIST`/
//! `ARRAY`/`STRUCT`/`MAP`/`UNION`/`ENUM`) as structured JSON; temporals as ISO-8601
//! strings. A `_` arm keeps this total against `DuckValue`'s `#[non_exhaustive]` future
//! variants (and the rare `TIME_TZ` / temporal-infinity cases).

use std::collections::HashMap;

use better_duck_core::types::value::DuckValue;
use serde_json::{Number, Value};

/// Convert a JSON parameter into a `DuckValue` for positional binding.
pub(crate) fn json_to_duck(value: &Value) -> DuckValue {
    match value {
        Value::Null => DuckValue::Null,
        Value::Bool(b) => DuckValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DuckValue::BigInt(i)
            } else if let Some(u) = n.as_u64() {
                DuckValue::UBigInt(u)
            } else {
                DuckValue::Double(n.as_f64().unwrap_or(0.0))
            }
        },
        Value::String(s) => DuckValue::Text(s.clone()),
        Value::Array(items) => DuckValue::List(items.iter().map(json_to_duck).collect()),
        Value::Object(map) => DuckValue::Struct(
            map.iter().map(|(k, v)| (k.clone(), json_to_duck(v))).collect::<HashMap<_, _>>(),
        ),
    }
}

/// Convert a `DuckValue` into JSON.
#[allow(clippy::enum_glob_use, clippy::wildcard_imports)]
pub(crate) fn duck_to_json(value: &DuckValue) -> Value {
    use DuckValue::*;
    match value {
        Null => Value::Null,
        Boolean(b) => Value::Bool(*b),
        TinyInt(n) => Value::from(i64::from(*n)),
        SmallInt(n) => Value::from(i64::from(*n)),
        Int(n) => Value::from(i64::from(*n)),
        BigInt(n) => Value::from(*n),
        UTinyInt(n) => Value::from(u64::from(*n)),
        USmallInt(n) => Value::from(u64::from(*n)),
        UInt(n) => Value::from(u64::from(*n)),
        UBigInt(n) => Value::from(*n),
        HugeInt(n) => Value::String(n.to_string()),
        UHugeInt(n) => Value::String(n.to_string()),
        Float(f) => Number::from_f64(f64::from(*f)).map_or(Value::Null, Value::Number),
        Double(f) => Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Text(s) => Value::String(s.clone()),
        Decimal(d) => Value::String(decimal_to_string(d.value, d.scale)),
        Uuid(u) => Value::String(uuid_to_string(u.0)),
        Bit(b) => Value::String(bit_to_string(&b.0)),
        Bignum(n) => Value::String(bignum_to_string(&n.magnitude, n.is_negative)),
        Blob(b) => Value::Array(b.as_bytes().iter().map(|x| Value::from(u64::from(*x))).collect()),
        List(items) => Value::Array(items.iter().map(duck_to_json).collect()),
        Array(items) => Value::Array(items.iter().map(duck_to_json).collect()),
        Struct(map) => {
            Value::Object(map.iter().map(|(k, v)| (k.clone(), duck_to_json(v))).collect())
        },
        Map(entries) => Value::Array(
            entries
                .iter()
                .map(|(k, v)| Value::Array(vec![duck_to_json(k), duck_to_json(v)]))
                .collect(),
        ),
        Enum(e) => Value::String(e.label().to_owned()),
        Union(u) => {
            let mut obj = serde_json::Map::new();
            obj.insert(u.active_name().to_owned(), duck_to_json(u.value()));
            Value::Object(obj)
        },
        // INTERVAL → its microsecond span (a number when it fits i64).
        Interval(d) => {
            d.num_microseconds().map_or_else(|| Value::String(format!("{d:?}")), Value::from)
        },
        // Temporals (DATE/TIME/TIMESTAMP*/TIMESTAMPTZ), TIME_TZ, temporal ±infinity, and
        // any future non_exhaustive variant serialize via their Debug form (ISO-8601 for
        // the chrono types — needs no chrono formatting feature).
        other => Value::String(format!("{other:?}")),
    }
}

/// Formats a DECIMAL mantissa (`value * 10^-scale`) as an exact decimal string.
fn decimal_to_string(
    value: i128,
    scale: u8,
) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let scale = usize::from(scale);
    let digits = value.unsigned_abs().to_string();
    let body = if digits.len() <= scale {
        format!("0.{digits:0>scale$}")
    } else {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if value < 0 {
        format!("-{body}")
    } else {
        body
    }
}

/// Formats a 128-bit UUID value as the canonical hyphenated string.
fn uuid_to_string(n: u128) -> String {
    let b = n.to_be_bytes();
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

/// Decodes DuckDB `BIT` wire bytes (leading padding-count byte + MSB-first data) into a
/// `'0'`/`'1'` string.
fn bit_to_string(bytes: &[u8]) -> String {
    let Some((&pad_byte, data)) = bytes.split_first() else {
        return String::new();
    };
    let pad = usize::from(pad_byte.min(7));
    let mut out = String::with_capacity((data.len() * 8).saturating_sub(pad));
    for (byte_idx, byte) in data.iter().enumerate() {
        for bit in (0..8).rev() {
            let idx = byte_idx * 8 + (7 - bit);
            if idx < pad {
                continue; // leading padding bits are not part of the value
            }
            out.push(if (byte >> bit) & 1 == 1 { '1' } else { '0' });
        }
    }
    out
}

/// Formats a BIGNUM (little-endian magnitude + sign) as a decimal string.
fn bignum_to_string(
    magnitude: &[u8],
    is_negative: bool,
) -> String {
    let mut limbs = magnitude.to_vec(); // little-endian base-256
    while limbs.last() == Some(&0) {
        limbs.pop();
    }
    if limbs.is_empty() {
        return "0".to_owned();
    }
    let mut decimal_digits = Vec::new();
    while !limbs.is_empty() {
        let mut remainder: u32 = 0;
        for limb in limbs.iter_mut().rev() {
            let current = (remainder << 8) | u32::from(*limb);
            *limb = (current / 10) as u8;
            remainder = current % 10;
        }
        decimal_digits.push(b'0' + remainder as u8);
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
    }
    let mut s = String::with_capacity(decimal_digits.len() + usize::from(is_negative));
    if is_negative {
        s.push('-');
    }
    s.extend(decimal_digits.iter().rev().map(|d| *d as char));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_formats_exactly() {
        assert_eq!(decimal_to_string(123_456, 2), "1234.56");
        assert_eq!(decimal_to_string(-5, 3), "-0.005");
        assert_eq!(decimal_to_string(42, 0), "42");
        assert_eq!(decimal_to_string(0, 2), "0.00");
    }

    #[test]
    fn uuid_formats_canonically() {
        assert_eq!(uuid_to_string(0), "00000000-0000-0000-0000-000000000000");
        let n = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128;
        assert_eq!(uuid_to_string(n), "01020304-0506-0708-090a-0b0c0d0e0f10");
    }

    #[test]
    fn bignum_decimal_string() {
        assert_eq!(bignum_to_string(&[], false), "0");
        assert_eq!(bignum_to_string(&[0], false), "0");
        assert_eq!(bignum_to_string(&[210, 4], false), "1234"); // LE 0x04d2
        assert_eq!(bignum_to_string(&[210, 4], true), "-1234");
    }

    #[test]
    fn bit_string_skips_padding() {
        assert_eq!(bit_to_string(&[0, 0b1011_0000]), "10110000");
        assert_eq!(bit_to_string(&[3, 0b0001_0110]), "10110");
        assert_eq!(bit_to_string(&[]), "");
    }
}
