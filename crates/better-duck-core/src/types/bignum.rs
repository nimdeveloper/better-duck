//! [`DuckDialect`] implementation for DuckDB's `BIGNUM` (arbitrary-precision integer) type.
//!
//! `BIGNUM` was previously named `VARINT` in some DuckDB releases; this crate targets
//! the bundled DuckDB version, which uses `BIGNUM` (`duckdb_get_bignum` /
//! `duckdb_create_bignum`).

use super::*;
use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_bignum, duckdb_create_bignum, duckdb_create_logical_type, duckdb_free,
        duckdb_get_bignum, duckdb_logical_type, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGNUM,
    },
    impl_appendable_via_to_duck_native,
};

/// A DuckDB `BIGNUM` value: an arbitrary-precision signed integer.
///
/// `magnitude` holds the absolute value's bytes in little-endian order, matching
/// `duckdb_bignum.data` exactly. `is_negative` is `true` for negative values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DuckBignum {
    /// The absolute value, little-endian byte order.
    pub magnitude: Vec<u8>,
    /// `true` if this value is negative.
    pub is_negative: bool,
}

impl DuckBignum {
    /// Creates a `DuckBignum` from its little-endian magnitude bytes and sign.
    #[inline]
    pub fn new(
        magnitude: Vec<u8>,
        is_negative: bool,
    ) -> DuckBignum {
        DuckBignum { magnitude, is_negative }
    }
}

impl DuckDialect for DuckBignum {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type BIGNUM.
        let raw = unsafe { duckdb_get_bignum(value) };
        if raw.data.is_null() {
            return Err(DuckDBConversionError::NullValue);
        }
        // SAFETY: `raw.data` is valid for `raw.size` bytes for the duration of this call;
        // we copy immediately before freeing.
        let magnitude = unsafe { std::slice::from_raw_parts(raw.data, raw.size as usize) }.to_vec();
        // SAFETY: `raw.data` was allocated by DuckDB (`duckdb_get_bignum`) and must be
        // freed with `duckdb_free` exactly once.
        unsafe { duckdb_free(raw.data as *mut std::ffi::c_void) };
        Ok(DuckBignum { magnitude, is_negative: raw.is_negative })
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        // `duckdb_create_bignum` does not tolerate a zero-length buffer (it overruns a
        // stack buffer internally), so an empty magnitude — the value zero — is encoded
        // as a single zero byte instead.
        let zero = [0u8];
        let (ptr, len) = if self.magnitude.is_empty() {
            (zero.as_ptr(), 1)
        } else {
            (self.magnitude.as_ptr(), self.magnitude.len())
        };
        let bignum = duckdb_bignum {
            // SAFETY-relevant: `duckdb_create_bignum` copies the bytes internally; the
            // pointer only needs to stay valid for the duration of this call.
            data: ptr as *mut u8,
            size: len as crate::ffi::idx_t,
            is_negative: self.is_negative,
        };
        // SAFETY: `bignum.data` is valid for `bignum.size` bytes for the duration of
        // this call.
        Ok(unsafe { duckdb_create_bignum(bignum) })
    }
}

impl_appendable_via_to_duck_native!(DuckBignum);

impl DuckLogicalType for DuckBignum {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_BIGNUM is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIGNUM) })
    }
}

impl From<DuckBignum> for value::DuckValue {
    fn from(b: DuckBignum) -> Self {
        value::DuckValue::Bignum(b)
    }
}

/// Decodes DuckDB's internal sortable wire format for `BIGNUM` physical column
/// storage — used when reading directly from a data chunk vector (`from_duckdb_vec`
/// in `value.rs`), which is a *different* representation from the `duckdb_value` API
/// (`duckdb_get_bignum`/`duckdb_create_bignum`) used by [`DuckDialect::from_duck`]/
/// [`DuckDialect::to_duck`] above.
///
/// Format (reverse-engineered by comparing `to_duck` input against the bytes observed
/// after a real `INSERT` + chunk-vector read — DuckDB does not document this layout,
/// so treat it as an internal, unversioned detail that could change in a future
/// `libduckdb-sys` release): a 3-byte big-endian header `0x800000 | magnitude_len`,
/// followed by `magnitude_len` bytes of magnitude in the same little-endian order as
/// `duckdb_bignum.data`. For negative values, every byte (header and magnitude) is
/// bitwise-complemented — this both flags the sign (the header's top bit becomes 0)
/// and makes more-negative values sort before less-negative ones under a raw
/// byte-wise comparison.
pub(crate) fn decode_bignum_wire(raw: &[u8]) -> Option<DuckBignum> {
    if raw.len() < 3 {
        return None;
    }
    let is_negative = raw[0] & 0x80 == 0;
    let header: u32 = if is_negative {
        (u32::from(!raw[0]) << 16) | (u32::from(!raw[1]) << 8) | u32::from(!raw[2])
    } else {
        (u32::from(raw[0]) << 16) | (u32::from(raw[1]) << 8) | u32::from(raw[2])
    };
    let len = (header & 0x007F_FFFF) as usize;
    let rest = &raw[3..];
    if rest.len() < len {
        return None;
    }
    let mut magnitude = rest[..len].to_vec();
    if is_negative {
        for b in &mut magnitude {
            *b = !*b;
        }
    }
    Some(DuckBignum { magnitude, is_negative })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::duckdb_destroy_value;

    #[test]
    fn decode_wire_positive() {
        let decoded = decode_bignum_wire(&[128, 0, 2, 255, 1]).unwrap();
        assert_eq!(decoded, DuckBignum::new(vec![255, 1], false));
    }

    #[test]
    fn decode_wire_zero() {
        let decoded = decode_bignum_wire(&[128, 0, 1, 0]).unwrap();
        assert_eq!(decoded, DuckBignum::new(vec![0], false));
    }

    #[test]
    fn decode_wire_negative() {
        let decoded = decode_bignum_wire(&[127, 255, 254, 213]).unwrap();
        assert_eq!(decoded, DuckBignum::new(vec![42], true));
    }

    #[test]
    fn decode_wire_too_short_is_none() {
        assert!(decode_bignum_wire(&[128, 0]).is_none());
    }

    #[test]
    fn roundtrip_zero() {
        let value = DuckBignum::new(vec![], false);
        let mut duck_value = value.to_duck().unwrap();
        let converted = DuckBignum::from_duck(duck_value).unwrap();
        assert!(!converted.is_negative);
        assert!(converted.magnitude.iter().all(|&b| b == 0));
        // SAFETY: `duck_value` was created by `to_duck` above.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn roundtrip_positive() {
        let value = DuckBignum::new(vec![0xFF, 0x01], false);
        let mut duck_value = value.to_duck().unwrap();
        let converted = DuckBignum::from_duck(duck_value).unwrap();
        assert_eq!(value, converted);
        // SAFETY: `duck_value` was created by `to_duck` above.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn roundtrip_negative() {
        let value = DuckBignum::new(vec![0x2A], true);
        let mut duck_value = value.to_duck().unwrap();
        let converted = DuckBignum::from_duck(duck_value).unwrap();
        assert_eq!(value, converted);
        // SAFETY: `duck_value` was created by `to_duck` above.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
}
