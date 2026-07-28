//! [`DuckDialect`] implementation for DuckDB's `BIT` (bitstring) type.

use super::*;
use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_bit, duckdb_create_bit, duckdb_create_logical_type, duckdb_free, duckdb_get_bit,
        duckdb_logical_type, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIT,
    },
    impl_appendable_via_to_duck_native,
};

/// A DuckDB `BIT` (bitstring) value.
///
/// Stores the exact wire-format bytes DuckDB uses: the first byte holds the number
/// of padding bits (0–7), and the remaining bytes hold the bit-packed data,
/// most-significant bit first. This is the same layout as `duckdb_bit`, so a value
/// read from DuckDB round-trips byte-for-byte without reinterpretation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DuckBit(pub Vec<u8>);

impl DuckBit {
    /// Wraps raw `BIT` wire-format bytes (padding-count byte + packed bit data).
    #[inline]
    pub fn new(bytes: Vec<u8>) -> DuckBit {
        DuckBit(bytes)
    }
}

impl DuckDialect for DuckBit {
    fn from_duck(value: duckdb_value) -> Result<Self, DuckDBConversionError> {
        // SAFETY: `value` is a valid duckdb_value of type BIT.
        let raw = unsafe { duckdb_get_bit(value) };
        if raw.data.is_null() {
            return Err(DuckDBConversionError::NullValue);
        }
        // SAFETY: `raw.data` is valid for `raw.size` bytes for the duration of this call;
        // we copy immediately before freeing.
        let bytes = unsafe { std::slice::from_raw_parts(raw.data, raw.size as usize) }.to_vec();
        // SAFETY: `raw.data` was allocated by DuckDB (`duckdb_get_bit`) and must be freed
        // with `duckdb_free` exactly once.
        unsafe { duckdb_free(raw.data as *mut std::ffi::c_void) };
        Ok(DuckBit(bytes))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let bit = duckdb_bit {
            // SAFETY-relevant: `duckdb_create_bit` copies the bytes internally; the pointer
            // only needs to stay valid for the duration of this call.
            data: self.0.as_ptr() as *mut u8,
            size: self.0.len() as crate::ffi::idx_t,
        };
        // SAFETY: `bit.data` is valid for `bit.size` bytes for the duration of this call.
        Ok(unsafe { duckdb_create_bit(bit) })
    }
}

impl_appendable_via_to_duck_native!(DuckBit);

impl DuckLogicalType for DuckBit {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_BIT is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_BIT) })
    }
}

impl From<DuckBit> for value::DuckValue {
    fn from(b: DuckBit) -> Self {
        value::DuckValue::Bit(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::duckdb_destroy_value;

    #[test]
    fn roundtrip_empty_bit() {
        let value = DuckBit(vec![0u8]);
        let mut duck_value = value.to_duck().unwrap();
        let converted = DuckBit::from_duck(duck_value).unwrap();
        assert_eq!(value, converted);
        // SAFETY: `duck_value` was created by `to_duck` above.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    #[test]
    fn roundtrip_arbitrary_bits() {
        let value = DuckBit(vec![3u8, 0b1110_0000]);
        let mut duck_value = value.to_duck().unwrap();
        let converted = DuckBit::from_duck(duck_value).unwrap();
        assert_eq!(value, converted);
        // SAFETY: `duck_value` was created by `to_duck` above.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }
}
