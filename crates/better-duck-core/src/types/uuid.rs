//! [`DuckDialect`] implementation for DuckDB's `UUID` type.
//!
//! UUID's physical column storage is fixed-width (16 bytes, like HUGEINT), so reads
//! from a data chunk vector use the packed `duckdb_uhugeint` struct directly (via
//! `read_packed!` in `value.rs`) rather than going through a heap-allocated
//! `duckdb_value`.

use super::*;
use crate::types::appendable::AppendAble;
use crate::{
    ffi::{
        duckdb_create_logical_type, duckdb_create_uuid, duckdb_logical_type, duckdb_uhugeint,
        duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    },
    impl_appendable_via_to_duck_native,
};

/// A DuckDB `UUID` value: a 128-bit value in standard big-endian bit order (the
/// order you'd get from `u128::from_be_bytes` on the 16-byte UUID representation).
///
/// DuckDB stores UUIDs internally as a `duckdb_uhugeint` with the most-significant
/// bit of the upper half flipped, so that unsigned hugeint comparisons sort the
/// same way as byte-wise UUID comparisons. `duckdb_create_uuid` performs that flip
/// itself, so [`to_duck`](DuckUuid::to_duck) passes the value straight through;
/// [`from_duck`](DuckUuid::from_duck) reads the already-flipped physical storage
/// directly (bypassing the value API), so it must undo the flip itself. This type
/// hides all of that: `DuckUuid(0)` is the nil UUID, and ordering on `DuckUuid`
/// matches standard UUID ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DuckUuid(pub u128);

const UUID_SIGN_FLIP: u64 = 0x8000_0000_0000_0000;

impl DuckUuid {
    /// Creates a `DuckUuid` from its 128-bit value in standard big-endian bit order.
    #[inline]
    pub fn new(value: u128) -> DuckUuid {
        DuckUuid(value)
    }
}

impl DuckDialect<duckdb_uhugeint> for DuckUuid {
    fn from_duck(raw: duckdb_uhugeint) -> Result<Self, DuckDBConversionError> {
        // Undo the sign-bit flip `duckdb_create_uuid` applies internally — this read
        // path pulls the raw physical struct directly out of the chunk vector, bypassing
        // the value API that would otherwise undo it for us.
        let upper = raw.upper ^ UUID_SIGN_FLIP;
        Ok(DuckUuid((upper as u128) << 64 | (raw.lower as u128)))
    }

    fn to_duck(&self) -> Result<duckdb_value, DuckDBConversionError> {
        let upper = (self.0 >> 64) as u64;
        let lower = self.0 as u64;
        // SAFETY: `duckdb_uhugeint { lower, upper }` is a valid FFI value for any bit
        // pattern. `duckdb_create_uuid` performs the sign-bit flip itself, so `upper` is
        // passed through unflipped here.
        Ok(unsafe { duckdb_create_uuid(duckdb_uhugeint { lower, upper }) })
    }
}

impl_appendable_via_to_duck_native!(DuckUuid);

impl DuckLogicalType for DuckUuid {
    fn duck_logical_type() -> Result<duckdb_logical_type, DuckDBConversionError> {
        // SAFETY: DUCKDB_TYPE_DUCKDB_TYPE_UUID is always a valid duckdb_type constant.
        Ok(unsafe { duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_UUID) })
    }
}

impl From<DuckUuid> for value::DuckValue {
    fn from(u: DuckUuid) -> Self {
        value::DuckValue::Uuid(u)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::duckdb_destroy_value;

    /// `duckdb_get_uuid`/`duckdb_create_uuid` operate on a bare `duckdb_value` and do
    /// not apply the storage-level sign flip — that flip only happens once a value is
    /// actually written into a column's physical vector (verified empirically via the
    /// end-to-end `rt_uuid_*` tests in `tests/types_roundtrip.rs`, which go through a
    /// real INSERT + SELECT). So `to_duck` must be a plain passthrough, checked here.
    fn to_duck_is_passthrough(value: DuckUuid) {
        use crate::ffi::duckdb_get_uuid;
        let mut duck_value = value.to_duck().unwrap();
        // SAFETY: `duck_value` was created by `to_duck` above.
        let raw = unsafe { duckdb_get_uuid(duck_value) };
        assert_eq!(raw.lower, value.0 as u64);
        assert_eq!(raw.upper, (value.0 >> 64) as u64);
        // SAFETY: `duck_value` was created by `to_duck` above; destroyed exactly once.
        unsafe { duckdb_destroy_value(&mut duck_value) };
    }

    /// `from_duck` reads the packed physical column representation directly (bypassing
    /// the value API), which — unlike `to_duck`/`duckdb_get_uuid` — *is* sign-flipped.
    /// Simulate that physical representation directly.
    fn from_duck_undoes_storage_flip(value: DuckUuid) {
        let physical = duckdb_uhugeint {
            lower: value.0 as u64,
            upper: ((value.0 >> 64) as u64) ^ UUID_SIGN_FLIP,
        };
        let converted = DuckUuid::from_duck(physical).unwrap();
        assert_eq!(value, converted);
    }

    #[test]
    fn nil_uuid() {
        to_duck_is_passthrough(DuckUuid(0));
        from_duck_undoes_storage_flip(DuckUuid(0));
    }

    #[test]
    fn max_uuid() {
        to_duck_is_passthrough(DuckUuid(u128::MAX));
        from_duck_undoes_storage_flip(DuckUuid(u128::MAX));
    }

    #[test]
    fn arbitrary_uuid() {
        let v = DuckUuid(0x1234_5678_9abc_def0_0fed_cba9_8765_4321);
        to_duck_is_passthrough(v);
        from_duck_undoes_storage_flip(v);
    }
}
