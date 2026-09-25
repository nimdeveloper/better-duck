//! Float canonicalization helpers for [`DuckValue`](crate::types::value::DuckValue)
//! `Eq` + `Hash` implementations.
//!
//! DuckDB `FLOAT` and `DOUBLE` values may be `NaN` or `-0.0`.  To give
//! `HashMap<DuckValue, _>` correct semantics we normalize both before comparing or hashing:
//!
//! - All `NaN` patterns → one canonical NaN bit pattern (`f32::NAN.to_bits()` /
//!   `f64::NAN.to_bits()`).
//! - `-0.0` → `+0.0`.
//!
//! This ensures `a == b ⟹ hash(a) == hash(b)` holds for every pair of `DuckValue`s,
//! satisfying the `HashMap` contract.

/// Canonicalize an `f32` to a stable `u32` bit pattern.
///
/// All `NaN` patterns → `f32::NAN.to_bits()`; `-0.0` → `+0.0`.
#[inline]
pub(crate) fn canonical_f32(v: f32) -> u32 {
    if v.is_nan() {
        f32::NAN.to_bits()
    } else if v == 0.0_f32 {
        0.0_f32.to_bits()
    } else {
        v.to_bits()
    }
}

/// Canonicalize an `f64` to a stable `u64` bit pattern.
///
/// All `NaN` patterns → `f64::NAN.to_bits()`; `-0.0` → `+0.0`.
#[inline]
pub(crate) fn canonical_f64(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else if v == 0.0_f64 {
        0.0_f64.to_bits()
    } else {
        v.to_bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nan_f32_canonicalized() {
        assert_eq!(canonical_f32(f32::NAN), canonical_f32(f32::NAN));
        // Any NaN bit pattern → same result
        let nan2 = f32::from_bits(0x7F80_0001); // signaling NaN
        assert_eq!(canonical_f32(f32::NAN), canonical_f32(nan2));
    }

    #[test]
    fn neg_zero_f32_canonicalized() {
        assert_eq!(canonical_f32(-0.0_f32), canonical_f32(0.0_f32));
    }

    #[test]
    fn nan_f64_canonicalized() {
        assert_eq!(canonical_f64(f64::NAN), canonical_f64(f64::NAN));
        let nan2 = f64::from_bits(0x7FF0_0000_0000_0001);
        assert_eq!(canonical_f64(f64::NAN), canonical_f64(nan2));
    }

    #[test]
    fn neg_zero_f64_canonicalized() {
        assert_eq!(canonical_f64(-0.0_f64), canonical_f64(0.0_f64));
    }

    #[test]
    fn negative_nan_patterns_are_canonicalized() {
        let f32_nan = f32::from_bits(0xFFC0_0001);
        let f64_nan = f64::from_bits(0xFFF8_0000_0000_0001);
        assert_eq!(canonical_f32(f32_nan), f32::NAN.to_bits());
        assert_eq!(canonical_f64(f64_nan), f64::NAN.to_bits());
    }

    #[test]
    fn finite_values_keep_their_bits() {
        for value in [f32::NEG_INFINITY, -42.5, f32::MIN_POSITIVE, f32::INFINITY] {
            assert_eq!(canonical_f32(value), value.to_bits());
        }
        for value in [f64::NEG_INFINITY, -42.5, f64::MIN_POSITIVE, f64::INFINITY] {
            assert_eq!(canonical_f64(value), value.to_bits());
        }
    }
}
