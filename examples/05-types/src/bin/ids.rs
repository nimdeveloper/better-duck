//! Identifier-ish scalar wrappers: `Uuid`, `Bit`, and `Bignum`.
//!
//!   * [`DuckUuid`] — a 128-bit `UUID` in standard big-endian bit order. `DuckUuid(0)`
//!     is the nil UUID; ordering matches standard UUID ordering.
//!   * [`DuckBit`] — a `BIT` bitstring in DuckDB's exact wire layout: the first byte
//!     is the number of padding bits (0..=7), the rest is bit-packed data (MSB first).
//!   * [`DuckBignum`] — an arbitrary-precision `BIGNUM` integer: a little-endian
//!     `magnitude` plus an `is_negative` flag.
//!
//! Each wraps into `DuckValue::Uuid` / `DuckValue::Bit` / `DuckValue::Bignum` and
//! round-trips byte-for-byte, with one documented exception noted inline for BIGNUM.

use better_duck_core::connection::Connection;
use better_duck_core::types::bignum::DuckBignum;
use better_duck_core::types::bit::DuckBit;
use better_duck_core::types::uuid::DuckUuid;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// Binds `dv` as `$1` and reads the single result column back.
fn round_trip(
    conn: &mut Connection,
    mut dv: DuckValue,
) -> Result<DuckValue> {
    let row = conn.execute_with("SELECT $1 AS v", &mut [&mut dv])?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("Uuid — DuckUuid(u128)");
    // The nil UUID is DuckUuid(0).
    assert_eq!(round_trip(&mut conn, DuckValue::Uuid(DuckUuid(0)))?, DuckValue::Uuid(DuckUuid(0)));
    // An arbitrary 128-bit value survives the storage-level sign-bit flip DuckDB
    // applies internally (DuckUuid hides that detail).
    let u = DuckUuid(0x1234_5678_9abc_def0_0fed_cba9_8765_4321);
    let v = round_trip(&mut conn, DuckValue::Uuid(u))?;
    show("Uuid(arbitrary)", &v);
    assert_eq!(v, DuckValue::Uuid(u));

    section("Bit — DuckBit(vec![padding_count, packed_bytes...])");
    // First byte 0 = no padding bits; second byte holds packed bits, MSB first.
    let bit = DuckBit(vec![0u8, 0b1010_0000]);
    let v = round_trip(&mut conn, DuckValue::Bit(bit.clone()))?;
    show("Bit([0, 0b1010_0000])", &v);
    assert_eq!(v, DuckValue::Bit(bit));
    // A BIT literal produced by DuckDB itself also decodes into a DuckBit.
    let row = conn.execute("SELECT '101'::BIT AS b")?.next().expect("one row")?;
    show("'101'::BIT", row.get("b"));
    assert!(matches!(row.get("b"), Some(DuckValue::Bit(_))));

    section("Bignum — DuckBignum::new(magnitude_le, is_negative)");
    // Positive: little-endian magnitude bytes, is_negative = false.
    let pos = DuckBignum::new(vec![0xFF, 0x01], false);
    let v = round_trip(&mut conn, DuckValue::Bignum(pos.clone()))?;
    show("Bignum(+[FF 01])", &v);
    assert_eq!(v, DuckValue::Bignum(pos));
    // Negative magnitude.
    let neg = DuckBignum::new(vec![0x2A], true);
    assert_eq!(round_trip(&mut conn, DuckValue::Bignum(neg.clone()))?, DuckValue::Bignum(neg));

    // Zero has no valid wire encoding for an empty buffer, so an empty magnitude is
    // encoded as a single zero byte: new(vec![], false) round-trips to new(vec![0], false).
    let zero_in = DuckBignum::new(vec![], false);
    let zero_out = round_trip(&mut conn, DuckValue::Bignum(zero_in))?;
    show("Bignum(empty) round-trips to", &zero_out);
    assert_eq!(zero_out, DuckValue::Bignum(DuckBignum::new(vec![0], false)));

    println!("\nuuid, bit, and bignum values verified");
    Ok(())
}
