//! Every integer `DuckValue` variant and its Rust <-> DuckDB round trip.
//!
//! DuckDB has ten integer column types; `DuckValue` mirrors them one-to-one:
//!   signed:   `TinyInt(i8)`, `SmallInt(i16)`, `Int(i32)`, `BigInt(i64)`, `HugeInt(i128)`
//!   unsigned: `UTinyInt(u8)`, `USmallInt(u16)`, `UInt(u32)`, `UBigInt(u64)`, `UHugeInt(u128)`
//!
//! The round-trip idiom is the same for all of them: wrap the Rust integer in the
//! matching `DuckValue` variant, bind it as `$1` via
//! [`Connection::execute_with`](better_duck_core::connection::Connection::execute_with),
//! and read the single result column back with
//! [`DuckRow::get`](better_duck_core::DuckRow::get). What went in comes back out with
//! its exact variant and value preserved.

use better_duck_core::connection::Connection;
use better_duck_core::types::value::DuckValue;
use common::{section, show, Result};

/// Binds `dv` as `$1` in `SELECT $1 AS v` and returns the decoded value.
///
/// `execute_with` takes `&mut [&mut dyn AppendAble]`; `DuckValue` implements
/// `AppendAble`, so a `&mut DuckValue` is a valid bind slot. `.next()` yields the
/// first row (an `Option<Result<DuckRow>>`), and `row.get("v")` reads the column.
fn round_trip(
    conn: &mut Connection,
    mut dv: DuckValue,
) -> Result<DuckValue> {
    let row = conn.execute_with("SELECT $1 AS v", &mut [&mut dv])?.next().expect("one row")?;
    Ok(row.get("v").expect("column v").clone())
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    section("signed integers");
    // TinyInt(i8): DuckDB TINYINT, the full i8 range.
    let v = round_trip(&mut conn, DuckValue::TinyInt(i8::MIN))?;
    show("TinyInt(i8::MIN)", &v);
    assert_eq!(v, DuckValue::TinyInt(i8::MIN));
    assert_eq!(round_trip(&mut conn, DuckValue::TinyInt(i8::MAX))?, DuckValue::TinyInt(i8::MAX));

    // SmallInt(i16): DuckDB SMALLINT.
    let v = round_trip(&mut conn, DuckValue::SmallInt(-12_345))?;
    show("SmallInt(-12345)", &v);
    assert_eq!(v, DuckValue::SmallInt(-12_345));
    assert_eq!(round_trip(&mut conn, DuckValue::SmallInt(i16::MAX))?, DuckValue::SmallInt(i16::MAX));

    // Int(i32): DuckDB INTEGER.
    let v = round_trip(&mut conn, DuckValue::Int(42))?;
    show("Int(42)", &v);
    assert_eq!(v, DuckValue::Int(42));
    assert_eq!(round_trip(&mut conn, DuckValue::Int(i32::MIN))?, DuckValue::Int(i32::MIN));

    // BigInt(i64): DuckDB BIGINT.
    let v = round_trip(&mut conn, DuckValue::BigInt(i64::MAX))?;
    show("BigInt(i64::MAX)", &v);
    assert_eq!(v, DuckValue::BigInt(i64::MAX));

    // HugeInt(i128): DuckDB HUGEINT (128-bit). DuckDB's own conversion supports
    // magnitudes up to 2^127 - 2^63, so this near-maximum value round-trips exactly.
    let huge = 170_141_183_460_469_231_722_463_931_679_029_329_919_i128;
    let v = round_trip(&mut conn, DuckValue::HugeInt(huge))?;
    show("HugeInt(+large)", &v);
    assert_eq!(v, DuckValue::HugeInt(huge));
    assert_eq!(round_trip(&mut conn, DuckValue::HugeInt(-huge))?, DuckValue::HugeInt(-huge));

    section("unsigned integers");
    // UTinyInt(u8): DuckDB UTINYINT.
    let v = round_trip(&mut conn, DuckValue::UTinyInt(u8::MAX))?;
    show("UTinyInt(u8::MAX)", &v);
    assert_eq!(v, DuckValue::UTinyInt(u8::MAX));

    // USmallInt(u16): DuckDB USMALLINT.
    let v = round_trip(&mut conn, DuckValue::USmallInt(u16::MAX))?;
    show("USmallInt(u16::MAX)", &v);
    assert_eq!(v, DuckValue::USmallInt(u16::MAX));

    // UInt(u32): DuckDB UINTEGER.
    let v = round_trip(&mut conn, DuckValue::UInt(u32::MAX))?;
    show("UInt(u32::MAX)", &v);
    assert_eq!(v, DuckValue::UInt(u32::MAX));

    // UBigInt(u64): DuckDB UBIGINT.
    let v = round_trip(&mut conn, DuckValue::UBigInt(u64::MAX))?;
    show("UBigInt(u64::MAX)", &v);
    assert_eq!(v, DuckValue::UBigInt(u64::MAX));

    // UHugeInt(u128): DuckDB UHUGEINT (unsigned 128-bit), the full u128 range.
    let v = round_trip(&mut conn, DuckValue::UHugeInt(u128::MAX))?;
    show("UHugeInt(u128::MAX)", &v);
    assert_eq!(v, DuckValue::UHugeInt(u128::MAX));

    println!("\nall integer variants round-tripped");
    Ok(())
}
