//! `DuckValue::Decimal` — DuckDB `DECIMAL(width, scale)`, a fixed-point number.
//!
//! A DECIMAL is stored as a scaled integer (the *mantissa*) plus a declared
//! `width` (total significant digits, 1..=38) and `scale` (fractional digits). The
//! numeric value is `mantissa * 10^(-scale)`. [`DuckDecimal`] carries all three, so
//! the declared precision survives a round trip rather than collapsing to a float.
//!
//! Public items shown here:
//!   [`DuckDecimal::new`] (mantissa + width + scale),
//!   [`DuckDecimal::from_double`] / [`DuckDecimal::to_double`] (lossy f64 interop),
//!   and the `decimal` feature's `From<rust_decimal::Decimal>` / `TryFrom` bridge.

use better_duck_core::connection::Connection;
use better_duck_core::error::Error;
use better_duck_core::types::decimal::DuckDecimal;
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

    section("DuckDecimal::new(mantissa, width, scale)");
    // 1234.56 as DECIMAL(6, 2): mantissa 123456, two fractional digits.
    let money = DuckDecimal::new(123_456, 6, 2).map_err(Error::ConversionError)?;
    show("value/width/scale", (money.value, money.width, money.scale));
    let v = round_trip(&mut conn, DuckValue::Decimal(money))?;
    show("round-tripped", &v);
    assert_eq!(v, DuckValue::Decimal(money));
    // The declared precision (width, scale) is preserved, not just the numeric value.
    match v {
        DuckValue::Decimal(d) => assert_eq!((d.value, d.width, d.scale), (123_456, 6, 2)),
        other => panic!("expected Decimal, got {other:?}"),
    }

    section("widest DECIMAL: width 38 (DuckDB's maximum)");
    // DECIMAL(38, 0) holds a 38-digit integer mantissa.
    let wide =
        DuckDecimal::new(98_765_432_109_876_543_210_i128, 38, 0).map_err(Error::ConversionError)?;
    let v = round_trip(&mut conn, DuckValue::Decimal(wide))?;
    show("DECIMAL(38,0)", &v);
    assert_eq!(v, DuckValue::Decimal(wide));

    section("DuckDecimal::from_double / to_double (lossy f64 interop)");
    // from_double rounds to `scale` fractional digits using DuckDB's own routine:
    // 12.345 into DECIMAL(6, 2) rounds to 12.35 (mantissa 1235).
    let rounded = DuckDecimal::from_double(12.345, 6, 2).map_err(Error::ConversionError)?;
    show("from_double(12.345, 6, 2)", (rounded.value, rounded.width, rounded.scale));
    assert_eq!(rounded.value, 1235);
    // to_double reverses the scale (within f64 precision).
    show("to_double()", rounded.to_double());
    assert!((rounded.to_double() - 12.35).abs() < 1e-9);

    section("rust_decimal::Decimal <-> DuckDecimal (the `decimal` feature)");
    use rust_decimal::Decimal;
    // 1234.567 — From<Decimal> computes the minimum width that holds the value.
    let rd = Decimal::from_i128_with_scale(1_234_567, 3);
    let duck: DuckDecimal = rd.into();
    show("scale carried across", duck.scale);
    assert_eq!(duck.scale, 3);
    // TryFrom<DuckDecimal> converts back, preserving the exact value and scale.
    let back: Decimal = duck.try_into().expect("scale <= 28 fits rust_decimal");
    show("back to rust_decimal", &back);
    assert_eq!(back, rd);

    println!("\nDECIMAL round trips and conversions verified");
    Ok(())
}
