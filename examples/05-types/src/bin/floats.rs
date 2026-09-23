//! The two floating-point `DuckValue` variants: `Float(f32)` and `Double(f64)`.
//!
//! DuckDB `FLOAT` is IEEE-754 single precision and `DOUBLE` is double precision.
//! `DuckValue::Float`/`DuckValue::Double` map to them directly. Equality on the
//! `DuckValue` enum is *canonical* for floats (NaN == NaN, -0.0 == +0.0), so even
//! the special IEEE values compare as you'd expect after a round trip.

use better_duck_core::connection::Connection;
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

    section("Float(f32) — DuckDB FLOAT (single precision)");
    let v = round_trip(&mut conn, DuckValue::Float(3.5_f32))?;
    show("Float(3.5)", &v);
    assert_eq!(v, DuckValue::Float(3.5_f32));
    assert_eq!(round_trip(&mut conn, DuckValue::Float(-1.0_f32))?, DuckValue::Float(-1.0_f32));

    section("Double(f64) — DuckDB DOUBLE (double precision)");
    let v = round_trip(&mut conn, DuckValue::Double(2.718_281_828_459_045_f64))?;
    show("Double(e)", &v);
    assert_eq!(v, DuckValue::Double(2.718_281_828_459_045_f64));
    // A large finite magnitude survives DOUBLE exactly.
    let big = f64::MAX / 2.0;
    assert_eq!(round_trip(&mut conn, DuckValue::Double(big))?, DuckValue::Double(big));

    section("canonical float equality after a round trip");
    // DuckValue's Eq is canonical: a NaN that goes in comes back equal to NaN.
    let nan = round_trip(&mut conn, DuckValue::Double(f64::NAN))?;
    show("Double(NaN)", &nan);
    assert_eq!(nan, DuckValue::Double(f64::NAN));
    // -0.0 and +0.0 are equal under canonical comparison.
    assert_eq!(round_trip(&mut conn, DuckValue::Float(-0.0_f32))?, DuckValue::Float(0.0_f32));

    println!("\nboth float variants round-tripped");
    Ok(())
}
