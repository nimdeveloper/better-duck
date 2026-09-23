//! DECIMAL without the `decimal` feature, via the always-available `DuckDecimal`.
//!
//! The `decimal` feature only adds `rust_decimal` interop. Basic DECIMAL
//! representability lives in [`DuckDecimal`] (mantissa + width + scale), which is
//! compiled unconditionally. A `CAST(... AS DECIMAL(w, s))` column therefore reads
//! back with its declared precision intact, and the `new`/`from_double`/`to_double`
//! constructors work with no optional features enabled.

use better_duck_core::connection::Connection;
use better_duck_core::error::Result;
use better_duck_core::types::decimal::DuckDecimal;
use better_duck_core::types::value::DuckValue;

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;

    // A DECIMAL column read back preserves mantissa, width, and scale.
    let row =
        conn.execute("SELECT CAST(1234.56 AS DECIMAL(6,2)) AS d")?.next().expect("one row")?;
    let d = row.get("d").expect("column d").clone();
    println!("CAST(1234.56 AS DECIMAL(6,2)) -> {d:?}");
    assert_eq!(d, DuckValue::Decimal(DuckDecimal { value: 123_456, width: 6, scale: 2 }));

    // The validating constructor is available with no features.
    let made = DuckDecimal::new(-98_765, 8, 3).expect("valid width/scale");
    println!("DuckDecimal::new -> value={} width={} scale={}", made.value, made.width, made.scale);
    assert_eq!((made.value, made.width, made.scale), (-98_765, 8, 3));

    // from_double / to_double use DuckDB's own rounding, also feature-independent:
    // 12.345 into DECIMAL(6, 2) rounds to 12.35 (mantissa 1235).
    let rounded = DuckDecimal::from_double(12.345, 6, 2).expect("representable");
    println!("from_double(12.345, 6, 2) -> mantissa {}", rounded.value);
    assert_eq!(rounded.value, 1235);
    assert!((rounded.to_double() - 12.35).abs() < 1e-9);

    println!("\nDECIMAL round-trips without the `decimal` feature");
    Ok(())
}
