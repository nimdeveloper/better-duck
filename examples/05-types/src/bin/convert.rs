//! Numeric and temporal conversion helpers re-exported at the crate root.
//!
//! These wrap DuckDB's own C routines, so they round/overflow exactly the way DuckDB
//! does internally rather than re-implementing the maths in Rust:
//!   * 128-bit <-> f64: [`hugeint_to_double`] / [`double_to_hugeint`] (signed) and
//!     [`uhugeint_to_double`] / [`double_to_uhugeint`] (unsigned). The double->int
//!     direction rejects NaN/inf and reports overflow as an error.
//!   * `TIMESTAMP` <-> broken-down parts: [`timestamp_micros_to_parts`] /
//!     [`parts_to_timestamp_micros`], exchanging a microsecond count for a
//!     [`TimestampParts`] calendar/clock struct.
//!
//! No database connection is needed — these are pure conversions.

use better_duck_core::error::Error;
use better_duck_core::{
    double_to_hugeint, double_to_uhugeint, hugeint_to_double, parts_to_timestamp_micros,
    timestamp_micros_to_parts, uhugeint_to_double, TimestampParts,
};
use common::{section, show, Result};

fn main() -> Result<()> {
    section("signed 128-bit <-> f64");
    // Values within f64's 53-bit exact-integer range round-trip exactly.
    let n: i128 = 1_i128 << 52;
    let as_double = hugeint_to_double(n);
    show("hugeint_to_double(2^52)", as_double);
    let back = double_to_hugeint(as_double).map_err(Error::ConversionError)?;
    show("double_to_hugeint(...)", back);
    assert_eq!(back, n);
    // double_to_hugeint rejects non-finite inputs and out-of-range magnitudes.
    assert!(double_to_hugeint(f64::NAN).is_err());
    assert!(double_to_hugeint(1e40).is_err());

    section("unsigned 128-bit <-> f64");
    let u: u128 = 1_u128 << 52;
    let ud = uhugeint_to_double(u);
    show("uhugeint_to_double(2^52)", ud);
    assert_eq!(double_to_uhugeint(ud).map_err(Error::ConversionError)?, u);
    // A negative double cannot be an unsigned integer.
    assert!(double_to_uhugeint(-1.0).is_err());

    section("TIMESTAMP microseconds <-> calendar/clock parts");
    // Decompose a microsecond count into TimestampParts (DuckDB's own normalization).
    let parts = TimestampParts {
        year: 2024,
        month: 3,
        day: 15,
        hour: 14,
        min: 30,
        sec: 45,
        micros: 123_456,
    };
    let micros = parts_to_timestamp_micros(parts);
    show("parts_to_timestamp_micros", micros);
    let back = timestamp_micros_to_parts(micros);
    show("timestamp_micros_to_parts", back);
    // The two helpers are exact inverses.
    assert_eq!(back, parts);
    assert_eq!(back.year, 2024);
    assert_eq!(back.micros, 123_456);

    println!("\nconversion helpers verified");
    Ok(())
}
