//! DuckDB numeric scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/numeric>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Double, SingleValue};

define_sql_function! {
    /// `abs(x)` — absolute value, preserving the input type.
    fn abs<T: SingleValue>(x: T) -> T;
}
define_sql_function! {
    /// `sqrt(x)` — square root.
    fn sqrt(x: Double) -> Double;
}
define_sql_function! {
    /// `pow(base, exponent)` — `base` raised to `exponent`.
    fn pow(base: Double, exponent: Double) -> Double;
}
define_sql_function! {
    /// `ceil(x)` — round up to the nearest integer value.
    fn ceil(x: Double) -> Double;
}
define_sql_function! {
    /// `floor(x)` — round down to the nearest integer value.
    fn floor(x: Double) -> Double;
}
define_sql_function! {
    /// `round(x)` — round to the nearest integer value.
    fn round(x: Double) -> Double;
}
define_sql_function! {
    /// `cbrt(x)` — cube root.
    fn cbrt(x: Double) -> Double;
}
define_sql_function! {
    /// `exp(x)` — `e` raised to the power `x`.
    fn exp(x: Double) -> Double;
}
define_sql_function! {
    /// `ln(x)` — natural logarithm.
    fn ln(x: Double) -> Double;
}
define_sql_function! {
    /// `log2(x)` — base-2 logarithm.
    fn log2(x: Double) -> Double;
}
define_sql_function! {
    /// `log10(x)` — base-10 logarithm (DuckDB also spells this `log`).
    fn log10(x: Double) -> Double;
}
define_sql_function! {
    /// `sign(x)` — sign of `x` as `-1`, `0`, or `1`.
    fn sign(x: Double) -> Double;
}
define_sql_function! {
    /// `degrees(x)` — convert radians to degrees.
    fn degrees(x: Double) -> Double;
}
define_sql_function! {
    /// `radians(x)` — convert degrees to radians.
    fn radians(x: Double) -> Double;
}
define_sql_function! {
    /// `sin(x)` — sine of `x` (radians).
    fn sin(x: Double) -> Double;
}
define_sql_function! {
    /// `cos(x)` — cosine of `x` (radians).
    fn cos(x: Double) -> Double;
}
define_sql_function! {
    /// `tan(x)` — tangent of `x` (radians).
    fn tan(x: Double) -> Double;
}
define_sql_function! {
    /// `asin(x)` — arcsine, in radians.
    fn asin(x: Double) -> Double;
}
define_sql_function! {
    /// `acos(x)` — arccosine, in radians.
    fn acos(x: Double) -> Double;
}
define_sql_function! {
    /// `atan(x)` — arctangent, in radians.
    fn atan(x: Double) -> Double;
}
define_sql_function! {
    /// `atan2(y, x)` — arctangent of `y / x`, using the signs of both to pick the quadrant.
    fn atan2(y: Double, x: Double) -> Double;
}
define_sql_function! {
    /// `gcd(a, b)` — greatest common divisor.
    fn gcd(a: diesel::sql_types::BigInt, b: diesel::sql_types::BigInt) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `lcm(a, b)` — least common multiple.
    fn lcm(a: diesel::sql_types::BigInt, b: diesel::sql_types::BigInt) -> diesel::sql_types::BigInt;
}
