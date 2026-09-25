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
