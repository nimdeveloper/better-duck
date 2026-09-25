//! DuckDB bitwise aggregates (over `BIGINT`).
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/aggregates>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::BigInt;

define_sql_function! {
    /// `bit_and(x)` — bitwise AND of all inputs.
    #[aggregate]
    fn bit_and(x: BigInt) -> diesel::sql_types::Nullable<BigInt>;
}
define_sql_function! {
    /// `bit_or(x)` — bitwise OR of all inputs.
    #[aggregate]
    fn bit_or(x: BigInt) -> diesel::sql_types::Nullable<BigInt>;
}
define_sql_function! {
    /// `bit_xor(x)` — bitwise XOR of all inputs.
    #[aggregate]
    fn bit_xor(x: BigInt) -> diesel::sql_types::Nullable<BigInt>;
}
