//! DuckDB approximate + quantile aggregates.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/aggregates>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Double, Float, Text};

define_sql_function! {
    /// `approx_count_distinct(x)` — HyperLogLog cardinality estimate.
    #[aggregate]
    fn approx_count_distinct(x: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `approx_quantile(x, pos)` — T-Digest quantile estimate at `pos` in `[0,1]`.
    ///
    /// DuckDB types the position argument as `FLOAT`, not `DOUBLE`.
    #[aggregate]
    fn approx_quantile(x: Double, pos: Float) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `reservoir_quantile(x, q)` — reservoir-sampled quantile estimate.
    #[aggregate]
    fn reservoir_quantile(x: Double, q: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `quantile_cont(x, pos)` — interpolated (continuous) quantile at `pos`.
    #[aggregate]
    fn quantile_cont(x: Double, pos: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `quantile_disc(x, pos)` — discrete quantile at `pos`.
    #[aggregate]
    fn quantile_disc(x: Double, pos: Double) -> diesel::sql_types::Nullable<Double>;
}
