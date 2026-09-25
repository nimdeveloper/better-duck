//! DuckDB statistical aggregates.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/aggregates>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Double;

define_sql_function! {
    /// `stddev_samp(x)` — sample standard deviation.
    #[aggregate]
    fn stddev_samp(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `stddev_pop(x)` — population standard deviation.
    #[aggregate]
    fn stddev_pop(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `var_samp(x)` — sample variance.
    #[aggregate]
    fn var_samp(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `var_pop(x)` — population variance.
    #[aggregate]
    fn var_pop(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `median(x)` — median value.
    #[aggregate]
    fn median(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `corr(y, x)` — Pearson correlation coefficient.
    #[aggregate]
    fn corr(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `covar_samp(y, x)` — sample covariance.
    #[aggregate]
    fn covar_samp(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `covar_pop(y, x)` — population covariance.
    #[aggregate]
    fn covar_pop(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
