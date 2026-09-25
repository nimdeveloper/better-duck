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
define_sql_function! {
    /// `skewness(x)` — skewness of the distribution.
    #[aggregate]
    fn skewness(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `kurtosis(x)` — excess kurtosis of the distribution.
    #[aggregate]
    fn kurtosis(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `entropy(x)` — log-2 information entropy.
    #[aggregate]
    fn entropy(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `mad(x)` — mean absolute deviation.
    #[aggregate]
    fn mad(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `mode(x)` — most frequent value.
    #[aggregate]
    fn mode(x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `regr_slope(y, x)` — slope of the least-squares fit.
    #[aggregate]
    fn regr_slope(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `regr_intercept(y, x)` — intercept of the least-squares fit.
    #[aggregate]
    fn regr_intercept(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `regr_r2(y, x)` — coefficient of determination.
    #[aggregate]
    fn regr_r2(y: Double, x: Double) -> diesel::sql_types::Nullable<Double>;
}
define_sql_function! {
    /// `regr_count(y, x)` — count of non-NULL pairs.
    #[aggregate]
    fn regr_count(y: Double, x: Double) -> diesel::sql_types::Nullable<diesel::sql_types::BigInt>;
}
