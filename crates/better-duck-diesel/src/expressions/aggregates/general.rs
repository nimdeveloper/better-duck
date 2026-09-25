//! DuckDB general-purpose aggregates.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/aggregates>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Bool, Text};

define_sql_function! {
    /// `bool_and(x)` — `true` if every non-`NULL` input is true.
    #[aggregate]
    fn bool_and(x: Bool) -> diesel::sql_types::Nullable<Bool>;
}
define_sql_function! {
    /// `bool_or(x)` — `true` if any non-`NULL` input is true.
    #[aggregate]
    fn bool_or(x: Bool) -> diesel::sql_types::Nullable<Bool>;
}
define_sql_function! {
    /// `string_agg(x, sep)` — concatenate `x` values, separated by `sep`.
    #[aggregate]
    fn string_agg(x: Text, sep: Text) -> diesel::sql_types::Nullable<Text>;
}
define_sql_function! {
    /// `arg_max(arg, val)` — the `arg` of the row with the maximum `val`.
    #[aggregate]
    fn arg_max(arg: Text, val: diesel::sql_types::Double) -> diesel::sql_types::Nullable<Text>;
}
define_sql_function! {
    /// `arg_min(arg, val)` — the `arg` of the row with the minimum `val`.
    #[aggregate]
    fn arg_min(arg: Text, val: diesel::sql_types::Double) -> diesel::sql_types::Nullable<Text>;
}
define_sql_function! {
    /// `product(x)` — the product of all values.
    #[aggregate]
    fn product(x: diesel::sql_types::Double) -> diesel::sql_types::Nullable<diesel::sql_types::Double>;
}
define_sql_function! {
    /// `any_value(x)` — an arbitrary non-`NULL` input value.
    #[aggregate]
    fn any_value(x: Text) -> diesel::sql_types::Nullable<Text>;
}
