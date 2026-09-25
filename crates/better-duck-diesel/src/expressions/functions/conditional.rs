//! DuckDB conditional / null-handling scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/utility>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Nullable, SingleValue};

define_sql_function! {
    /// `nullif(a, b)` — `NULL` when `a = b`, otherwise `a`.
    fn nullif<T: SingleValue>(a: T, b: T) -> Nullable<T>;
}

define_sql_function! {
    /// `ifnull(a, b)` — `b` when `a` is `NULL`, otherwise `a`.
    fn ifnull<T: SingleValue>(a: Nullable<T>, b: T) -> T;
}
