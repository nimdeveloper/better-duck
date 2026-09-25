//! DuckDB UUID scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/utility>

use diesel::expression::functions::define_sql_function;

define_sql_function! {
    /// `gen_random_uuid()` — a random (v4) UUID.
    fn gen_random_uuid() -> crate::sql_types::DuckUuid;
}
define_sql_function! {
    /// `uuidv4()` — a random v4 UUID.
    fn uuidv4() -> crate::sql_types::DuckUuid;
}
define_sql_function! {
    /// `uuidv7()` — a time-ordered v7 UUID.
    fn uuidv7() -> crate::sql_types::DuckUuid;
}
