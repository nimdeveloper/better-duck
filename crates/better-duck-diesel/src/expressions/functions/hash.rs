//! DuckDB hashing scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/utility>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `sha256(x)` — hex SHA-256 digest of `x`.
    fn sha256(x: Text) -> Text;
}
define_sql_function! {
    /// `sha1(x)` — hex SHA-1 digest of `x`.
    fn sha1(x: Text) -> Text;
}
