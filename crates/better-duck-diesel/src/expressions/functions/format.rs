//! DuckDB string-formatting functions (`concat`, `printf`, `format`) that Diesel
//! does not ship by default.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/text>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `concat(a, b)` — concatenate two strings (`NULL` treated as empty).
    fn concat(a: Text, b: Text) -> Text;
}
define_sql_function! {
    /// `printf(format, arg)` — `printf`-style formatting with one `%s`/`%d`/… argument.
    fn printf(format: Text, arg: Text) -> Text;
}
define_sql_function! {
    /// `format(format, arg)` — Python-style `{}` formatting with one argument.
    #[sql_name = "format"]
    fn format_(format: Text, arg: Text) -> Text;
}
