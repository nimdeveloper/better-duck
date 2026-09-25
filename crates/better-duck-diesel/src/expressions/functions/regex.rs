//! DuckDB regular-expression scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/pattern_matching>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `regexp_matches(s, pattern)` — whether `pattern` matches anywhere in `s`.
    fn regexp_matches(s: Text, pattern: Text) -> diesel::sql_types::Bool;
}
define_sql_function! {
    /// `regexp_full_match(s, pattern)` — whether `pattern` matches the whole of `s`.
    fn regexp_full_match(s: Text, pattern: Text) -> diesel::sql_types::Bool;
}
define_sql_function! {
    /// `regexp_replace(s, pattern, replacement)` — replace the first match of `pattern`.
    fn regexp_replace(s: Text, pattern: Text, replacement: Text) -> Text;
}
define_sql_function! {
    /// `regexp_extract(s, pattern)` — the first substring of `s` matching `pattern`.
    fn regexp_extract(s: Text, pattern: Text) -> Text;
}
