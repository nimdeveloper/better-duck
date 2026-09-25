//! DuckDB string scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/text>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Integer, Text};

define_sql_function! {
    /// `lower(x)` — lowercase `x`.
    fn lower(x: Text) -> Text;
}
define_sql_function! {
    /// `upper(x)` — uppercase `x`.
    fn upper(x: Text) -> Text;
}
define_sql_function! {
    /// `length(x)` — number of characters in `x` (`BIGINT`).
    fn length(x: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `trim(x)` — strip leading and trailing spaces.
    fn trim(x: Text) -> Text;
}
define_sql_function! {
    /// `ltrim(x)` — strip leading spaces.
    fn ltrim(x: Text) -> Text;
}
define_sql_function! {
    /// `rtrim(x)` — strip trailing spaces.
    fn rtrim(x: Text) -> Text;
}
define_sql_function! {
    /// `reverse(x)` — reverse the characters of `x`.
    fn reverse(x: Text) -> Text;
}
define_sql_function! {
    /// `repeat(x, n)` — `x` repeated `n` times.
    fn repeat(x: Text, n: Integer) -> Text;
}
define_sql_function! {
    /// `replace(s, from, to)` — replace all occurrences of `from` in `s` with `to`.
    fn replace(s: Text, from: Text, to: Text) -> Text;
}
define_sql_function! {
    /// `md5(x)` — hex MD5 digest of `x`.
    fn md5(x: Text) -> Text;
}
define_sql_function! {
    /// `contains(haystack, needle)` — whether `needle` is a substring of `haystack`.
    fn contains(haystack: Text, needle: Text) -> diesel::sql_types::Bool;
}
define_sql_function! {
    /// `starts_with(s, prefix)` — whether `s` begins with `prefix`.
    fn starts_with(s: Text, prefix: Text) -> diesel::sql_types::Bool;
}
define_sql_function! {
    /// `ends_with(s, suffix)` — whether `s` ends with `suffix`.
    fn ends_with(s: Text, suffix: Text) -> diesel::sql_types::Bool;
}
