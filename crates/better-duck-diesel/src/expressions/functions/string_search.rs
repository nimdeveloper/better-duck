//! DuckDB string search / extraction / padding functions that Diesel does not
//! ship by default.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/text>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Integer, Text};

define_sql_function! {
    /// `strpos(haystack, needle)` — 1-based position of `needle` in `haystack`, or `0`.
    fn strpos(haystack: Text, needle: Text) -> diesel::sql_types::Integer;
}
define_sql_function! {
    /// `instr(haystack, needle)` — alias of `strpos` (1-based match position, or `0`).
    fn instr(haystack: Text, needle: Text) -> diesel::sql_types::Integer;
}
define_sql_function! {
    /// `substr(s, start, length)` — `length` characters of `s` from 1-based `start`.
    fn substr(s: Text, start: Integer, length: Integer) -> Text;
}
define_sql_function! {
    /// `split_part(s, sep, index)` — the 1-based `index`-th field of `s` split on `sep`.
    fn split_part(s: Text, sep: Text, index: Integer) -> Text;
}
define_sql_function! {
    /// `concat_ws(sep, a, b)` — join `a` and `b` with separator `sep`.
    fn concat_ws(sep: Text, a: Text, b: Text) -> Text;
}
define_sql_function! {
    /// `left(s, n)` — the leftmost `n` characters of `s`.
    fn left(s: Text, n: Integer) -> Text;
}
define_sql_function! {
    /// `right(s, n)` — the rightmost `n` characters of `s`.
    fn right(s: Text, n: Integer) -> Text;
}
define_sql_function! {
    /// `lpad(s, len, fill)` — left-pad `s` to `len` characters with `fill`.
    fn lpad(s: Text, len: Integer, fill: Text) -> Text;
}
define_sql_function! {
    /// `rpad(s, len, fill)` — right-pad `s` to `len` characters with `fill`.
    fn rpad(s: Text, len: Integer, fill: Text) -> Text;
}
define_sql_function! {
    /// `ascii(s)` — the codepoint of the first character of `s`.
    fn ascii(s: Text) -> diesel::sql_types::Integer;
}
define_sql_function! {
    /// `chr(code)` — the single-character string for codepoint `code`.
    fn chr(code: Integer) -> Text;
}
define_sql_function! {
    /// `levenshtein(a, b)` — edit distance between `a` and `b`.
    fn levenshtein(a: Text, b: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `jaccard(a, b)` — Jaccard similarity of the character sets of `a` and `b`.
    fn jaccard(a: Text, b: Text) -> diesel::sql_types::Double;
}
