//! DuckDB string-similarity / distance functions (Diesel ships none of these).
//!
//! `levenshtein` and `jaccard` live in `string_search.rs`; the rest are here.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/text>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `hamming(a, b)` — Hamming distance (number of differing positions).
    fn hamming(a: Text, b: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `mismatches(a, b)` — alias of `hamming`.
    fn mismatches(a: Text, b: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `damerau_levenshtein(a, b)` — edit distance allowing transpositions.
    fn damerau_levenshtein(a: Text, b: Text) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `jaro_similarity(a, b)` — Jaro similarity in `[0, 1]`.
    fn jaro_similarity(a: Text, b: Text) -> diesel::sql_types::Double;
}
define_sql_function! {
    /// `jaro_winkler_similarity(a, b)` — Jaro–Winkler similarity in `[0, 1]`.
    fn jaro_winkler_similarity(a: Text, b: Text) -> diesel::sql_types::Double;
}
