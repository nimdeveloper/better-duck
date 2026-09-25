//! DuckDB JSON scalar functions (JSON values handled as their `VARCHAR` text).
//!
//! These require DuckDB's `json` extension, which is normally autoloaded.
//!
//! Docs: <https://duckdb.org/docs/current/data/json/json_functions>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `json_valid(x)` — whether `x` is valid JSON.
    fn json_valid(x: Text) -> diesel::sql_types::Bool;
}
define_sql_function! {
    /// `json_type(x)` — the JSON type of the value (`OBJECT`, `ARRAY`, …).
    fn json_type(x: Text) -> Text;
}
define_sql_function! {
    /// `json_extract_string(json, path)` — the string at `path` (unquoted).
    fn json_extract_string(json: Text, path: Text) -> diesel::sql_types::Nullable<Text>;
}
define_sql_function! {
    /// `json_array_length(x)` — the number of elements in a JSON array.
    fn json_array_length(x: Text) -> diesel::sql_types::BigInt;
}
