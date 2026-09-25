//! DuckDB JSON aggregate functions (require the `json` extension, normally autoloaded).
//!
//! Docs: <https://duckdb.org/docs/current/data/json/json_functions>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

define_sql_function! {
    /// `json_group_array(x)` — aggregate values into a JSON array.
    #[aggregate]
    fn json_group_array(x: Text) -> Text;
}
define_sql_function! {
    /// `json_group_object(key, value)` — aggregate key/value pairs into a JSON object.
    #[aggregate]
    fn json_group_object(key: Text, value: Text) -> Text;
}
