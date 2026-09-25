//! DuckDB utility / metadata scalar functions that Diesel does not ship.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/utility>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::SingleValue;

define_sql_function! {
    /// `version()` — the DuckDB version string.
    fn version() -> diesel::sql_types::Text;
}
define_sql_function! {
    /// `typeof(x)` — the DuckDB type name of `x` as text.
    #[sql_name = "typeof"]
    fn type_of<T: SingleValue>(x: T) -> diesel::sql_types::Text;
}
