//! DuckDB `MAP` scalar functions, typed over the crate's [`DuckMap`] SQL type.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/map>

use diesel::expression::functions::define_sql_function;

use crate::types::duckdb_types::{DuckList, DuckMap, DuckUBigInt};

define_sql_function! {
    /// `cardinality(map)` — number of entries in `map` (returns `UBIGINT`).
    fn cardinality(map: DuckMap) -> DuckUBigInt;
}
define_sql_function! {
    /// `map_keys(map)` — the keys of `map` as a `LIST`.
    fn map_keys(map: DuckMap) -> DuckList;
}
define_sql_function! {
    /// `map_values(map)` — the values of `map` as a `LIST`.
    fn map_values(map: DuckMap) -> DuckList;
}
