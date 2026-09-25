//! DuckDB `LIST` scalar functions, typed over the crate's [`DuckList`] SQL type.
//!
//! These accept any expression whose SQL type is `DuckList` (e.g. a `column`
//! declared `sql_type = DuckList`), so the element type stays opaque.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/list>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::Text;

use crate::types::duckdb_types::DuckList;

define_sql_function! {
    /// `array_length(list)` — number of elements in `list`.
    fn array_length(list: DuckList) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `len(list)` — number of elements in `list` (alias of `array_length`).
    fn len(list: DuckList) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `list_reverse(list)` — the list with element order reversed.
    fn list_reverse(list: DuckList) -> DuckList;
}
define_sql_function! {
    /// `list_sort(list)` — the list sorted ascending.
    fn list_sort(list: DuckList) -> DuckList;
}
define_sql_function! {
    /// `list_distinct(list)` — the list with duplicate (and `NULL`) elements removed.
    fn list_distinct(list: DuckList) -> DuckList;
}
define_sql_function! {
    /// `list_concat(a, b)` — the concatenation of two lists.
    fn list_concat(a: DuckList, b: DuckList) -> DuckList;
}
define_sql_function! {
    /// `array_to_string(list, sep)` — join the list's elements into text with `sep`.
    fn array_to_string(list: DuckList, sep: Text) -> Text;
}
