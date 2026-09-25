//! DuckDB blob / encoding scalar functions.
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/blob>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Binary, Text};

define_sql_function! {
    /// `encode(x)` — the UTF-8 bytes of a string, as a `BLOB`.
    fn encode(x: Text) -> Binary;
}
define_sql_function! {
    /// `decode(b)` — a `BLOB` of UTF-8 bytes back to a string.
    fn decode(b: Binary) -> Text;
}
define_sql_function! {
    /// `base64(b)` — the base64 encoding of a `BLOB`.
    fn base64(b: Binary) -> Text;
}
define_sql_function! {
    /// `from_base64(s)` — decode a base64 string to a `BLOB`.
    fn from_base64(s: Text) -> Binary;
}
define_sql_function! {
    /// `hex(b)` — the hex encoding of a `BLOB`.
    fn hex(b: Binary) -> Text;
}
define_sql_function! {
    /// `unhex(s)` — decode a hex string to a `BLOB`.
    fn unhex(s: Text) -> Binary;
}
