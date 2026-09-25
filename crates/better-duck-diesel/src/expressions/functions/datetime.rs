//! DuckDB date/time scalar functions (over `TIMESTAMP`).
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/timestamp>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{Text, Timestamp};

define_sql_function! {
    /// `year(ts)` — the year component.
    fn year(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `month(ts)` — the month component (1–12).
    fn month(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `day(ts)` — the day-of-month component.
    fn day(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `hour(ts)` — the hour component.
    fn hour(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `minute(ts)` — the minute component.
    fn minute(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `second(ts)` — the second component.
    fn second(ts: Timestamp) -> diesel::sql_types::BigInt;
}
define_sql_function! {
    /// `date_trunc(part, ts)` — truncate `ts` to `part` (e.g. `'month'`).
    fn date_trunc(part: Text, ts: Timestamp) -> Timestamp;
}
define_sql_function! {
    /// `strftime(ts, format)` — format `ts` with a strftime pattern.
    fn strftime(ts: Timestamp, format: Text) -> Text;
}
define_sql_function! {
    /// `epoch(ts)` — seconds since the Unix epoch.
    fn epoch(ts: Timestamp) -> diesel::sql_types::Double;
}
