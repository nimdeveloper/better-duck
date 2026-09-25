//! DuckDB date/time construction and extended-part functions that Diesel does not
//! ship by default (the basic `year`/`month`/… extracts live in `datetime.rs`).
//!
//! Docs: <https://duckdb.org/docs/current/sql/functions/datepart>

use diesel::expression::functions::define_sql_function;
use diesel::sql_types::{BigInt, Date, Double, Integer, Text, Timestamp};

define_sql_function! {
    /// `make_date(year, month, day)` — build a `DATE` from its parts.
    fn make_date(year: Integer, month: Integer, day: Integer) -> Date;
}
define_sql_function! {
    /// `make_time(hour, minute, seconds)` — build a `TIME` from its parts.
    fn make_time(hour: Integer, minute: Integer, seconds: Double) -> diesel::sql_types::Time;
}
define_sql_function! {
    /// `make_timestamp(micros)` — build a `TIMESTAMP` from microseconds since the epoch.
    fn make_timestamp(micros: BigInt) -> Timestamp;
}
define_sql_function! {
    /// `dayname(ts)` — English weekday name (`Monday`, …).
    fn dayname(ts: Timestamp) -> Text;
}
define_sql_function! {
    /// `monthname(ts)` — English month name (`January`, …).
    fn monthname(ts: Timestamp) -> Text;
}
define_sql_function! {
    /// `last_day(d)` — the last day of `d`'s month.
    fn last_day(d: Date) -> Date;
}
define_sql_function! {
    /// `dayofweek(ts)` — day of week, Sunday = 0.
    fn dayofweek(ts: Timestamp) -> BigInt;
}
define_sql_function! {
    /// `isodow(ts)` — ISO day of week, Monday = 1.
    fn isodow(ts: Timestamp) -> BigInt;
}
define_sql_function! {
    /// `dayofyear(ts)` — 1-based day within the year.
    fn dayofyear(ts: Timestamp) -> BigInt;
}
define_sql_function! {
    /// `weekofyear(ts)` — ISO week number.
    fn weekofyear(ts: Timestamp) -> BigInt;
}
define_sql_function! {
    /// `quarter(ts)` — calendar quarter (1–4).
    fn quarter(ts: Timestamp) -> BigInt;
}
define_sql_function! {
    /// `datediff(part, start, end)` — number of `part` boundaries crossed from `start` to `end`.
    fn datediff(part: Text, start: Timestamp, finish: Timestamp) -> BigInt;
}
