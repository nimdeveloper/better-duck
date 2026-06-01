//! DuckDB-specific SQL types not available in Diesel's standard type set.
//!
//! Diesel's built-in SQL types cover the common SQL standard. DuckDB adds several
//! unsigned integer widths, `HUGEINT`/`UHUGEINT`, and `TIMESTAMPTZ`. These types
//! are defined here so that `HasSqlType` and `FromSql`/`ToSql` impls can reference
//! them without colliding with Diesel's own types.

use diesel_derives::{QueryId, SqlType};

macro_rules! duck_sql_type {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, QueryId, SqlType)]
        pub struct $name;
    };
}

duck_sql_type!(DuckTinyInt, "DuckDB `TINYINT` (`INT1`) — maps to `i8`.");
duck_sql_type!(DuckUTinyInt, "DuckDB `UTINYINT` — maps to `u8`.");
duck_sql_type!(DuckUSmallInt, "DuckDB `USMALLINT` — maps to `u16`.");
duck_sql_type!(DuckUInt, "DuckDB `UINTEGER` — maps to `u32`.");
duck_sql_type!(DuckUBigInt, "DuckDB `UBIGINT` — maps to `u64`.");
duck_sql_type!(DuckHugeInt, "DuckDB `HUGEINT` — maps to `i128`.");
duck_sql_type!(DuckUHugeInt, "DuckDB `UHUGEINT` — maps to `u128`.");
duck_sql_type!(
    DuckTimestamptz,
    "DuckDB `TIMESTAMPTZ` — a timestamp with time-zone annotation stored in UTC."
);
duck_sql_type!(
    DuckInterval,
    "DuckDB `INTERVAL` — maps to `chrono::Duration` (chrono feature) or `std::time::Duration`."
);
duck_sql_type!(DuckList, "DuckDB `LIST` — a variable-length array of a uniform element type.");
