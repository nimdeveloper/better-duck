#![allow(missing_docs)]

//! Coverage: read one row containing a column of (nearly) every DuckDB type via
//! SQL literals/casts, forcing `DuckValue::from_duckdb_vec`'s per-type read arms
//! (and the `DuckValueRef` → `DuckValue` conversions) to run for each type-id.

use better_duck_core::connection::Connection;

#[test]
fn reads_a_column_of_every_scalar_and_composite_type() {
    let mut conn = Connection::open_in_memory().unwrap();
    let sql = "SELECT
        CAST(1 AS TINYINT)   AS c_i8,
        CAST(1 AS SMALLINT)  AS c_i16,
        CAST(1 AS INTEGER)   AS c_i32,
        CAST(1 AS BIGINT)    AS c_i64,
        CAST(1 AS HUGEINT)   AS c_i128,
        CAST(1 AS UTINYINT)  AS c_u8,
        CAST(1 AS USMALLINT) AS c_u16,
        CAST(1 AS UINTEGER)  AS c_u32,
        CAST(1 AS UBIGINT)   AS c_u64,
        CAST(1 AS UHUGEINT)  AS c_u128,
        CAST(1.5 AS FLOAT)   AS c_f32,
        CAST(1.5 AS DOUBLE)  AS c_f64,
        CAST(1.23 AS DECIMAL(6,2)) AS c_dec,
        TRUE                 AS c_bool,
        'duck'               AS c_text,
        CAST('abc' AS BLOB)  AS c_blob,
        DATE '2021-06-15'    AS c_date,
        TIME '01:02:03'      AS c_time,
        TIMESTAMP '2021-06-15 01:02:03' AS c_ts,
        CAST(TIMESTAMP '2021-06-15 01:02:03' AS TIMESTAMP_S)  AS c_ts_s,
        CAST(TIMESTAMP '2021-06-15 01:02:03' AS TIMESTAMP_MS) AS c_ts_ms,
        CAST(TIMESTAMP '2021-06-15 01:02:03' AS TIMESTAMP_NS) AS c_ts_ns,
        TIMESTAMPTZ '2021-06-15 01:02:03+00' AS c_tstz,
        CAST('01:02:03+01' AS TIMETZ) AS c_timetz,
        INTERVAL '1 day'     AS c_interval,
        [1, 2, 3]            AS c_list,
        CAST([1, 2, 3] AS INTEGER[3]) AS c_array,
        {'x': 1, 'y': 2}     AS c_struct,
        MAP {1: 10, 2: 20}   AS c_map,
        CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS c_uuid,
        CAST('101' AS BIT)   AS c_bit,
        CAST(12345678901234567890 AS BIGNUM) AS c_bignum";

    let cols = [
        "c_i8",
        "c_i16",
        "c_i32",
        "c_i64",
        "c_i128",
        "c_u8",
        "c_u16",
        "c_u32",
        "c_u64",
        "c_u128",
        "c_f32",
        "c_f64",
        "c_dec",
        "c_bool",
        "c_text",
        "c_blob",
        "c_date",
        "c_time",
        "c_ts",
        "c_ts_s",
        "c_ts_ms",
        "c_ts_ns",
        "c_tstz",
        "c_timetz",
        "c_interval",
        "c_list",
        "c_array",
        "c_struct",
        "c_map",
        "c_uuid",
        "c_bit",
        "c_bignum",
    ];

    let mut result = conn.execute(sql).unwrap();
    let row = result.next().unwrap().unwrap();
    for col in cols {
        assert!(row.get(col).is_some(), "column {col} did not read");
    }
}
