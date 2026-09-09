#![allow(missing_docs)]
//! `FromSql`/`ToSql` round-trip tests for every implemented Diesel type.
//!
//! Each test:
//! 1. Opens a fresh in-memory DuckDB connection.
//! 2. Creates a table with the exact DuckDB column type.
//! 3. Inserts a value via the Diesel DSL (exercising `ToSql`).
//! 4. Selects it back (exercising `FromSql`).
//! 5. Asserts the value is preserved.

use better_duck_core::types::value::DuckValue;
use better_duck_diesel::{backend::DuckDb, DuckDbConnection};
use diesel::{connection::SimpleConnection, deserialize::FromSql, prelude::*, sql_types::SqlType};

/// Assert that a concrete adapter rejects a value carried by the wrong DuckDB variant.
fn assert_from_sql_rejects<ST, T>(value: better_duck_core::types::value_ref::DuckValueRef<'_>)
where
    ST: SqlType,
    T: FromSql<ST, DuckDb>,
{
    assert!(T::from_sql(value).is_err());
}

// Helper

/// Open a fresh in-memory connection and create a table with `ddl`.
fn conn_with(ddl: &str) -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(ddl).expect("create table");
    c
}

// Standard Diesel type tables

diesel::table! {
    t_bool (id) {
        id  -> Integer,
        val -> Bool,
    }
}
diesel::table! {
    t_i16 (id) {
        id  -> Integer,
        val -> SmallInt,
    }
}
diesel::table! {
    t_i32 (id) {
        id  -> Integer,
        val -> Integer,
    }
}
diesel::table! {
    t_i64 (id) {
        id  -> Integer,
        val -> BigInt,
    }
}
diesel::table! {
    t_f32 (id) {
        id  -> Integer,
        val -> Float,
    }
}
diesel::table! {
    t_f64 (id) {
        id  -> Integer,
        val -> Double,
    }
}
diesel::table! {
    t_text (id) {
        id  -> Integer,
        val -> Text,
    }
}
diesel::table! {
    t_blob (id) {
        id  -> Integer,
        val -> Binary,
    }
}
diesel::table! {
    t_nullable (id) {
        id  -> Integer,
        val -> Nullable<Integer>,
    }
}

// DuckDB-specific type tables

diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_i8 (id) {
        id  -> Integer,
        val -> DuckTinyInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_u8 (id) {
        id  -> Integer,
        val -> DuckUTinyInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_u16 (id) {
        id  -> Integer,
        val -> DuckUSmallInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_u32 (id) {
        id  -> Integer,
        val -> DuckUInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_u64 (id) {
        id  -> Integer,
        val -> DuckUBigInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_i128 (id) {
        id  -> Integer,
        val -> DuckHugeInt,
    }
}
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_u128 (id) {
        id  -> Integer,
        val -> DuckUHugeInt,
    }
}
#[cfg(feature = "decimal")]
diesel::table! {
    t_decimal (id) {
        id  -> Integer,
        val -> Numeric,
    }
}

// Chrono type tables (feature-gated)

#[cfg(feature = "chrono")]
diesel::table! {
    t_date (id) {
        id  -> Integer,
        val -> Date,
    }
}
#[cfg(feature = "chrono")]
diesel::table! {
    t_time (id) {
        id  -> Integer,
        val -> Time,
    }
}
#[cfg(feature = "chrono")]
diesel::table! {
    t_timestamp (id) {
        id  -> Integer,
        val -> Timestamp,
    }
}
#[cfg(feature = "chrono")]
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_timestamptz (id) {
        id  -> Integer,
        val -> DuckTimestamptz,
    }
}
#[cfg(feature = "chrono")]
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_timetz (id) {
        id  -> Integer,
        val -> DuckTimeTz,
    }
}
#[cfg(feature = "chrono")]
diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_time_ns (id) {
        id  -> Integer,
        val -> DuckTimeNs,
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: bool
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_bool_true() {
    let mut conn = conn_with("CREATE TABLE t_bool (id INTEGER PRIMARY KEY, val BOOLEAN NOT NULL)");
    diesel::insert_into(t_bool::table)
        .values((t_bool::id.eq(1i32), t_bool::val.eq(true)))
        .execute(&mut conn)
        .unwrap();
    let v: bool = t_bool::table.select(t_bool::val).first(&mut conn).unwrap();
    assert!(v);
}

#[test]
fn rt_bool_false() {
    let mut conn = conn_with("CREATE TABLE t_bool (id INTEGER PRIMARY KEY, val BOOLEAN NOT NULL)");
    diesel::insert_into(t_bool::table)
        .values((t_bool::id.eq(1i32), t_bool::val.eq(false)))
        .execute(&mut conn)
        .unwrap();
    let v: bool = t_bool::table.select(t_bool::val).first(&mut conn).unwrap();
    assert!(!v);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: DuckDB-specific integer types
//
// DuckDB-specific integer adapters are exercised through explicit bind parameters,
// because Diesel does not provide `AsExpression` implementations for these marker types.
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_i8_min_max() {
    let mut conn = conn_with("CREATE TABLE t_i8 (id INTEGER PRIMARY KEY, val TINYINT NOT NULL)");
    conn.batch_execute(&format!("INSERT INTO t_i8 VALUES (1, {}), (2, {})", i8::MIN, i8::MAX))
        .unwrap();
    let rows: Vec<i8> = t_i8::table.order(t_i8::id).select(t_i8::val).load(&mut conn).unwrap();
    assert_eq!(rows, [i8::MIN, i8::MAX]);
}

#[test]
fn rt_u8_zero_max() {
    let mut conn = conn_with("CREATE TABLE t_u8 (id INTEGER PRIMARY KEY, val UTINYINT NOT NULL)");
    conn.batch_execute(&format!("INSERT INTO t_u8 VALUES (1, 0), (2, {})", u8::MAX)).unwrap();
    let rows: Vec<u8> = t_u8::table.order(t_u8::id).select(t_u8::val).load(&mut conn).unwrap();
    assert_eq!(rows, [0u8, u8::MAX]);
}

#[test]
fn rt_u16_max() {
    let mut conn = conn_with("CREATE TABLE t_u16 (id INTEGER PRIMARY KEY, val USMALLINT NOT NULL)");
    conn.batch_execute(&format!("INSERT INTO t_u16 VALUES (1, {})", u16::MAX)).unwrap();
    let v: u16 = t_u16::table.select(t_u16::val).first(&mut conn).unwrap();
    assert_eq!(v, u16::MAX);
}

#[test]
fn rt_u32_max() {
    let mut conn = conn_with("CREATE TABLE t_u32 (id INTEGER PRIMARY KEY, val UINTEGER NOT NULL)");
    conn.batch_execute(&format!("INSERT INTO t_u32 VALUES (1, {})", u32::MAX)).unwrap();
    let v: u32 = t_u32::table.select(t_u32::val).first(&mut conn).unwrap();
    assert_eq!(v, u32::MAX);
}

#[test]
fn rt_u64_max() {
    let mut conn = conn_with("CREATE TABLE t_u64 (id INTEGER PRIMARY KEY, val UBIGINT NOT NULL)");
    conn.batch_execute(&format!("INSERT INTO t_u64 VALUES (1, {})", u64::MAX)).unwrap();
    let v: u64 = t_u64::table.select(t_u64::val).first(&mut conn).unwrap();
    assert_eq!(v, u64::MAX);
}

#[test]
fn rt_i128_boundary() {
    // Use QueryableByName — Diesel's Queryable blanket doesn't extend to i128 but
    // FromSql<DuckHugeInt, DuckDb> for i128 is implemented and works via sql_query.
    use better_duck_diesel::sql_types::DuckHugeInt;
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckHugeInt)]
        val: i128,
    }
    let mut conn = conn_with("CREATE TABLE t_i128 (id INTEGER PRIMARY KEY, val HUGEINT NOT NULL)");
    let big: i128 = i128::MAX;
    conn.batch_execute(&format!("INSERT INTO t_i128 VALUES (1, {})", big)).unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_i128 LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, big);
}

#[test]
fn rt_u128_boundary() {
    use better_duck_diesel::sql_types::DuckUHugeInt;
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckUHugeInt)]
        val: u128,
    }
    let mut conn = conn_with("CREATE TABLE t_u128 (id INTEGER PRIMARY KEY, val UHUGEINT NOT NULL)");
    let big: u128 = u128::MAX;
    conn.batch_execute(&format!("INSERT INTO t_u128 VALUES (1, {})", big)).unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_u128 LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, big);
}

#[test]
fn rt_duck_specific_integers_bind_and_read() {
    use better_duck_diesel::sql_types::{
        DuckHugeInt, DuckTinyInt, DuckUBigInt, DuckUHugeInt, DuckUInt, DuckUSmallInt, DuckUTinyInt,
    };

    macro_rules! assert_bind_roundtrip {
        ($ddl:literal, $sql_ty:ty, $rust_ty:ty, $value:expr) => {{
            #[derive(diesel::QueryableByName, Debug)]
            struct Row {
                #[diesel(sql_type = $sql_ty)]
                val: $rust_ty,
            }

            let mut conn = conn_with($ddl);
            let expected: $rust_ty = $value;
            diesel::sql_query("INSERT INTO adapter_value VALUES ($1)")
                .bind::<$sql_ty, _>(expected)
                .execute(&mut conn)
                .unwrap();
            let row: Row =
                diesel::sql_query("SELECT val FROM adapter_value").get_result(&mut conn).unwrap();
            assert_eq!(row.val, expected);
        }};
    }

    assert_bind_roundtrip!("CREATE TABLE adapter_value (val TINYINT)", DuckTinyInt, i8, i8::MIN);
    assert_bind_roundtrip!("CREATE TABLE adapter_value (val UTINYINT)", DuckUTinyInt, u8, u8::MAX);
    assert_bind_roundtrip!(
        "CREATE TABLE adapter_value (val USMALLINT)",
        DuckUSmallInt,
        u16,
        u16::MAX
    );
    assert_bind_roundtrip!("CREATE TABLE adapter_value (val UINTEGER)", DuckUInt, u32, u32::MAX);
    assert_bind_roundtrip!("CREATE TABLE adapter_value (val UBIGINT)", DuckUBigInt, u64, u64::MAX);
    assert_bind_roundtrip!(
        "CREATE TABLE adapter_value (val HUGEINT)",
        DuckHugeInt,
        i128,
        i128::MIN
    );
    assert_bind_roundtrip!(
        "CREATE TABLE adapter_value (val UHUGEINT)",
        DuckUHugeInt,
        u128,
        u128::MAX
    );
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: standard integer types
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_i16_min_max() {
    let mut conn = conn_with("CREATE TABLE t_i16 (id INTEGER PRIMARY KEY, val SMALLINT NOT NULL)");
    for (id, v) in [(1i32, i16::MIN), (2i32, i16::MAX)] {
        diesel::insert_into(t_i16::table)
            .values((t_i16::id.eq(id), t_i16::val.eq(v)))
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<i16> = t_i16::table.order(t_i16::id).select(t_i16::val).load(&mut conn).unwrap();
    assert_eq!(rows, [i16::MIN, i16::MAX]);
}

#[test]
fn rt_i32_min_max() {
    let mut conn = conn_with("CREATE TABLE t_i32 (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
    for (id, v) in [(1i32, i32::MIN), (2i32, i32::MAX)] {
        diesel::insert_into(t_i32::table)
            .values((t_i32::id.eq(id), t_i32::val.eq(v)))
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<i32> = t_i32::table.order(t_i32::id).select(t_i32::val).load(&mut conn).unwrap();
    assert_eq!(rows, [i32::MIN, i32::MAX]);
}

#[test]
fn rt_i64_min_max() {
    let mut conn = conn_with("CREATE TABLE t_i64 (id INTEGER PRIMARY KEY, val BIGINT NOT NULL)");
    for (id, v) in [(1i32, i64::MIN), (2i32, i64::MAX)] {
        diesel::insert_into(t_i64::table)
            .values((t_i64::id.eq(id), t_i64::val.eq(v)))
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<i64> = t_i64::table.order(t_i64::id).select(t_i64::val).load(&mut conn).unwrap();
    assert_eq!(rows, [i64::MIN, i64::MAX]);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: float types
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_f32() {
    let mut conn = conn_with("CREATE TABLE t_f32 (id INTEGER PRIMARY KEY, val FLOAT NOT NULL)");
    let x = 1.25_f32; // not close to any f32 constant; avoids clippy::approx_constant
    diesel::insert_into(t_f32::table)
        .values((t_f32::id.eq(1i32), t_f32::val.eq(x)))
        .execute(&mut conn)
        .unwrap();
    let v: f32 = t_f32::table.select(t_f32::val).first(&mut conn).unwrap();
    assert!((v - x).abs() < 1e-5);
}

#[test]
fn rt_f64() {
    let mut conn = conn_with("CREATE TABLE t_f64 (id INTEGER PRIMARY KEY, val DOUBLE NOT NULL)");
    let x = std::f64::consts::PI;
    diesel::insert_into(t_f64::table)
        .values((t_f64::id.eq(1i32), t_f64::val.eq(x)))
        .execute(&mut conn)
        .unwrap();
    let v: f64 = t_f64::table.select(t_f64::val).first(&mut conn).unwrap();
    assert!((v - x).abs() < 1e-14);
}

#[cfg(feature = "decimal")]
#[test]
fn rt_decimal_signed_scale_and_zero() {
    use diesel::sql_types::Numeric;
    use rust_decimal::Decimal;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = Numeric)]
        val: Decimal,
    }

    let mut conn = conn_with("CREATE TABLE t_decimal (id INTEGER, val DECIMAL(18, 6))");
    let values = [Decimal::new(-12_345_678, 6), Decimal::ZERO, Decimal::new(98_765_432, 6)];
    for (id, value) in values.iter().enumerate() {
        diesel::sql_query("INSERT INTO t_decimal VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<Numeric, _>(*value)
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT val FROM t_decimal ORDER BY id").load(&mut conn).unwrap();
    assert_eq!(rows.into_iter().map(|row| row.val).collect::<Vec<_>>(), values);
}

#[test]
fn rt_text_from_distinct_dictionary_encoding() {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        val: String,
    }

    let mut conn = conn_with("CREATE TABLE source_text (val VARCHAR)");
    conn.batch_execute("INSERT INTO source_text VALUES ('alpha'), ('alpha'), ('beta')").unwrap();
    let mut rows: Vec<String> = diesel::sql_query("SELECT DISTINCT val FROM source_text")
        .load::<Row>(&mut conn)
        .unwrap()
        .into_iter()
        .map(|row| row.val)
        .collect();
    rows.sort();
    assert_eq!(rows, ["alpha", "beta"]);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: text
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_text_ascii() {
    let mut conn = conn_with("CREATE TABLE t_text (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)");
    let s = "hello, DuckDB!";
    diesel::insert_into(t_text::table)
        .values((t_text::id.eq(1i32), t_text::val.eq(s)))
        .execute(&mut conn)
        .unwrap();
    let v: String = t_text::table.select(t_text::val).first(&mut conn).unwrap();
    assert_eq!(v, s);
}

#[test]
fn rt_text_empty() {
    let mut conn = conn_with("CREATE TABLE t_text (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)");
    diesel::insert_into(t_text::table)
        .values((t_text::id.eq(1i32), t_text::val.eq("")))
        .execute(&mut conn)
        .unwrap();
    let v: String = t_text::table.select(t_text::val).first(&mut conn).unwrap();
    assert_eq!(v, "");
}

#[test]
fn rt_text_unicode() {
    let mut conn = conn_with("CREATE TABLE t_text (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)");
    let s = "Héllo Wörld 🦆";
    diesel::insert_into(t_text::table)
        .values((t_text::id.eq(1i32), t_text::val.eq(s)))
        .execute(&mut conn)
        .unwrap();
    let v: String = t_text::table.select(t_text::val).first(&mut conn).unwrap();
    assert_eq!(v, s);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: blob / binary
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_blob_bytes() {
    let mut conn = conn_with("CREATE TABLE t_blob (id INTEGER PRIMARY KEY, val BLOB NOT NULL)");
    let data: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    diesel::insert_into(t_blob::table)
        .values((t_blob::id.eq(1i32), t_blob::val.eq(data.as_slice())))
        .execute(&mut conn)
        .unwrap();
    let v: Vec<u8> = t_blob::table.select(t_blob::val).first(&mut conn).unwrap();
    assert_eq!(v, data);
}

#[test]
fn rt_blob_empty() {
    let mut conn = conn_with("CREATE TABLE t_blob (id INTEGER PRIMARY KEY, val BLOB NOT NULL)");
    let data: Vec<u8> = vec![];
    diesel::insert_into(t_blob::table)
        .values((t_blob::id.eq(1i32), t_blob::val.eq(data.as_slice())))
        .execute(&mut conn)
        .unwrap();
    let v: Vec<u8> = t_blob::table.select(t_blob::val).first(&mut conn).unwrap();
    assert_eq!(v, data);
}

#[test]
fn rt_blob_nullable_none() {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Binary>)]
        val: Option<Vec<u8>>,
    }

    let mut conn = conn_with("CREATE TABLE nullable_blob (val BLOB)");
    diesel::sql_query("INSERT INTO nullable_blob VALUES ($1)")
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Binary>, _>(None::<Vec<u8>>)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM nullable_blob").get_result(&mut conn).unwrap();
    assert_eq!(row.val, None);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: nullable
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_nullable_some() {
    let mut conn = conn_with("CREATE TABLE t_nullable (id INTEGER PRIMARY KEY, val INTEGER)");
    diesel::insert_into(t_nullable::table)
        .values((t_nullable::id.eq(1i32), t_nullable::val.eq(Some(42i32))))
        .execute(&mut conn)
        .unwrap();
    let v: Option<i32> = t_nullable::table.select(t_nullable::val).first(&mut conn).unwrap();
    assert_eq!(v, Some(42));
}

#[test]
fn rt_nullable_none() {
    let mut conn = conn_with("CREATE TABLE t_nullable (id INTEGER PRIMARY KEY, val INTEGER)");
    diesel::insert_into(t_nullable::table)
        .values((t_nullable::id.eq(1i32), t_nullable::val.eq(None::<i32>)))
        .execute(&mut conn)
        .unwrap();
    let v: Option<i32> = t_nullable::table.select(t_nullable::val).first(&mut conn).unwrap();
    assert_eq!(v, None);
}

#[test]
fn non_nullable_adapter_rejects_sql_null() {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        _val: i32,
    }

    let mut conn = conn_with("CREATE TABLE non_null_target (val INTEGER)");
    conn.batch_execute("INSERT INTO non_null_target VALUES (NULL)").unwrap();
    let error = diesel::sql_query("SELECT val FROM non_null_target")
        .get_result::<Row>(&mut conn)
        .unwrap_err();
    assert!(matches!(error, diesel::result::Error::DeserializationError(_)));
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: decimal
// ══════════════════════════════════════════════════════════════════════════

#[cfg(feature = "decimal")]
#[test]
fn rt_decimal_positive_negative_and_zero() {
    use diesel::sql_types::{Integer, Numeric};
    use rust_decimal::Decimal;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Numeric)]
        val: Decimal,
    }

    let mut conn =
        conn_with("CREATE TABLE t_decimal_values (id INTEGER PRIMARY KEY, val DECIMAL(18, 4))");
    let values =
        [(1, Decimal::new(123_456, 4)), (2, Decimal::new(-98_765, 4)), (3, Decimal::new(0, 4))];
    for (id, value) in values {
        diesel::sql_query("INSERT INTO t_decimal_values VALUES ($1, $2)")
            .bind::<Integer, _>(id)
            .bind::<Numeric, _>(value)
            .execute(&mut conn)
            .unwrap();
    }

    let actual: Vec<Decimal> = diesel::sql_query("SELECT val FROM t_decimal_values ORDER BY id")
        .load::<Row>(&mut conn)
        .unwrap()
        .into_iter()
        .map(|row| row.val)
        .collect();
    assert_eq!(actual, values.map(|(_, value)| value));
    assert!(actual.iter().all(|value| value.scale() == 4));
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: chrono date / time / timestamp / interval / gap types
// ══════════════════════════════════════════════════════════════════════════

#[cfg(feature = "chrono")]
#[test]
fn rt_date() {
    use chrono::NaiveDate;
    let mut conn = conn_with("CREATE TABLE t_date (id INTEGER PRIMARY KEY, val DATE NOT NULL)");
    let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    diesel::insert_into(t_date::table)
        .values((t_date::id.eq(1i32), t_date::val.eq(d)))
        .execute(&mut conn)
        .unwrap();
    let v: NaiveDate = t_date::table.select(t_date::val).first(&mut conn).unwrap();
    assert_eq!(v, d);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_time() {
    use chrono::NaiveTime;
    let mut conn = conn_with("CREATE TABLE t_time (id INTEGER PRIMARY KEY, val TIME NOT NULL)");
    let t = NaiveTime::from_hms_micro_opt(14, 30, 55, 123_456).unwrap();
    diesel::insert_into(t_time::table)
        .values((t_time::id.eq(1i32), t_time::val.eq(t)))
        .execute(&mut conn)
        .unwrap();
    let v: NaiveTime = t_time::table.select(t_time::val).first(&mut conn).unwrap();
    assert_eq!(v, t);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_timestamp() {
    use chrono::NaiveDateTime;
    let mut conn =
        conn_with("CREATE TABLE t_timestamp (id INTEGER PRIMARY KEY, val TIMESTAMP NOT NULL)");
    let ts = NaiveDateTime::parse_from_str("2024-01-15 10:20:30", "%Y-%m-%d %H:%M:%S").unwrap();
    diesel::insert_into(t_timestamp::table)
        .values((t_timestamp::id.eq(1i32), t_timestamp::val.eq(ts)))
        .execute(&mut conn)
        .unwrap();
    let v: NaiveDateTime = t_timestamp::table.select(t_timestamp::val).first(&mut conn).unwrap();
    assert_eq!(v, ts);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_timestamptz() {
    use chrono::{DateTime, TimeZone, Utc};
    let mut conn =
        conn_with("CREATE TABLE t_timestamptz (id INTEGER PRIMARY KEY, val TIMESTAMPTZ NOT NULL)");
    let expected: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap();
    diesel::sql_query("INSERT INTO t_timestamptz VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1)
        .bind::<better_duck_diesel::sql_types::DuckTimestamptz, _>(expected)
        .execute(&mut conn)
        .unwrap();
    let v: DateTime<Utc> =
        t_timestamptz::table.select(t_timestamptz::val).first(&mut conn).unwrap();
    assert_eq!(v, expected);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_interval_chrono() {
    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Interval)]
        val: chrono::Duration,
    }

    let mut conn = conn_with("CREATE TABLE t_interval_chrono (val INTERVAL NOT NULL)");
    let value = chrono::Duration::days(3)
        + chrono::Duration::seconds(7_321)
        + chrono::Duration::microseconds(456_789);
    diesel::sql_query("INSERT INTO t_interval_chrono VALUES ($1)")
        .bind::<diesel::sql_types::Interval, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query("SELECT val FROM t_interval_chrono LIMIT 1")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(row.val, value);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_time_tz() {
    use better_duck_core::types::date_chrono::TimeTz;
    use better_duck_diesel::sql_types::DuckTimeTz;
    use chrono::NaiveTime;

    #[derive(diesel::QueryableByName, Debug)]
    struct TzRow {
        #[diesel(sql_type = DuckTimeTz)]
        val: TimeTz,
    }

    let mut conn = conn_with("CREATE TABLE t_timetz (id INTEGER PRIMARY KEY, val TIMETZ NOT NULL)");
    let expected = TimeTz {
        time: NaiveTime::from_hms_micro_opt(14, 30, 0, 123_456).unwrap(),
        offset_secs: 3_600,
    };
    diesel::sql_query("INSERT INTO t_timetz VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckTimeTz, _>(expected)
        .execute(&mut conn)
        .unwrap();
    let row: TzRow =
        diesel::sql_query("SELECT val FROM t_timetz LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, expected);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_chrono_interval_signed_and_fractional() {
    use chrono::Duration;
    use diesel::sql_types::Interval;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = Interval)]
        val: Duration,
    }

    let mut conn = conn_with("CREATE TABLE t_interval (id INTEGER, val INTERVAL)");
    let values = [Duration::microseconds(-1_234_567), Duration::microseconds(9_876_543)];
    for (id, value) in values.iter().enumerate() {
        diesel::sql_query("INSERT INTO t_interval VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<Interval, _>(*value)
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT val FROM t_interval ORDER BY id").load(&mut conn).unwrap();
    assert_eq!(rows.into_iter().map(|row| row.val).collect::<Vec<_>>(), values);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: LIST (via sql_query, element type: Vec<DuckValue>)
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_list_int_elements() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckList;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckList)]
        val: Vec<DuckValue>,
    }

    let mut conn =
        conn_with("CREATE TABLE list_t (id INTEGER PRIMARY KEY, val INTEGER[] NOT NULL)");
    let items = vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)];
    diesel::sql_query("INSERT INTO list_t VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckList, _>(&items)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM list_t LIMIT 1").get_result(&mut conn).unwrap();
    let expected: Vec<i32> = row
        .val
        .iter()
        .map(|v| if let DuckValue::Int(n) = v { *n } else { panic!("not int") })
        .collect();
    assert_eq!(expected, [1, 2, 3]);
}

#[test]
fn rt_list_text_elements() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckList;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckList)]
        val: Vec<DuckValue>,
    }

    let mut conn =
        conn_with("CREATE TABLE list_t (id INTEGER PRIMARY KEY, val VARCHAR[] NOT NULL)");
    let items = vec![DuckValue::text("alpha"), DuckValue::text("beta"), DuckValue::text("gamma")];
    diesel::sql_query("INSERT INTO list_t VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckList, _>(&items)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM list_t LIMIT 1").get_result(&mut conn).unwrap();
    let strs: Vec<String> = row
        .val
        .iter()
        .map(|v| if let DuckValue::Text(s) = v { s.clone() } else { panic!("not text") })
        .collect();
    assert_eq!(strs, ["alpha", "beta", "gamma"]);
}

#[test]
fn rt_list_with_null_element() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckList;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckList)]
        val: Vec<DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE list_t (id INTEGER PRIMARY KEY, val INTEGER[])");
    // Insert via batch_execute to control the exact SQL with NULL element.
    conn.batch_execute("INSERT INTO list_t VALUES (1, [1, NULL, 3])").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM list_t LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val.len(), 3);
    assert!(matches!(row.val[0], DuckValue::Int(1)));
    assert!(matches!(row.val[1], DuckValue::Null));
    assert!(matches!(row.val[2], DuckValue::Int(3)));
}

#[test]
fn rt_list_nullable_none() {
    use better_duck_diesel::sql_types::DuckList;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckList>)]
        val: Option<Vec<DuckValue>>,
    }

    let mut conn = conn_with("CREATE TABLE nullable_list (val INTEGER[])");
    diesel::sql_query("INSERT INTO nullable_list VALUES ($1)")
        .bind::<diesel::sql_types::Nullable<DuckList>, _>(None::<Vec<DuckValue>>)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM nullable_list").get_result(&mut conn).unwrap();
    assert_eq!(row.val, None);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: ENUM (via sql_query)
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_enum_value() {
    use better_duck_diesel::sql_types::DuckEnum;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckEnum)]
        val: String,
    }

    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute(
        "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
         CREATE TABLE t_enum (id INTEGER PRIMARY KEY, val mood NOT NULL);",
    )
    .unwrap();
    conn.batch_execute("INSERT INTO t_enum VALUES (1, 'happy')").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_enum LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, "happy");
}

#[test]
fn rt_enum_nullable_null() {
    use better_duck_diesel::sql_types::DuckEnum;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckEnum>)]
        val: Option<String>,
    }

    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    conn.batch_execute(
        "CREATE TYPE status AS ENUM ('ok', 'err');
         CREATE TABLE t_enum (id INTEGER PRIMARY KEY, val status);",
    )
    .unwrap();
    conn.batch_execute("INSERT INTO t_enum VALUES (1, NULL)").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_enum LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, None);
}

#[test]
fn rt_enum_bind_all_labels_and_reject_unknown() {
    use better_duck_diesel::sql_types::DuckEnum;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckEnum)]
        val: String,
    }

    let mut conn = conn_with(
        "CREATE TYPE adapter_status AS ENUM ('ok', 'warning', 'err');
         CREATE TABLE enum_bind (id INTEGER, val adapter_status)",
    );
    for (id, label) in ["ok", "warning", "err"].into_iter().enumerate() {
        diesel::sql_query("INSERT INTO enum_bind VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<DuckEnum, _>(label)
            .execute(&mut conn)
            .unwrap();
    }
    let labels: Vec<String> = diesel::sql_query("SELECT val FROM enum_bind ORDER BY id")
        .load::<Row>(&mut conn)
        .unwrap()
        .into_iter()
        .map(|row| row.val)
        .collect();
    assert_eq!(labels, ["ok", "warning", "err"]);

    let error = diesel::sql_query("INSERT INTO enum_bind VALUES (9, $1)")
        .bind::<DuckEnum, _>("unknown")
        .execute(&mut conn)
        .unwrap_err();
    assert!(!error.to_string().is_empty());
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: non-chrono DATE (date_native)
// ══════════════════════════════════════════════════════════════════════════

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_date_native() {
    use better_duck_core::types::date_native::DuckDate;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Date)]
        val: DuckDate,
    }

    let mut conn = conn_with("CREATE TABLE t_date_native (val DATE NOT NULL)");
    let value = DuckDate { year: 2024, month: 6, day: 15 };
    diesel::sql_query("INSERT INTO t_date_native VALUES ($1)")
        .bind::<diesel::sql_types::Date, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_date_native LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_time_native() {
    use better_duck_core::types::date_native::DuckTime;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Time)]
        val: DuckTime,
    }

    let mut conn = conn_with("CREATE TABLE t_time_native (val TIME NOT NULL)");
    let value = DuckTime { hour: 14, min: 30, sec: 45, micros: 123_456 };
    diesel::sql_query("INSERT INTO t_time_native VALUES ($1)")
        .bind::<diesel::sql_types::Time, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_time_native LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_timestamp_native() {
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Timestamp)]
        val: std::time::SystemTime,
    }

    let mut conn = conn_with("CREATE TABLE t_timestamp_native (val TIMESTAMP NOT NULL)");
    let value = UNIX_EPOCH + Duration::from_secs(1_717_243_200) + Duration::from_micros(654_321);
    diesel::sql_query("INSERT INTO t_timestamp_native VALUES ($1)")
        .bind::<diesel::sql_types::Timestamp, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query("SELECT val FROM t_timestamp_native LIMIT 1")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_interval_native() {
    use std::time::Duration;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Interval)]
        val: Duration,
    }

    let mut conn = conn_with("CREATE TABLE t_interval_native (val INTERVAL NOT NULL)");
    let value = Duration::from_secs(3 * 86_400 + 7_321) + Duration::from_micros(456_789);
    diesel::sql_query("INSERT INTO t_interval_native VALUES ($1)")
        .bind::<diesel::sql_types::Interval, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query("SELECT val FROM t_interval_native LIMIT 1")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_timestamptz_native() {
    use better_duck_diesel::sql_types::DuckTimestamptz;
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckTimestamptz)]
        val: std::time::SystemTime,
    }

    let mut conn = conn_with("CREATE TABLE t_timestamptz_native (val TIMESTAMPTZ NOT NULL)");
    let value = UNIX_EPOCH + Duration::from_secs(1_717_243_200) + Duration::from_micros(654_321);
    diesel::sql_query("INSERT INTO t_timestamptz_native VALUES ($1)")
        .bind::<DuckTimestamptz, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query("SELECT val FROM t_timestamptz_native LIMIT 1")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_timetz_native() {
    use better_duck_core::types::date_native::DuckTimeTz;
    use better_duck_diesel::sql_types::DuckTimeTz as DuckTimeTzTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckTimeTzTy)]
        val: DuckTimeTz,
    }

    let mut conn = conn_with("CREATE TABLE t_timetz_native (val TIMETZ NOT NULL)");
    let value = DuckTimeTz { hour: 14, min: 30, sec: 45, micros: 123_456, offset_secs: 3_600 };
    diesel::sql_query("INSERT INTO t_timetz_native VALUES ($1)")
        .bind::<DuckTimeTzTy, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_timetz_native LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_time_ns_native() {
    use better_duck_core::types::date_native::DuckTimeNs;
    use better_duck_diesel::sql_types::DuckTimeNs as DuckTimeNsTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckTimeNsTy)]
        val: DuckTimeNs,
    }

    let mut conn = conn_with("CREATE TABLE t_time_ns_native (val TIME_NS NOT NULL)");
    let value = DuckTimeNs { hour: 14, min: 30, sec: 45, nanos: 123_456_789 };
    diesel::sql_query("INSERT INTO t_time_ns_native VALUES ($1)")
        .bind::<DuckTimeNsTy, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query("SELECT val FROM t_time_ns_native LIMIT 1")
        .get_result(&mut conn)
        .unwrap();
    assert_eq!(row.val, value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_native_date_time_adapters_bind_and_read() {
    use better_duck_core::types::date_native::{DuckDate, DuckTime, DuckTimeNs, DuckTimeTz};
    use better_duck_diesel::sql_types::{DuckTimeNs as DuckTimeNsTy, DuckTimeTz as DuckTimeTzTy};
    use diesel::sql_types::{Date, Time};

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = Date)]
        date_value: DuckDate,
        #[diesel(sql_type = Time)]
        time_value: DuckTime,
        #[diesel(sql_type = DuckTimeTzTy)]
        time_tz_value: DuckTimeTz,
        #[diesel(sql_type = DuckTimeNsTy)]
        time_ns_value: DuckTimeNs,
    }

    let mut conn = conn_with(
        "CREATE TABLE native_components (
            date_value DATE, time_value TIME, time_tz_value TIME WITH TIME ZONE,
            time_ns_value TIME_NS
        )",
    );
    let date_value = DuckDate { year: 1999, month: 12, day: 31 };
    let time_value = DuckTime { hour: 23, min: 59, sec: 58, micros: 654_321 };
    let time_tz_value =
        DuckTimeTz { hour: 1, min: 2, sec: 3, micros: 456_789, offset_secs: 5 * 3600 + 30 * 60 };
    let time_ns_value = DuckTimeNs { hour: 4, min: 5, sec: 6, nanos: 123_456_789 };
    diesel::sql_query("INSERT INTO native_components VALUES ($1, $2, $3, $4)")
        .bind::<Date, _>(date_value)
        .bind::<Time, _>(time_value)
        .bind::<DuckTimeTzTy, _>(time_tz_value)
        .bind::<DuckTimeNsTy, _>(time_ns_value)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query(
        "SELECT date_value, time_value, time_tz_value, time_ns_value FROM native_components",
    )
    .get_result(&mut conn)
    .unwrap();
    assert_eq!(row.date_value, date_value);
    assert_eq!(row.time_value, time_value);
    assert_eq!(row.time_tz_value, time_tz_value);
    assert_eq!(row.time_ns_value, time_ns_value);
}

#[cfg(not(feature = "chrono"))]
#[test]
fn rt_native_timestamp_interval_and_timestamptz_bind_and_read() {
    use better_duck_diesel::sql_types::DuckTimestamptz;
    use diesel::sql_types::{Interval, Timestamp};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = Timestamp)]
        timestamp_value: std::time::SystemTime,
        #[diesel(sql_type = DuckTimestamptz)]
        timestamptz_value: std::time::SystemTime,
        #[diesel(sql_type = Interval)]
        interval_value: Duration,
    }

    let mut conn = conn_with(
        "CREATE TABLE native_temporal (
            timestamp_value TIMESTAMP, timestamptz_value TIMESTAMPTZ, interval_value INTERVAL
        )",
    );
    let timestamp = UNIX_EPOCH + Duration::from_micros(1_234_567_890);
    let timestamptz = UNIX_EPOCH + Duration::from_micros(9_876_543_210);
    let interval = Duration::from_micros(3_600_123_456);
    diesel::sql_query("INSERT INTO native_temporal VALUES ($1, $2, $3)")
        .bind::<Timestamp, _>(timestamp)
        .bind::<DuckTimestamptz, _>(timestamptz)
        .bind::<Interval, _>(interval)
        .execute(&mut conn)
        .unwrap();
    let row: Row = diesel::sql_query(
        "SELECT timestamp_value, timestamptz_value, interval_value FROM native_temporal",
    )
    .get_result(&mut conn)
    .unwrap();
    assert_eq!(row.timestamp_value, timestamp);
    assert_eq!(row.timestamptz_value, timestamptz);
    assert_eq!(row.interval_value, interval);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: STRUCT / MAP / UNION / ARRAY (via sql_query)
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_struct_fields() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckStruct;
    use std::collections::HashMap;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckStruct)]
        val: HashMap<String, DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE t_struct (val STRUCT(x INTEGER, y VARCHAR))");
    conn.batch_execute("INSERT INTO t_struct VALUES (ROW(1, 'a'))").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_struct LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val.get("x"), Some(&DuckValue::Int(1)));
    assert_eq!(row.val.get("y"), Some(&DuckValue::text("a")));
}

#[test]
fn rt_struct_bind_and_read() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckStruct;
    use std::collections::HashMap;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckStruct)]
        val: HashMap<String, DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE t_struct2 (id INTEGER, val STRUCT(x INTEGER))");
    let fields: HashMap<String, DuckValue> = HashMap::from([("x".to_string(), DuckValue::Int(42))]);
    diesel::sql_query("INSERT INTO t_struct2 VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckStruct, _>(&fields)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_struct2 LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val.get("x"), Some(&DuckValue::Int(42)));
}

#[test]
fn rt_map_entries() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckMap;
    use std::collections::HashMap;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckMap)]
        val: HashMap<DuckValue, DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE t_map (val MAP(INTEGER, VARCHAR))");
    conn.batch_execute("INSERT INTO t_map VALUES (MAP {1: 'one'})").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_map LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val.get(&DuckValue::Int(1)), Some(&DuckValue::text("one")));
}

#[test]
fn rt_union_active_member() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckUnion;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckUnion)]
        val: Box<DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE t_union (val UNION(n INTEGER, s VARCHAR))");
    conn.batch_execute("INSERT INTO t_union VALUES (union_value(n := 7))").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_union LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(*row.val, DuckValue::Int(7));
}

#[test]
fn rt_array_elements() {
    use better_duck_core::types::value::DuckValue;
    use better_duck_diesel::sql_types::DuckArray;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckArray)]
        val: Vec<DuckValue>,
    }

    let mut conn = conn_with("CREATE TABLE t_array (val INTEGER[3])");
    conn.batch_execute("INSERT INTO t_array VALUES ([1, 2, 3])").unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_array LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)]);
}

#[test]
fn complex_types_nullable_none() {
    use better_duck_diesel::sql_types::{DuckArray, DuckMap, DuckStruct, DuckUnion};
    use std::collections::HashMap;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckStruct>)]
        struct_value: Option<HashMap<String, DuckValue>>,
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckMap>)]
        map_value: Option<HashMap<DuckValue, DuckValue>>,
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckArray>)]
        array_value: Option<Vec<DuckValue>>,
        #[diesel(sql_type = diesel::sql_types::Nullable<DuckUnion>)]
        union_value: Option<Box<DuckValue>>,
    }

    let mut conn = conn_with(
        "CREATE TABLE nullable_complex (
            struct_value STRUCT(name VARCHAR), map_value MAP(INTEGER, VARCHAR),
            array_value INTEGER[2], union_value UNION(number INTEGER, text VARCHAR)
        )",
    );
    conn.batch_execute("INSERT INTO nullable_complex VALUES (NULL, NULL, NULL, NULL)").unwrap();
    let row: Row = diesel::sql_query(
        "SELECT struct_value, map_value, array_value, union_value FROM nullable_complex",
    )
    .get_result(&mut conn)
    .unwrap();
    assert_eq!(row.struct_value, None);
    assert_eq!(row.map_value, None);
    assert_eq!(row.array_value, None);
    assert_eq!(row.union_value, None);
}

// ══════════════════════════════════════════════════════════════════════════
// Tests: UUID / BIT / BIGNUM (via sql_query)
// ══════════════════════════════════════════════════════════════════════════

#[test]
fn rt_uuid_bind_and_read() {
    use better_duck_core::types::uuid::DuckUuid;
    use better_duck_diesel::sql_types::DuckUuid as DuckUuidTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckUuidTy)]
        val: DuckUuid,
    }

    let mut conn = conn_with("CREATE TABLE t_uuid (id INTEGER, val UUID)");
    let value = DuckUuid(0x1234_5678_9abc_def0_0fed_cba9_8765_4321);
    diesel::sql_query("INSERT INTO t_uuid VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckUuidTy, _>(value)
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_uuid LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[test]
fn rt_bit_bind_and_read() {
    use better_duck_core::types::bit::DuckBit;
    use better_duck_diesel::sql_types::DuckBit as DuckBitTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckBitTy)]
        val: DuckBit,
    }

    let mut conn = conn_with("CREATE TABLE t_bit (id INTEGER, val BIT)");
    let value = DuckBit(vec![0u8, 0b1010_0000]);
    diesel::sql_query("INSERT INTO t_bit VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckBitTy, _>(value.clone())
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_bit LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[test]
fn rt_bignum_bind_and_read() {
    use better_duck_core::types::bignum::DuckBignum;
    use better_duck_diesel::sql_types::DuckBignum as DuckBignumTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckBignumTy)]
        val: DuckBignum,
    }

    let mut conn = conn_with("CREATE TABLE t_bignum (id INTEGER, val BIGNUM)");
    let value = DuckBignum::new(vec![0xFF, 0x01], false);
    diesel::sql_query("INSERT INTO t_bignum VALUES ($1, $2)")
        .bind::<diesel::sql_types::Integer, _>(1i32)
        .bind::<DuckBignumTy, _>(value.clone())
        .execute(&mut conn)
        .unwrap();
    let row: Row =
        diesel::sql_query("SELECT val FROM t_bignum LIMIT 1").get_result(&mut conn).unwrap();
    assert_eq!(row.val, value);
}

#[test]
fn rt_uuid_nil_and_max_bind_and_read() {
    use better_duck_core::types::uuid::DuckUuid;
    use better_duck_diesel::sql_types::DuckUuid as DuckUuidTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckUuidTy)]
        val: DuckUuid,
    }

    let mut conn = conn_with("CREATE TABLE uuid_edges (id INTEGER, val UUID)");
    let values = [DuckUuid(0), DuckUuid(u128::MAX)];
    for (id, value) in values.iter().enumerate() {
        diesel::sql_query("INSERT INTO uuid_edges VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<DuckUuidTy, _>(*value)
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT val FROM uuid_edges ORDER BY id").load(&mut conn).unwrap();
    assert_eq!(rows.into_iter().map(|row| row.val).collect::<Vec<_>>(), values);
}

#[test]
fn rt_bit_edge_patterns_bind_and_read() {
    use better_duck_core::types::bit::DuckBit;
    use better_duck_diesel::sql_types::DuckBit as DuckBitTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckBitTy)]
        val: DuckBit,
    }

    let mut conn = conn_with("CREATE TABLE bit_edges (id INTEGER, val BIT)");
    let values = [
        DuckBit::new(vec![0]),
        DuckBit::new(vec![7, 0b1000_0000]),
        DuckBit::new(vec![3, 0b1110_0000]),
        DuckBit::new(vec![0, 0xff]),
    ];
    for (id, value) in values.iter().enumerate() {
        diesel::sql_query("INSERT INTO bit_edges VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<DuckBitTy, _>(value.clone())
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT val FROM bit_edges ORDER BY id").load(&mut conn).unwrap();
    assert_eq!(rows.into_iter().map(|row| row.val).collect::<Vec<_>>(), values);
}

#[test]
fn rt_bignum_sign_zero_and_large_bind_and_read() {
    use better_duck_core::types::bignum::DuckBignum;
    use better_duck_diesel::sql_types::DuckBignum as DuckBignumTy;

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = DuckBignumTy)]
        val: DuckBignum,
    }

    let mut conn = conn_with("CREATE TABLE bignum_edges (id INTEGER, val BIGNUM)");
    let values = [
        DuckBignum::new(vec![0], false),
        DuckBignum::new(vec![42], true),
        DuckBignum::new(vec![0xff; 32], false),
    ];
    for (id, value) in values.iter().enumerate() {
        diesel::sql_query("INSERT INTO bignum_edges VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(id as i32)
            .bind::<DuckBignumTy, _>(value.clone())
            .execute(&mut conn)
            .unwrap();
    }
    let rows: Vec<Row> =
        diesel::sql_query("SELECT val FROM bignum_edges ORDER BY id").load(&mut conn).unwrap();
    assert_eq!(rows.into_iter().map(|row| row.val).collect::<Vec<_>>(), values);
}

#[test]
fn from_sql_rejects_wrong_variants() {
    use better_duck_core::types::value_ref::DuckValueRef;
    use better_duck_diesel::sql_types::{
        DuckArray, DuckBignum, DuckBit, DuckEnum, DuckHugeInt, DuckList, DuckMap, DuckStruct,
        DuckUnion, DuckUuid,
    };
    use diesel::sql_types::{Binary, Bool, Integer, Text};
    use std::collections::HashMap;

    assert_from_sql_rejects::<Bool, bool>(DuckValueRef::Int(1));
    assert_from_sql_rejects::<Integer, i32>(DuckValueRef::Text("1".into()));
    assert_from_sql_rejects::<DuckHugeInt, i128>(DuckValueRef::Int(1));
    assert_from_sql_rejects::<Text, String>(DuckValueRef::Int(1));
    assert_from_sql_rejects::<DuckEnum, String>(DuckValueRef::Text("ok".into()));
    assert_from_sql_rejects::<Binary, Vec<u8>>(DuckValueRef::Text("blob".into()));
    assert_from_sql_rejects::<DuckList, Vec<DuckValue>>(DuckValueRef::Array(Box::new([])));
    assert_from_sql_rejects::<DuckArray, Vec<DuckValue>>(DuckValueRef::List(Vec::new()));
    assert_from_sql_rejects::<DuckStruct, HashMap<String, DuckValue>>(DuckValueRef::Map(
        HashMap::new(),
    ));
    assert_from_sql_rejects::<DuckMap, HashMap<DuckValue, DuckValue>>(DuckValueRef::Struct(
        HashMap::new(),
    ));
    assert_from_sql_rejects::<DuckUnion, Box<DuckValue>>(DuckValueRef::Int(1));
    assert_from_sql_rejects::<DuckUuid, better_duck_core::types::uuid::DuckUuid>(
        DuckValueRef::Int(1),
    );
    assert_from_sql_rejects::<DuckBit, better_duck_core::types::bit::DuckBit>(DuckValueRef::Int(1));
    assert_from_sql_rejects::<DuckBignum, better_duck_core::types::bignum::DuckBignum>(
        DuckValueRef::Int(1),
    );
}
