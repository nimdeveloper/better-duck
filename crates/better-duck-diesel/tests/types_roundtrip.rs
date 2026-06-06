#![allow(missing_docs)]
//! `FromSql`/`ToSql` round-trip tests for every implemented Diesel type.
//!
//! Each test:
//! 1. Opens a fresh in-memory DuckDB connection.
//! 2. Creates a table with the exact DuckDB column type.
//! 3. Inserts a value via the Diesel DSL (exercising `ToSql`).
//! 4. Selects it back (exercising `FromSql`).
//! 5. Asserts the value is preserved.

use better_duck_diesel::DuckDbConnection;
use diesel::{connection::SimpleConnection, prelude::*};

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
// INSERT uses raw SQL (AsExpression<DuckXxx> is not provided for primitive
// types). SELECT uses the Diesel DSL to exercise FromSql.
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
    // INSERT via raw SQL; SELECT via Diesel DSL (exercises FromSql).
    conn.batch_execute(
        "INSERT INTO t_timestamptz VALUES (1, '2024-06-01 12:00:00+00'::TIMESTAMPTZ)",
    )
    .unwrap();
    let v: DateTime<Utc> =
        t_timestamptz::table.select(t_timestamptz::val).first(&mut conn).unwrap();
    let expected: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap();
    assert_eq!(v.timestamp(), expected.timestamp());
}

#[cfg(feature = "chrono")]
#[test]
fn rt_time_tz() {
    // Use QueryableByName — Diesel's Queryable blanket doesn't extend to the custom TimeTz
    // struct, but FromSql<DuckTimeTz, DuckDb> for TimeTz is implemented and works via sql_query.
    use better_duck_core::types::date_chrono::TimeTz;
    use better_duck_diesel::sql_types::DuckTimeTz;
    use chrono::NaiveTime;

    #[derive(diesel::QueryableByName, Debug)]
    struct TzRow {
        #[diesel(sql_type = DuckTimeTz)]
        val: TimeTz,
    }

    let mut conn = conn_with("CREATE TABLE t_timetz (id INTEGER PRIMARY KEY, val TIMETZ NOT NULL)");
    conn.batch_execute("INSERT INTO t_timetz VALUES (1, '14:30:00+01'::TIMETZ)").unwrap();
    let row: TzRow =
        diesel::sql_query("SELECT val FROM t_timetz LIMIT 1").get_result(&mut conn).unwrap();
    let expected_time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    assert_eq!(row.val.time, expected_time);
    assert_eq!(row.val.offset_secs, 3_600);
}

#[cfg(feature = "chrono")]
#[test]
fn rt_time_ns() {
    use chrono::{NaiveTime, Timelike};
    let mut conn =
        conn_with("CREATE TABLE t_time_ns (id INTEGER PRIMARY KEY, val TIME_NS NOT NULL)");
    // INSERT via raw SQL; SELECT via Diesel DSL (exercises FromSql).
    conn.batch_execute("INSERT INTO t_time_ns VALUES (1, '14:30:00'::TIME_NS)").unwrap();
    let v: NaiveTime = t_time_ns::table.select(t_time_ns::val).first(&mut conn).unwrap();
    let expected = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    assert_eq!(v.hour(), expected.hour());
    assert_eq!(v.minute(), expected.minute());
    assert_eq!(v.second(), expected.second());
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
