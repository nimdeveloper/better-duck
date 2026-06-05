#![allow(missing_docs)]
use better_duck_core::{
    connection::Connection,
    types::{value::DuckValue, Blob},
    AppendAble,
};

fn open() -> Connection {
    Connection::open_in_memory().unwrap()
}

/// Bind `dv` as `$1` in `SELECT $1 AS v`, return the decoded result.
fn rt(mut dv: DuckValue) -> DuckValue {
    let mut conn = open();
    let mut rows = conn
        .execute_with("SELECT $1 AS v", &mut [&mut dv as &mut dyn AppendAble])
        .unwrap();
    rows.next().unwrap().unwrap().get("v").unwrap().clone()
}

// Boolean

#[test]
fn rt_boolean_true() {
    assert_eq!(rt(DuckValue::Boolean(true)), DuckValue::Boolean(true));
}

#[test]
fn rt_boolean_false() {
    assert_eq!(rt(DuckValue::Boolean(false)), DuckValue::Boolean(false));
}

// Signed integers

#[test]
fn rt_tinyint_min() {
    assert_eq!(rt(DuckValue::TinyInt(i8::MIN)), DuckValue::TinyInt(i8::MIN));
}

#[test]
fn rt_tinyint_max() {
    assert_eq!(rt(DuckValue::TinyInt(i8::MAX)), DuckValue::TinyInt(i8::MAX));
}

#[test]
fn rt_smallint_min() {
    assert_eq!(rt(DuckValue::SmallInt(i16::MIN)), DuckValue::SmallInt(i16::MIN));
}

#[test]
fn rt_smallint_max() {
    assert_eq!(rt(DuckValue::SmallInt(i16::MAX)), DuckValue::SmallInt(i16::MAX));
}

#[test]
fn rt_integer_min() {
    assert_eq!(rt(DuckValue::Int(i32::MIN)), DuckValue::Int(i32::MIN));
}

#[test]
fn rt_integer_max() {
    assert_eq!(rt(DuckValue::Int(i32::MAX)), DuckValue::Int(i32::MAX));
}

#[test]
fn rt_bigint_min() {
    assert_eq!(rt(DuckValue::BigInt(i64::MIN)), DuckValue::BigInt(i64::MIN));
}

#[test]
fn rt_bigint_max() {
    assert_eq!(rt(DuckValue::BigInt(i64::MAX)), DuckValue::BigInt(i64::MAX));
}

// hugeint_from_i128 supports values up to MAX_SUPPORTED_I128 = 2^127 - 2^63
#[test]
fn rt_hugeint_positive() {
    let v = 170_141_183_460_469_231_722_463_931_679_029_329_919_i128;
    assert_eq!(rt(DuckValue::HugeInt(v)), DuckValue::HugeInt(v));
}

#[test]
fn rt_hugeint_negative() {
    let v = -170_141_183_460_469_231_722_463_931_679_029_329_919_i128;
    assert_eq!(rt(DuckValue::HugeInt(v)), DuckValue::HugeInt(v));
}

// Unsigned integers

#[test]
fn rt_utinyint_zero() {
    assert_eq!(rt(DuckValue::UTinyInt(0)), DuckValue::UTinyInt(0));
}

#[test]
fn rt_utinyint_max() {
    assert_eq!(rt(DuckValue::UTinyInt(u8::MAX)), DuckValue::UTinyInt(u8::MAX));
}

#[test]
fn rt_usmallint_max() {
    assert_eq!(rt(DuckValue::USmallInt(u16::MAX)), DuckValue::USmallInt(u16::MAX));
}

#[test]
fn rt_uinteger_max() {
    assert_eq!(rt(DuckValue::UInt(u32::MAX)), DuckValue::UInt(u32::MAX));
}

#[test]
fn rt_ubigint_max() {
    assert_eq!(rt(DuckValue::UBigInt(u64::MAX)), DuckValue::UBigInt(u64::MAX));
}

#[test]
fn rt_uhugeint_max() {
    assert_eq!(rt(DuckValue::UHugeInt(u128::MAX)), DuckValue::UHugeInt(u128::MAX));
}

// Floats

#[test]
fn rt_float() {
    assert_eq!(rt(DuckValue::Float(3.0_f32)), DuckValue::Float(3.0_f32));
}

#[test]
fn rt_float_negative() {
    assert_eq!(rt(DuckValue::Float(-1.0_f32)), DuckValue::Float(-1.0_f32));
}

#[test]
fn rt_double() {
    assert_eq!(rt(DuckValue::Double(2.0_f64)), DuckValue::Double(2.0_f64));
}

#[test]
fn rt_double_large() {
    let v = f64::MAX / 2.0;
    assert_eq!(rt(DuckValue::Double(v)), DuckValue::Double(v));
}

// Text

#[test]
fn rt_text_ascii() {
    assert_eq!(rt(DuckValue::text("hello world")), DuckValue::text("hello world"));
}

#[test]
fn rt_text_empty() {
    assert_eq!(rt(DuckValue::text("")), DuckValue::text(""));
}

#[test]
fn rt_text_unicode() {
    let s = "Héllo Wörld 🦆";
    assert_eq!(rt(DuckValue::text(s)), DuckValue::text(s));
}

#[test]
fn rt_text_long() {
    let s = "a".repeat(1000);
    assert_eq!(rt(DuckValue::text(s.as_str())), DuckValue::text(s));
}

// BLOB

#[test]
fn rt_blob_empty() {
    let blob = DuckValue::Blob(Blob::new(vec![]));
    assert_eq!(rt(blob.clone()), blob);
}

#[test]
fn rt_blob_bytes() {
    let blob = DuckValue::Blob(Blob::new(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    assert_eq!(rt(blob.clone()), blob);
}

#[test]
fn rt_blob_large() {
    let bytes: Vec<u8> = (0u8..=255u8).cycle().take(2048).collect();
    let blob = DuckValue::Blob(Blob::new(bytes));
    assert_eq!(rt(blob.clone()), blob);
}

// NULL paths

/// Binding DuckValue::Null as $1 → SQLNULL column → DuckValue::Null
#[test]
fn rt_null_direct() {
    assert_eq!(rt(DuckValue::Null), DuckValue::Null);
}

#[test]
fn rt_null_integer() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT NULL::INTEGER AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Null));
    Ok(())
}

#[test]
fn rt_null_varchar() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT NULL::VARCHAR AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Null));
    Ok(())
}

#[test]
fn rt_null_boolean() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT NULL::BOOLEAN AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Null));
    Ok(())
}

#[test]
fn rt_null_float() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT NULL::FLOAT AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Null));
    Ok(())
}

#[test]
fn rt_null_blob() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT NULL::BLOB AS v")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Null));
    Ok(())
}

/// Three-column row: Null, Int(42), Null — exercises multi-column null handling.
#[test]
fn rt_multi_nulls() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result =
        conn.execute("SELECT NULL::INTEGER AS a, 42 AS b, NULL::VARCHAR AS c")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("a"), Some(&DuckValue::Null));
    assert_eq!(row.get("b"), Some(&DuckValue::Int(42)));
    assert_eq!(row.get("c"), Some(&DuckValue::Null));
    Ok(())
}

// Temporal gap types (require chrono feature)

#[cfg(feature = "chrono")]
#[test]
fn rt_timestamptz() -> better_duck_core::error::Result<()> {
    use chrono::{TimeZone, Utc};
    let mut conn = open();
    let mut result =
        conn.execute("SELECT '2024-06-01 12:00:00+00'::TIMESTAMPTZ AS ts")?;
    let row = result.next().unwrap()?;
    let expected =
        DuckValue::TimestampTz(Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap());
    assert_eq!(row.get("ts"), Some(&expected));
    Ok(())
}

#[cfg(feature = "chrono")]
#[test]
fn rt_timetz() -> better_duck_core::error::Result<()> {
    use better_duck_core::types::date_chrono::TimeTz;
    use chrono::NaiveTime;
    let mut conn = open();
    // UTC (offset = 0) avoids normalization ambiguity between DuckDB versions.
    let mut result = conn.execute("SELECT '12:00:00+00'::TIMETZ AS t")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::TimeTz(TimeTz {
        time: NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        offset_secs: 0,
    });
    assert_eq!(row.get("t"), Some(&expected));
    Ok(())
}

#[cfg(feature = "chrono")]
#[test]
fn rt_time_ns() -> better_duck_core::error::Result<()> {
    use chrono::NaiveTime;
    let mut conn = open();
    conn.execute_batch("CREATE TABLE t_ns (v TIME_NS)")?;
    conn.execute_batch("INSERT INTO t_ns VALUES ('12:00:00')")?;
    let mut result = conn.execute("SELECT v FROM t_ns")?;
    let row = result.next().unwrap()?;
    let expected = DuckValue::TimeNs(NaiveTime::from_hms_opt(12, 0, 0).unwrap());
    assert_eq!(row.get("v"), Some(&expected));
    Ok(())
}

// DuckValue::column_count + get_idx

#[test]
fn row_column_count_and_get_idx() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    let mut result = conn.execute("SELECT 1 AS a, 'x' AS b, true AS c")?;
    let row = result.next().unwrap()?;
    assert_eq!(row.column_count(), 3);
    assert_eq!(row.get_idx(0), Some(&DuckValue::Int(1)));
    assert_eq!(row.get_idx(1), Some(&DuckValue::text("x")));
    assert_eq!(row.get_idx(2), Some(&DuckValue::Boolean(true)));
    assert_eq!(row.get_idx(99), None);
    Ok(())
}

// Multiple rows

#[test]
fn multiple_rows_ordered() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TABLE mr (id INTEGER, name TEXT)")?;
    conn.execute_batch("INSERT INTO mr VALUES (1, 'a'), (2, 'b'), (3, 'c')")?;
    let rows: Vec<_> = conn
        .execute("SELECT id, name FROM mr ORDER BY id")?
        .collect::<Result<_, _>>()?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("id"), Some(&DuckValue::Int(1)));
    assert_eq!(rows[1].get("name"), Some(&DuckValue::text("b")));
    assert_eq!(rows[2].get("id"), Some(&DuckValue::Int(3)));
    Ok(())
}

// DuckValue bind in execute_with (AppendAble for DuckValue)

#[test]
fn bind_duckvalue_directly() -> better_duck_core::error::Result<()> {
    let mut conn = open();
    conn.execute_batch("CREATE TABLE bv (v INTEGER)")?;
    conn.execute_batch("INSERT INTO bv VALUES (10), (20), (30)")?;
    let mut v = DuckValue::Int(20);
    let mut result = conn.execute_with(
        "SELECT v FROM bv WHERE v = $1",
        &mut [&mut v as &mut dyn AppendAble],
    )?;
    let row = result.next().unwrap()?;
    assert_eq!(row.get("v"), Some(&DuckValue::Int(20)));
    assert!(result.next().is_none());
    Ok(())
}
