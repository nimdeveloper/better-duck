//! Round-tripping standard Diesel SQL types through the typed query DSL.
//!
//! `bool`, `i16`, `i32`, `i64`, `f32`, `f64`, `String`, `Vec<u8>` and the chrono date/time
//! types insert and read back through the ordinary `.values(...)` / `.select(...)` DSL. Two
//! groups need care:
//!   * the DuckDB-width integers (`i8`/`u8`/`u16`/`u32`/`u64`) have no `AsExpression`, so they
//!     are inserted with raw SQL and only *loaded* through the DSL (as a tuple here);
//!   * `Numeric` maps to `rust_decimal::Decimal`, which likewise binds via `sql_query` rather
//!     than the typed DSL.

use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;

diesel::table! { t_bool (id) { id -> Integer, val -> Bool } }
diesel::table! { t_i16 (id) { id -> Integer, val -> SmallInt } }
diesel::table! { t_i32 (id) { id -> Integer, val -> Integer } }
diesel::table! { t_i64 (id) { id -> Integer, val -> BigInt } }
diesel::table! { t_f32 (id) { id -> Integer, val -> Float } }
diesel::table! { t_f64 (id) { id -> Integer, val -> Double } }
diesel::table! { t_text (id) { id -> Integer, val -> Text } }
diesel::table! { t_blob (id) { id -> Integer, val -> Binary } }
diesel::table! { t_date (id) { id -> Integer, val -> Date } }
diesel::table! { t_time (id) { id -> Integer, val -> Time } }
diesel::table! { t_ts (id) { id -> Integer, val -> Timestamp } }

diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    t_widths (id) {
        id     -> Integer,
        tiny   -> DuckTinyInt,
        utiny  -> DuckUTinyInt,
        usmall -> DuckUSmallInt,
        uint   -> DuckUInt,
        ubig   -> DuckUBigInt,
    }
}

fn conn_with(ddl: &str) -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(ddl).expect("create table");
    c
}

fn main() -> QueryResult<()> {
    println!("=== bool ===");
    let mut c = conn_with("CREATE TABLE t_bool (id INTEGER PRIMARY KEY, val BOOLEAN NOT NULL)");
    diesel::insert_into(t_bool::table)
        .values((t_bool::id.eq(1), t_bool::val.eq(true)))
        .execute(&mut c)?;
    let v: bool = t_bool::table.select(t_bool::val).first(&mut c)?;
    assert!(v);
    println!("  bool -> {v}");

    println!("=== SmallInt / Integer / BigInt (i16 / i32 / i64) ===");
    let mut c = conn_with("CREATE TABLE t_i16 (id INTEGER PRIMARY KEY, val SMALLINT NOT NULL)");
    diesel::insert_into(t_i16::table)
        .values((t_i16::id.eq(1), t_i16::val.eq(i16::MAX)))
        .execute(&mut c)?;
    let v16: i16 = t_i16::table.select(t_i16::val).first(&mut c)?;
    assert_eq!(v16, i16::MAX);

    let mut c = conn_with("CREATE TABLE t_i32 (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
    diesel::insert_into(t_i32::table)
        .values((t_i32::id.eq(1), t_i32::val.eq(i32::MIN)))
        .execute(&mut c)?;
    let v32: i32 = t_i32::table.select(t_i32::val).first(&mut c)?;
    assert_eq!(v32, i32::MIN);

    let mut c = conn_with("CREATE TABLE t_i64 (id INTEGER PRIMARY KEY, val BIGINT NOT NULL)");
    diesel::insert_into(t_i64::table)
        .values((t_i64::id.eq(1), t_i64::val.eq(i64::MAX)))
        .execute(&mut c)?;
    let v64: i64 = t_i64::table.select(t_i64::val).first(&mut c)?;
    assert_eq!(v64, i64::MAX);
    println!("  i16={v16} i32={v32} i64={v64}");

    println!("=== Float / Double (f32 / f64) ===");
    let mut c = conn_with("CREATE TABLE t_f32 (id INTEGER PRIMARY KEY, val FLOAT NOT NULL)");
    diesel::insert_into(t_f32::table)
        .values((t_f32::id.eq(1), t_f32::val.eq(1.25_f32)))
        .execute(&mut c)?;
    let vf32: f32 = t_f32::table.select(t_f32::val).first(&mut c)?;
    assert!((vf32 - 1.25).abs() < 1e-6);

    let mut c = conn_with("CREATE TABLE t_f64 (id INTEGER PRIMARY KEY, val DOUBLE NOT NULL)");
    let x = std::f64::consts::PI;
    diesel::insert_into(t_f64::table)
        .values((t_f64::id.eq(1), t_f64::val.eq(x)))
        .execute(&mut c)?;
    let vf64: f64 = t_f64::table.select(t_f64::val).first(&mut c)?;
    assert!((vf64 - x).abs() < 1e-14);
    println!("  f32={vf32} f64={vf64}");

    println!("=== Text ===");
    let mut c = conn_with("CREATE TABLE t_text (id INTEGER PRIMARY KEY, val VARCHAR NOT NULL)");
    let s = "Héllo Wörld 🦆";
    diesel::insert_into(t_text::table)
        .values((t_text::id.eq(1), t_text::val.eq(s)))
        .execute(&mut c)?;
    let vs: String = t_text::table.select(t_text::val).first(&mut c)?;
    assert_eq!(vs, s);
    println!("  text -> {vs:?}");

    println!("=== Binary ===");
    let mut c = conn_with("CREATE TABLE t_blob (id INTEGER PRIMARY KEY, val BLOB NOT NULL)");
    let bytes: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
    diesel::insert_into(t_blob::table)
        .values((t_blob::id.eq(1), t_blob::val.eq(bytes.as_slice())))
        .execute(&mut c)?;
    let vb: Vec<u8> = t_blob::table.select(t_blob::val).first(&mut c)?;
    assert_eq!(vb, bytes);
    println!("  binary -> {vb:02X?}");

    println!("=== Date / Time / Timestamp (chrono) ===");
    let mut c = conn_with("CREATE TABLE t_date (id INTEGER PRIMARY KEY, val DATE NOT NULL)");
    let d = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    diesel::insert_into(t_date::table)
        .values((t_date::id.eq(1), t_date::val.eq(d)))
        .execute(&mut c)?;
    let vd: chrono::NaiveDate = t_date::table.select(t_date::val).first(&mut c)?;
    assert_eq!(vd, d);

    let mut c = conn_with("CREATE TABLE t_time (id INTEGER PRIMARY KEY, val TIME NOT NULL)");
    let t = chrono::NaiveTime::from_hms_micro_opt(14, 30, 55, 123_456).unwrap();
    diesel::insert_into(t_time::table)
        .values((t_time::id.eq(1), t_time::val.eq(t)))
        .execute(&mut c)?;
    let vt: chrono::NaiveTime = t_time::table.select(t_time::val).first(&mut c)?;
    assert_eq!(vt, t);

    let mut c = conn_with("CREATE TABLE t_ts (id INTEGER PRIMARY KEY, val TIMESTAMP NOT NULL)");
    let ts = chrono::NaiveDateTime::parse_from_str("2024-01-15 10:20:30", "%Y-%m-%d %H:%M:%S")
        .unwrap();
    diesel::insert_into(t_ts::table)
        .values((t_ts::id.eq(1), t_ts::val.eq(ts)))
        .execute(&mut c)?;
    let vts: chrono::NaiveDateTime = t_ts::table.select(t_ts::val).first(&mut c)?;
    assert_eq!(vts, ts);
    println!("  date={vd} time={vt} timestamp={vts}");

    println!("=== Numeric (rust_decimal::Decimal, via sql_query bind) ===");
    // rust_decimal::Decimal has no AsExpression<Numeric>, so it binds through sql_query
    // rather than the typed DSL, but its FromSql/ToSql for the DuckDB backend still apply.
    use diesel::sql_types::Numeric;
    use rust_decimal::Decimal;
    #[derive(diesel::QueryableByName, Debug)]
    struct DecRow {
        #[diesel(sql_type = Numeric)]
        val: Decimal,
    }
    let mut c = conn_with("CREATE TABLE t_dec (val DECIMAL(18, 4))");
    let dec = Decimal::new(-12_345, 4); // -1.2345
    diesel::sql_query("INSERT INTO t_dec VALUES ($1)")
        .bind::<Numeric, _>(dec)
        .execute(&mut c)?;
    let got: Decimal =
        diesel::sql_query("SELECT val FROM t_dec").get_result::<DecRow>(&mut c)?.val;
    assert_eq!(got, dec);
    println!("  numeric -> {got}");

    println!("=== DuckDB-width integers loaded as a tuple (i8/u8/u16/u32/u64) ===");
    // These widths satisfy Diesel's Queryable blanket so they LOAD via the DSL, but they
    // have no AsExpression, so the row is seeded with raw SQL rather than .values(...).
    let mut c = conn_with(
        "CREATE TABLE t_widths (
            id INTEGER PRIMARY KEY, tiny TINYINT, utiny UTINYINT,
            usmall USMALLINT, uint UINTEGER, ubig UBIGINT
        )",
    );
    c.batch_execute(&format!(
        "INSERT INTO t_widths VALUES (1, {}, {}, {}, {}, {})",
        i8::MIN,
        u8::MAX,
        u16::MAX,
        u32::MAX,
        u64::MAX
    ))?;
    let widths: (i8, u8, u16, u32, u64) = t_widths::table
        .select((
            t_widths::tiny,
            t_widths::utiny,
            t_widths::usmall,
            t_widths::uint,
            t_widths::ubig,
        ))
        .first(&mut c)?;
    assert_eq!(widths, (i8::MIN, u8::MAX, u16::MAX, u32::MAX, u64::MAX));
    println!("  (i8,u8,u16,u32,u64) -> {widths:?}");

    println!("\nAll standard-type round-trips verified.");
    Ok(())
}
