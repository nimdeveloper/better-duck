//! DuckDB-specific SQL types (`better_duck_diesel::sql_types::Duck*`).
//!
//! IMPORTANT: these marker types have no `AsExpression`/`Queryable` blanket impls, so they
//! cannot be bound or read through the *typed* insert/select DSL. Instead they round-trip via
//! raw SQL: `diesel::sql_query(...).bind::<DuckXxx, _>(value)` to write, and a
//! `#[derive(QueryableByName)]` struct whose field carries `#[diesel(sql_type = DuckXxx)]` to
//! read. Values are ordinary `better_duck_core` types (`DuckValue`, `DuckUuid`, `DuckBit`, ...).

use better_duck_core::types::value::DuckValue;
use better_duck_diesel::DuckDbConnection;
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use std::collections::HashMap;

fn conn_with(ddl: &str) -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").expect("open in-memory DuckDB");
    c.batch_execute(ddl).expect("create table");
    c
}

fn main() -> QueryResult<()> {
    println!("=== DuckHugeInt (i128) / DuckUHugeInt (u128) ===");
    // i128 / u128 do NOT propagate through Diesel's Queryable blanket, so they can only be
    // read via QueryableByName — never through the typed .select(...) DSL.
    {
        use better_duck_diesel::sql_types::{DuckHugeInt, DuckUHugeInt};
        #[derive(diesel::QueryableByName, Debug)]
        struct HugeRow {
            #[diesel(sql_type = DuckHugeInt)]
            val: i128,
        }
        #[derive(diesel::QueryableByName, Debug)]
        struct UHugeRow {
            #[diesel(sql_type = DuckUHugeInt)]
            val: u128,
        }
        let mut c = conn_with("CREATE TABLE t_huge (val HUGEINT)");
        diesel::sql_query("INSERT INTO t_huge VALUES ($1)")
            .bind::<DuckHugeInt, _>(i128::MIN)
            .execute(&mut c)?;
        let got: i128 =
            diesel::sql_query("SELECT val FROM t_huge").get_result::<HugeRow>(&mut c)?.val;
        assert_eq!(got, i128::MIN);

        let mut c = conn_with("CREATE TABLE t_uhuge (val UHUGEINT)");
        diesel::sql_query("INSERT INTO t_uhuge VALUES ($1)")
            .bind::<DuckUHugeInt, _>(u128::MAX)
            .execute(&mut c)?;
        let ugot: u128 =
            diesel::sql_query("SELECT val FROM t_uhuge").get_result::<UHugeRow>(&mut c)?.val;
        assert_eq!(ugot, u128::MAX);
        println!("  hugeint={got} uhugeint={ugot}");
    }

    println!("=== DuckUuid ===");
    {
        use better_duck_core::types::uuid::DuckUuid;
        use better_duck_diesel::sql_types::DuckUuid as DuckUuidTy;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckUuidTy)]
            val: DuckUuid,
        }
        let mut c = conn_with("CREATE TABLE t_uuid (val UUID)");
        let value = DuckUuid(0x1234_5678_9abc_def0_0fed_cba9_8765_4321);
        diesel::sql_query("INSERT INTO t_uuid VALUES ($1)")
            .bind::<DuckUuidTy, _>(value)
            .execute(&mut c)?;
        let got: DuckUuid =
            diesel::sql_query("SELECT val FROM t_uuid").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, value);
        println!("  uuid -> {got:?}");
    }

    println!("=== DuckEnum ===");
    {
        use better_duck_diesel::sql_types::DuckEnum;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckEnum)]
            val: String,
        }
        let mut c = conn_with(
            "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
             CREATE TABLE t_enum (id INTEGER, val mood)",
        );
        diesel::sql_query("INSERT INTO t_enum VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(1)
            .bind::<DuckEnum, _>("happy")
            .execute(&mut c)?;
        let got: String =
            diesel::sql_query("SELECT val FROM t_enum").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, "happy");
        println!("  enum -> {got:?}");
    }

    println!("=== DuckList ===");
    {
        use better_duck_diesel::sql_types::DuckList;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckList)]
            val: Vec<DuckValue>,
        }
        let mut c = conn_with("CREATE TABLE t_list (val INTEGER[])");
        let items = vec![DuckValue::Int(1), DuckValue::Int(2), DuckValue::Int(3)];
        diesel::sql_query("INSERT INTO t_list VALUES ($1)")
            .bind::<DuckList, _>(&items)
            .execute(&mut c)?;
        let got: Vec<DuckValue> =
            diesel::sql_query("SELECT val FROM t_list").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, items);
        println!("  list -> {got:?}");
    }

    println!("=== DuckStruct ===");
    {
        use better_duck_diesel::sql_types::DuckStruct;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckStruct)]
            val: HashMap<String, DuckValue>,
        }
        let mut c = conn_with("CREATE TABLE t_struct (id INTEGER, val STRUCT(x INTEGER))");
        let fields: HashMap<String, DuckValue> =
            HashMap::from([("x".to_string(), DuckValue::Int(42))]);
        diesel::sql_query("INSERT INTO t_struct VALUES ($1, $2)")
            .bind::<diesel::sql_types::Integer, _>(1)
            .bind::<DuckStruct, _>(&fields)
            .execute(&mut c)?;
        let got: HashMap<String, DuckValue> =
            diesel::sql_query("SELECT val FROM t_struct").get_result::<Row>(&mut c)?.val;
        assert_eq!(got.get("x"), Some(&DuckValue::Int(42)));
        println!("  struct.x -> {:?}", got.get("x"));
    }

    println!("=== DuckMap ===");
    {
        use better_duck_diesel::sql_types::DuckMap;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckMap)]
            val: HashMap<DuckValue, DuckValue>,
        }
        let mut c = conn_with("CREATE TABLE t_map (val MAP(INTEGER, VARCHAR))");
        let expected = HashMap::from([
            (DuckValue::Int(1), DuckValue::text("one")),
            (DuckValue::Int(2), DuckValue::text("two")),
        ]);
        diesel::sql_query("INSERT INTO t_map VALUES ($1)")
            .bind::<DuckMap, _>(&expected)
            .execute(&mut c)?;
        let got: HashMap<DuckValue, DuckValue> =
            diesel::sql_query("SELECT val FROM t_map").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, expected);
        println!("  map entries -> {}", got.len());
    }

    println!("=== DuckUnion ===");
    {
        use better_duck_diesel::sql_types::DuckUnion;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckUnion)]
            val: Box<DuckValue>,
        }
        let mut c = conn_with("CREATE TABLE t_union (val UNION(value INTEGER))");
        let expected = Box::new(DuckValue::Int(7));
        diesel::sql_query("INSERT INTO t_union VALUES ($1)")
            .bind::<DuckUnion, _>(expected.clone())
            .execute(&mut c)?;
        let got: Box<DuckValue> =
            diesel::sql_query("SELECT val FROM t_union").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, expected);
        println!("  union active value -> {got:?}");
    }

    println!("=== DuckTimestamptz (chrono DateTime<Utc>) ===");
    {
        use better_duck_diesel::sql_types::DuckTimestamptz;
        use chrono::{DateTime, TimeZone, Utc};
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckTimestamptz)]
            val: DateTime<Utc>,
        }
        let mut c = conn_with("CREATE TABLE t_tstz (val TIMESTAMPTZ)");
        let expected: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 6, 1, 12, 0, 0).unwrap();
        diesel::sql_query("INSERT INTO t_tstz VALUES ($1)")
            .bind::<DuckTimestamptz, _>(expected)
            .execute(&mut c)?;
        let got: DateTime<Utc> =
            diesel::sql_query("SELECT val FROM t_tstz").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, expected);
        println!("  timestamptz -> {got}");
    }

    println!("=== INTERVAL (chrono::Duration via diesel::sql_types::Interval) ===");
    {
        // DuckDB's INTERVAL uses the standard diesel Interval SQL type, bound the same way.
        use diesel::sql_types::Interval;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = Interval)]
            val: chrono::Duration,
        }
        let mut c = conn_with("CREATE TABLE t_interval (val INTERVAL)");
        let value = chrono::Duration::days(3)
            + chrono::Duration::seconds(7_321)
            + chrono::Duration::microseconds(456_789);
        diesel::sql_query("INSERT INTO t_interval VALUES ($1)")
            .bind::<Interval, _>(value)
            .execute(&mut c)?;
        let got: chrono::Duration =
            diesel::sql_query("SELECT val FROM t_interval").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, value);
        println!("  interval -> {got:?}");
    }

    println!("=== DuckBit ===");
    {
        use better_duck_core::types::bit::DuckBit;
        use better_duck_diesel::sql_types::DuckBit as DuckBitTy;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckBitTy)]
            val: DuckBit,
        }
        let mut c = conn_with("CREATE TABLE t_bit (val BIT)");
        let value = DuckBit(vec![0u8, 0b1010_0000]);
        diesel::sql_query("INSERT INTO t_bit VALUES ($1)")
            .bind::<DuckBitTy, _>(value.clone())
            .execute(&mut c)?;
        let got: DuckBit =
            diesel::sql_query("SELECT val FROM t_bit").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, value);
        println!("  bit -> {got:?}");
    }

    println!("=== DuckBignum ===");
    {
        use better_duck_core::types::bignum::DuckBignum;
        use better_duck_diesel::sql_types::DuckBignum as DuckBignumTy;
        #[derive(diesel::QueryableByName, Debug)]
        struct Row {
            #[diesel(sql_type = DuckBignumTy)]
            val: DuckBignum,
        }
        let mut c = conn_with("CREATE TABLE t_bignum (val BIGNUM)");
        let value = DuckBignum::new(vec![0xFF, 0x01], false);
        diesel::sql_query("INSERT INTO t_bignum VALUES ($1)")
            .bind::<DuckBignumTy, _>(value.clone())
            .execute(&mut c)?;
        let got: DuckBignum =
            diesel::sql_query("SELECT val FROM t_bignum").get_result::<Row>(&mut c)?.val;
        assert_eq!(got, value);
        println!("  bignum -> {got:?}");
    }

    println!("\nAll DuckDB-specific type round-trips verified.");
    Ok(())
}
