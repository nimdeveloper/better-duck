#![allow(missing_docs)]

//! DSL tests for the extended aggregate set (regression, bitwise, approximate)
//! and the JSON scalar functions.

use better_duck_diesel::dsl::{approx_count_distinct, bit_and, json_extract_string, json_valid, regr_slope};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    pts (id) {
        id -> diesel::sql_types::Integer,
        x -> diesel::sql_types::Double,
        y -> diesel::sql_types::Double,
        m -> diesel::sql_types::BigInt,
        j -> diesel::sql_types::Text,
    }
}

fn setup() -> DuckDbConnection {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE pts (id INTEGER, x DOUBLE, y DOUBLE, m BIGINT, j VARCHAR)")
        .execute(&mut conn)
        .unwrap();
    diesel::sql_query(
        "INSERT INTO pts VALUES (1,1.0,2.0,6,'{\"a\":1}'),(2,2.0,4.0,3,'[1,2,3]'),(3,3.0,6.0,5,'x')",
    )
    .execute(&mut conn)
    .unwrap();
    conn
}

#[test]
fn regr_slope_of_a_perfect_line() {
    let mut conn = setup();
    // y = 2x → slope 2.
    let s: Option<f64> = pts::table.select(regr_slope(pts::y, pts::x)).first(&mut conn).unwrap();
    assert!((s.unwrap() - 2.0).abs() < 1e-9);
}

#[test]
fn bit_and_folds_bits() {
    let mut conn = setup();
    // 6 & 3 & 5 == 0.
    let a: Option<i64> = pts::table.select(bit_and(pts::m)).first(&mut conn).unwrap();
    assert_eq!(a, Some(0));
}

#[test]
fn approx_count_distinct_is_exact_for_small_input() {
    let mut conn = setup();
    let c: i64 = pts::table.select(approx_count_distinct(pts::j)).first(&mut conn).unwrap();
    assert_eq!(c, 3);
}

#[test]
fn json_functions_when_available() {
    let mut conn = setup();
    // The `json` extension is normally autoloaded; skip cleanly if it isn't.
    match diesel::select(json_valid("{\"a\":1}")).get_result::<bool>(&mut conn) {
        Ok(valid) => assert!(valid),
        Err(_) => {
            eprintln!("skipping json_functions_when_available: `json` extension unavailable");
            return;
        },
    }
    let t: Option<String> = pts::table
        .filter(pts::id.eq(1))
        .select(json_extract_string(pts::j, "$.a"))
        .first(&mut conn)
        .unwrap();
    assert_eq!(t, Some("1".to_owned()));
}
