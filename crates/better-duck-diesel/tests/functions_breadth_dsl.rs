#![allow(missing_docs)]

//! DSL tests for the breadth tranche: extra numeric ops, datetime builders,
//! formatting, string-similarity, list/map functions, and utility functions.

use better_duck_diesel::dsl::{
    array_length, bit_count, cardinality, concat, dayname, greatest, jaro_winkler_similarity,
    make_timestamp, mod_, printf, quarter, type_of, version, xor,
};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;
use diesel::sql_types::Integer;

#[test]
fn numeric_and_bitwise() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let m: i64 = diesel::select(mod_(7_i64, 3_i64)).get_result(&mut conn).unwrap();
    assert_eq!(m, 1);
    let g: i32 =
        diesel::select(greatest::<Integer, _, _>(2_i32, 5_i32)).get_result(&mut conn).unwrap();
    assert_eq!(g, 5);
    let b: better_duck_diesel::values::TinyInt =
        diesel::select(bit_count(7_i64)).get_result(&mut conn).unwrap();
    assert_eq!(b.0, 3);
    let x: i64 = diesel::select(xor(6_i64, 3_i64)).get_result(&mut conn).unwrap();
    assert_eq!(x, 5);
}

#[test]
fn datetime_builders_and_parts() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    // micros = 0 → 1970-01-01, a Thursday in Q1.
    let name: String =
        diesel::select(dayname(make_timestamp(0_i64))).get_result(&mut conn).unwrap();
    assert_eq!(name, "Thursday");
    let q: i64 = diesel::select(quarter(make_timestamp(0_i64))).get_result(&mut conn).unwrap();
    assert_eq!(q, 1);
}

#[test]
fn formatting_and_similarity() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let c: String = diesel::select(concat("a", "b")).get_result(&mut conn).unwrap();
    assert_eq!(c, "ab");
    let p: String = diesel::select(printf("%s!", "hi")).get_result(&mut conn).unwrap();
    assert_eq!(p, "hi!");
    let s: f64 =
        diesel::select(jaro_winkler_similarity("martha", "marhta")).get_result(&mut conn).unwrap();
    assert!(s > 0.9, "jaro-winkler similarity was {s}");
}

#[test]
fn utility_functions() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let v: String = diesel::select(version()).get_result(&mut conn).unwrap();
    assert!(!v.is_empty());
    let t: String = diesel::select(type_of::<Integer, _>(1_i32)).get_result(&mut conn).unwrap();
    assert_eq!(t, "INTEGER");
}

diesel::table! {
    lst (id) {
        id -> Integer,
        l -> better_duck_diesel::sql_types::DuckList,
    }
}

diesel::table! {
    mp (id) {
        id -> Integer,
        m -> better_duck_diesel::sql_types::DuckMap,
    }
}

#[test]
fn list_and_map_functions() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE lst (id INTEGER, l INTEGER[])").execute(&mut conn).unwrap();
    diesel::sql_query("INSERT INTO lst VALUES (1, [10, 20, 30])").execute(&mut conn).unwrap();
    let n: i64 = lst::table.select(array_length(lst::l)).first(&mut conn).unwrap();
    assert_eq!(n, 3);

    diesel::sql_query("CREATE TABLE mp (id INTEGER, m MAP(VARCHAR, INTEGER))")
        .execute(&mut conn)
        .unwrap();
    diesel::sql_query("INSERT INTO mp VALUES (1, MAP {'a': 1, 'b': 2})")
        .execute(&mut conn)
        .unwrap();
    let c: better_duck_diesel::values::UBigInt =
        mp::table.select(cardinality(mp::m)).first(&mut conn).unwrap();
    assert_eq!(c.0, 2);
}
