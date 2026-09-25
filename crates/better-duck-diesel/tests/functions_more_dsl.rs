#![allow(missing_docs)]

//! DSL tests for the extended numeric (trig/log/gcd) and string-search / padding
//! scalar functions.

use better_duck_diesel::dsl::{gcd, left, lpad, sqrt, strpos, substr};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

#[test]
fn numeric_extras() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let r: f64 = diesel::select(sqrt(16.0_f64)).get_result(&mut conn).unwrap();
    assert!((r - 4.0).abs() < 1e-9);
    let g: i64 = diesel::select(gcd(12_i64, 18_i64)).get_result(&mut conn).unwrap();
    assert_eq!(g, 6);
}

#[test]
fn string_search_and_padding() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let p: i32 = diesel::select(strpos("hello", "ll")).get_result(&mut conn).unwrap();
    assert_eq!(p, 3);
    let s: String = diesel::select(substr("hello", 2, 3)).get_result(&mut conn).unwrap();
    assert_eq!(s, "ell");
    let l: String = diesel::select(left("hello", 2)).get_result(&mut conn).unwrap();
    assert_eq!(l, "he");
    let pad: String = diesel::select(lpad("7", 3, "0")).get_result(&mut conn).unwrap();
    assert_eq!(pad, "007");
}
