#![allow(missing_docs)]

//! Coverage: exercise every DuckDB operator extension method.

use better_duck_diesel::dsl::{DuckExpressionMethods, DuckTextExpressionMethods};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    ops (id) {
        id -> diesel::sql_types::Integer,
        t -> diesel::sql_types::Text,
    }
}

fn setup() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE ops (id INTEGER, t VARCHAR)").execute(&mut c).unwrap();
    diesel::sql_query("INSERT INTO ops VALUES (1, 'Apple'), (2, 'banana')")
        .execute(&mut c)
        .unwrap();
    c
}

#[test]
fn text_operators() {
    let mut c = setup();
    ops::table.filter(ops::t.ilike("apple%")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.not_ilike("apple%")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.similar_to("[A-Z].*")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.not_similar_to("[A-Z].*")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.glob("A*")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.regexp_matches("^A")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.not_regexp_matches("^A")).execute(&mut c).unwrap();
    ops::table.filter(ops::t.starts_with_op("A")).execute(&mut c).unwrap();
}

#[test]
fn null_safe_operators() {
    let mut c = setup();
    ops::table.filter(ops::id.is_distinct_from(1_i32)).execute(&mut c).unwrap();
    ops::table.filter(ops::id.is_not_distinct_from(1_i32)).execute(&mut c).unwrap();
}
