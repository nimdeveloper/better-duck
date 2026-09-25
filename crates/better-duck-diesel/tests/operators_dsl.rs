#![allow(missing_docs)]

//! DSL tests for the DuckDB operator extensions (`ILIKE`, `GLOB`, `IS DISTINCT
//! FROM`, …) that Diesel does not provide for a third-party backend.

use better_duck_diesel::dsl::{DuckExpressionMethods, DuckTextExpressionMethods};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    items (id) {
        id -> diesel::sql_types::Integer,
        name -> diesel::sql_types::Text,
    }
}

fn setup() -> DuckDbConnection {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE items (id INTEGER, name VARCHAR)").execute(&mut conn).unwrap();
    diesel::sql_query("INSERT INTO items VALUES (1,'Apple'),(2,'banana'),(3,'Cherry')")
        .execute(&mut conn)
        .unwrap();
    conn
}

#[test]
fn ilike_is_case_insensitive() {
    let mut conn = setup();
    let names: Vec<String> = items::table
        .filter(items::name.ilike("a%"))
        .select(items::name)
        .order(items::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(names, vec!["Apple".to_owned()]);
}

#[test]
fn is_distinct_from_excludes_the_match() {
    let mut conn = setup();
    let ids: Vec<i32> = items::table
        .filter(items::name.is_distinct_from("Apple"))
        .select(items::id)
        .order(items::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn glob_matches_case_sensitively() {
    let mut conn = setup();
    let ids: Vec<i32> = items::table
        .filter(items::name.glob("*e*"))
        .select(items::id)
        .order(items::id)
        .load(&mut conn)
        .unwrap();
    // Lowercase 'e' appears in 'Apple' and 'Cherry', not 'banana'.
    assert_eq!(ids, vec![1, 3]);
}
