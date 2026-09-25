#![allow(missing_docs)]

//! DSL tests for the DuckDB scalar functions exposed via `define_sql_function!`.

use better_duck_diesel::dsl::{length, lower, nullif};
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
fn lower_and_length_project_scalar_functions() {
    let mut conn = setup();
    let rows: Vec<(String, i64)> = items::table
        .select((lower(items::name), length(items::name)))
        .order(items::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(rows[0], ("apple".to_owned(), 5));
    assert_eq!(rows[1], ("banana".to_owned(), 6));
}

#[test]
fn nullif_returns_null_on_match() {
    let mut conn = setup();
    let vals: Vec<Option<String>> =
        items::table.select(nullif(items::name, "Apple")).order(items::id).load(&mut conn).unwrap();
    assert_eq!(vals, vec![None, Some("banana".to_owned()), Some("Cherry".to_owned())]);
}
