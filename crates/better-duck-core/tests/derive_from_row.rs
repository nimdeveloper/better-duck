#![allow(missing_docs)]
#![cfg(feature = "derive")]

//! End-to-end tests for `#[derive(FromRow)]`: they exercise the generated
//! `FromRow` impl against real query rows (column lookup, `Option`/NULL, per-field
//! `rename`, and container `rename_all`).

use better_duck_core::connection::Connection;
use better_duck_core::FromRow;

#[derive(FromRow, Debug, PartialEq)]
struct Person {
    id: i32,
    name: String,
    #[duck(rename = "years")]
    age: i64,
    nickname: Option<String>,
}

#[test]
fn derive_reads_named_columns_and_options() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE people (id INTEGER, name VARCHAR, years BIGINT, nickname VARCHAR)",
    )
    .unwrap();
    conn.execute_batch(
        "INSERT INTO people VALUES (1, 'Ada', 36, 'countess'), (2, 'Bob', 40, NULL)",
    )
    .unwrap();

    let rs = conn
        .execute("SELECT id, name, years, nickname FROM people ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap();
    let people: Vec<Person> = rs.to_structs().unwrap();
    assert_eq!(
        people,
        vec![
            Person {
                id: 1,
                name: "Ada".to_owned(),
                age: 36,
                nickname: Some("countess".to_owned())
            },
            Person { id: 2, name: "Bob".to_owned(), age: 40, nickname: None },
        ]
    );
}

#[test]
fn rename_all_maps_field_names() {
    #[derive(FromRow, Debug, PartialEq)]
    #[duck(rename_all = "SCREAMING_SNAKE_CASE")]
    struct Row {
        first_name: String,
        item_count: i32,
    }

    let mut conn = Connection::open_in_memory().unwrap();
    // Quoted aliases preserve exact case, so the result columns are
    // `FIRST_NAME`/`ITEM_COUNT`, matching the SCREAMING_SNAKE_CASE rule.
    let rs = conn
        .execute(r#"SELECT 'x' AS "FIRST_NAME", 7 AS "ITEM_COUNT""#)
        .unwrap()
        .materialize()
        .unwrap();
    let rows: Vec<Row> = rs.to_structs().unwrap();
    assert_eq!(rows, vec![Row { first_name: "x".to_owned(), item_count: 7 }]);
}

#[test]
fn missing_column_is_an_error() {
    let mut conn = Connection::open_in_memory().unwrap();
    let rs = conn.execute("SELECT 1 AS id").unwrap().materialize().unwrap();
    // `Person` needs `name`/`years`/`nickname` too, which the query omits.
    assert!(rs.to_structs::<Person>().is_err());
}
