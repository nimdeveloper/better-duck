#![allow(missing_docs)]

//! DSL tests for the DuckDB regex and date/time scalar functions.

use better_duck_diesel::dsl::{regexp_matches, regexp_replace, year};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    events (id) {
        id -> diesel::sql_types::Integer,
        name -> diesel::sql_types::Text,
        ts -> diesel::sql_types::Timestamp,
    }
}

fn setup() -> DuckDbConnection {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE events (id INTEGER, name VARCHAR, ts TIMESTAMP)")
        .execute(&mut conn)
        .unwrap();
    diesel::sql_query(
        "INSERT INTO events VALUES \
         (1,'apple123',TIMESTAMP '2020-05-15 10:00:00'),\
         (2,'banana',TIMESTAMP '2021-07-01 00:00:00')",
    )
    .execute(&mut conn)
    .unwrap();
    conn
}

#[test]
fn regexp_matches_filters_rows() {
    let mut conn = setup();
    let ids: Vec<i32> = events::table
        .filter(regexp_matches(events::name, "[0-9]+"))
        .select(events::id)
        .order(events::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(ids, vec![1]);
}

#[test]
fn regexp_replace_projects() {
    let mut conn = setup();
    let names: Vec<String> = events::table
        .select(regexp_replace(events::name, "[0-9]+", "#"))
        .order(events::id)
        .load(&mut conn)
        .unwrap();
    assert_eq!(names, vec!["apple#".to_owned(), "banana".to_owned()]);
}

#[test]
fn year_extracts_the_year_component() {
    let mut conn = setup();
    let years: Vec<i64> =
        events::table.select(year(events::ts)).order(events::id).load(&mut conn).unwrap();
    assert_eq!(years, vec![2020, 2021]);
}
