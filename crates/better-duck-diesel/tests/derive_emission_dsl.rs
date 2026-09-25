#![allow(missing_docs)]
#![cfg(feature = "derive")]

//! Feature-gated Diesel emission for `#[derive(DuckEnum)]`: with the diesel
//! `derive` feature on, a derived enum also implements `ToSql`/`FromSql` at
//! `sql_types::DuckEnum`, so it binds and reads through a diesel connection
//! (via `sql_query` + `QueryableByName`, the path for DuckDB-specific types).

use better_duck_diesel::sql_types::DuckEnum as DuckEnumSql;
use better_duck_diesel::{DuckDbConnection, DuckEnum};
use diesel::prelude::*;
use diesel::sql_types::Integer;

#[derive(DuckEnum, Debug, PartialEq, Clone, Copy)]
enum Priority {
    Low,
    High,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct Row {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = DuckEnumSql)]
    p: Priority,
}

#[test]
fn derived_enum_binds_and_reads_through_diesel() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TYPE priority AS ENUM ('Low','High')").execute(&mut conn).unwrap();
    diesel::sql_query("CREATE TABLE tasks (id INTEGER, p priority)").execute(&mut conn).unwrap();

    // Bind the derived enum as a parameter (ToSql).
    diesel::sql_query("INSERT INTO tasks VALUES (1, $1)")
        .bind::<DuckEnumSql, _>(Priority::High)
        .execute(&mut conn)
        .unwrap();

    // Read it back (FromSql via QueryableByName).
    let rows: Vec<Row> =
        diesel::sql_query("SELECT id, p FROM tasks ORDER BY id").get_results(&mut conn).unwrap();
    assert_eq!(rows, vec![Row { id: 1, p: Priority::High }]);
}

#[derive(better_duck_diesel::DuckStruct, Debug, PartialEq, Clone)]
struct Point {
    x: i32,
    y: i32,
}

#[derive(QueryableByName, Debug, PartialEq)]
struct PointRow {
    #[diesel(sql_type = better_duck_diesel::sql_types::DuckStruct)]
    p: Point,
}

#[test]
fn derived_struct_reads_through_diesel() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    let rows: Vec<PointRow> =
        diesel::sql_query("SELECT {'x': CAST(3 AS INTEGER), 'y': CAST(4 AS INTEGER)} AS p")
            .get_results(&mut conn)
            .unwrap();
    assert_eq!(rows, vec![PointRow { p: Point { x: 3, y: 4 } }]);
}
