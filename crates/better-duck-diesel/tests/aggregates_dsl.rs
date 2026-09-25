#![allow(missing_docs)]

//! DSL tests for the DuckDB aggregate functions exposed via
//! `define_sql_function!` with `#[aggregate]`.

use better_duck_diesel::dsl::{bool_and, stddev_samp};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    nums (id) {
        id -> diesel::sql_types::Integer,
        v -> diesel::sql_types::Double,
    }
}

fn setup() -> DuckDbConnection {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE nums (id INTEGER, v DOUBLE)").execute(&mut conn).unwrap();
    diesel::sql_query("INSERT INTO nums VALUES (1,1.0),(2,2.0),(3,3.0)")
        .execute(&mut conn)
        .unwrap();
    conn
}

#[test]
fn stddev_samp_computes_sample_stddev() {
    let mut conn = setup();
    let sd: Option<f64> = nums::table.select(stddev_samp(nums::v)).first(&mut conn).unwrap();
    // stddev_samp of {1,2,3} is exactly 1.0.
    assert!((sd.unwrap() - 1.0).abs() < 1e-9);
}

#[test]
fn bool_and_folds_a_predicate() {
    let mut conn = setup();
    let all_positive: Option<bool> =
        nums::table.select(bool_and(nums::v.gt(0.0))).first(&mut conn).unwrap();
    assert_eq!(all_positive, Some(true));

    let all_big: Option<bool> =
        nums::table.select(bool_and(nums::v.gt(1.5))).first(&mut conn).unwrap();
    assert_eq!(all_big, Some(false));
}
