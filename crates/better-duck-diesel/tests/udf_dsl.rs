#![allow(missing_docs)]
#![cfg(feature = "udf")]

//! End-to-end test for calling a Rust user-defined function through the Diesel
//! DSL: register the UDF on the connection, declare the matching
//! `define_sql_function!`, then call it as a typed method (no raw SQL).

use better_duck_core::duckdb_scalar;
use better_duck_diesel::DuckDbConnection;
use diesel::expression::functions::define_sql_function;
use diesel::prelude::*;
use diesel::sql_types::Integer;

/// A scalar UDF implemented in Rust, registered under the SQL name `add_one`.
#[duckdb_scalar(name = "add_one")]
fn add_one_impl(x: i32) -> i32 {
    x + 1
}

define_sql_function! {
    /// Calls the registered `add_one` scalar UDF.
    fn add_one(x: Integer) -> Integer;
}

diesel::table! {
    nums (id) {
        id -> diesel::sql_types::Integer,
    }
}

#[test]
fn registered_scalar_udf_is_callable_via_dsl() {
    let mut conn = DuckDbConnection::establish(":memory:").unwrap();
    // Register the Rust UDF on the underlying core connection...
    add_one_impl::register(conn.inner_mut()).unwrap();

    diesel::sql_query("CREATE TABLE nums (id INTEGER)").execute(&mut conn).unwrap();
    diesel::sql_query("INSERT INTO nums VALUES (1),(2),(3)").execute(&mut conn).unwrap();

    // ...then call it as a typed DSL function, with no raw SQL.
    let out: Vec<i32> =
        nums::table.select(add_one(nums::id)).order(nums::id).load(&mut conn).unwrap();
    assert_eq!(out, vec![2, 3, 4]);
}
