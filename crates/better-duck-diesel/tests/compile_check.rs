//! Compile-only test that verifies the full Diesel DSL type-checks against
//! [`DuckDbConnection`]. No database is opened; all functions are dead code.

use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    users (id) {
        id   -> Integer,
        name -> Text,
    }
}

#[allow(dead_code)]
fn verify_select_compiles(conn: &mut DuckDbConnection) {
    let _: Vec<(i32, String)> =
        users::table.select((users::id, users::name)).load(conn).expect("load");
}

#[allow(dead_code)]
fn verify_insert_compiles(conn: &mut DuckDbConnection) {
    let _: usize = diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("Alice")))
        .execute(conn)
        .expect("insert");
}
