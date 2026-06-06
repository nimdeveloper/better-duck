#![allow(missing_docs)]
//! Compile-only tests that verify the full Diesel DSL type-checks against
//! [`DuckDbConnection`]. No database is opened; all functions are dead code.

use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    users (id) {
        id   -> Integer,
        name -> Text,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use better_duck_diesel::sql_types::*;

    duck_types_table (id) {
        id         -> Integer,
        tiny       -> DuckTinyInt,
        utiny      -> DuckUTinyInt,
        usmall     -> DuckUSmallInt,
        uint       -> DuckUInt,
        ubig       -> DuckUBigInt,
        huge       -> DuckHugeInt,
        uhuge      -> DuckUHugeInt,
    }
}

// Standard DSL shapes

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

#[allow(dead_code)]
fn verify_update_compiles(conn: &mut DuckDbConnection) {
    let _: usize = diesel::update(users::table.filter(users::id.eq(1)))
        .set(users::name.eq("Bob"))
        .execute(conn)
        .expect("update");
}

#[allow(dead_code)]
fn verify_delete_compiles(conn: &mut DuckDbConnection) {
    let _: usize =
        diesel::delete(users::table.filter(users::id.eq(1))).execute(conn).expect("delete");
}

#[allow(dead_code)]
fn verify_count_compiles(conn: &mut DuckDbConnection) {
    let _: i64 = users::table.count().first(conn).expect("count");
}

#[allow(dead_code)]
fn verify_order_limit_offset_compiles(conn: &mut DuckDbConnection) {
    let _: Vec<(i32, String)> = users::table
        .order(users::id.desc())
        .limit(10)
        .offset(5)
        .select((users::id, users::name))
        .load(conn)
        .expect("order/limit/offset");
}

#[allow(dead_code)]
fn verify_nullable_compiles(conn: &mut DuckDbConnection) {
    diesel::table! {
        nullable_t (id) {
            id  -> Integer,
            val -> Nullable<Text>,
        }
    }
    let _: Vec<Option<String>> =
        nullable_t::table.select(nullable_t::val).load(conn).expect("nullable");
}

// DuckDB-specific SQL types compile-check

#[allow(dead_code)]
fn verify_duck_types_select_compiles(conn: &mut DuckDbConnection) {
    // The five DuckDB-exclusive integer widths that satisfy Diesel's Queryable<ST, DB> blanket
    // (i8, u8, u16, u32, u64) can be loaded as a tuple through the standard DSL.
    let _: Vec<(i8, u8, u16, u32, u64)> = duck_types_table::table
        .select((
            duck_types_table::tiny,
            duck_types_table::utiny,
            duck_types_table::usmall,
            duck_types_table::uint,
            duck_types_table::ubig,
        ))
        .load(conn)
        .expect("duck types select");

    // i128 / u128 (DuckHugeInt / DuckUHugeInt) don't propagate through Diesel's
    // Queryable<ST, DB> blanket chain. Their FromSql impls exist and are exercised
    // via #[derive(QueryableByName)] in types_roundtrip::rt_i128_boundary / rt_u128_boundary.
}
