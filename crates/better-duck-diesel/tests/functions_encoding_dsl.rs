#![allow(missing_docs)]

//! DSL tests for the DuckDB uuid / hash / blob-encoding scalar functions.

use better_duck_diesel::dsl::{base64, encode, gen_random_uuid, sha256};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    uuid_tbl (id) {
        id -> better_duck_diesel::sql_types::DuckUuid,
    }
}

fn conn() -> DuckDbConnection {
    DuckDbConnection::establish(":memory:").unwrap()
}

#[test]
fn sha256_matches_known_digest() {
    let mut conn = conn();
    let h: String = diesel::select(sha256("abc")).get_result(&mut conn).unwrap();
    assert_eq!(h, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
}

#[test]
fn base64_of_encoded_text() {
    let mut conn = conn();
    let b: String = diesel::select(base64(encode("hi"))).get_result(&mut conn).unwrap();
    assert_eq!(b, "aGk=");
}

#[test]
fn gen_random_uuid_is_usable_in_an_insert() {
    // `DuckUuid` isn't `Queryable` (the 128-bit gap), so exercise the function in
    // an INSERT position instead — the common use for a generated default.
    let mut conn = conn();
    diesel::sql_query("CREATE TABLE uuid_tbl (id UUID)").execute(&mut conn).unwrap();
    for _ in 0..2 {
        diesel::insert_into(uuid_tbl::table)
            .values(uuid_tbl::id.eq(gen_random_uuid()))
            .execute(&mut conn)
            .unwrap();
    }
    let n: i64 = uuid_tbl::table.count().get_result(&mut conn).unwrap();
    assert_eq!(n, 2);
}
