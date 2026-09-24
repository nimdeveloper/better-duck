#![allow(missing_docs)]

//! Round-trip tests for the `values::*` DSL newtypes: each must be usable as a
//! bound expression (`AsExpression`, via the derive) and loadable back
//! (`Queryable`/`FromSqlRow`), which bare `i128`/`u64`/… cannot be for a custom
//! `Duck*` SQL type in a third-party crate.

use better_duck_diesel::sql_types::{DuckHugeInt, DuckUBigInt, DuckUInt};
use better_duck_diesel::values::{HugeInt, UBigInt, UInt};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;
use diesel::IntoSql;

fn conn() -> DuckDbConnection {
    DuckDbConnection::establish(":memory:").unwrap()
}

#[test]
fn hugeint_newtype_binds_and_loads_via_dsl() {
    let mut conn = conn();
    let v = HugeInt(i128::MAX);
    let got: HugeInt = diesel::select(v.into_sql::<DuckHugeInt>()).get_result(&mut conn).unwrap();
    assert_eq!(got, v);
}

#[test]
fn ubigint_newtype_roundtrips() {
    let mut conn = conn();
    let v = UBigInt(u64::MAX);
    let got: UBigInt = diesel::select(v.into_sql::<DuckUBigInt>()).get_result(&mut conn).unwrap();
    assert_eq!(got, v);
}

#[test]
fn uint_newtype_roundtrips() {
    let mut conn = conn();
    let v = UInt(4_294_967_295);
    let got: UInt = diesel::select(v.into_sql::<DuckUInt>()).get_result(&mut conn).unwrap();
    assert_eq!(got, v);
}
