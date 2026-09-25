#![allow(missing_docs)]

//! Round-trip tests for the `values::*` DSL newtypes: each must be usable as a
//! bound expression (`AsExpression`, via the derive) and loadable back
//! (`Queryable`/`FromSqlRow`), which bare `i128`/`u64`/… cannot be for a custom
//! `Duck*` SQL type in a third-party crate.

use better_duck_diesel::sql_types::{DuckHugeInt, DuckUBigInt, DuckUInt, DuckUuid};
use better_duck_diesel::values::{HugeInt, UBigInt, UInt, Uuid};
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;
use diesel::IntoSql;

fn conn() -> DuckDbConnection {
    DuckDbConnection::establish(":memory:").unwrap()
}

#[test]
fn uuid_newtype_binds_and_loads_via_dsl() {
    let mut conn = conn();
    let v = Uuid(better_duck_core::types::DuckUuid(0x1234_5678_9abc_def0_1122_3344_5566_7788_u128));
    let got: Uuid = diesel::select(v.into_sql::<DuckUuid>()).get_result(&mut conn).unwrap();
    assert_eq!(got, v);
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

#[test]
fn remaining_integer_newtypes_roundtrip() {
    use better_duck_diesel::sql_types::{DuckTinyInt, DuckUHugeInt, DuckUSmallInt, DuckUTinyInt};
    use better_duck_diesel::values::{TinyInt, UHugeInt, USmallInt, UTinyInt};
    let mut conn = conn();

    let a = TinyInt(-5);
    assert_eq!(diesel::select(a.into_sql::<DuckTinyInt>()).get_result::<TinyInt>(&mut conn).unwrap(), a);
    let b = UTinyInt(200);
    assert_eq!(diesel::select(b.into_sql::<DuckUTinyInt>()).get_result::<UTinyInt>(&mut conn).unwrap(), b);
    let d = USmallInt(60_000);
    assert_eq!(diesel::select(d.into_sql::<DuckUSmallInt>()).get_result::<USmallInt>(&mut conn).unwrap(), d);
    let e = UHugeInt(u128::MAX);
    assert_eq!(diesel::select(e.into_sql::<DuckUHugeInt>()).get_result::<UHugeInt>(&mut conn).unwrap(), e);
}

#[test]
fn bit_and_bignum_newtypes_roundtrip() {
    use better_duck_diesel::sql_types::{DuckBignum, DuckBit};
    use better_duck_diesel::values::{Bignum, Bit};
    let mut conn = conn();

    let bit = Bit(better_duck_core::types::DuckBit(vec![0u8, 0b1010_0000]));
    let got: Bit = diesel::select(bit.clone().into_sql::<DuckBit>()).get_result(&mut conn).unwrap();
    assert_eq!(got, bit);

    let bn = Bignum(better_duck_core::types::DuckBignum::new(vec![42], false));
    let got: Bignum = diesel::select(bn.clone().into_sql::<DuckBignum>()).get_result(&mut conn).unwrap();
    assert_eq!(got, bn);
}

#[test]
fn newtype_from_conversions_both_ways() {
    use better_duck_core::types::{DuckBignum, DuckBit, DuckUuid};
    use better_duck_diesel::values::{
        Bignum, Bit, HugeInt, TinyInt, UBigInt, UHugeInt, UInt, USmallInt, UTinyInt, Uuid,
    };

    // `From<inner> for Newtype` and `From<Newtype> for inner` (both macro-generated).
    assert_eq!(i8::from(TinyInt::from(-1_i8)), -1);
    assert_eq!(u8::from(UTinyInt::from(2_u8)), 2);
    assert_eq!(u16::from(USmallInt::from(3_u16)), 3);
    assert_eq!(u32::from(UInt::from(4_u32)), 4);
    assert_eq!(u64::from(UBigInt::from(5_u64)), 5);
    assert_eq!(i128::from(HugeInt::from(6_i128)), 6);
    assert_eq!(u128::from(UHugeInt::from(7_u128)), 7);
    assert_eq!(DuckUuid::from(Uuid::from(DuckUuid(8))), DuckUuid(8));
    let raw_bit = DuckBit(vec![0u8, 1u8]);
    assert_eq!(DuckBit::from(Bit::from(raw_bit.clone())), raw_bit);
    let raw_bn = DuckBignum::new(vec![9], false);
    assert_eq!(DuckBignum::from(Bignum::from(raw_bn.clone())), raw_bn);
}
