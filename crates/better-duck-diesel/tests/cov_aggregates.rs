#![allow(missing_docs)]

//! Coverage: call every aggregate DSL function over a table so its generated
//! `QueryFragment` code runs.

use better_duck_diesel::dsl::*;
use better_duck_diesel::DuckDbConnection;
use diesel::prelude::*;

diesel::table! {
    agg_t (id) {
        id -> diesel::sql_types::Integer,
        d -> diesel::sql_types::Double,
        y -> diesel::sql_types::Double,
        b -> diesel::sql_types::Bool,
        m -> diesel::sql_types::BigInt,
        t -> diesel::sql_types::Text,
    }
}

fn setup() -> DuckDbConnection {
    let mut c = DuckDbConnection::establish(":memory:").unwrap();
    diesel::sql_query("CREATE TABLE agg_t (id INTEGER, d DOUBLE, y DOUBLE, b BOOLEAN, m BIGINT, t VARCHAR)")
        .execute(&mut c)
        .unwrap();
    diesel::sql_query(
        "INSERT INTO agg_t VALUES (1,1.0,2.0,true,6,'a'),(2,2.0,4.0,false,3,'b'),(3,3.0,6.0,true,5,'a')",
    )
    .execute(&mut c)
    .unwrap();
    c
}

#[test]
fn statistical_aggregates() {
    let mut c = setup();
    agg_t::table.select(stddev_samp(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(stddev_pop(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(var_samp(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(var_pop(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(median(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(skewness(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(kurtosis(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(entropy(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(mad(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(mode(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(corr(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(covar_samp(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(covar_pop(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(regr_slope(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(regr_intercept(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(regr_r2(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(regr_count(agg_t::y, agg_t::d)).execute(&mut c).unwrap();
}

#[test]
fn approximate_and_bit_aggregates() {
    let mut c = setup();
    agg_t::table.select(approx_count_distinct(agg_t::t)).execute(&mut c).unwrap();
    agg_t::table.select(approx_quantile(agg_t::d, 0.5_f32)).execute(&mut c).unwrap();
    agg_t::table.select(reservoir_quantile(agg_t::d, 0.5_f64)).execute(&mut c).unwrap();
    agg_t::table.select(quantile_cont(agg_t::d, 0.5_f64)).execute(&mut c).unwrap();
    agg_t::table.select(quantile_disc(agg_t::d, 0.5_f64)).execute(&mut c).unwrap();
    agg_t::table.select(bit_and(agg_t::m)).execute(&mut c).unwrap();
    agg_t::table.select(bit_or(agg_t::m)).execute(&mut c).unwrap();
    agg_t::table.select(bit_xor(agg_t::m)).execute(&mut c).unwrap();
}

#[test]
fn general_aggregates() {
    let mut c = setup();
    agg_t::table.select(bool_and(agg_t::b)).execute(&mut c).unwrap();
    agg_t::table.select(bool_or(agg_t::b)).execute(&mut c).unwrap();
    agg_t::table.select(string_agg(agg_t::t, ",")).execute(&mut c).unwrap();
    agg_t::table.select(arg_max(agg_t::t, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(arg_min(agg_t::t, agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(product(agg_t::d)).execute(&mut c).unwrap();
    agg_t::table.select(any_value(agg_t::t)).execute(&mut c).unwrap();
    agg_t::table.select(array_agg(agg_t::t)).execute(&mut c).unwrap();
}

#[test]
fn json_aggregates_when_available() {
    let mut c = setup();
    if agg_t::table.select(json_group_array(agg_t::t)).execute(&mut c).is_err() {
        eprintln!("skipping json aggregate coverage: `json` extension unavailable");
        return;
    }
    agg_t::table.select(json_group_object(agg_t::t, agg_t::t)).execute(&mut c).unwrap();
}
